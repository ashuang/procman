#!/usr/bin/env python

import argparse
import os
import pickle
import signal
import subprocess
import sys
import time
import traceback

import gi

from lcm import LCM

gi.require_version("Gtk", "3.0")
from gi.repository import GLib
from gi.repository import GObject
from gi.repository import Gtk

import procman.sheriff as sheriff
from procman.sheriff import Sheriff, SheriffListener
from procman.sheriff_cli import SheriffHeadless
from procman.sheriff_script import ScriptManager
import procman.sheriff_config as sheriff_config

import procman.sheriff_gtk.command_model as cm
import procman.sheriff_gtk.command_treeview as ctv
import procman.sheriff_gtk.sheriff_dialogs as sd
import procman.sheriff_gtk.command_console as cc
import procman.sheriff_gtk.deputies_treeview as ht

from procman.sheriff_cli import SheriffHeadless, find_procman_deputy_cmd

try:
    from procman.build_prefix import BUILD_PREFIX
except ImportError:
    BUILD_PREFIX = None


def _dbg(text):
    #    return
    sys.stderr.write("{}\n".format(text))


def find_procman_glade():
    search_path = []
    if BUILD_PREFIX:
        search_path.append(os.path.join(BUILD_PREFIX, "share", "procman"))
    search_path.append("/usr/share/procman")
    search_path.append("/usr/local/share/procman")
    for spath in search_path:
        fname = os.path.join(spath, "procman-sheriff.glade")
        if os.path.isfile(fname):
            return fname
    sys.stderr.write("ERROR!  Unable to find procman-sheriff.glade\n")
    sys.stderr.write("Locations checked:\n")
    for spath in search_path:
        sys.stderr.write("    %s\n" % spath)
    sys.exit(1)

def find_icon():
    search_path = []
    if BUILD_PREFIX:
        search_path.append(os.path.join(BUILD_PREFIX, "share", "procman"))
    search_path.append("/usr/local/share/icons/hicolor/scalable/apps/")
    for spath in search_path:
        fname = os.path.join(spath, "procman_icon.svg")
        if os.path.isfile(fname):
            return fname
    sys.stderr.write("Warning: Unable to find procman_icon.svg\n")
    sys.stderr.write("Locations checked:\n")
    for spath in search_path:
        sys.stderr.write("    %s\n" % spath)
    return None
    

def split_script_name(name):
    name = name.strip("/")
    while name.find("//") >= 0:
        name = name.replace("//", "/")
    return name.split("/")


class SheriffGtk(SheriffListener):
    def __init__(self, lcm_obj):
        self.lcm_obj = lcm_obj
        self.cmds_update_scheduled = False
        self.config_filename = None
        self.script_done_action = None

        # deputy spawned by the sheriff
        self.spawned_deputy = None

        # create sheriff and subscribe to events
        self.sheriff = Sheriff(self.lcm_obj)
        self.sheriff.add_listener(self)

        self.script_manager = ScriptManager(self.sheriff)
        self.script_manager.add_listener(self)

        # setup GUI

        self.builder = Gtk.Builder()
        self.builder.add_from_file(find_procman_glade())
        self.builder.connect_signals(self)

        self.window = self.builder.get_object("main_window")
        icon = find_icon()
        if icon is not None:
            self.window.set_icon_from_file(icon)

        self.cmds_ts = cm.SheriffCommandModel(self.sheriff)
        self.cmds_tv = ctv.SheriffCommandTreeView(
            self.sheriff, self.script_manager, self.cmds_ts
        )

        # load save menu
        self.load_cfg_mi = self.builder.get_object("load_cfg_mi")
        self.save_cfg_mi = self.builder.get_object("save_cfg_mi")
        self.load_dlg = None
        self.save_dlg = None
        self.load_save_dir = None
        self.cfg_to_load = None

        # options menu
        self.is_observer_cmi = self.builder.get_object("is_observer_cmi")
        self.spawn_deputy_mi = self.builder.get_object("spawn_deputy_mi")
        self.terminate_spawned_deputy_mi = self.builder.get_object(
            "terminate_spawned_deputy_mi"
        )

        self.procman_deputy_cmd = find_procman_deputy_cmd()
        if not self.procman_deputy_cmd:
            sys.stderr.write("Can't find procman-deputy.  Spawn Deputy disabled")
            self.spawn_deputy_mi.set_sensitive(False)


        # commands menu
        self.start_cmd_mi = self.builder.get_object("start_cmd_mi")
        self.stop_cmd_mi = self.builder.get_object("stop_cmd_mi")
        self.restart_cmd_mi = self.builder.get_object("restart_cmd_mi")
        self.remove_cmd_mi = self.builder.get_object("remove_cmd_mi")
        self.edit_cmd_mi = self.builder.get_object("edit_cmd_mi")
        self.new_cmd_mi = self.builder.get_object("new_cmd_mi")

        # scripts menu
        self.abort_script_mi = self.builder.get_object("abort_script_mi")
        self.edit_script_mi = self.builder.get_object("edit_script_mi")
        self.remove_script_mi = self.builder.get_object("remove_script_mi")
        self.scripts_menu = self.builder.get_object("scripts_menu")
        self.edit_scripts_menu = self.builder.get_object("edit_scripts_menu")
        self.remove_scripts_menu = self.builder.get_object("remove_scripts_menu")

        vpane = self.builder.get_object("vpaned")

        sw = Gtk.ScrolledWindow()
        sw.set_policy(Gtk.PolicyType.AUTOMATIC, Gtk.PolicyType.AUTOMATIC)
        hpane = self.builder.get_object("hpaned")
        hpane.pack1(sw, resize=True)
        sw.add(self.cmds_tv)

        cmds_sel = self.cmds_tv.get_selection()
        cmds_sel.connect("changed", self._on_cmds_selection_changed)

        # create a checkable item in the View menu for each column to toggle
        # its visibility in the treeview
        view_menu = self.builder.get_object("view_menu")
        for col in self.cmds_tv.get_columns():
            name = col.get_title()
            col_cmi = Gtk.CheckMenuItem(name)
            col_cmi.set_active(col.get_visible())

            def on_activate(cmi, col_):
                should_be_visible = cmi.get_active()
                if col_.get_visible() != should_be_visible:
                    col_.set_visible(should_be_visible)
                    if col_ == self.cmds_tv.columns[0]:
                        self.cmds_ts.set_populate_exec_with_group_name(
                            not should_be_visible
                        )
                        self.cmds_ts.repopulate()

            def on_visibility_changed(col_, param, cmi_):
                is_visible = col_.get_visible()
                if is_visible != cmi_.get_active():
                    cmi_.set_active(is_visible)

            col_cmi.connect("activate", on_activate, col)
            col.connect("notify::visible", on_visibility_changed, col_cmi)
            view_menu.append(col_cmi)

        # setup the deputies treeview
        self.deputies_ts = ht.DeputyModel(self.sheriff)
        self.deputies_tv = ht.DeputyTreeView(self.sheriff, self.deputies_ts)
        sw = Gtk.ScrolledWindow()
        sw.set_policy(Gtk.PolicyType.AUTOMATIC, Gtk.PolicyType.AUTOMATIC)
        hpane.pack2(sw, resize=False)
        sw.add(self.deputies_tv)

        GObject.timeout_add(1000, lambda *_: self.deputies_ts.update() or True)

        # stdout textview
        self.cmd_console = cc.SheriffCommandConsole(self.sheriff, self.lcm_obj)
        vpane.add2(self.cmd_console)

        # status bar
        self.statusbar = self.builder.get_object("statusbar")
        self.statusbar_context_script = self.statusbar.get_context_id("script")
        self.statusbar_context_main = self.statusbar.get_context_id("main")
        self.statusbar_context_script_msg = None

        config_dir = os.path.join(GLib.get_user_config_dir(), "procman-sheriff")
        if not os.path.exists(config_dir):
            os.makedirs(config_dir)
        self.config_fname = os.path.join(config_dir, "config")
        self.load_settings()

        self.window.show_all()

        # update very soon
        # Update information about deputies
        GObject.timeout_add(100, lambda *_: self.deputies_ts.update() and False)
        GObject.timeout_add(100, lambda *_: self._schedule_cmds_update() and False)

        # and then periodically
        GObject.timeout_add(1000, self._check_spawned_deputy)
        GObject.timeout_add(1000, lambda *_: self._schedule_cmds_update() or True)

    def command_added(self, deputy_obj, cmd_obj):
        self._schedule_cmds_update()

    def command_removed(self, deputy_obj, cmd_obj):
        self._schedule_cmds_update()

    def command_status_changed(self, cmd_obj, old_status, new_status):
        self._schedule_cmds_update()

    def command_group_changed(self, cmd_obj):
        self._schedule_cmds_update()

    def script_added(self, script_object):
        GLib.idle_add(self._gtk_on_script_added, script_object)

    def sheriff_conflict_detected(self, other_sheriff_id):
        GLib.idle_add(self._gtk_sheriff_conflict_detected)

    def _gtk_sheriff_conflict_detected(self):
        # detected the presence of another sheriff that is not this one.
        # self-demote to prevent command thrashing
        self.sheriff.set_observer(True)

        self.statusbar.push(
            self.statusbar.get_context_id("main"),
            "WARNING: multiple sheriffs detected!  Switching to observer mode",
        )
        GObject.timeout_add(
            6000, lambda *_: self.statusbar.pop(self.statusbar.get_context_id("main"))
        )

    def observer_status_changed(self, is_observer):
        GLib.idle_add(self._gtk_observer_status_changed, is_observer)

    def _gtk_observer_status_changed(self, is_observer):
        self._update_menu_item_sensitivities()

        if is_observer:
            self.window.set_title("Procman Observer")
        else:
            self.window.set_title("Procman Sheriff")

        self.is_observer_cmi.set_active(is_observer)

    def script_removed(self, script_object):
        GLib.idle_add(self._gtk_on_script_removed, script_object)

    def script_started(self, script_object):
        GLib.idle_add(self._gtk_on_script_started, script_object)

    def script_action_executing(self, script_object, action):
        GLib.idle_add(self._gtk_on_script_action_executing, script_object, action)

    def script_finished(self, script_object):
        GLib.idle_add(self._gtk_on_script_finished, script_object)

    def on_preferences_mi_activate(self, *args):
        sd.do_preferences_dialog(self, self.window)

    def on_quit_requested(self, *args):
        Gtk.main_quit()

    def on_start_cmd_mi_activate(self, *args):
        self.cmds_tv._start_selected_commands()

    def on_stop_cmd_mi_activate(self, *args):
        self.cmds_tv._stop_selected_commands()

    def on_restart_cmd_mi_activate(self, *args):
        self.cmds_tv._restart_selected_commands()

    def on_remove_cmd_mi_activate(self, *args):
        self.cmds_tv._remove_selected_commands()

    def on_edit_cmd_mi_activate(self, *args):
        self.cmds_tv._edit_selected_command()

    def on_new_cmd_mi_activate(self, *args):
        sd.do_add_command_dialog(self.sheriff, self.cmds_ts, self.window)

    def cleanup(self, save_settings):
        self._terminate_spawned_deputy()
        self.sheriff.shutdown()
        self.script_manager.shutdown()
        if save_settings:
            self.save_settings()

    def load_settings(self):
        if not os.path.exists(self.config_fname):
            return
        try:
            with open(self.config_fname, 'rb') as config_file:
                d = pickle.load(config_file)
        except Exception as err:
            print(err)
            return

        self.cmds_tv.load_settings(d)
        self.cmd_console.load_settings(d)
        self.deputies_tv.load_settings(d)

    def save_settings(self):
        config_dir = os.path.join(GLib.get_user_config_dir(), "procman-sheriff")
        if not os.path.exists(config_dir):
            os.makedirs(config_dir)
        self.config_fname = os.path.join(config_dir, "config")
        d = {}

        self.cmds_tv.save_settings(d)
        self.cmd_console.save_settings(d)
        self.deputies_tv.save_settings(d)

        try:
            with open(self.config_fname, 'wb') as config_file:
                pickle.dump(d, config_file)
        except Exception as err:
            print(err)

    def _do_repopulate(self):
        self.cmds_ts.repopulate()
        self.cmds_update_scheduled = False

    def _schedule_cmds_update(self, *unused):
        if not self.cmds_update_scheduled:
            GObject.timeout_add(100, self._do_repopulate)
        return True

    def _terminate_spawned_deputy(self):
        if self.spawned_deputy:
            try:
                self.spawned_deputy.terminate()
            except AttributeError:  # python 2.4, 2.5 don't have Popen.terminate()
                os.kill(self.spawned_deputy.pid, signal.SIGTERM)
                self.spawned_deputy.wait()
        self.spawned_deputy = None

    def _check_spawned_deputy(self):
        if not self.spawned_deputy:
            return True

        self.spawned_deputy.poll()
        if self.spawned_deputy.returncode is None:
            return True

        returncode_msgs = {
            0: "Terminated",
            1: "OS or other networking error",
            2: "Conflicting deputy with same id already exists",
        }

        msg = returncode_msgs.get(self.spawned_deputy.returncode, "Unknown error")

        self.spawn_deputy_mi.set_sensitive(True)
        self.terminate_spawned_deputy_mi.set_sensitive(False)
        self.spawned_deputy = None

        dialog = Gtk.MessageDialog(
            self.window,
            0,
            Gtk.MessageType.ERROR,
            Gtk.ButtonsType.OK,
            "Spawned deputy exited prematurely: {}".format(msg),
        )
        dialog.run()
        dialog.destroy()
        return True

    def run_script(self, menuitem, script, script_done_action=None):
        self.script_done_action = script_done_action
        errors = self.script_manager.execute_script(script)
        if errors:
            msgdlg = Gtk.MessageDialog(
                self.window,
                Gtk.DialogFlags.MODAL | Gtk.DialogFlags.DESTROY_WITH_PARENT,
                Gtk.MessageType.ERROR,
                Gtk.ButtonsType.CLOSE,
                "Script failed to run.  Errors detected:\n" + "\n".join(errors),
            )
            msgdlg.run()
            msgdlg.destroy()

    def _gtk_on_script_started(self, script):
        self._update_menu_item_sensitivities()
        cid = self.statusbar_context_script
        if self.statusbar_context_script_msg is not None:
            self.statusbar.pop(cid)
            self.statusbar_context_script_msg = self.statusbar.push(
                cid, "Script {}: start".format(script.name)
            )

    def _gtk_on_script_action_executing(self, script, action):
        cid = self.statusbar_context_script
        self.statusbar.pop(cid)
        msg = "Action: {}".format(str(action))
        self.statusbar_context_script_msg = self.statusbar.push(cid, msg)

    def _gtk_on_script_finished(self, script):
        self._update_menu_item_sensitivities()
        cid = self.statusbar_context_script
        self.statusbar.pop(cid)
        self.statusbar_context_script_msg = self.statusbar.push(
            cid, "Script {}: finished".format(script.name)
        )

        def _remove_msg_func(msg_id):
            return (
                lambda *_: msg_id == self.statusbar_context_script_msg
                and self.statusbar.pop(cid)
            )

        GObject.timeout_add(6000, _remove_msg_func(self.statusbar_context_script_msg))
        if self.script_done_action == "exit":
            Gtk.main_quit()
        elif self.script_done_action == "observe":
            self.script_done_action = None
            self.sheriff.set_observer(True)

    def on_abort_script_mi_activate(self, menuitem):
        self.script_manager.abort_script()

    def on_new_script_mi_activate(self, menuitem):
        sd.do_add_script_dialog(self.script_manager, self.window)

    def _get_script_menuitem(self, menu, script, name_parts, create):
        assert name_parts
        partname = name_parts[0]
        if len(name_parts) == 1:
            insert_point = 0
            for i, smi in enumerate(menu.get_children()):
                other_script = getattr(smi, 'sheriff_script', None)
                if other_script is script:
                    return smi
                if other_script is None:
                    break
                if other_script.name < script.name:
                    insert_point += 1
            if create:
                mi = Gtk.MenuItem(partname, use_underline=False)
                mi.sheriff_script = script
                menu.insert(mi, insert_point)
                mi.show()
                return mi
            return None
        else:
            insert_point = 0
            for i, smi in enumerate(menu.get_children()):
                if not getattr(smi, 'sheriff_script_submenu', None):
                    continue
                submenu_name = smi.get_label()
                if submenu_name == partname:
                    return self._get_script_menuitem(
                        smi.get_submenu(), script, name_parts[1:], create
                    )
                elif submenu_name < partname:
                    insert_point = i

            if create:
                smi = Gtk.MenuItem(partname)
                submenu = Gtk.Menu()
                smi.set_submenu(submenu)
                smi.sheriff_script_submenu = True
                menu.insert(smi, insert_point)
                smi.show()
                return self._get_script_menuitem(
                    submenu, script, name_parts[1:], create
                )

    def _remove_script_menuitems(self, menu, script, name_parts):
        assert name_parts
        partname = name_parts[0]

        if len(name_parts) == 1:
            for smi in menu.get_children():
                if script == smi.sheriff_script:
                    menu.remove(smi)
                    return
        else:
            for smi in menu.get_children():
                if not smi.sheriff_script_submenu:
                    continue
                submenu_name = smi.get_label()
                submenu = smi.get_submenu()
                if submenu_name == partname:
                    self._remove_script_menuitems(submenu, script, name_parts[1:])
                    if not submenu.get_children():
                        smi.remove_submenu()
                        menu.remove(smi)
                    return

    def _maybe_add_script_menu_item(self, script):
        name_parts = split_script_name(script.name)

        # make menu items for executing, editing, and removing the script
        run_mi = self._get_script_menuitem(self.scripts_menu, script, name_parts, True)
        run_mi.connect("activate", self.run_script, script)

        edit_mi = self._get_script_menuitem(
            self.edit_scripts_menu, script, name_parts, True
        )
        edit_mi.connect(
            "activate",
            lambda mi: sd.do_edit_script_dialog(
                self.script_manager, self.window, script
            ),
        )

        remove_mi = self._get_script_menuitem(
            self.remove_scripts_menu, script, name_parts, True
        )
        remove_mi.connect(
            "activate",
            lambda mi: self.script_manager.remove_script(mi.sheriff_script),
        )

        self.edit_script_mi.set_sensitive(True)
        self.remove_script_mi.set_sensitive(True)

    def _gtk_on_script_added(self, script):
        self._maybe_add_script_menu_item(script)

    def _gtk_on_script_removed(self, script):
        name_parts = split_script_name(script.name)
        for menu in [
            self.scripts_menu,
            self.edit_scripts_menu,
            self.remove_scripts_menu,
        ]:
            self._remove_script_menuitems(menu, script, name_parts)

        if not self.script_manager.get_scripts():
            self.edit_script_mi.set_sensitive(False)
            self.remove_script_mi.set_sensitive(False)

    def _do_load_config(self):
        assert self.cfg_to_load is not None
        self.sheriff.load_config(self.cfg_to_load)
        self.script_manager.load_config(self.cfg_to_load)
        self.cfg_to_load = None

    def load_config(self, cfg):
        self.cfg_to_load = cfg

        # Automatically remove all existing commands before actually loading a
        # config file.
        current_cmds = self.sheriff.get_all_commands()
        if current_cmds:
            for cmd in current_cmds:
                self.sheriff.remove_command(cmd)
            # the remove_command function only schedules commands to be removed, it takes a bit of time for them to
            # actually be removed. All commands must be removed before a config can be loaded
            while len(self.sheriff.get_all_commands()):
                pass

        self._do_load_config()

    # GTK signal handlers
    def on_load_cfg_mi_activate(self, *args):
        if not self.load_dlg:
            self.load_dlg = Gtk.FileChooserDialog(
                "Load Config",
                self.window,
                buttons=(
                    Gtk.STOCK_OPEN,
                    Gtk.ResponseType.ACCEPT,
                    Gtk.STOCK_CANCEL,
                    Gtk.ResponseType.REJECT,
                ),
            )
        if self.load_save_dir:
            self.load_dlg.set_current_folder(self.load_save_dir)
        if Gtk.ResponseType.ACCEPT == self.load_dlg.run():
            self.config_filename = self.load_dlg.get_filename()
            self.load_save_dir = os.path.dirname(self.config_filename)
            try:
                cfg = sheriff.load_config_file(open(self.config_filename))
            except Exception:
                msgdlg = Gtk.MessageDialog(
                    self.window,
                    Gtk.DialogFlags.MODAL | Gtk.DialogFlags.DESTROY_WITH_PARENT,
                    Gtk.MessageType.ERROR,
                    Gtk.ButtonsType.CLOSE,
                    traceback.format_exc(),
                )
                msgdlg.run()
                msgdlg.destroy()
            else:
                self.load_config(cfg)
        self.load_dlg.hide()
        self.load_dlg.destroy()
        self.load_dlg = None

    def on_save_cfg_mi_activate(self, *args):
        if not self.save_dlg:
            self.save_dlg = Gtk.FileChooserDialog(
                "Save Config",
                self.window,
                action=Gtk.FileChooserAction.SAVE,
                buttons=(
                    Gtk.STOCK_SAVE,
                    Gtk.ResponseType.ACCEPT,
                    Gtk.STOCK_CANCEL,
                    Gtk.ResponseType.REJECT,
                ),
            )
        if self.load_save_dir:
            self.save_dlg.set_current_folder(self.load_save_dir)
        if self.config_filename is not None:
            self.save_dlg.set_filename(self.config_filename)
        if Gtk.ResponseType.ACCEPT == self.save_dlg.run():
            self.config_filename = self.save_dlg.get_filename()
            self.load_save_dir = os.path.dirname(self.config_filename)
            cfg_node = sheriff_config.ConfigNode()
            self.sheriff.save_config(cfg_node)
            self.script_manager.save_config(cfg_node)
            try:
                open(self.config_filename, "w").write(str(cfg_node))
            except OSError as e:
                msgdlg = Gtk.MessageDialog(
                    self.window,
                    Gtk.DialogFlags.MODAL | Gtk.DialogFlags.DESTROY_WITH_PARENT,
                    Gtk.MessageType.ERROR,
                    Gtk.ButtonsType.CLOSE,
                    str(e),
                )
                msgdlg.run()
                msgdlg.destroy()
        self.save_dlg.hide()
        self.save_dlg.destroy()
        self.save_dlg = None

    def on_is_observer_cmi_toggled(self, menu_item):
        self.sheriff.set_observer(menu_item.get_active())

    def on_spawn_deputy_mi_activate(self, *args):
        print("Spawn deputy!")
        self._terminate_spawned_deputy()
        args = [ self.procman_deputy_cmd, "-i", "localhost" ]
        self.spawned_deputy = subprocess.Popen(args)
        # TODO disable
        self.spawn_deputy_mi.set_sensitive(False)
        self.terminate_spawned_deputy_mi.set_sensitive(True)

    def on_terminate_spawned_deputy_mi_activate(self, *args):
        print("Terminate!")
        self._terminate_spawned_deputy()
        self.spawn_deputy_mi.set_sensitive(True)
        self.terminate_spawned_deputy_mi.set_sensitive(False)

    def _update_menu_item_sensitivities(self):
        # enable/disable menu options based on sheriff state and user selection
        is_observer = self.sheriff.is_observer()
        selected_cmds = self.cmds_tv.get_selected_commands()
        script_active = self.script_manager.get_active_script() is not None

        can_modify = len(selected_cmds) > 0 and not is_observer and not script_active
        can_add_load = not is_observer and not script_active

        #        _dbg("_update_menu_item_sensitivities (%s / %s / %s / %s / %s)" % (is_observer,
        #            can_modify, can_add_load, len(selected_cmds), script_active))

        self.start_cmd_mi.set_sensitive(can_modify)
        self.stop_cmd_mi.set_sensitive(can_modify)
        self.restart_cmd_mi.set_sensitive(can_modify)
        self.remove_cmd_mi.set_sensitive(can_modify)
        self.edit_cmd_mi.set_sensitive(can_modify)

        self.new_cmd_mi.set_sensitive(can_add_load)
        self.load_cfg_mi.set_sensitive(can_add_load)

        # TODO script menu sensitivities
        self.abort_script_mi.set_sensitive(script_active)

    def _on_cmds_selection_changed(self, selection):
        selected_cmds = self.cmds_tv.get_selected_commands()
        if len(selected_cmds) == 1:
            self.cmd_console.show_command_buffer(list(selected_cmds)[0])
        elif len(selected_cmds) == 0:
            self.cmd_console.show_sheriff_buffer()
        self._update_menu_item_sensitivities()


def main():
    parser = argparse.ArgumentParser(
        description="Process management operating console.",
        epilog="If procman_config_file is specified, then the sheriff tries to load "
        "deputy commands from the file.\n\nIf script_name is additionally "
        "specified, then the sheriff executes the named script once the config "
        "file is loaded.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    if "-o" not in sys.argv:
        parser.add_argument("procman_config_file", help="The configuration file to load")

    parser.add_argument(
        "--script", help="A script to execute after the config file is loaded."
    )

    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "-l",
        "--lone-ranger",
        action="store_true",
        dest="spawn_deputy",
        help="Automatically run a deputy within the sheriff process. This deputy terminates with the "
        "sheriff, along with all the commands it hosts.",
    )
    mode.add_argument(
        "-o",
        "--observer",
        action="store_true",
        help="Runs in observer mode on startup.  This "
        "prevents the sheriff from sending any "
        "commands, and is useful for monitoring "
        "existing procman sheriff and/or deputy "
        "instances.",
    )

    parser.add_argument(
        "-n",
        "--no-gui",
        action="store_false",
        dest="use_gui",
        help="Runs in headless mode (no GUI). Requires a script.",
    )

    parser.add_argument(
        "--on-script-complete",
        choices=["exit", "observer"],
        dest="script_done_action",
        help='Only valid if a script is specified.  If set to "exit", then the sheriff exits when '
        'the script is done executing. If set to "observe", then the sheriff self-demotes to '
        "observer mode.",
    )

    # Parse only known args, ignore unknown_args
    args, _ = parser.parse_known_args(sys.argv[1:])

    if hasattr(args, "procman_config_file"):
        try:
            cfg = sheriff.load_config_file(open(args.procman_config_file))
        except Exception as xcp:
            print("Unable to load config file.")
            print(xcp)
            sys.exit(1)
    else:
        cfg = None

    if args.observer:
        if cfg:
            print(
                "Loading a config file is not allowed when starting in observer mode, ignoring"
            )
            cfg = None
        if not args.use_gui:
            print(
                "Refusing to start an observer without a gui -- that would be useless."
            )
            sys.exit(1)
        if args.spawn_deputy:
            print("Lone ranger mode and observer mode are mutually exclusive.")
            sys.exit(1)

    lcm_obj = LCM()
    def handle(*a):
        try:
            lcm_obj.handle()
        except Exception:
            traceback.print_exc()
        return True
    GObject.io_add_watch(lcm_obj, GObject.IO_IN, handle)
    GObject.threads_init()


    if args.use_gui:
        gui = SheriffGtk(lcm_obj)
        if args.observer:
            gui.sheriff.set_observer(True)
        if args.spawn_deputy:
            gui.on_spawn_deputy_mi_activate()
        if cfg:
            try:
                gui.load_config(cfg)
            except ValueError as e:
                print("Error while loading config: {}".format(e))
                gui.cleanup(False)
                sys.exit(1)

            gui.load_save_dir = os.path.dirname(args.procman_config_file)

        if args.script:
            script = gui.script_manager.get_script(args.script)
            if not script:
                print("No such script: {}".format(args.script))
                gui.cleanup(False)
                sys.exit(1)
            errors = gui.script_manager.check_script_for_errors(script)
            if errors:
                print("Unable to run script.  Errors were detected:\n\n")
                print("\n    ".join(errors))
                gui.cleanup(False)
                sys.exit(1)
            # Use lambda with *_ as input - we ignore all parameters to the callback
            GObject.timeout_add(
                200, lambda *_: gui.run_script(None, script, args.script_done_action)
            )

        signal.signal(signal.SIGINT, lambda *_: Gtk.main_quit())
        signal.signal(signal.SIGTERM, lambda *_: Gtk.main_quit())
        signal.signal(signal.SIGHUP, lambda *_: Gtk.main_quit())

        def on_delete(widget=None, *data):
            """
            Runs on alt-f4 or close button
            """

        gui.window.connect("destroy", Gtk.main_quit)
        gui.window.connect("delete-event", on_delete)

        try:
            Gtk.main()
        except KeyboardInterrupt:
            print("Exiting")
        gui.cleanup(True)
    else:
        if not args.script:
            print("No script specified and running in headless mode.  Exiting")
            sys.exit(1)
        SheriffHeadless(
            lcm_obj, cfg, args.spawn_deputy, args.script, args.script_done_action
        ).run()


if __name__ == "__main__":
    main()
