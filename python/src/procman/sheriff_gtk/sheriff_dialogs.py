import io as StringIO
import traceback
import signal

from gi.repository import GObject
from gi.repository import Gtk
from gi.repository import Gdk

from procman.sheriff_config import Parser, ScriptNode
from procman.sheriff_script import SheriffScript, ScriptManager
from procman.sheriff import DEFAULT_STOP_SIGNAL, DEFAULT_STOP_TIME_ALLOWED


class AddModifyCommandDialog(Gtk.Dialog):
    def __init__(
        self,
        parent,
        deputies,
        groups,
        initial_cmd="",
        initial_cmd_id="",
        initial_deputy="",
        initial_group="",
        initial_auto_respawn=False,
        initial_stop_signal=DEFAULT_STOP_SIGNAL,
        initial_stop_time_allowed=DEFAULT_STOP_TIME_ALLOWED,
        is_add=True,
    ):
        # add command dialog
        Gtk.Dialog.__init__(
            self,
            "Add/Modify Command",
            parent,
            Gtk.DialogFlags.MODAL | Gtk.DialogFlags.DESTROY_WITH_PARENT,
        )
        self.add_buttons(
            Gtk.STOCK_OK,
            Gtk.ResponseType.ACCEPT,
            Gtk.STOCK_CANCEL,
            Gtk.ResponseType.REJECT,
        )
        table = Gtk.Table(7, 2)

        # deputy
        table.attach(Gtk.Label(label="Deputy"), 0, 1, 0, 1, 0, 0)
        self.deputy_cb = Gtk.ComboBoxText()

        dep_ind = 0
        deputies.sort()
        for deputy in deputies:
            self.deputy_cb.append_text(deputy)
            if deputy == initial_deputy:
                self.deputy_cb.set_active(dep_ind)
            dep_ind += 1
        if self.deputy_cb.get_active() < 0 and len(deputies) > 0:
            self.deputy_cb.set_active(0)

        table.attach(self.deputy_cb, 1, 2, 0, 1)
        if not is_add:
            self.deputy_cb.set_sensitive(False)
        self.deputies = deputies

        # command id
        table.attach(Gtk.Label(label="Id"), 0, 1, 1, 2, 0, 0)
        self.cmd_id_te = Gtk.Entry()
        self.cmd_id_te.set_text(initial_cmd_id)
        self.cmd_id_te.set_width_chars(60)
        table.attach(self.cmd_id_te, 1, 2, 1, 2)
        self.cmd_id_te.connect(
            "activate", lambda e: self.response(Gtk.ResponseType.ACCEPT)
        )
        if not is_add:
            self.cmd_id_te.set_sensitive(False)

        # command name
        table.attach(Gtk.Label(label="Command"), 0, 1, 2, 3, 0, 0)
        self.name_te = Gtk.Entry()
        self.name_te.set_text(initial_cmd)
        self.name_te.set_width_chars(60)
        table.attach(self.name_te, 1, 2, 2, 3)
        self.name_te.connect(
            "activate", lambda e: self.response(Gtk.ResponseType.ACCEPT)
        )
        self.name_te.grab_focus()

        # group
        table.attach(Gtk.Label(label="Group"), 0, 1, 3, 4, 0, 0)
        self.group_cbe = Gtk.ComboBoxText.new_with_entry()
        #        groups = groups[:]
        groups.sort()
        for group_name in groups:
            self.group_cbe.append_text(group_name)
        table.attach(self.group_cbe, 1, 2, 3, 4)
        self.group_cbe.get_child().set_text(initial_group)
        self.group_cbe.get_child().connect(
            "activate", lambda e: self.response(Gtk.ResponseType.ACCEPT)
        )

        # auto respawn
        auto_restart_tt = "If the command terminates while running, should the deputy automatically restart it?"
        auto_restart_label = Gtk.Label(label="Auto-restart")
        auto_restart_label.set_tooltip_text(auto_restart_tt)
        table.attach(auto_restart_label, 0, 1, 4, 5, 0, 0)
        self.auto_respawn_cb = Gtk.CheckButton()
        self.auto_respawn_cb.set_active(initial_auto_respawn)
        if initial_auto_respawn < 0:
            self.auto_respawn_cb.set_inconsistent(True)
        self.auto_respawn_cb.connect("toggled", self.auto_respawn_cb_callback)
        self.auto_respawn_cb.set_tooltip_text(auto_restart_tt)
        table.attach(self.auto_respawn_cb, 1, 2, 4, 5)

        # stop signal
        stop_signal_tt = "When stopping a signal, what OS signal to initially send to request a clean exit"
        stop_signal_label = Gtk.Label(label="Stop signal")
        stop_signal_label.set_tooltip_text(stop_signal_tt)
        table.attach(stop_signal_label, 0, 1, 5, 6, 0, 0)
        try:
            self.stop_signal_c = Gtk.ComboBoxText()
        except AttributeError:
            self.stop_signal_c = Gtk.ComboBoxText()
        self.stop_signal_entries = [
            (signal.SIGINT, "SIGINT"),
            (signal.SIGTERM, "SIGTERM"),
            (signal.SIGKILL, "SIGKILL"),
        ]
        for i, entry in enumerate(self.stop_signal_entries):
            signum, signame = entry
            self.stop_signal_c.append_text(signame)
            if signum == initial_stop_signal:
                self.stop_signal_c.set_active(i)
        self.stop_signal_c.set_tooltip_text(stop_signal_tt)
        table.attach(self.stop_signal_c, 1, 2, 5, 6)

        # stop time allowed
        stop_time_allowed_tt = "When stopping a running command, how long to wait between sending the stop signal and a SIGKILL if the command doesn't stop."
        stop_time_allowed_label = Gtk.Label(label="Time allowed when stopping")
        stop_time_allowed_label.set_tooltip_text(stop_time_allowed_tt)
        table.attach(stop_time_allowed_label, 0, 1, 6, 7, 0, 0)
        self.stop_time_allowed_sb = Gtk.SpinButton()
        self.stop_time_allowed_sb.set_increments(1, 5)
        self.stop_time_allowed_sb.set_range(1, 999999)
        self.stop_time_allowed_sb.set_value(int(initial_stop_time_allowed))
        self.stop_time_allowed_sb.set_tooltip_text(stop_time_allowed_tt)
        table.attach(self.stop_time_allowed_sb, 1, 2, 6, 7)

        self.vbox.pack_start(table, False, False, 0)
        table.show_all()

    def auto_respawn_cb_callback(self, widget, data=None):
        if widget.get_inconsistent():
            widget.set_inconsistent(False)

    def get_deputy(self):
        model = self.deputy_cb.get_model()
        active = self.deputy_cb.get_active()
        if active < 0:
            return None
        return model[active][0]

    def get_command(self):
        return self.name_te.get_text()

    def get_command_id(self):
        return self.cmd_id_te.get_text().strip()

    def get_group(self):
        return self.group_cbe.get_child().get_text().strip()

    def get_auto_respawn(self):
        if self.auto_respawn_cb.get_inconsistent():
            return -1
        else:
            return self.auto_respawn_cb.get_active()

    def get_stop_signal(self):
        return self.stop_signal_entries[self.stop_signal_c.get_active()][0]

    def get_stop_time_allowed(self):
        return self.stop_time_allowed_sb.get_value()


class PreferencesDialog(Gtk.Dialog):
    def __init__(self, sheriff_gtk, parent):
        # add command dialog
        Gtk.Dialog.__init__(
            self,
            "Preferences",
            parent,
            Gtk.DialogFlags.MODAL | Gtk.DialogFlags.DESTROY_WITH_PARENT,
        )
        self.add_buttons(
            Gtk.STOCK_OK,
            Gtk.ResponseType.ACCEPT,
            Gtk.STOCK_CANCEL,
            Gtk.ResponseType.REJECT,
        )

        table = Gtk.Table(4, 2)

        # console rate limit
        table.attach(Gtk.Label(label="Console rate limit (kB/s)"), 0, 1, 0, 1, 0, 0)
        self.rate_limit_sb = Gtk.SpinButton()
        self.rate_limit_sb.set_digits(0)
        self.rate_limit_sb.set_increments(1, 1000)
        self.rate_limit_sb.set_range(0, 999999)
        self.rate_limit_sb.set_value(sheriff_gtk.cmd_console.get_output_rate_limit())

        table.attach(self.rate_limit_sb, 1, 2, 0, 1)

        # background color
        table.attach(Gtk.Label(label="Console background color"), 0, 1, 1, 2, 0, 0)
        self.bg_color_bt = Gtk.ColorButton.new_with_rgba(
            Gdk.RGBA.from_color(sheriff_gtk.cmd_console.get_background_color())
        )
        table.attach(self.bg_color_bt, 1, 2, 1, 2)

        # foreground color
        table.attach(Gtk.Label(label="Console text color"), 0, 1, 2, 3, 0, 0)
        self.text_color_bt = Gtk.ColorButton.new_with_rgba(
            Gdk.RGBA.from_color(sheriff_gtk.cmd_console.get_text_color())
        )
        table.attach(self.text_color_bt, 1, 2, 2, 3)

        # font
        table.attach(Gtk.Label(label="Console font"), 0, 1, 3, 4, 0, 0)
        self.font_bt = Gtk.FontButton.new_with_font(sheriff_gtk.cmd_console.get_font())
        table.attach(self.font_bt, 1, 2, 3, 4)

        self.vbox.pack_start(table, False, False, 0)
        table.show_all()


def do_add_command_dialog(sheriff, cmds_ts, window):
    deputies = sheriff.get_deputies()
    if not deputies:
        msgdlg = Gtk.MessageDialog(
            window,
            Gtk.DialogFlags.MODAL | Gtk.DialogFlags.DESTROY_WITH_PARENT,
            Gtk.MessageType.ERROR,
            Gtk.ButtonsType.CLOSE,
            "Can't add a command without an active deputy",
        )
        msgdlg.run()
        msgdlg.destroy()
        return
    deputy_ids = [deputy.deputy_id for deputy in deputies]

    # pick an initial command id
    existing_ids = {cmd.command_id for cmd in sheriff.get_all_commands()}
    initial_cmd_id = ""
    for i in range(len(existing_ids) + 1):
        initial_cmd_id = "command_{}".format(i)
        if initial_cmd_id not in existing_ids:
            break
    assert initial_cmd_id and initial_cmd_id not in existing_ids

    dlg = AddModifyCommandDialog(
        window,
        deputy_ids,
        cmds_ts.get_known_group_names(),
        initial_cmd_id=initial_cmd_id,
    )

    while dlg.run() == Gtk.ResponseType.ACCEPT:
        try:
            sheriff.add_command(
                dlg.get_command_id(),
                dlg.get_deputy(),
                dlg.get_command(),
                dlg.get_group().strip(),
                dlg.get_auto_respawn(),
                dlg.get_stop_signal(),
                dlg.get_stop_time_allowed(),
            )
            break
        except ValueError as xcp:
            msgdlg = Gtk.MessageDialog(
                window,
                Gtk.DialogFlags.MODAL | Gtk.DialogFlags.DESTROY_WITH_PARENT,
                Gtk.MessageType.ERROR,
                Gtk.ButtonsType.CLOSE,
                str(xcp),
            )
            msgdlg.run()
            msgdlg.destroy()
    dlg.destroy()


class AddModifyScriptDialog(Gtk.Dialog):
    def __init__(self, parent, script):
        # add command dialog
        title = "Edit script"
        if script is None:
            title = "New script"
        Gtk.Dialog.__init__(
            self,
            title,
            parent,
            Gtk.DialogFlags.MODAL | Gtk.DialogFlags.DESTROY_WITH_PARENT,
        )
        self.add_buttons(
            Gtk.STOCK_OK,
            Gtk.ResponseType.ACCEPT,
            Gtk.STOCK_CANCEL,
            Gtk.ResponseType.REJECT,
        )

        self.set_default_size(800, 400)

        hbox = Gtk.HBox()

        default_contents = 'script "script-name" {\n' "    # script commands here\n" "}"

        # script contents
        self.script_tv = Gtk.TextView()
        self.script_tv.set_editable(True)
        self.script_tv.set_accepts_tab(False)
        if script is not None:
            self.script_tv.get_buffer().set_text(str(script))
        else:
            self.script_tv.get_buffer().set_text(default_contents)
        sw = Gtk.ScrolledWindow()
        sw.add(self.script_tv)
        hbox.pack_start(sw, True, True, 0)
        if script is not None:
            self.script_tv.grab_focus()

        #        # Help text
        help_tv = Gtk.TextView()
        help_tv.set_editable(False)
        help_tv.set_sensitive(False)
        help_tv.get_buffer().set_text(
            """
    Example commands:
        start cmd "server" wait "running";
        start cmd "client";
        stop cmd "server" wait "stopped";
        restart group "mygroup";
        wait group "mygroup" status "running";
        wait ms 500;
        run_script "other-script-name";
"""
        )

        #    Refer to commands and groups by what appears in the Name column.
        #    Valid actions are:
        #        start|stop|restart cmd|group "cmd_id" [wait "running"|"stopped"];
        #        wait ms ###;
        #        run_script "other-script-name";

        hbox.pack_start(help_tv, False, False, 0)
        self.vbox.pack_start(hbox, True, True, 0)
        hbox.show_all()

    #    def get_script_name (self): return self.name_te.get_text ()
    def get_script_contents(self):
        buf = self.script_tv.get_buffer()
        return buf.get_text(buf.get_start_iter(), buf.get_end_iter(), True)


def _do_err_dialog(window, msg):
    msgdlg = Gtk.MessageDialog(
        window,
        Gtk.DialogFlags.MODAL | Gtk.DialogFlags.DESTROY_WITH_PARENT,
        Gtk.MessageType.ERROR,
        Gtk.ButtonsType.CLOSE,
    )
    msgdlg.set_markup('<span font_family="monospace">{}</span>'.format(msg))
    msgdlg.run()
    msgdlg.destroy()


def _parse_script(script_manager, window, dlg):
    contents = dlg.get_script_contents()

    # check script for errors
    parser = Parser()
    try:
        cfg_node = parser.parse(StringIO.StringIO(contents))
    except ValueError as xcp:
        #        traceback.print_exc()
        _do_err_dialog(window, str(xcp))
        return None

    script_nodes = list(cfg_node.scripts.values())
    if not script_nodes:
        _do_err_dialog(window, "That's not a script...")
        return None

    if len(script_nodes) > 1:
        _do_err_dialog(window, "Only one script {} stanza allowed!")
        return None

    script = SheriffScript.from_script_node(script_nodes[0])

    errors = script_manager.check_script_for_errors(script)
    if errors:
        print(errors)
        _do_err_dialog(window, "Script error.\n\n" + "\n   ".join(errors))
        return None
    return script


def do_add_script_dialog(script_manager, window):
    dlg = AddModifyScriptDialog(window, None)
    while dlg.run() == Gtk.ResponseType.ACCEPT:
        script = _parse_script(script_manager, window, dlg)
        if script is None:
            dlg.script_tv.grab_focus()
            continue
        if script_manager.get_script(script.name) is not None:
            _do_err_dialog(
                window, "A script named {} already exists!".format(script.name)
            )
            continue
        script_manager.add_script(script)
        break
    dlg.destroy()


def do_edit_script_dialog(script_manager, window, script):
    if script_manager.get_active_script():
        _do_err_dialog(
            window, "Script editing is not allowed while a script is running."
        )
        return

    dlg = AddModifyScriptDialog(window, script)
    while dlg.run() == Gtk.ResponseType.ACCEPT:
        new_script = _parse_script(script_manager, window, dlg)
        if new_script is None:
            dlg.script_tv.grab_focus()
            continue
        if new_script.name != script.name:
            if script_manager.get_script(new_script.name) is not None:
                _do_err_dialog(
                    window, "A script named {} already exists!".format(script.name)
                )
                dlg.script_tv.grab_focus()
                continue
        script_manager.remove_script(script)
        script_manager.add_script(new_script)
        break
    dlg.destroy()


def do_preferences_dialog(sheriff_gtk, window):
    dlg = PreferencesDialog(sheriff_gtk, window)

    if dlg.run() == Gtk.ResponseType.ACCEPT:
        sheriff_gtk.cmd_console.set_background_color(dlg.bg_color_bt.get_color())
        sheriff_gtk.cmd_console.set_text_color(dlg.text_color_bt.get_color())
        sheriff_gtk.cmd_console.set_output_rate_limit(
            dlg.rate_limit_sb.get_value_as_int()
        )
        sheriff_gtk.cmd_console.set_font(dlg.font_bt.get_font_name())

    #        sheriff_Gtk.cmds_tv.set_background_color(dlg.bg_color_bt.get_color())
    #        sheriff_Gtk.cmds_tv.set_text_color(dlg.text_color_bt.get_color())
    #
    #        sheriff_Gtk.deputies_tv.set_background_color(dlg.bg_color_bt.get_color())
    #        sheriff_Gtk.deputies_tv.set_text_color(dlg.text_color_bt.get_color())

    dlg.destroy()
