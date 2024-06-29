from gi.repository import Gtk
from gi.repository import Gdk
from gi.repository import Pango

import procman.sheriff as sheriff
import procman.sheriff_gtk.command_model as cm
import procman.sheriff_gtk.sheriff_dialogs as sd


class SheriffCommandTreeView(Gtk.TreeView):
    def __init__(self, _sheriff, _script_manager, cmds_ts):
        super(SheriffCommandTreeView, self).__init__(cmds_ts)
        self.cmds_ts = cmds_ts
        self.sheriff = _sheriff
        self.script_manager = _script_manager

        cmds_tr = Gtk.CellRendererText()
        cmds_tr.set_property("ellipsize", Pango.EllipsizeMode.END)
        plain_tr = Gtk.CellRendererText()
        status_tr = Gtk.CellRendererText()

        cols_to_make = [
            ("Id", cmds_tr, cm.COL_CMDS_TV_COMMAND_ID, None),
            ("Command", cmds_tr, cm.COL_CMDS_TV_EXEC, None),
            ("Deputy", plain_tr, cm.COL_CMDS_TV_DEPUTY, None),
            (
                "Status",
                status_tr,
                cm.COL_CMDS_TV_STATUS_ACTUAL,
                self._status_cell_data_func,
            ),
            ("CPU %", plain_tr, cm.COL_CMDS_TV_CPU_USAGE, None),
            ("Mem (kB)", plain_tr, cm.COL_CMDS_TV_MEM_RSS, None),
        ]

        self.columns = []
        for command_id, renderer, col_id, cell_data_func in cols_to_make:
            col = Gtk.TreeViewColumn(command_id, renderer, text=col_id)
            col.set_sort_column_id(col_id)
            col.col_id = col_id
            if cell_data_func:
                col.set_cell_data_func(renderer, cell_data_func)
            self.columns.append(col)

        # set an initial width for the id column
        self.columns[0].set_sizing(Gtk.TreeViewColumnSizing.FIXED)
        self.columns[0].set_fixed_width(150)

        for col in self.columns:
            col.set_resizable(True)
            self.append_column(col)

        cmds_sel = self.get_selection()
        cmds_sel.set_mode(Gtk.SelectionMode.MULTIPLE)

        self.add_events(
            Gdk.EventMask.KEY_PRESS_MASK
            | Gdk.EventType.BUTTON_PRESS
            | Gdk.EventType._2BUTTON_PRESS
        )
        self.connect("key-press-event", self._on_cmds_tv_key_press_event)
        self.connect("button-press-event", self._on_cmds_tv_button_press_event)
        self.connect("row-activated", self._on_cmds_tv_row_activated)

        # commands treeview context menu
        self.cmd_ctxt_menu = Gtk.Menu()

        self.start_cmd_ctxt_mi = Gtk.MenuItem.new_with_mnemonic("_Start")
        self.cmd_ctxt_menu.append(self.start_cmd_ctxt_mi)
        self.start_cmd_ctxt_mi.connect("activate", self._start_selected_commands)

        self.stop_cmd_ctxt_mi = Gtk.MenuItem.new_with_mnemonic("S_top")
        self.cmd_ctxt_menu.append(self.stop_cmd_ctxt_mi)
        self.stop_cmd_ctxt_mi.connect("activate", self._stop_selected_commands)

        self.restart_cmd_ctxt_mi = Gtk.MenuItem.new_with_mnemonic("R_estart")
        self.cmd_ctxt_menu.append(self.restart_cmd_ctxt_mi)
        self.restart_cmd_ctxt_mi.connect("activate", self._restart_selected_commands)

        self.remove_cmd_ctxt_mi = Gtk.MenuItem.new_with_mnemonic("_Remove")
        self.cmd_ctxt_menu.append(self.remove_cmd_ctxt_mi)
        self.remove_cmd_ctxt_mi.connect("activate", self._remove_selected_commands)

        self.cmd_ctxt_menu.append(Gtk.SeparatorMenuItem())

        self.edit_cmd_ctxt_mi = Gtk.MenuItem.new_with_mnemonic("_Edit")
        self.cmd_ctxt_menu.append(self.edit_cmd_ctxt_mi)
        self.edit_cmd_ctxt_mi.connect("activate", self._edit_selected_command)

        self.new_cmd_ctxt_mi = Gtk.MenuItem.new_with_mnemonic("_New Command")
        self.cmd_ctxt_menu.append(self.new_cmd_ctxt_mi)
        self.new_cmd_ctxt_mi.connect(
            "activate",
            lambda *s: sd.do_add_command_dialog(
                self.sheriff, self.cmds_ts, self.get_toplevel()
            ),
        )

        self.cmd_ctxt_menu.show_all()

    #        # set some default appearance parameters
    #        self.base_color = Gdk.Color(65535, 65535, 65535)
    #        self.text_color = Gdk.Color(0, 0, 0)
    #        self.set_background_color(self.base_color)
    #        self.set_text_color(self.text_color)

    #        # drag and drop command rows for grouping
    #        dnd_targets = [ ('PROCMAN_CMD_ROW',
    #            Gtk.TargetFlags.SAME_APP | Gtk.TargetFlags.SAME_WIDGET, 0) ]
    #        self.enable_model_drag_source (Gdk.ModifierType.BUTTON1_MASK,
    #                dnd_targets, Gdk.DragAction.MOVE)
    #        self.enable_model_drag_dest (dnd_targets,
    #                Gdk.DragAction.MOVE)

    def get_columns(self):
        return self.columns

    def get_selected_commands(self):
        selection = self.get_selection()
        if selection is None:
            return []
        model, rows = selection.get_selected_rows()
        assert model is self.cmds_ts
        return self.cmds_ts.rows_to_commands(rows)

    #    def get_background_color(self):
    #        return self.base_color
    #
    #    def get_text_color(self):
    #        return self.text_color
    #
    #    def set_background_color(self, color):
    #        self.base_color = color
    #        self.modify_base(Gtk.StateType.NORMAL, color)
    #        self.modify_base(Gtk.StateType.ACTIVE, color)
    #        self.modify_base(Gtk.StateType.PRELIGHT, color)
    #
    #    def set_text_color(self, color):
    #        self.text_color = color
    #        self.modify_text(Gtk.StateType.NORMAL, color)
    #        self.modify_text(Gtk.StateType.ACTIVE, color)
    #        self.modify_text(Gtk.StateType.PRELIGHT, color)

    def save_settings(self, save_map):
        for col in self.get_columns():
            col_id = col.col_id

            visible_key = "command_treeview:visible:{}".format(col_id)
            width_key = "command_treeview:width:{}".format(col_id)
            save_map[visible_key] = col.get_visible()
            save_map[width_key] = col.get_width()

    #        save_map["command_treeview_background_color"] = self.base_color.to_string()
    #        save_map["command_treeview_text_color"] = self.text_color.to_string()

    def load_settings(self, save_map):
        for col in self.get_columns():
            col_id = col.col_id

            visible_key = "command_treeview:visible:{}".format(col_id)
            width_key = "command_treeview:width:{}".format(col_id)
            should_be_visible = save_map.get(visible_key, True)
            col.set_visible(should_be_visible)
            if int(col_id) == cm.COL_CMDS_TV_COMMAND_ID:
                self.cmds_ts.set_populate_exec_with_group_name(not should_be_visible)

            width = save_map.get(width_key, 0)
            if width > 0:
                col.set_sizing(Gtk.TreeViewColumnSizing.FIXED)
                col.set_fixed_width(width)
                col.set_resizable(True)

    #        if "command_treeview_background_color" in save_map:
    #            self.set_background_color(Gdk.Color(save_map["command_treeview_background_color"]))
    #
    #        if "command_treeview_text_color" in save_map:
    #            self.set_text_color(Gdk.Color(save_map["command_treeview_text_color"]))

    def _start_selected_commands(self, *args):
        for cmd in self.get_selected_commands():
            self.sheriff.start_command(cmd)

    def _stop_selected_commands(self, *args):
        for cmd in self.get_selected_commands():
            self.sheriff.stop_command(cmd)

    def _restart_selected_commands(self, *args):
        for cmd in self.get_selected_commands():
            self.sheriff.restart_command(cmd)

    def _remove_selected_commands(self, *args):
        for cmd in self.get_selected_commands():
            self.sheriff.remove_command(cmd)

    def _edit_selected_command(self, *args):
        cmds = self.get_selected_commands()
        self._do_edit_command_dialog(cmds)

    def _on_cmds_tv_key_press_event(self, widget, event):
        if event.keyval == Gdk.keyval_from_name("Right"):
            # expand a group row when user presses right arrow key
            model, rows = self.get_selection().get_selected_rows()
            if len(rows) == 1:
                model_iter = model.get_iter(rows[0])
                if model.iter_has_child(model_iter):
                    self.expand_row(rows[0], True)
                return True
        elif event.keyval == Gdk.keyval_from_name("Left"):
            # collapse a group row when user presses left arrow key
            model, rows = self.get_selection().get_selected_rows()
            if len(rows) == 1:
                model_iter = model.get_iter(rows[0])
                if model.iter_has_child(model_iter):
                    self.collapse_row(rows[0])
                else:
                    parent = model.iter_parent(model_iter)
                    if parent:
                        parent_path = self.cmds_ts.get_path(parent)
                        self.set_cursor(parent_path)
                return True
        return False

    def _on_cmds_tv_button_press_event(self, treeview, event):
        if event.type == Gdk.EventType.BUTTON_PRESS and event.button == 3:
            time = event.time
            treeview.grab_focus()
            sel = self.get_selection()
            model, rows = sel.get_selected_rows()
            pathinfo = treeview.get_path_at_pos(int(event.x), int(event.y))
            selected_cmds = []

            if pathinfo is not None:
                if pathinfo[0] not in rows:
                    # if user right-clicked on a previously unselected row,
                    # then unselect all other rows and select only the row
                    # under the mouse cursor
                    path, col, cellx, celly = pathinfo
                    treeview.grab_focus()
                    treeview.set_cursor(path, col, 0)

                # build a submenu of all deputies
                selected_cmds = self.get_selected_commands()
            #                can_start_stop_remove = len(selected_cmds) > 0 and \
            #                        not self.sheriff.is_observer ()

            else:
                sel.unselect_all()

            # enable/disable menu options based on sheriff state and user
            # selection
            can_add_load = (
                not self.sheriff.is_observer()
                and not self.script_manager.get_active_script()
            )
            can_modify = (
                pathinfo is not None
                and not self.sheriff.is_observer()
                and not self.script_manager.get_active_script()
            )

            self.start_cmd_ctxt_mi.set_sensitive(can_modify)
            self.stop_cmd_ctxt_mi.set_sensitive(can_modify)
            self.restart_cmd_ctxt_mi.set_sensitive(can_modify)
            self.remove_cmd_ctxt_mi.set_sensitive(can_modify)
            self.edit_cmd_ctxt_mi.set_sensitive(can_modify)
            self.new_cmd_ctxt_mi.set_sensitive(can_add_load)

            self.cmd_ctxt_menu.popup(None, None, None, None, event.button, time)
            return 1
        elif event.type == Gdk.EventType._2BUTTON_PRESS and event.button == 1:
            # expand or collapse groups when double clicked
            sel = self.get_selection()
            model, rows = sel.get_selected_rows()
            if len(rows) == 1:
                if model.iter_has_child(model.get_iter(rows[0])):
                    if self.row_expanded(rows[0]):
                        self.collapse_row(rows[0])
                    else:
                        self.expand_row(rows[0], True)
        elif event.type == Gdk.EventType.BUTTON_PRESS and event.button == 1:
            # unselect all rows when the user clicks on empty space in the
            # commands treeview
            time = event.time
            x = int(event.x)
            y = int(event.y)
            pathinfo = treeview.get_path_at_pos(x, y)
            if pathinfo is None:
                self.get_selection().unselect_all()

    def _do_edit_command_dialog(self, cmds):
        unchanged_val = "[Unchanged]"

        old_deputies = [self.sheriff.get_command_deputy(cmd).deputy_id for cmd in cmds]

        old_exec_strs = [cmd.exec_str for cmd in cmds]
        old_command_ids = [cmd.command_id for cmd in cmds]
        old_groups = [cmd.group for cmd in cmds]
        old_auto_respawns = [cmd.auto_respawn for cmd in cmds]
        old_stop_signals = [cmd.stop_signal for cmd in cmds]
        old_stop_times_allowed = [cmd.stop_time_allowed for cmd in cmds]

        # handle all same/different deputies
        if all(x == old_deputies[0] for x in old_deputies):
            deputies_list = [deputy.deputy_id for deputy in self.sheriff.get_deputies()]
            cur_deputy = old_deputies[0]
        else:
            deputies_list = [unchanged_val]
            deputies_list.extend(
                [deputy.deputy_id for deputy in self.sheriff.get_deputies()]
            )
            cur_deputy = unchanged_val

        # handle all same/different groups
        if all(x == old_groups[0] for x in old_groups):
            groups_list = self.cmds_ts.get_known_group_names()
            cur_group = old_groups[0]
        else:
            groups_list = [unchanged_val]
            groups_list.extend(self.cmds_ts.get_known_group_names())
            cur_group = unchanged_val

        # executable string, command id
        if all(x == old_exec_strs[0] for x in old_exec_strs):
            cur_exec_str = old_exec_strs[0]
        else:
            cur_exec_str = unchanged_val
        if all(x == old_command_ids[0] for x in old_command_ids):
            cur_command_id = old_command_ids[0]
        else:
            cur_command_id = unchanged_val

        # auto respawn
        if all(x == old_auto_respawns[0] for x in old_auto_respawns):
            if old_auto_respawns[0]:
                cur_auto_respawn = 1
            else:
                cur_auto_respawn = 0
        else:
            cur_auto_respawn = -1

        # stop signal
        if all(x == old_stop_signals[0] for x in old_stop_signals):
            cur_stop_signal = old_stop_signals[0]
        else:
            cur_stop_signal = unchanged_val

        # stop time allowed
        if all(x == old_stop_times_allowed[0] for x in old_stop_times_allowed):
            cur_stop_time_allowed = old_stop_times_allowed[0]
        else:
            cur_stop_time_allowed = unchanged_val

        # create the dialog box
        dlg = sd.AddModifyCommandDialog(
            self.get_toplevel(),
            deputies_list,
            groups_list,
            cur_exec_str,
            cur_command_id,
            cur_deputy,
            cur_group,
            cur_auto_respawn,
            cur_stop_signal,
            cur_stop_time_allowed,
            is_add=False,
        )

        while dlg.run() == Gtk.ResponseType.ACCEPT:
            new_exec_str = dlg.get_command()
            newgroup = dlg.get_group()
            newauto_respawn = dlg.get_auto_respawn()
            new_stop_signal = dlg.get_stop_signal()
            new_stop_time_allowed = dlg.get_stop_time_allowed()
            cmd_ind = 0

            for cmd in cmds:
                if new_exec_str != cmd.exec_str and new_exec_str != unchanged_val:
                    self.sheriff.set_command_exec(cmd, new_exec_str)

                if newauto_respawn != cmd.auto_respawn and newauto_respawn >= 0:
                    self.sheriff.set_command_auto_respawn(cmd, newauto_respawn)

                if newgroup != cmd.group and newgroup != unchanged_val:
                    self.sheriff.set_command_group(cmd, newgroup)

                if (
                    new_stop_signal != cmd.stop_signal
                    and new_stop_signal != unchanged_val
                ):
                    self.sheriff.set_command_stop_signal(cmd, new_stop_signal)

                if (
                    new_stop_time_allowed != cmd.stop_time_allowed
                    and new_stop_time_allowed != unchanged_val
                ):
                    self.sheriff.set_command_stop_time_allowed(
                        cmd, new_stop_time_allowed
                    )

                cmd_ind = cmd_ind + 1
            break
        dlg.destroy()

    def _on_cmds_tv_row_activated(self, treeview, path, column):
        cmd = self.cmds_ts.path_to_command(path)
        if not cmd:
            return
        self._do_edit_command_dialog([cmd])

    def _status_cell_data_func(self, column, cell, model, model_iter, *data):
        color_map = {
            sheriff.TRYING_TO_START: "Orange",
            sheriff.RESTARTING: "Orange",
            sheriff.RUNNING: "Green",
            sheriff.TRYING_TO_STOP: "Yellow",
            sheriff.REMOVING: "Yellow",
            sheriff.STOPPED_ERROR: "Red",
            sheriff.UNKNOWN: "Gray",
        }

        assert model is self.cmds_ts
        cmd = self.cmds_ts.iter_to_command(model_iter)
        if not cmd:
            # group node
            children = self.cmds_ts.get_group_row_child_commands_recursive(model_iter)

            if not children:
                cell.set_property("cell-background-set", False)
            else:
                statuses = [cmd.status() for cmd in children]

                if all([s == statuses[0] for s in statuses]):
                    # if all the commands in a group have the same status, then
                    # color them by that status
                    if statuses[0] == sheriff.STOPPED_OK:
                        cell.set_property("cell-background-set", False)
                        cell.set_property("foreground-set", False)
                    else:
                        cell.set_property("cell-background-set", True)
                        cell.set_property("foreground-set", True)
                        cell.set_property("cell-background", color_map[statuses[0]])
                        cell.set_property("foreground", "Black")
                else:
                    # otherwise, color them yellow
                    cell.set_property("cell-background-set", True)
                    cell.set_property("foreground-set", True)
                    cell.set_property("cell-background", "Yellow")
                    cell.set_property("foreground", "Black")

            return

        if cmd.status() == sheriff.STOPPED_OK:
            cell.set_property("cell-background-set", False)
            cell.set_property("foreground-set", False)
        else:
            cell.set_property("cell-background-set", True)
            cell.set_property("foreground-set", True)
            cell.set_property("cell-background", color_map[cmd.status()])
            cell.set_property("foreground", "Black")
