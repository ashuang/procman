import time
from gi.repository import GObject
from gi.repository import Gtk
from gi.repository import Gdk

import procman.sheriff as sheriff
import procman.sheriff_gtk.command_model as cm
import procman.sheriff_gtk.sheriff_dialogs as sd


class DeputyModel(Gtk.ListStore):
    COL_OBJ, COL_DEPUTY_ID, COL_LAST_UPDATE, COL_LOAD, NUM_ROWS = list(range(5))

    def __init__(self, _sheriff):
        super(DeputyModel, self).__init__(
            GObject.TYPE_PYOBJECT,
            GObject.TYPE_STRING,  # deputy id
            GObject.TYPE_STRING,  # last update time
            GObject.TYPE_STRING,  # load
        )
        self.sheriff = _sheriff

    def update(self):
        to_update = set(self.sheriff.get_deputies())
        to_remove = []

        def _deputy_last_update_str(dep):
            if dep.last_update_utime:
                now_utime = time.time() * 1000000
                return "{:.1f} seconds ago".format(
                    (now_utime - dep.last_update_utime) * 1e-6
                )
            else:
                return "<never>"

        def _update_deputy_row(model, path, model_iter, user_data):
            deputy = model.get_value(model_iter, DeputyModel.COL_OBJ)
            if deputy in to_update:
                model.set(
                    model_iter,
                    DeputyModel.COL_LAST_UPDATE,
                    _deputy_last_update_str(deputy),
                    DeputyModel.COL_LOAD,
                    "{:.4f}".format(deputy.cpu_load),
                )
                to_update.remove(deputy)
            else:
                to_remove.append(Gtk.TreeRowReference(model, path))

        self.foreach(_update_deputy_row, None)

        for trr in to_remove:
            self.remove(self.get_iter(trr.get_path()))

        for deputy in to_update:
            #            print "adding %s to treeview" % deputy.deputy_id
            new_row = (
                deputy,
                deputy.deputy_id,
                _deputy_last_update_str(deputy),
                "{}".format(deputy.cpu_load),
            )
            self.append(new_row)


class DeputyTreeView(Gtk.TreeView):
    def __init__(self, _sheriff, deputies_ts):
        super(DeputyTreeView, self).__init__(deputies_ts)
        self.sheriff = _sheriff
        self.deputies_ts = deputies_ts

        plain_tr = Gtk.CellRendererText()
        col = Gtk.TreeViewColumn("Deputy", plain_tr, text=DeputyModel.COL_DEPUTY_ID)
        col.set_sort_column_id(1)
        col.set_resizable(True)
        self.append_column(col)

        last_update_tr = Gtk.CellRendererText()
        col = Gtk.TreeViewColumn(
            "Last update", last_update_tr, text=DeputyModel.COL_LAST_UPDATE
        )
        #        col.set_sort_column_id (2) # XXX this triggers really weird bugs...
        col.set_resizable(True)
        col.set_cell_data_func(last_update_tr, self._deputy_last_update_cell_data_func)
        self.append_column(col)

        col = Gtk.TreeViewColumn("Load", plain_tr, text=DeputyModel.COL_LOAD)
        col.set_resizable(True)
        self.append_column(col)

        self.connect("button-press-event", self._on_deputies_tv_button_press_event)

        # deputies treeview context menu
        self.deputies_ctxt_menu = Gtk.Menu()

        self.cleanup_deputies_ctxt_mi = Gtk.MenuItem.new_with_mnemonic("_Cleanup")
        self.deputies_ctxt_menu.append(self.cleanup_deputies_ctxt_mi)
        self.cleanup_deputies_ctxt_mi.connect("activate", self._cleanup_deputies)
        self.deputies_ctxt_menu.show_all()

    def _on_deputies_tv_button_press_event(self, treeview, event):
        if event.type == Gdk.EventType.BUTTON_PRESS and event.button == 3:
            self.deputies_ctxt_menu.popup(None, None, None, None, event.button, event.time)
            return True

    def _cleanup_deputies(self, *args):
        self.sheriff.remove_empty_deputies()
        self.deputies_ts.update()

    def _deputy_last_update_cell_data_func(self, column, cell, model, model_iter, *data):
        # bit of a hack to pull out the last update time
        try:
            last_update = float(
                model.get_value(model_iter, DeputyModel.COL_LAST_UPDATE).split()[0]
            )
        except:
            last_update = None
        if last_update is None or last_update > 5:
            cell.set_property("cell-background-set", True)
            cell.set_property("cell-background", "Red")
        elif last_update > 2:
            cell.set_property("cell-background-set", True)
            cell.set_property("cell-background", "Yellow")
        else:
            cell.set_property("cell-background-set", False)

    def save_settings(self, save_map):
        pass

    def load_settings(self, save_map):
        pass
