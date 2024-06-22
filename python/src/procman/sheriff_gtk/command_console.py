import os
import time
import threading
import functools

from gi.repository import GLib
from gi.repository import GObject
from gi.repository import Gtk
from gi.repository import Gdk
from gi.repository import Pango

from procman.sheriff import SheriffListener
from procman_lcm.output_t import output_t

DEFAULT_MAX_KB_PER_SECOND = 500

ANSI_CODES_TO_TEXT_TAG_PROPERTIES = {
    "1": ("weight", Pango.Weight.BOLD),
    "2": ("weight", Pango.Weight.LIGHT),
    "4": ("underline", Pango.Underline.SINGLE),
    "30": ("foreground", "black"),
    "31": ("foreground", "red"),
    "32": ("foreground", "green"),
    "33": ("foreground", "yellow3"),
    "34": ("foreground", "blue"),
    "35": ("foreground", "magenta"),
    "36": ("foreground", "cyan"),
    "37": ("foreground", "white"),
    "40": ("background", "black"),
    "41": ("background", "red"),
    "42": ("background", "green"),
    "43": ("background", "yellow3"),
    "44": ("background", "blue"),
    "45": ("background", "magenta"),
    "46": ("background", "cyan"),
    "47": ("background", "white"),
}


def now_str():
    return time.strftime("[%H:%M:%S] ")


class CommandExtraData:
    def __init__(self, text_tag_table):
        self.tb = Gtk.TextBuffer.new(text_tag_table)
        self.printf_keep_count = [0, 0, 0, 0, 0, 0]
        self.printf_drop_count = 0


class SheriffCommandConsole(Gtk.ScrolledWindow, SheriffListener):
    def __init__(self, _sheriff, lc):
        super(SheriffCommandConsole, self).__init__()

        self._prev_can_reach_master = True

        self.stdout_maxlines = 250
        self.max_kb_per_sec = 0
        self.max_chars_per_2500_ms = 0

        self.sheriff = _sheriff

        # stdout textview
        self.stdout_textview = Gtk.TextView()
        self.stdout_textview.set_property("editable", False)
        self.sheriff_tb = self.stdout_textview.get_buffer()
        self.add(self.stdout_textview)

        stdout_adj = self.get_vadjustment()
        stdout_adj.scrolled_to_end = 1
        stdout_adj.connect("changed", self.on_adj_changed)
        stdout_adj.connect("value-changed", self.on_adj_value_changed)

        # deal with keyboard shortcuts
        self.connect("key-release-event", self.on_key_release)

        # add callback so we can add a clear option to the default right click popup
        self.stdout_textview.connect(
            "populate-popup", self.on_tb_populate_menu)

        # set some default appearance parameters
        self.font_str = "Monospace 10"
        self.set_font(self.font_str)
        self.base_color = Gdk.Color(65535, 65535, 65535)
        self.text_color = Gdk.Color(0, 0, 0)
        self.set_background_color(self.base_color)
        self.set_text_color(self.text_color)

        # stdout rate limit maintenance events
        GObject.timeout_add(500, self._stdout_rate_limit_upkeep)

        self.sheriff.add_listener(self)

        self._cmd_extradata = {}

        lc.subscribe ("PM_OUTPUT", self.on_procman_output)

        self.text_tags = {"normal": Gtk.TextTag.new("normal")}
        for tt in list(self.text_tags.values()):
            self.sheriff_tb.get_tag_table().add(tt)

        self.set_output_rate_limit(DEFAULT_MAX_KB_PER_SECOND)

    def command_added(self, deputy_obj, cmd_obj):
        GLib.idle_add(self._gtk_on_sheriff_command_added, deputy_obj, cmd_obj)

    def command_removed(self, deputy_obj, cmd_obj):
        GLib.idle_add(self._gtk_on_sheriff_command_removed,
                      deputy_obj, cmd_obj)

    def command_status_changed(self, cmd_obj, old_status, new_status):
        GLib.idle_add(
            self._gtk_on_command_desired_changed, cmd_obj, old_status, new_status
        )

    def get_background_color(self):
        return self.base_color

    def get_text_color(self):
        return self.text_color

    def get_font(self):
        return self.font_str

    def set_background_color(self, color):
        self.base_color = color
        self.stdout_textview.modify_base(Gtk.StateType.NORMAL, color)
        self.stdout_textview.modify_base(Gtk.StateType.ACTIVE, color)
        self.stdout_textview.modify_base(Gtk.StateType.PRELIGHT, color)

    def set_text_color(self, color):
        self.text_color = color
        self.stdout_textview.modify_text(Gtk.StateType.NORMAL, color)
        self.stdout_textview.modify_text(Gtk.StateType.ACTIVE, color)
        self.stdout_textview.modify_text(Gtk.StateType.PRELIGHT, color)

    def set_font(self, font_str):
        self.font_str = font_str
        self.stdout_textview.modify_font(Pango.FontDescription(font_str))


    def _stdout_rate_limit_upkeep(self):
        for cmd in self.sheriff.get_all_commands():
            extradata = self._cmd_extradata.get(cmd, None)
            if not extradata:
                continue
            if extradata.printf_drop_count:
                deputy = self.sheriff.get_command_deputy(cmd)
                self._add_text_to_buffer(
                    extradata.tb,
                    now_str()
                    + "\nSHERIFF RATE LIMIT: Ignored {} bytes of output\n".format(
                        extradata.printf_drop_count
                    ),
                )
                self._add_text_to_buffer(
                    self.sheriff_tb,
                    now_str()
                    + "Ignored {} bytes of output from [{}] [{}]\n".format(
                        extradata.printf_drop_count, deputy.deputy_id, cmd.command_id
                    ),
                )

            extradata.printf_keep_count.pop(0)
            extradata.printf_keep_count.append(0)
            extradata.printf_drop_count = 0
        return True

    def _tag_from_seg(self, seg):
        esc_seq, seg = seg.split("m", 1)
        if not esc_seq:
            esc_seq = "0"
        key = esc_seq
        codes = esc_seq.split(";")
        if len(codes) > 0:
            codes.sort()
            key = ";".join(codes)
        if key not in self.text_tags:
            tag = Gtk.TextTag.new(key)
            for code in codes:
                if code in ANSI_CODES_TO_TEXT_TAG_PROPERTIES:
                    propname, propval = ANSI_CODES_TO_TEXT_TAG_PROPERTIES[code]
                    tag.set_property(propname, propval)
            self.sheriff_tb.get_tag_table().add(tag)
            self.text_tags[key] = tag
        return self.text_tags[key], seg

    def _add_text_to_buffer(self, tb, text):
        if not text:
            return

        # interpret text as ANSI escape sequences?  Try to format colors...
        tag = self.text_tags["normal"]
        for segnum, seg in enumerate(text.split("\x1b[")):
            if not seg:
                continue
            if segnum > 0:
                try:
                    tag, seg = self._tag_from_seg(seg)
                except ValueError:
                    pass
            end_iter = tb.get_end_iter()
            tb.insert_with_tags(end_iter, seg, tag)

        # toss out old text if the buffer is getting too big
        num_lines = tb.get_line_count()
        if num_lines > self.stdout_maxlines:
            start_iter = tb.get_start_iter()
            chop_iter = tb.get_iter_at_line(num_lines - self.stdout_maxlines)
            # Must use idle_add here otherwise the output console will not be updated correctly
            GLib.idle_add(functools.partial(
                self.del_tb, start_iter, chop_iter))

    def del_tb(self, start, chop):
        self.sheriff_tb.delete(start, chop)

    # Sheriff event handlers
    def _gtk_on_sheriff_command_added(self, deputy, command):
        extradata = CommandExtraData(self.sheriff_tb.get_tag_table())
        self._cmd_extradata[command] = extradata
        self._add_text_to_buffer(
            self.sheriff_tb,
            now_str()
            + "Added [{}] [{}] [{}]\n".format(
                deputy.deputy_id, command.command_id, command.exec_str
            ),
        )

    def _gtk_on_sheriff_command_removed(self, deputy, command):
        if command in self._cmd_extradata:
            del self._cmd_extradata[command]
        self._add_text_to_buffer(
            self.sheriff_tb,
            now_str()
            + "[{}] removed [{}] [{}]\n".format(
                deputy.deputy_id, command.command_id, command.exec_str
            ),
        )

    def _gtk_on_command_desired_changed(self, cmd, old_status, new_status):
        self._add_text_to_buffer(
            self.sheriff_tb,
            now_str() +
            "[{}] new status: {}\n".format(cmd.command_id, new_status),
        )

    def on_tb_populate_menu(self, textview, menu):
        sep = Gtk.SeparatorMenuItem()
        menu.append(sep)
        sep.show()
        mi = Gtk.MenuItem("_Clear")
        menu.append(mi)
        mi.connect("activate", self._tb_clear)
        mi.show()

    def _tb_clear(self, menu):
        tb = self.stdout_textview.get_buffer()
        start_iter = tb.get_start_iter()
        end_iter = tb.get_end_iter()
        tb.delete(start_iter, end_iter)

    def _tb_copy_selection(self):
        tb = self.stdout_textview.get_buffer()
        bounds = tb.get_selection_bounds()

        if bounds:
            text = tb.get_text(bounds[0], bounds[1], True)
            clipboard = Gtk.Clipboard()
            clipboard.set_text(text)
            clipboard.store()

    def on_key_release(self, widget, event):
        key_value = event.keyval
        key_name = Gdk.keyval_name(key_value)
        state = event.get_state()
        ctrl = state & Gdk.ModifierType.CONTROL_MASK
        if ctrl and key_name == "c":
            self._tb_copy_selection()
            return True
        else:
            return False

    def set_output_rate_limit(self, max_kb_per_sec):
        self.max_kb_per_sec = max_kb_per_sec
        self.max_chars_per_2500_ms = int(max_kb_per_sec * 1000 * 2.5)

    def get_output_rate_limit(self):
        return self.max_kb_per_sec

    def load_settings(self, save_map):
        if "console_rate_limit" in save_map:
            self.set_output_rate_limit(save_map["console_rate_limit"])

        if "console_background_color" in save_map:
            bg_color = Gdk.RGBA()
            bg_color.parse(save_map["console_background_color"])
            self.set_background_color(bg_color.to_color())

        if "console_text_color" in save_map:
            text_color = Gdk.RGBA()
            text_color.parse(save_map["console_text_color"])
            self.set_text_color(text_color.to_color())

        if "console_font" in save_map:
            self.set_font(save_map["console_font"])

    def save_settings(self, save_map):
        save_map["console_rate_limit"] = self.max_kb_per_sec
        save_map["console_background_color"] = self.base_color.to_string()
        save_map["console_text_color"] = self.text_color.to_string()
        save_map["console_font"] = self.font_str

    def on_adj_changed(self, adj):
        if adj.scrolled_to_end:
            adj.set_value(adj.get_upper() - adj.get_page_size())

    def on_adj_value_changed(self, adj):
        adj.scrolled_to_end = adj.get_value() == (
            adj.get_upper() - adj.get_page_size())

    def _handle_command_output(self, command_id, text):
        cmd = self.sheriff.get_command(command_id)
        if not cmd:
            return

        extradata = self._cmd_extradata.get(cmd, None)
        if not extradata:
            return

        # rate limit
        msg_count = sum(extradata.printf_keep_count)
        if msg_count >= self.max_chars_per_2500_ms:
            extradata.printf_drop_count += len(text)
            return

        tokeep = min(self.max_chars_per_2500_ms - msg_count, len(text))
        extradata.printf_keep_count[-1] += tokeep

        if len(text) > tokeep:
            toadd = text[:tokeep]
        else:
            toadd = text

        self._add_text_to_buffer(extradata.tb, toadd)

    def on_procman_output(self, channel, data):
        msg = output_t.decode(data)
        for i in range(msg.num_commands):
            command_id = msg.command_ids[i]
            text = msg.text[i]
            GLib.idle_add(self._handle_command_output, command_id, text)

    def show_command_buffer(self, cmd):
        extradata = self._cmd_extradata.get(cmd, None)
        if extradata:
            self.stdout_textview.set_buffer(extradata.tb)

    def show_sheriff_buffer(self):
        self.stdout_textview.set_buffer(self.sheriff_tb)
