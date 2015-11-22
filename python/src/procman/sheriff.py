"""@package sheriff

\defgroup python_api Python API
"""
import os
import platform
import sys
import time
import random
import signal
import threading
import thread

import lcm
from procman_lcm.cmd_t import cmd_t
from procman_lcm.deputy_info_t import deputy_info_t
from procman_lcm.orders_t import orders_t
from procman_lcm.cmd_desired_t import cmd_desired_t
from procman_lcm.cmd_status_t import cmd_status_t
from procman_lcm.discovery_t import discovery_t
import procman.sheriff_config as sheriff_config
from procman.signal_slot import Signal

def _dbg(text):
#    return
    sys.stderr.write("%s\n" % text)

def _warn(text):
    sys.stderr.write("[WARNING] %s\n" % text)

def _now_utime():
    return int(time.time() * 1000000)

## \addtogroup python_api
# @{

## Command status - trying to start
TRYING_TO_START = "Starting (Command Sent)"

## Command status - running
RUNNING = "Running"

## Command status - trying to stop a command
TRYING_TO_STOP = "Stopping (Command Sent)"

## Command status - removing a command
REMOVING = "Removing (Command Sent)"

## Command status - command stopped without error
STOPPED_OK = "Stopped (OK)"

## Command status - command stopped with error
STOPPED_ERROR = "Stopped (Error)"

## Command status - unknown status
UNKNOWN = "Unknown"

## Command status - restarting a command
RESTARTING = "Restarting (Command Sent)"

## @} ##

DEFAULT_STOP_SIGNAL = signal.SIGINT
DEFAULT_STOP_TIME_ALLOWED = 7

class SheriffCommandSpec(object):
    """Basic command specification.

    \ingroup python_api
    """
    __slots__ = [ "deputy_name", "exec_str", "command_id", "group_name",
            "auto_respawn", "stop_signal", "stop_time_allowed" ]

    def __init__(self):
        """Initializer.
        """

        ## the name of the deputy that will manage this command.
        self.deputy_name = ""

        ## the actual command string to execute.
        self.exec_str = ""

        ## an identifier string for this command.  Must be unique within a deputy.
        self.command_id = ""

        ## the command group name, or the empty string for no group.
        self.group_name = ""

        ## True if the deputy should automatically restart the
        # command when it exits.  Auto respawning only happens when the desired
        # state of the command is running.
        self.auto_respawn = False

        ## When stopping the command, this OS-level signal will be sent to the
        # command to request a clean exit.  The default is SIGINT
        self.stop_signal = DEFAULT_STOP_SIGNAL

        ## When stopping the command, the deputy will wait this amount of time
        # (seconds) in between requesting a clean exit and forcing the command
        # to stop via a SIGKILL
        self.stop_time_allowed = DEFAULT_STOP_TIME_ALLOWED

class SheriffDeputyCommand(object):
    """A command managed by a deputy, which is in turn managed by the %Sheriff.

    \ingroup python_api
    """
    def __init__(self):
        ## Sheriff-assigned number used to identify the process to this
        # sheriff.
        self.sheriff_id = 0

        ## Process ID of the command as reported by the deputy.
        # Has value -1 if the PID is unknown.
        # Has value 0 if the process is not running (i.e., stopped)
        # Has a positive value if the process is running.
        self.pid = -1

        ## If the command is stopped, its exit code.
        self.exit_code = 0

        ## Command CPU usage, as reported by the deputy.  Ranges from [0, 1]
        self.cpu_usage = 0

        ## Virtual memory used by the command.
        self.mem_vsize_bytes = 0

        ## Resident memory used by the command.
        self.mem_rss_bytes = 0

        ## The executable string for the command.
        self.exec_str = ""

        ## A user-assigned string identifier for the comand.
        self.command_id = ""

        ## A user-assigned group name for the command, possibly empty.
        self.group = ""

        ## Sheriff-managed variable used to start and stop the command.
        self.desired_runid = 0

        ## Sheriff-managed variable used to start and stop the command.
        self.force_quit = 0

        ## True if the command is being removed.
        self.scheduled_for_removal = False

        ## Deputy-reported variable identifying the run ID of the command.
        # This variable changes each time the command is started.
        self.actual_runid = 0

        ## True if the deputy should automatically restart the command when it
        # exits.  Auto respawn only happens if the command is set to running.
        self.auto_respawn = False

        ## When stopping the command, which OS signal to send the command to
        # request that it cleanly exit.  This usually defaults to SIGINT.
        self.stop_signal = DEFAULT_STOP_SIGNAL

        ## When stopping the command, how much time to wait in between sending
        # it stop_signal and a SIGKILL.
        self.stop_time_allowed = DEFAULT_STOP_TIME_ALLOWED

        ## True if this data structure has been updated with information
        # received from a deputy, False if not.
        self.updated_from_info = False

    def _update_from_cmd_info(self, cmd_msg):
        self.pid = cmd_msg.pid
        self.actual_runid = cmd_msg.actual_runid
        self.exit_code = cmd_msg.exit_code
        self.cpu_usage = cmd_msg.cpu_usage
        self.mem_vsize_bytes = cmd_msg.mem_vsize_bytes
        self.mem_rss_bytes = cmd_msg.mem_rss_bytes
        self.updated_from_info = True

        # if the command has run to completion and we don't need it to respawn,
        # then prevent it from respawning if the deputy restarts
        if self.pid == 0 and \
            self.actual_runid == self.desired_runid and \
            not self.auto_respawn and \
            not self.force_quit:
                self.force_quit = 1

    def _update_from_cmd_orders(self, cmd_msg):
        assert self.sheriff_id == cmd_msg.sheriff_id
        self.exec_str = cmd_msg.cmd.exec_str
        self.command_id = cmd_msg.cmd.command_id
        self.group = cmd_msg.cmd.group
        self.desired_runid = cmd_msg.desired_runid
        self.force_quit = cmd_msg.force_quit
        self.stop_signal = cmd_msg.cmd.stop_signal
        self.stop_time_allowed = cmd_msg.cmd.stop_time_allowed

    def _set_group(self, group):
        self.group = group

    def _start(self):
        # if the command is already running, then ignore
        if self.pid > 0 and not self.force_quit:
            return

        self.desired_runid += 1
        if self.desired_runid > (2 << 31):
            self.desired_runid = 1
        self.force_quit = 0

    def _restart(self):
        self.desired_runid += 1
        if self.desired_runid > (2 << 31):
            self.desired_runid = 1
        self.force_quit = 0

    def _stop(self):
        self.force_quit = 1

    def status(self):
        """Retrieve the status of the command, as understood by the
        sheriff.

        Returns one of:
        - procman.sheriff.TRYING_TO_START
        - procman.sheriff.RUNNING
        - procman.sheriff.TRYING_TO_STOP
        - procman.sheriff.REMOVING
        - procman.sheriff.STOPPED_OK
        - procman.sheriff.STOPPED_ERROR
        - procman.sheriff.UNKNOWN
        - procman.sheriff.RESTARTING
        """
        if not self.updated_from_info:
            return UNKNOWN
        if self.desired_runid != self.actual_runid and not self.force_quit:
            if self.pid == 0:
                return TRYING_TO_START
            else:
                return RESTARTING
        elif self.desired_runid == self.actual_runid:
            if self.pid > 0:
                if not self.force_quit and not self.scheduled_for_removal:
                    return RUNNING
                else:
                    return TRYING_TO_STOP
            else:
                if self.scheduled_for_removal:
                    return REMOVING
                elif self.exit_code == 0:
                    return STOPPED_OK
                elif self.force_quit and \
                     os.WIFSIGNALED(self.exit_code) and \
                     os.WTERMSIG(self.exit_code) in [ signal.SIGTERM,
                             signal.SIGINT, signal.SIGKILL ]:
                         return STOPPED_OK
                else:                          return STOPPED_ERROR
        else:
            return UNKNOWN

    def __str__(self):
        return """[%(exec_str)s]
   group:        %(group)s
   sheriff_id:   %(sheriff_id)d
   pid:          %(pid)d
   exit_code:    %(exit_code)d
   cpu_usage:    %(cpu_usage)f
   mem_vsize:    %(mem_vsize_bytes)d
   mem_rss:      %(mem_rss_bytes)d
   actual_runid: %(actual_runid)d""" % self.__dict__

class SheriffDeputy(object):
    """%Sheriff view of a deputy

    \ingroup python_api
    """
    def __init__(self, name):
        """Initializes a deputy with the specified name.  Do not use this
        constructor directly.  Instead, get a list of deputies from the
        Sheriff.
        """

        ## Deputy name
        self.name = name

        ## Last reported CPU load on the deputy.  Ranges from [0, 1], where 0
        # is no load and 1 is fully loaded.
        self.cpu_load = 0

        ## Last reported total memory (in bytes) on the deputy.
        self.phys_mem_total_bytes = 0

        ## Last reported free memory (in bytes) on the deputy.
        self.phys_mem_free_bytes = 0

        ## Last time info from te deputy was received.  Zero if no info has
        # ever been received.  Represented in microseconds since the epoch.
        self.last_update_utime = 0

        # Dictionary of commands owned by the deputy
        self._commands = {}

    def get_commands(self):
        """Retrieve a list of all commands managed by the deputy

        @return a list of SheriffDeputyCommand objects
        """
        return self._commands.values()

    def owns_command(self, cmd_object):
        """Check to see if this deputy manages the specified command

        @param cmd_object a SheriffDeputyCommand object.

        @return True if this deputy object manages \p command, False if not.
        """
        return cmd_object.sheriff_id in self._commands and \
                self._commands[cmd_object.sheriff_id] is cmd_object

    def _update_from_deputy_info(self, dep_info_msg):
        """
        @dep_info_msg: an instance of procman.deputy_info_t
        """
        status_changes = []
        for cmd_msg in dep_info_msg.cmds:
            # look up the command, or create a new one if it's not found
            if cmd_msg.sheriff_id in self._commands:
                cmd = self._commands[cmd_msg.sheriff_id]
                old_status = cmd.status()
            else:
                cmd = SheriffDeputyCommand()
                cmd.exec_str = cmd_msg.cmd.exec_str
                cmd.command_id = cmd_msg.cmd.command_id
                cmd.group = cmd_msg.cmd.group
                cmd.auto_respawn = cmd_msg.cmd.auto_respawn
                cmd.stop_signal = cmd_msg.cmd.stop_signal
                cmd.sheriff_id = cmd_msg.sheriff_id
                cmd.desired_runid = cmd_msg.actual_runid
                cmd.stop_time_allowed = cmd_msg.cmd.stop_time_allowed
                # TODO handle options
                self._add_command(cmd)
                old_status = None

            cmd._update_from_cmd_info(cmd_msg)
            new_status = cmd.status()

            if old_status != new_status:
                status_changes.append((cmd, old_status, new_status))

        updated_ids = [ cmd_msg.sheriff_id for cmd_msg in dep_info_msg.cmds ]

        can_safely_remove = [ cmd for cmd in self._commands.values() \
                if cmd.scheduled_for_removal and \
                cmd.sheriff_id not in updated_ids ]

        for toremove in can_safely_remove:
            cmd = self._commands[toremove.sheriff_id]
            old_status = cmd.status()
            status_changes.append((cmd, old_status, None))
            del self._commands[toremove.sheriff_id]

        self.last_update_utime = _now_utime()
        self.cpu_load = dep_info_msg.cpu_load
        self.phys_mem_total_bytes = dep_info_msg.phys_mem_total_bytes
        self.phys_mem_free_bytes = dep_info_msg.phys_mem_free_bytes
        return status_changes

    def _update_from_deputy_orders(self, orders_msg):
        status_changes = []
        for cmd_msg in orders_msg.cmds:
            if cmd_msg.sheriff_id in self._commands:
                cmd = self._commands[cmd_msg.sheriff_id]
                old_status = cmd.status()
            else:
                cmd = SheriffDeputyCommand()
                cmd.sheriff_id = cmd_msg.sheriff_id
                cmd.exec_str = cmd_msg.cmd.exec_str
                cmd.command_id = cmd_msg.cmd.command_id
                cmd.group = cmd_msg.cmd.group
                cmd.auto_respawn = cmd_msg.cmd.auto_respawn
                cmd.stop_signal = cmd_msg.cmd.stop_signal
                cmd.stop_time_allowed = cmd_msg.cmd.stop_time_allowed
                cmd.desired_runid = cmd_msg.desired_runid
                self._add_command(cmd)
                old_status = None
            cmd._update_from_cmd_orders(cmd_msg)
            new_status = cmd.status()
            if old_status != new_status:
                status_changes.append((cmd, old_status, new_status))
        updated_ids = set([ cmd_msg.sheriff_id for cmd_msg in orders_msg.cmds ])
        for cmd in self._commands.values():
            if cmd.sheriff_id not in updated_ids:
                old_status = cmd.status()
                cmd.scheduled_for_removal = True
                new_status = cmd.status()
                if old_status != new_status:
                    status_changes.append((cmd, old_status, new_status))
        return status_changes

    def _add_command(self, newcmd):
        assert newcmd.sheriff_id != 0
        assert isinstance(newcmd, SheriffDeputyCommand)
        self._commands[newcmd.sheriff_id] = newcmd

    def _schedule_for_removal(self, cmd):
        if not self.owns_command(cmd):
            raise KeyError("invalid command")
        old_status = cmd.status()
        cmd.scheduled_for_removal = True
        if not self.last_update_utime:
            del self._commands[cmd.sheriff_id]
            new_status = None
        else:
            new_status = cmd.status()
        return ((cmd, old_status, new_status),)

    def _make_orders_message(self, sheriff_name):
        msg = orders_t()
        msg.utime = _now_utime()
        msg.host = self.name
        msg.ncmds = len(self._commands)
        msg.sheriff_name = sheriff_name
        for cmd in self._commands.values():
            if cmd.scheduled_for_removal:
                msg.ncmds -= 1
                continue
            cmd_msg = cmd_desired_t()
            cmd_msg.cmd = cmd_t()
            cmd_msg.cmd.exec_str = cmd.exec_str
            cmd_msg.cmd.command_id = cmd.command_id
            cmd_msg.cmd.group = cmd.group
            cmd_msg.cmd.auto_respawn = cmd.auto_respawn
            cmd_msg.cmd.stop_signal = cmd.stop_signal
            cmd_msg.cmd.stop_time_allowed = cmd.stop_time_allowed
            cmd_msg.cmd.num_options = 0
            cmd_msg.cmd.option_names = []
            cmd_msg.cmd.option_values = []
            cmd_msg.sheriff_id = cmd.sheriff_id
            cmd_msg.desired_runid = cmd.desired_runid
            cmd_msg.force_quit = cmd.force_quit
            msg.cmds.append(cmd_msg)
        msg.num_options = 0
        msg.option_names = []
        msg.option_values = []
        return msg

class Sheriff(object):
    """Controls deputies and processes.

    \ingroup python_api

    The Sheriff class provides the primary interface for controlling processes
    using the Procman Python API.  It requires a GLib event loop to run.

    example usage:
    \code
    import procman

    lcm_obj = lcm.LCM()
    sheriff = procman.Sheriff(lcm_obj)

    # add commands or load a config file

    while True:
        lcm_obj.handle()
    \endcode

    ## Signals ##
    The Sheriff exposes a number of signals that you can use to
    register a callback function that gets called when a particular event
    happens.

    For example, to be notified when the status of a command changes:

    \code
    def on_command_status_changed(cmd_object, old_status, new_status):
        print("Command %s status changed from %s -> %s" % (cmd_obj.command_id,
            old_status, new_status)

    sheriff.command_status_changed.connect(on_command_status_changed)
    \endcode
    """

    def __init__ (self, lcm_obj = None):
        """Initialize a new Sheriff object.

        \param lcm_obj the LCM object to use for communication.  If None, then
        the sheriff creates a new lcm.LCM() instance.
        """
        self._lcm = lcm_obj
        if self._lcm is None:
            self._lcm = lcm.LCM()
        self._lcm.subscribe("PM_INFO", self.on_pmd_info)
        self._lcm.subscribe("PM_ORDERS", self.on_pmd_orders)
        self._deputies = {}
        self._is_observer = False
        self._name = platform.node() + ":" + str(os.getpid()) + \
                ":" + str(_now_utime())

        # publish a discovery message to query for existing deputies
        discover_msg = discovery_t()
        discover_msg.utime = _now_utime()
        discover_msg.host = ""
        discover_msg.nonce = 0
        self._lcm.publish("PM_DISCOVER", discover_msg.encode())

        # Create a worker thread for periodically publishing orders
        self._worker_thread = threading.Thread(target = self._worker_thread)
        self._exiting = False
        self._lock = threading.Lock()
        self._condvar = threading.Condition(self._lock)
        self._worker_thread.start()

        # signals
        self._to_emit = []

        ## [Signal](\ref procman.signal_slot.Signal) emitted when
        # information from a deputy is received and processed.
        # `deputy_info_received(deputy_object)`
        #
        # \param deputy_object is a SheriffDeputy corresponding to the updated deputy.
        self.deputy_info_received = Signal()

        ## [Signal](\ref procman.signal_slot.Signal) emitted when a new
        # command is added to the sheriff.
        #
        # \param deputy_object is a SheriffDeputy for the deputy that owns the
        # command.
        # \param cmd_object is a SheriffDeputyCommand for the new command.
        self.command_added = Signal()

        ## [Signal](\ref procman.signal_slot.Signal) emitted when a command
        # is removed from the sheriff.
        # `command_removed(deputy_object, cmd_object)`
        #
        # \param deputy_object is a SheriffDeputy for the deputy that owned the command.
        # \param cmd_object is a SheriffDeputyCommand for the removed command.
        self.command_removed = Signal()

        ## [Signal](\ref procman.signal_slot.Signal) emitted when the
        # status of a command changes (e.g., running, stopped, etc.).
        # `command_status_changed(cmd_object, old_status, new_status)`
        #
        # \param cmd_object is a SheriffDeputyCommand for the command.
        # \param old_status indicates the old command status.
        # \param new_status indicates the new command status.
        self.command_status_changed = Signal()

        ## [Signal](\ref procman.signal_slot.Signal) emitted when a command
        # is moved into a different group.
        #
        # \param cmd_object the command whose group changes.
        self.command_group_changed = Signal()

    def _get_or_make_deputy(self, deputy_name):
        # _lock should already be acquired
        if deputy_name not in self._deputies:
            self._deputies[deputy_name] = SheriffDeputy(deputy_name)
        return self._deputies[deputy_name]

    def _schedule_emit(self, signal, *args):
        # _lock should already be acquired
        self._to_emit.append((signal, args))
        self._condvar.notify()

    def _maybe_emit_status_change_signals(self, deputy, status_changes):
        # _lock should already be acquired
        for cmd, old_status, new_status in status_changes:
            if old_status == new_status:
                continue
            if old_status is None:
                self._schedule_emit(self.command_added, deputy, cmd)
            elif new_status is None:
                self._schedule_emit(self.command_removed, deputy, cmd)
            else:
                self._schedule_emit(self.command_status_changed, cmd, old_status, new_status)

    def _get_command_deputy(self, cmd):
        # _lock should already be acquired
        for deputy in self._deputies.values():
            if deputy.owns_command(cmd):
                return deputy
        raise KeyError()

    def on_pmd_info(self, _, data):
        try:
            info_msg = deputy_info_t.decode(data)
        except ValueError:
            print("invalid deputy_info_t message")
            return

        now = _now_utime()
        if(now - info_msg.utime) * 1e-6 > 30 and not self.is_observer():
            # ignore old messages
            return

        _dbg("received pmd info from [%s]" % info_msg.host)

        with self._lock:
            deputy = self._get_or_make_deputy(info_msg.host)

            # If this is the first time we've heard from the deputy and we already
            # have a desired state for the deputy, then try to reconcile the stored
            # desired state with the deputy's reported state.
            if not deputy.last_update_utime and deputy._commands:
                _dbg("First update from [%s]" % info_msg.host)
                # for each command we already have lined up in the deputy, check to
                # see if the deputy is already managing that command.  If the
                # deputy is already managing that command, then reassign the
                # internal ID for the command to match what the deputy is
                # reporting.
                for cmd in deputy._commands.values():
                    for cmd_msg in info_msg.cmds:
                        matched = cmd.exec_str == cmd_msg.cmd.exec_str and \
                                  cmd.command_id == cmd_msg.cmd.command_id and \
                                  cmd.group == cmd_msg.cmd.group and \
                                  cmd.auto_respawn == cmd_msg.cmd.auto_respawn
                        if not matched:
                            continue
                        collision = False
                        for other_deputy in self._deputies.values():
                            if other_deputy._commands.get(cmd_msg.sheriff_id, cmd) \
                                    is not cmd:
                                collision = True
                                break
                        if collision:
                            continue
                        # found a command managed by the deputy that looks
                        # exactly like the command the sheriff wants the
                        # deputy to run.  Reassign the sheriff ID to match
                        # what the deputy is reporting.
                        del deputy._commands[cmd.sheriff_id]
                        cmd.sheriff_id = cmd_msg.sheriff_id
                        deputy._commands[cmd.sheriff_id] = cmd
                        _dbg("Merging command [%s] with command reported by deputy" \
                                % cmd.command_id)
                        break

            status_changes = deputy._update_from_deputy_info(info_msg)

            self._schedule_emit(self.deputy_info_received, deputy)
            self._maybe_emit_status_change_signals(deputy, status_changes)

    def on_pmd_orders(self, _, data):
        if not self._is_observer:
            return
        orders_msg = orders_t.decode(data)
        with self._lock:
            deputy = self._get_or_make_deputy(orders_msg.host)
            status_changes = deputy._update_from_deputy_orders(orders_msg)
            self._maybe_emit_status_change_signals(deputy, status_changes)

    def __get_free_sheriff_id(self):
        id_to_try = random.randint(0, (1 << 31) - 1)

        for _ in range(1 << 16):
            collision = False
            for deputy in self._deputies.values():
                if id_to_try in deputy._commands:
                    collision = True
                    break

            if not collision:
                result = id_to_try

            id_to_try = random.randint(0, (1 << 31) - 1)

            if not collision:
                return result
        raise RuntimeError("no available sheriff id")

    def shutdown(self):
        # signal worker thread
        with self._lock:
            self._exiting = True
            self._condvar.notify()

        # wait for worker thread to exit
        self._worker_thread.join()

    def get_name(self):
        """Retrieve the sheriff name, as self reported to deputies.
        The sheriff name is automatically set to a combination of the
        hostname, current PID, and the time the sheriff was created.
        """
        with self._lock:
            return self._name

    def _send_orders(self):
        # self._lock should already be acquired
        if self._is_observer:
            raise ValueError("Can't send orders in Observer mode")
        for deputy in self._deputies.values():
            # only send orders to a deputy if we've heard from it.
            if deputy.last_update_utime > 0:
                msg = deputy._make_orders_message(self._name)
                self._lcm.publish("PM_ORDERS", msg.encode())

    def send_orders(self):
        """Transmit orders to all deputies.  Call this method for the sheriff
        to send updated orders to its deputies.  This method is automatically
        called when you call other sheriff methods such as add_command(),
        start_command(), etc.  In general, you should only need to explicitly
        call this method for a periodic transmission to be robust against
        network failures and dropped messages.

        @note Orders will only be sent to a deputy if the sheriff has received at
        least one update from the deputy.
        """
        with self._lock:
            self._send_orders()

    def _add_command(self, spec):
        # self._lock should already be acquired
        if self._is_observer:
            raise ValueError("Can't add commands in Observer mode")

        if not spec.exec_str:
            raise ValueError("Invalid command")
        if not spec.command_id:
            raise ValueError("Invalid command id")
        if self._get_commands_by_deputy_and_id(spec.deputy_name, spec.command_id):
            _warn("Duplicate command id %s in group [%s]" % (spec.command_id, spec.group_name))
        if not spec.deputy_name:
            raise ValueError("Invalid deputy")

        dep = self._get_or_make_deputy(spec.deputy_name)
        newcmd = SheriffDeputyCommand()
        newcmd.exec_str = spec.exec_str
        newcmd.command_id = spec.command_id
        newcmd.group = spec.group_name
        newcmd.sheriff_id = self.__get_free_sheriff_id()
        newcmd.auto_respawn = spec.auto_respawn
        newcmd.stop_signal = spec.stop_signal
        newcmd.stop_time_allowed = spec.stop_time_allowed
        dep._add_command(newcmd)
        self._schedule_emit(self.command_added, dep, newcmd)
        self._send_orders()
        return newcmd

    def add_command(self, spec):
        """Add a new command.

        @param spec a SheriffCommandSpec that describes the new command to add

        @return a SheriffDeputyCommand object representing the command.
        """
        with self._lock:
            return self._add_command(spec)

    def _start_command(self, cmd):
        _dbg("start_command [%s]" % cmd.command_id)
        # self._lock should already be acquired
        if self._is_observer:
            raise ValueError("Can't modify commands in Observer mode")
        old_status = cmd.status()
        cmd._start()
        new_status = cmd.status()
        deputy = self._get_command_deputy(cmd)
        self._maybe_emit_status_change_signals(deputy,
                ((cmd, old_status, new_status),))
        self._send_orders()

    def start_command(self, cmd):
        """Sets a command's desired status to running.  If the command is not
        running, then the deputy will start it.  If the command is already
        running, then no action is taken.

        @param cmd a SheriffDeputyCommand object specifying the command to run.
        """
        with self._lock:
            self._start_command(cmd)

    def _restart_command(self, cmd):
        # self._lock should already be acquired
        if self._is_observer:
            raise ValueError("Can't modify commands in Observer mode")
        old_status = cmd.status()
        cmd._restart()
        new_status = cmd.status()
        deputy = self._get_command_deputy(cmd)
        self._maybe_emit_status_change_signals(deputy,
                ((cmd, old_status, new_status),))
        self._send_orders()

    def restart_command(self, cmd):
        """Starts a command if it's not running, or stop and then start it if it's
        already running.  If the command is not running, then the deputy will
        start it.  If the command is already running, then the deputy will
        terminate it and then start it again.

        @param cmd a SheriffDeputyCommand object specifying the command to
        restart.
        """
        with self._lock:
            self._restart_command(cmd)

    def _stop_command(self, cmd):
        # self._lock should already be acquired
        if self._is_observer:
            raise ValueError("Can't modify commands in Observer mode")
        old_status = cmd.status()
        cmd._stop()
        new_status = cmd.status()
        deputy = self._get_command_deputy(cmd)
        self._maybe_emit_status_change_signals(deputy,
                ((cmd, old_status, new_status),))
        self._send_orders()

    def stop_command(self, cmd):
        """Sets a command's desired status to stopped.  If the command is
        running, then the deputy will stop it.  If the command is not running,
        then no action is taken.

        @param cmd a SheriffDeputyCommand object specifying the command to stop.
        """
        with self._lock:
            self._stop_command(cmd)

    def set_command_exec(self, cmd, exec_str):
        """Set the executable string for a command.  Calling this will not
        terminate the command if it's already running, and the new execution
        command will not take effect until the next time the command is run by
        the deputy.

        @param cmd a SheriffDeputyCommand object.
        @param exec_str the actual command string to execute.
        """
        with self._lock:
            cmd.exec_str = exec_str

    def set_command_id(self, cmd, new_id):
        """Set the command id.

        @param cmd a SheriffDeputyCommand object.
        @param new_id the new id to identify a command with.
        """
        if not new_id.strip():
            raise ValueError("Empty command id not allowed")
        if self.get_commands_by_id(new_id):
            _warn("Duplicate command id [%s]" % new_id)
        with self._lock:
            cmd.command_id = new_id

    def set_command_group(self, cmd, group_name):
        """Set the command group.

        @param cmd a SheriffDeputyCommand object.
        @param group_name the new group name for the command.
        """
        group_name = group_name.strip("/")
        while group_name.find("//") >= 0:
            group_name = group_name.replace("//", "/")
        with self._lock:
            if self._is_observer:
                raise ValueError("Can't modify commands in Observer mode")
    #        deputy = self._get_command_deputy(cmd)
            old_group = cmd.group
            if old_group != group_name:
                cmd._set_group(group_name)
                self._schedule_emit(self.command_group_changed, cmd)

    def set_auto_respawn(self, cmd, newauto_respawn):
        """Set if a deputy should auto-respawn the command when the command
        terminates.

        @param cmd a SheriffDeputyCommand object.
        @param newauto_respawn True if the command should be automatically
        restarted.
        """
        with self._lock:
            cmd.auto_respawn = newauto_respawn

    def set_command_stop_signal(self, cmd, new_stop_signal):
        """Set the OS signal that is sent to a command when requesting it to
        stop cleanly.  If the command doesn't cleanly exit within the stop time
        allowed, then it is sent a SIGKILL."""
        with self._lock:
            cmd.stop_signal = new_stop_signal

    def set_command_stop_time_allowed(self, cmd, new_stop_time_allowed):
        """Set how much time (seconds) to wait for a command to exit cleanly when
        stopping the command, before sending it a SIGKILL.  Integer values only.
        """
        with self._lock:
            cmd.stop_time_allowed = int(new_stop_time_allowed)

    def _schedule_command_for_removal(self, cmd):
        if self._is_observer:
            raise ValueError("Can't remove commands in Observer mode")
        deputy = self._get_command_deputy(cmd)
        status_changes = deputy._schedule_for_removal(cmd)
        self._maybe_emit_status_change_signals(deputy, status_changes)
        self._send_orders()

    def schedule_command_for_removal(self, cmd):
        """Remove a command.  This starts the process of purging a command from
        the sheriff and deputies.  It is not instantaneous, because the sheriff
        needs to wait for removal confirmation from the deputy.

        @param cmd a SheriffDeputyCommand object to remove.
        """
        with self._lock:
            self._schedule_command_for_removal(cmd)

    def move_cmd_to_deputy(self, cmd, newdeputy_name):
        """Move a command from one deputy to another.  This removes the command
        from one deputy, and creates it in another.  On return, the passed in
        command object is no longer valid and should not be used.

        @param cmd a SheriffDeputyCommand object to move.  This object is invalidated by this method.
        @param newdeputy_name the name of the new deputy for the command.

        @return the newly created command
        """
        self.schedule_command_for_removal(cmd)
        spec = SheriffCommandSpec()
        spec.deputy_name = newdeputy_name
        spec.exec_str = cmd.exec_str
        spec.command_id = cmd.command_id
        spec.group_name = cmd.group
        spec.auto_respawn = cmd.auto_respawn
        spec.stop_signal = cmd.stop_signal
        spec.stop_time_allowed = cmd.stop_time_allowed
        return self.add_command(spec)

    def set_observer(self, is_observer):
        """Set the sheriff into observation mode, or remove it from observation
        mode.

        @param is_observer True if the sheriff should enter observation mode,
        False if it should leave it.
        """
        with self._lock:
            self._is_observer = is_observer

    def is_observer(self):
        """Check if the sheriff is in observer mode.

        @return True if the sheriff is in observer mode, False if not.
        """
        with self._lock:
            return self._is_observer

    def get_deputies(self):
        """Retrieve a list of known deputies.

        @return a list of SheriffDeputy objects.
        """
        with self._lock:
            return self._deputies.values()

    def find_deputy(self, name):
        """Retrieve the SheriffDeputy object by deputy name.

        @param name the name of the desired deputy.

        @return a SheriffDeputy object.
        """
        with self._lock:
            return self._deputies[name]

    def purge_useless_deputies(self):
        """Clean up the Sheriff internal state.

        This method is meant to be called when a deputy process has no more
        commands and terminates.  It purges the Sheriff's internal representation
        of deputies that don't have any commands.
        """
        with self._lock:
            for deputy_name, deputy in self._deputies.items():
                cmds = deputy._commands.values()
                if not deputy._commands or \
                        all([ cmd.scheduled_for_removal for cmd in cmds ]):
                    del self._deputies[deputy_name]

    def get_command_by_sheriff_id(self, sheriff_id):
        """Retrieve a command by its sheriff ID.

        The sheriff ID is assigned and managed by the sheriff automtically.  It
        is not the same as the user-assigned command ID.  You generally should
        not need to use this function.
        """
        with self._lock:
            for deputy in self._deputies.values():
                if sheriff_id in deputy._commands:
                    return deputy._commands[sheriff_id]
            raise KeyError("No such command")

    def _get_command_deputy(self, command):
        for deputy in self._deputies.values():
            if command.sheriff_id in deputy._commands:
                return deputy
        raise KeyError("No such command")

    def get_command_deputy(self, command):
        """Retrieve the SheriffDeputy that manages the specified command.

        @param command a SheriffDeputyCommand object

        @return a SheriffDeputy object corresponding to the deputy that manages
        the specified command.
        """
        with self._lock:
            return self._get_command_deputy(command)

    def _get_all_commands(self):
        cmds = []
        for dep in self._deputies.values():
            cmds.extend(dep._commands.values())
        return cmds

    def get_all_commands(self):
        """Retrieve all commands managed by all deputies.

        @return a list of SheriffDeputyCommand objects.
        """
        with self._lock:
            return self._get_all_commands()

    def _get_commands_by_deputy_and_id(self, deputy_name, cmd_id):
        if deputy_name not in self._deputies:
            return []
        result = []
        for cmd in self._deputies[deputy_name]._commands.values():
            if cmd.command_id == cmd_id:
                result.append(cmd)
        return result

    def get_commands_by_deputy_and_id(self, deputy_name, cmd_id):
        """Search for commands with the specified deputy name and command
        id.  This should return at most one command.

        @param deputy_name the desired deputy name
        @param cmd_id the desired command id.

        @return a list of SheriffDeputyCommand objects matching the query, or an
        empty list if none are found.
        """
        with self._lock:
            return self._get_commands_by_deputy_and_id(deputy_name, cmd_id)

    def _get_commands_by_id(self, cmd_id):
        result = []
        for deputy in self._deputies.values():
            for cmd in deputy._commands.values():
                if cmd.command_id == cmd_id:
                    result.append(cmd)
        return result

    def get_commands_by_id(self, cmd_id):
        """Retrieve all commands with the specified id.  This should only
        return one command.

        @param cmd_id the desired command id.

        @return a list of SheriffDeputyCommand objects matching the query, or an
        empty list if none are found.
        """
        with self._lock:
            return self._get_commands_by_id(cmd_id)

    def _get_commands_by_group(self, group_name):
        result = []
        group_name = group_name.strip("/")
        while group_name.find("//") >= 0:
            group_name = group_name.replace("//", "/")
        group_parts = group_name.split("/")
        for deputy in self._deputies.values():
            for cmd in deputy._commands.values():
                cmd_group_parts = cmd.group.split("/")
                if len(group_parts) <= len(cmd_group_parts) and \
                        all([ cgp == gp for cgp, gp in zip(group_parts,
                            cmd_group_parts)]):
                    result.append(cmd)
        return result

    def get_commands_by_group(self, group_name):
        """Retrieve a list of all commands in the specified group.  Use this
        method to find out what commands are in a group.  Commands in subgroups
        of the specified group are also included.

        @param group_name the name of the desired group

        @return a list of SheriffDeputyCommand objects.
        """
        with self._lock:
            return self._get_commands_by_group(group_name)

    def load_config(self, config_node, merge_with_existing):
        """
        config_node should be an instance of sheriff_config.ConfigNode
        """
        with self._lock:
            if self._is_observer:
                raise ValueError("Can't load config in Observer mode")

            current_command_strs = set()
            if merge_with_existing:
                # if merging new config with existing commands, then build an index
                # of the existing commands.
                for dep in self._deputies.values():
                    for cmd in dep._commands.values():
                        cmdstr = "%s!%s!%s!%s!%s" % (dep.name, cmd.exec_str, cmd.command_id, cmd.group, cmd.auto_respawn)
                        current_command_strs.add(cmdstr)
            else:
                # remove all current commands if we're not merging.
                for dep in self._deputies.values():
                    for cmd in dep._commands.values():
                        self._schedule_command_for_removal(cmd)

            commands_to_add = []

            def add_group_commands(group_node, name_prefix):
                for cmd_node in group_node.commands:
                    auto_respawn = cmd_node.attributes.get("auto_respawn", "").lower() in [ "true", "yes" ]
                    assert group_node.name == cmd_node.attributes["group"]

                    add_command = True

                    # if merging is enabled, then only add this command if we don't
                    # have an entry for it already.
                    if merge_with_existing:
                        cmdstr = "%s!%s!%s!%s!%s" % (cmd_node.attributes["host"], cmd_node.attributes["exec"],
                                cmd_node.attributes["nickname"], name_prefix + group_node.name, str(auto_respawn))
                        if cmdstr in current_command_strs:
                            add_command = False

                    if add_command:
                        spec = SheriffCommandSpec()
                        spec.deputy_name = cmd_node.attributes["host"]
                        spec.exec_str = cmd_node.attributes["exec"]
                        spec.command_id = cmd_node.attributes["nickname"]
                        spec.group_name = name_prefix + group_node.name
                        spec.auto_respawn = auto_respawn
                        spec.stop_signal = cmd_node.attributes["stop_signal"]
                        spec.stop_time_allowed = cmd_node.attributes["stop_time_allowed"]
                        if spec.stop_signal == 0:
                            spec.stop_signal = DEFAULT_STOP_SIGNAL
                        if spec.stop_time_allowed == 0:
                            spec.stop_time_allowed = DEFAULT_STOP_TIME_ALLOWED

                        commands_to_add.append(spec)

                for subgroup in group_node.subgroups.values():
                    if group_node.name:
                        add_group_commands(subgroup, name_prefix + group_node.name + "/")
                    else:
                        add_group_commands(subgroup, "")

            add_group_commands(config_node.root_group, "")

            for spec in commands_to_add:
                self._add_command(spec)
    #            _dbg("[%s] %s (%s) -> %s" % (newcmd.group, newcmd.exec_str, newcmd.nickname, cmd.attributes["host"]))

    def save_config(self, config_node):
        """Write the current sheriff configuration to the specified file
        object.  The current sheriff configuration consists of all commands
        managed by all deputies along with their settings.  This information is
        written out to the specified file object, which can then be loaded into
        the sheriff again at a later point in time.

        @param config_node output ConfigNode object
        """
        with self._lock:
            for deputy in self._deputies.values():
                for cmd in deputy._commands.values():
                    cmd_node = sheriff_config.CommandNode()
                    cmd_node.attributes["exec"] = cmd.exec_str
                    cmd_node.attributes["nickname"] = cmd.command_id
                    cmd_node.attributes["host"] = deputy.name
                    if cmd.auto_respawn:
                        cmd_node.attributes["auto_respawn"] = "true"

                    group = config_node.get_group(cmd.group, True)
                    group.add_command(cmd_node)

    def _worker_thread(self):
        send_interval = 1.0
        next_send = time.time() + send_interval
        to_emit = []
        while True:
            with self._lock:
                if self._exiting:
                    return

                now = time.time()

                # Calculate how long to wait on the condition variable.
                wait_time = next_send - now

                if wait_time > 0:
                    self._condvar.wait(wait_time)

                to_emit[:] = self._to_emit[:]
                del self._to_emit[:]

                now = time.time()
                if now > next_send and not self._is_observer:
                    self._send_orders()
                next_send = min(time.time() + send_interval,
                        next_send + send_interval)

            # Emit any queued up signals
            for signal, args in to_emit:
                signal(*args)
