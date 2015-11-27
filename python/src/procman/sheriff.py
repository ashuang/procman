"""@package sheriff

\defgroup python_api Python API
"""
import os
import platform
import sys
import time
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

def _dbg(text):
    return
#    sys.stderr.write("%s\n" % text)

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
    __slots__ = [ "deputy_id", "exec_str", "command_id", "group_name",
            "auto_respawn", "stop_signal", "stop_time_allowed" ]

    def __init__(self):
        """Initializer.
        """

        ## the name of the deputy that will manage this command.
        self.deputy_id = ""

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
    def __init__(self, lock):
        self._lock = lock

        # Process ID of the command as reported by the deputy.
        # Has value -1 if the PID is unknown.
        # Has value 0 if the process is not running (i.e., stopped)
        # Has a positive value if the process is running.
        self._pid = -1
        # If the command is stopped, its exit code.
        self._exit_code = 0
        self._cpu_usage = 0
        self._mem_vsize_bytes = 0
        # Resident memory used by the command.
        self._mem_rss_bytes = 0
        self._exec_str = ""
        self._command_id = ""
        self._group = ""
        self._desired_runid = 0
        self._force_quit = False
        # True if the command is being removed.
        self._scheduled_for_removal = False
        # Deputy-reported variable identifying the run ID of the command.
        # This variable changes each time the command is started.
        self._actual_runid = 0
        self._auto_respawn = False
        self._stop_signal = DEFAULT_STOP_SIGNAL
        self._stop_time_allowed = DEFAULT_STOP_TIME_ALLOWED
        # True if this data structure has been updated with information
        # received from a deputy, False if not.
        self._updated_from_info = False

    @property
    def cpu_usage(self):
        """Command CPU usage, as reported by the deputy.  Ranges from [0, 1]"""
        with self._lock:
            return self._cpu_usage

    @property
    def mem_vsize_bytes(self):
        """Virtual memory used by the command."""
        with self._lock:
            return self._mem_vsize_bytes

    @property
    def mem_rss_bytes(self):
        """Physical memory used by the command."""
        with self._lock:
            return self._mem_rss_bytes

    @property
    def exec_str(self):
        """The executable string for the command."""
        with self._lock:
            return self._exec_str

    @property
    def command_id(self):
        """A user-assigned string that uniquely idenitifies the comand."""
        with self._lock:
            return self._command_id

    @property
    def group(self):
        """A user-assigned group name for the command, possibly empty."""
        with self._lock:
            return self._group

    @property
    def auto_respawn(self):
        """True if the deputy should automatically restart the command when it
        exits.  Auto respawn only happens if the command is set to running.
        """
        with self._lock:
            return self._auto_respawn

    @property
    def stop_signal(self):
        """When stopping the command, which OS signal to send the command to
        request that it cleanly exit.  This usually defaults to SIGINT."""
        with self._lock:
            return self._stop_signal

    @property
    def stop_time_allowed(self):
        """When stopping the command, how much time to wait in between sending
        it stop_signal and a SIGKILL."""
        with self._lock:
            return self._stop_time_allowed

    def _update_from_cmd_info(self, cmd_msg):
        self._pid = cmd_msg.pid
        self._actual_runid = cmd_msg.actual_runid
        self._exit_code = cmd_msg.exit_code
        self._cpu_usage = cmd_msg.cpu_usage
        self._mem_vsize_bytes = cmd_msg.mem_vsize_bytes
        self._mem_rss_bytes = cmd_msg.mem_rss_bytes
        self._updated_from_info = True

        # if the command has run to completion and we don't need it to respawn,
        # then prevent it from respawning if the deputy restarts
        if self._pid == 0 and \
            self._actual_runid == self._desired_runid and \
            not self._auto_respawn and \
            not self._force_quit:
                self._force_quit = True

    def _update_from_cmd_orders(self, cmd_msg):
        # Expects that self._lock is already acquired
        self._exec_str = cmd_msg.cmd.exec_str
        self._command_id = cmd_msg.cmd.command_id
        self._group = cmd_msg.cmd.group
        self._desired_runid = cmd_msg.desired_runid
        self._force_quit = cmd_msg.force_quit
        self._stop_signal = cmd_msg.cmd.stop_signal
        self._stop_time_allowed = cmd_msg.cmd.stop_time_allowed

    def _start(self):
        # if the command is already running, then ignore
        if self._pid > 0 and not self._force_quit:
            return

        self._desired_runid += 1
        if self._desired_runid > (2 << 31):
            self._desired_runid = 1
        self._force_quit = False

    def _restart(self):
        self._desired_runid += 1
        if self._desired_runid > (2 << 31):
            self._desired_runid = 1
        self._force_quit = False

    def _stop(self):
        self._force_quit = True

    def _status(self):
        if not self._updated_from_info:
            return UNKNOWN
        if self._desired_runid != self._actual_runid and not self._force_quit:
            if self._pid == 0:
                return TRYING_TO_START
            else:
                return RESTARTING
        elif self._desired_runid == self._actual_runid:
            if self._pid > 0:
                if not self._force_quit and not self._scheduled_for_removal:
                    return RUNNING
                else:
                    return TRYING_TO_STOP
            else:
                if self._scheduled_for_removal:
                    return REMOVING
                elif self._exit_code == 0:
                    return STOPPED_OK
                elif self._force_quit and \
                     os.WIFSIGNALED(self._exit_code) and \
                     os.WTERMSIG(self._exit_code) in [ signal.SIGTERM,
                             signal.SIGINT, signal.SIGKILL ]:
                         return STOPPED_OK
                else:
                    return STOPPED_ERROR
        else:
            return UNKNOWN

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
        with self._lock:
            return self._status()

class SheriffDeputy(object):
    """%Sheriff view of a deputy

    \ingroup python_api
    """
    def __init__(self, deputy_id, lock):
        """Initializes a deputy with the specified id.  Do not use this
        constructor directly.  Instead, get a list of deputies from the
        Sheriff.
        """
        self._deputy_id = deputy_id
        self._cpu_load = 0
        self._phys_mem_total_bytes = 0
        self._phys_mem_free_bytes = 0
        self._last_update_utime = 0
        self._lock = lock

        # Dictionary of commands owned by the deputy
        self._commands = {}

    def get_commands(self):
        """Retrieve a list of all commands managed by the deputy

        @return a list of SheriffDeputyCommand objects
        """
        with self._lock:
            return self._commands.values()

    @property
    def deputy_id(self):
        """Deputy id"""
        with self._lock:
            return self._deputy_id

    @property
    def cpu_load(self):
        """Last reported CPU load on the deputy.  Ranges from [0, 1], where 0
        is no load and 1 is fully loaded.
        """
        with self._lock:
            return self._cpu_load

    @property
    def phys_mem_total_bytes(self):
        """Last reported total memory (in bytes) on the deputy."""
        with self._lock:
            return self._phys_mem_total_bytes

    @property
    def phys_mem_free_bytes(self):
        """Last reported free memory (in bytes) on the deputy."""
        with self._lock:
            return self._phys_mem_free_bytes

    @property
    def last_update_utime(self):
        """Last time info from te deputy was received.  Zero if no info has
        ever been received.  Represented in microseconds since the epoch.
        """
        with self._lock:
            return self._last_update_utime

    def _owns_command(self, cmd_obj):
        return cmd_obj._command_id in self._commands and \
                self._commands[cmd_obj._command_id] is cmd_obj

    def _update_from_deputy_info(self, dep_info_msg):
        # Expects that self._lock is already acquired

        status_changes = []
        for cmd_msg in dep_info_msg.cmds:
            # look up the command, or create a new one if it's not found
            if cmd_msg.cmd.command_id in self._commands:
                cmd = self._commands[cmd_msg.cmd.command_id]
                old_status = cmd._status()
            else:
                cmd = SheriffDeputyCommand(self._lock)
                cmd._exec_str = cmd_msg.cmd.exec_str
                cmd._command_id = cmd_msg.cmd.command_id
                cmd._group = cmd_msg.cmd.group
                cmd._auto_respawn = cmd_msg.cmd.auto_respawn
                cmd._desired_runid = cmd_msg.actual_runid
                cmd._stop_signal = cmd_msg.cmd.stop_signal
                cmd._stop_time_allowed = cmd_msg.cmd.stop_time_allowed
                self._add_command(cmd)
                old_status = None

            cmd._update_from_cmd_info(cmd_msg)
            new_status = cmd._status()

            if old_status != new_status:
                status_changes.append((cmd, old_status, new_status))

        updated_ids = [ cmd_msg.cmd.command_id for cmd_msg in dep_info_msg.cmds ]

        can_safely_remove = [ cmd for cmd in self._commands.values() \
                if cmd._scheduled_for_removal and \
                cmd._command_id not in updated_ids ]

        for toremove in can_safely_remove:
            cmd = self._commands[toremove._command_id]
            old_status = cmd._status()
            status_changes.append((cmd, old_status, None))
            del self._commands[toremove._command_id]

        self._last_update_utime = _now_utime()
        self._cpu_load = dep_info_msg.cpu_load
        self._phys_mem_total_bytes = dep_info_msg.phys_mem_total_bytes
        self._phys_mem_free_bytes = dep_info_msg.phys_mem_free_bytes
        return status_changes

    def _update_from_deputy_orders(self, orders_msg):
        status_changes = []
        for cmd_msg in orders_msg.cmds:
            if cmd_msg.cmd.command_id in self._commands:
                cmd = self._commands[cmd_msg.cmd.command_id]
                old_status = cmd._status()
            else:
                cmd = SheriffDeputyCommand(self._lock)
                cmd._exec_str = cmd_msg.cmd.exec_str
                cmd._command_id = cmd_msg.cmd.command_id
                cmd._group = cmd_msg.cmd.group
                cmd._auto_respawn = cmd_msg.cmd.auto_respawn
                cmd._stop_signal = cmd_msg.cmd.stop_signal
                cmd._stop_time_allowed = cmd_msg.cmd.stop_time_allowed
                cmd._desired_runid = cmd_msg.desired_runid
                self._add_command(cmd)
                old_status = None
            cmd._update_from_cmd_orders(cmd_msg)
            new_status = cmd._status()
            if old_status != new_status:
                status_changes.append((cmd, old_status, new_status))
        updated_ids = set([ cmd_msg.cmd.command_id for cmd_msg in orders_msg.cmds ])
        for cmd in self._commands.values():
            if cmd._command_id not in updated_ids:
                old_status = cmd._status()
                cmd._scheduled_for_removal = True
                new_status = cmd._status()
                if old_status != new_status:
                    status_changes.append((cmd, old_status, new_status))
        return status_changes

    def _add_command(self, newcmd):
        assert isinstance(newcmd, SheriffDeputyCommand)
        self._commands[newcmd._command_id] = newcmd

    def _schedule_for_removal(self, cmd):
        if not self._owns_command(cmd):
            raise KeyError("invalid command")
        old_status = cmd._status()
        cmd._scheduled_for_removal = True
        if not self._last_update_utime:
            del self._commands[cmd._command_id]
            new_status = None
        else:
            new_status = cmd._status()
        return ((cmd, old_status, new_status),)

    def _make_orders_message(self, sheriff_id):
        msg = orders_t()
        msg.utime = _now_utime()
        msg.deputy_id = self._deputy_id
        msg.ncmds = len(self._commands)
        msg.sheriff_id = sheriff_id
        for cmd in self._commands.values():
            if cmd._scheduled_for_removal:
                msg.ncmds -= 1
                continue
            cmd_msg = cmd_desired_t()
            cmd_msg.cmd = cmd_t()
            cmd_msg.cmd.exec_str = cmd._exec_str
            cmd_msg.cmd.command_id = cmd._command_id
            cmd_msg.cmd.group = cmd._group
            cmd_msg.cmd.auto_respawn = cmd._auto_respawn
            cmd_msg.cmd.stop_signal = cmd._stop_signal
            cmd_msg.cmd.stop_time_allowed = cmd._stop_time_allowed
            cmd_msg.desired_runid = cmd._desired_runid
            cmd_msg.force_quit = cmd._force_quit
            msg.cmds.append(cmd_msg)
        return msg

class SheriffListener(object):
    """Inherit from this class to receive notifications of Sheriff activity.
    """
    def deputy_info_received(self, deputy_obj):
        """Called when information from a deputy is received and processed.

        \param deputy_obj is a SheriffDeputy corresponding to the updated deputy.
        """
        return

    def command_added(self, deputy_obj, cmd_obj):
        """Called when a new command is added to the sheriff.

        \param deputy_obj is a SheriffDeputy for the deputy that owns the
        command.
        \param cmd_obj is a SheriffDeputyCommand for the new command.
        """
        return

    def command_removed(self, deputy_obj, cmd_obj):
        """Called when a command is removed from the sheriff.

        \param deputy_obj is a SheriffDeputy for the deputy that owned the command.
        \param cmd_obj is a SheriffDeputyCommand for the removed command.
        """
        return

    def command_status_changed(self, cmd_obj, old_status, new_status):
        """Called when the status of a command changes (e.g., running, stopped,
        etc.).

        \param cmd_obj is a SheriffDeputyCommand for the command.
        \param old_status indicates the old command status.
        \param new_status indicates the new command status.
        """
        return

    def command_group_changed(self, cmd_obj):
        """Called when a command is moved into a different group.

        \param cmd_obj the command whose group changes.
        """
        return

    def sheriff_conflict_detected(self, other_sheriff_id):
        """Called when a conflict with another sheriff is detected.

        It is up to the user to determine how to resolve the conflict (e.g.,
        demote a sheriff to an observer)

        \param other_sheriff_id the id of the other sheriff
        """
        return

    def observer_status_changed(self, is_observer):
        """Called when the observer status of the sheriff changes.

        \param is_observer True if the sheriff is now an observer.
        """
        return

class Sheriff(object):
    """Controls deputies and processes.

    \ingroup python_api

    The Sheriff class provides the primary interface for controlling processes
    using the Procman Python API.

    example usage:
    \code
    import procman

    sheriff = procman.Sheriff(lcm_obj)

    # add commands or load a config file

    while True:
        lcm_obj.handle()
    \endcode

    ## SheriffListener ##
    To be notified of when a command is added, started, stopped, etc., create a
    subclass of procman.SheriffListener and attach it to the sheriff via
    add_listener().
    """

    def __init__ (self, lcm_obj = None):
        """Initialize a new Sheriff object.

        \param lcm_obj the LCM object to use for communication.  If None, then
        the sheriff creates a new lcm.LCM() instance and spawns a thread that
        endlessly calls LCM.handle().
        """
        self._lcm = lcm_obj
        self._lcm_thread = None
        if self._lcm is None:
            self._lcm = lcm.LCM()
            self._lcm_thread_obj = threading.Thread(target = self._lcm_thread)
        self._lcm.subscribe("PM_INFO", self.on_pmd_info)
        self._lcm.subscribe("PM_ORDERS", self.on_pmd_orders)
        self._deputies = {}
        self._is_observer = False
        self._id = platform.node() + ":" + str(os.getpid()) + \
                ":" + str(_now_utime())

        # publish a discovery message to query for existing deputies
        discover_msg = discovery_t()
        discover_msg.utime = _now_utime()
        discover_msg.transmitter_id = self._id
        discover_msg.nonce = 0
        self._lcm.publish("PM_DISCOVER", discover_msg.encode())

        # Create a worker thread for periodically publishing orders
        self._worker_thread_obj = threading.Thread(target = self._worker_thread)
        self._exiting = False
        self._lock = threading.Lock()
        self._condvar = threading.Condition(self._lock)
        self._worker_thread_obj.start()

        self._listeners = []
        self._queued_events = []

    def _get_or_make_deputy(self, deputy_id):
        # _lock should already be acquired
        if deputy_id not in self._deputies:
            self._deputies[deputy_id] = SheriffDeputy(deputy_id, self._lock)
        return self._deputies[deputy_id]

    def __deputy_info_received(self, deputy_obj):
        self._queued_events.append(lambda listener:
                listener.deputy_info_received(deputy_obj))
        self._condvar.notify()

    def __command_added(self, deputy_obj, cmd_obj):
        self._queued_events.append(lambda listener:
                listener.command_added(deputy_obj, cmd_obj))
        self._condvar.notify()

    def __command_removed(self, deputy_obj, cmd_obj):
        self._queued_events.append(lambda listener:
                listener.command_removed(deputy_obj, cmd_obj))
        self._condvar.notify()

    def __command_status_changed(self, cmd_obj, old_status, new_status):
        self._queued_events.append(lambda listener:
                listener.command_status_changed(cmd_obj,
                    old_status, new_status))
        self._condvar.notify()

    def __command_group_changed(self, cmd_obj):
        self._queued_events.append(lambda listener:
                listener.command_group_changed(cmd_obj))
        self._condvar.notify()

    def __sheriff_conflict_detected(self, other_sheriff_id):
        self._queued_events.append(lambda listener:
                listener.sheriff_conflict_detected(other_sheriff_id))
        self._condvar.notify()

    def __observer_status_changed(self, is_observer):
        self._queued_events.append(lambda listener:
                listener.observer_status_changed(is_observer))
        self._condvar.notify()

    def _maybe_emit_status_change_signals(self, deputy, status_changes):
        # _lock should already be acquired
        for cmd, old_status, new_status in status_changes:
            if old_status == new_status:
                continue
            if old_status is None:
                self.__command_added(deputy, cmd)
            elif new_status is None:
                self.__command_removed(deputy, cmd)
            else:
                self.__command_status_changed(cmd, old_status, new_status)

    def _get_command_deputy(self, cmd):
        # _lock should already be acquired
        for deputy in self._deputies.values():
            if deputy._owns_command(cmd):
                return deputy
        raise KeyError()

    def add_listener(self, sheriff_listener):
        """Adds a listener that gets notified of certain Sheriff activity.

        @param sheriff_listener a SheriffListener object.
        """
        with self._lock:
            self._listeners.append(sheriff_listener)

    def remove_listener(self, sheriff_listener):
        with self._lock:
            self._listeners.remove(sheriff_listener)

    def on_pmd_info(self, _, data):
        try:
            info_msg = deputy_info_t.decode(data)
        except ValueError:
            _warn("invalid deputy_info_t message")
            return

        now = _now_utime()
        if(now - info_msg.utime) * 1e-6 > 30 and not self.is_observer():
            # ignore old messages
            return

#        _dbg("received pmd info from [%s]" % info_msg.deputy_id)

        with self._lock:
            deputy = self._get_or_make_deputy(info_msg.deputy_id)

            # Check if this is the first time we've heard from the deputy and
            # we already have a desired state for the deputy.
            if not deputy._last_update_utime and deputy._commands:
                _dbg("First update from [%s]" % info_msg.deputy_id)

            status_changes = deputy._update_from_deputy_info(info_msg)

            self.__deputy_info_received(deputy)
            self._maybe_emit_status_change_signals(deputy, status_changes)

    def on_pmd_orders(self, _, data):
        try:
            orders_msg = orders_t.decode(data)
        except ValueError:
            _warn("Invalid orders_t message")
            return

        with self._lock:
            if self._is_observer:
                deputy = self._get_or_make_deputy(orders_msg.deputy_id)
                status_changes = deputy._update_from_deputy_orders(orders_msg)
                self._maybe_emit_status_change_signals(deputy, status_changes)
            elif self._id != orders_msg.sheriff_id:
                self.__sheriff_conflict_detected(orders_msg.sheriff_id)

    def shutdown(self):
        # signal worker thread
        with self._lock:
            self._exiting = True
            self._condvar.notify()

        # wait for worker thread to exit
        self._worker_thread_obj.join()

    def get_id(self):
        """Retrieve the sheriff id.
        The sheriff id is designed to be globally unique, and used to detect
        conflicting sheriffs.
        """
        with self._lock:
            return self._id

    def _send_orders(self):
        # self._lock should already be acquired
        if self._is_observer:
            raise ValueError("Can't send orders in Observer mode")
        for deputy in self._deputies.values():
            # only send orders to a deputy if we've heard from it.
            if deputy._last_update_utime > 0:
                msg = deputy._make_orders_message(self._id)
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
        if self._get_command_by_id(spec.command_id):
            raise ValueError("Duplicate command id %s" % spec.command_id)
        if not spec.deputy_id:
            raise ValueError("Invalid deputy")

        dep = self._get_or_make_deputy(spec.deputy_id)
        newcmd = SheriffDeputyCommand(self._lock)
        newcmd._exec_str = spec.exec_str
        newcmd._command_id = spec.command_id
        newcmd._group = spec.group_name
        newcmd._auto_respawn = spec.auto_respawn
        newcmd._stop_signal = spec.stop_signal
        newcmd._stop_time_allowed = spec.stop_time_allowed
        dep._add_command(newcmd)
        self.__command_added(dep, newcmd)
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
        _dbg("start_command [%s]" % cmd._command_id)
        # self._lock should already be acquired
        if self._is_observer:
            raise ValueError("Can't modify commands in Observer mode")
        old_status = cmd._status()
        cmd._start()
        new_status = cmd._status()
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
        old_status = cmd._status()
        cmd._restart()
        new_status = cmd._status()
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
        old_status = cmd._status()
        cmd._stop()
        new_status = cmd._status()
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
            cmd._exec_str = exec_str

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
            old_group = cmd._group
            if old_group != group_name:
                cmd._group = group_name
                self.__command_group_changed(cmd)

    def set_auto_respawn(self, cmd, newauto_respawn):
        """Set if a deputy should auto-respawn the command when the command
        terminates.

        @param cmd a SheriffDeputyCommand object.
        @param newauto_respawn True if the command should be automatically
        restarted.
        """
        with self._lock:
            cmd._auto_respawn = newauto_respawn

    def set_command_stop_signal(self, cmd, new_stop_signal):
        """Set the OS signal that is sent to a command when requesting it to
        stop cleanly.  If the command doesn't cleanly exit within the stop time
        allowed, then it is sent a SIGKILL."""
        with self._lock:
            cmd._stop_signal = new_stop_signal

    def set_command_stop_time_allowed(self, cmd, new_stop_time_allowed):
        """Set how much time (seconds) to wait for a command to exit cleanly when
        stopping the command, before sending it a SIGKILL.  Integer values only.
        """
        with self._lock:
            cmd._stop_time_allowed = int(new_stop_time_allowed)

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

    def set_observer(self, is_observer):
        """Set the sheriff into observation mode, or remove it from observation
        mode.

        @param is_observer True if the sheriff should enter observation mode,
        False if it should leave it.
        """
        with self._lock:
            if self._is_observer == is_observer:
                return

            self._is_observer = is_observer
            self.__observer_status_changed(is_observer)

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

    def find_deputy(self, deputy_id):
        """Retrieve the SheriffDeputy object by deputy id.

        @param deputy_id the id of the desired deputy.

        @return a SheriffDeputy object.
        """
        with self._lock:
            return self._deputies[deputy_id]

    def purge_useless_deputies(self):
        """Clean up the Sheriff internal state.

        This method is meant to be called when a deputy process has no more
        commands and terminates.  It purges the Sheriff's internal representation
        of deputies that don't have any commands.
        """
        with self._lock:
            for deputy_id, deputy in self._deputies.items():
                cmds = deputy._commands.values()
                if not deputy._commands or \
                        all([ cmd._scheduled_for_removal for cmd in cmds ]):
                    del self._deputies[deputy_id]

    def _get_command_deputy(self, command):
        for deputy in self._deputies.values():
            if command._command_id in deputy._commands:
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

    def _get_command_by_id(self, cmd_id):
        for deputy in self._deputies.values():
            if cmd_id in deputy._commands:
                return deputy._commands[cmd_id]
        return None

    def get_command_by_id(self, cmd_id):
        """Retrieve the command with the specified id.

        @param cmd_id the desired command id.

        @return a SheriffDeputyCommand object matching the query, or None.
        """
        with self._lock:
            return self._get_command_by_id(cmd_id)

    def _get_commands_by_group(self, group_name):
        result = []
        group_name = group_name.strip("/")
        while group_name.find("//") >= 0:
            group_name = group_name.replace("//", "/")
        group_parts = group_name.split("/")
        for deputy in self._deputies.values():
            for cmd in deputy._commands.values():
                cmd_group_parts = cmd._group.split("/")
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

    @staticmethod
    def _group_node_to_command_specs(group_node, name_prefix):
        result = []
        for cmd_node in group_node.commands:
            auto_respawn_val = cmd_node.attributes.get("auto_respawn", "").lower()
            auto_respawn = auto_respawn_val in [ "true", "yes" ]
            assert group_node.name == cmd_node.attributes["group"]

            spec = SheriffCommandSpec()
            spec.deputy_id = cmd_node.attributes["deputy"]
            spec.exec_str = cmd_node.attributes["exec"]
            spec.command_id = cmd_node.attributes["command_id"]
            spec.group_name = name_prefix + group_node.name
            spec.auto_respawn = auto_respawn
            spec.stop_signal = cmd_node.attributes["stop_signal"]
            spec.stop_time_allowed = cmd_node.attributes["stop_time_allowed"]
            if spec.stop_signal == 0:
                spec.stop_signal = DEFAULT_STOP_SIGNAL
            if spec.stop_time_allowed == 0:
                spec.stop_time_allowed = DEFAULT_STOP_TIME_ALLOWED

            result.append(spec)

        for subgroup in group_node.subgroups.values():
            if group_node.name:
                result.extend(Sheriff._group_node_to_command_specs(subgroup,
                    name_prefix + group_node.name + "/"))
            else:
                result.extend(Sheriff._group_node_to_command_specs(subgroup,
                    ""))
        return result

    def load_config(self, config_node):
        """
        config_node should be an instance of sheriff_config.ConfigNode
        """
        with self._lock:
            if self._is_observer:
                raise ValueError("Can't load config in Observer mode")

            if self._get_all_commands():
                raise RuntimeError("Remove all commands before loading a config file")

            # Recursively create a SheriffCommandSpec for all command nodes
            # starting with the root group node.
            commands_to_add = Sheriff._group_node_to_command_specs(
                    config_node.root_group, "")

            for spec in commands_to_add:
                self._add_command(spec)
#            _dbg("[%s] %s (%s) -> %s" % (newcmd.group, newcmd.exec_str,
#                  newcmd.command_id, cmd.attributes["deputy"]))

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
                    cmd_node.attributes["exec"] = cmd._exec_str
                    cmd_node.attributes["command_id"] = cmd._command_id
                    cmd_node.attributes["deputy"] = deputy._deputy_id
                    if cmd._auto_respawn:
                        cmd_node.attributes["auto_respawn"] = "true"

                    group = config_node.get_group(cmd._group, True)
                    group.add_command(cmd_node)

    def _lcm_thread(self):
        while not self._exiting:
            self._lcm.handle_timeout(200)

    def _worker_thread(self):
        send_interval = 1.0
        next_send = time.time() + send_interval
        to_call = []
        while True:
            with self._lock:
                if self._exiting:
                    return

                now = time.time()

                # Calculate how long to wait on the condition variable.
                wait_time = next_send - now

                if wait_time > 0:
                    self._condvar.wait(wait_time)

                # Queue up any listener notifications to invoke afer releasing the lock
                for event in self._queued_events:
                    for listener in self._listeners:
                        to_call.append((event, listener))
                del self._queued_events[:]

                now = time.time()
                if now > next_send and not self._is_observer:
                    self._send_orders()
                next_send = min(time.time() + send_interval,
                        next_send + send_interval)

            # Emit any queued up signals
            for func, listener in to_call:
                func(listener)
            del to_call[:]
