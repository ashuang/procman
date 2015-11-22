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
from procman.sheriff import RUNNING, STOPPED_OK, STOPPED_ERROR

from procman.sheriff_config import ScriptNode, \
                                   WaitStatusActionNode, \
                                   WaitMsActionNode, \
                                   StartStopRestartActionNode, \
                                   RunScriptActionNode, \
                                   escape_str

from procman.signal_slot import Signal

def _dbg(text):
#    return
    sys.stderr.write("%s\n" % text)

class StartStopRestartAction(object):
    """Script action to start, stop, or restart a command or group.

    \ingroup python_api

    """
    def __init__(self, action_type, ident_type, ident, wait_status):
        assert action_type in ["start", "stop", "restart"]
        assert ident_type in [ "everything", "group", "cmd" ]
        self.action_type = action_type
        self.ident_type = ident_type
        self.wait_status = wait_status
        if self.ident_type == "everything":
            self.ident = None
        else:
            self.ident = ident
            assert self.ident is not None

    def toScriptNode(self):
        return StartStopRestartActionNode(self.action_type,
                self.ident_type, self.ident, self.wait_status)

    def __str__(self):
        if self.ident_type == "everything":
            ident_str = self.ident_type
        else:
            ident_str = "%s \"%s\"" % (self.ident_type, escape_str(self.ident))
        if self.wait_status is not None:
            return "%s %s wait \"%s\";" % (self.action_type,
                    ident_str, self.wait_status)
        else:
            return "%s %s;" % (self.action_type, ident_str)

class WaitMsAction(object):
    """Script action to wait a fixed number of milliseconds.

    \ingroup python_api

    """
    def __init__(self, delay_ms):
        self.delay_ms = delay_ms
        self.action_type = "wait_ms"

    def toScriptNode(self):
        return WaitMsAction(self.delay_ms)

    def __str__(self):
        return "wait ms %d;" % self.delay_ms

class WaitStatusAction(object):
    """Script action to wait for a command or group to change status.

    \ingroup python_api

    """
    def __init__(self, ident_type, ident, wait_status):
        self.ident_type = ident_type
        self.ident = ident
        self.wait_status = wait_status
        self.action_type = "wait_status"

    def toScriptNode(self):
        return WaitStatusActionNode(self.ident_type,
                self.ident, self.wait_status)

    def __str__(self):
        return "wait %s \"%s\" status \"%s\";" % \
                (self.ident_type, escape_str(self.ident), self.wait_status)

class RunScriptAction(object):
    """Script action to run a subscript.

    \ingroup python_api

    """
    def __init__(self, script_name):
        self.script_name = script_name
        self.action_type = "run_script"

    def toScriptNode(self):
        return RunScriptActionNode(self.script_name)

    def __str__(self):
        return "run_script \"%s\";" % escape_str(self.script_name)

class SheriffScript(object):
    """A simple script that can be executed by the Sheriff.

    \ingroup python_api

    """
    def __init__(self, name):
        self.name = name
        self.actions = []

    def add_action(self, action):
        self.actions.append(action)

    def toScriptNode(self):
        node = ScriptNode(self.name)
        for action in self.actions:
            node.add_action(action.toScriptNode())
        return node

    def __str__(self):
        val = "script \"%s\" {" % escape_str(self.name)
        for action in self.actions:
            val = val + "\n    " + str(action)
        val = val + "\n}\n"
        return val

    @staticmethod
    def from_script_node(node):
        script = SheriffScript(node.name)
        for action_node in node.actions:
            if action_node.action_type in [ "start", "stop", "restart" ]:
                action = StartStopRestartAction(action_node.action_type,
                        action_node.ident_type,
                        action_node.ident,
                        action_node.wait_status)
            elif action_node.action_type == "wait_ms":
                action = WaitMsAction(action_node.delay_ms)
            elif action_node.action_type == "wait_status":
                action = WaitStatusAction(action_node.ident_type,
                        action_node.ident,
                        action_node.wait_status)
            elif action_node.action_type == "run_script":
                action = RunScriptAction(action_node.script_name)
            else:
                raise ValueError("unrecognized action %s" % \
                        action_node.action_type)
            script.add_action(action)
        return script

class ScriptExecutionContext(object):
    def __init__(self, sheriff, script):
        assert(script is not None)
        self.script = script
        self.current_action = -1
        self.subscript_context = None
        self.sheriff = sheriff

    def get_next_action(self):
        if self.subscript_context:
            # if we're recursing into a called script, return its next action
            action = self.subscript_context.get_next_action()
            if action:
                return action
            else:
                # unless it's done, in which case fall through to our next
                # action
                self.subscript_context = None
        self.current_action += 1
        if self.current_action >= len(self.script.actions):
            # no more actions
            return None
        action = self.script.actions[self.current_action]

        if action.action_type == "run_script":
            subscript = self.sheriff.get_script(action.script_name)
            self.subscript_context = ScriptExecutionContext(self.sheriff,
                    subscript)
            return self.get_next_action()
        else:
            return action

class ScriptManager(object):
    def __init__(self, sheriff):
        self._sheriff = sheriff

        self._scripts = []
        self._active_script_context = None
        self._waiting_on_commands = []
        self._waiting_for_status = None
        self._last_script_action_time = None
        self._next_action_time = 0

        self._to_emit = []

        self._worker_thread = threading.Thread(target = self._worker_thread)
        self._exiting = False
        self._lock = threading.Lock()
        self._condvar = threading.Condition(self._lock)
        self._worker_thread.start()

        self._sheriff.command_status_changed.connect(self.on_command_status_changed)

        ## [Signal](\ref procman.signal_slot.Signal) emitted when a script
        # is added.
        #
        # \param script_object a [SheriffScript](\ref procman.sheriff_script.SheriffScript) object.
        self.script_added = Signal()

        ## [Signal](\ref procman.signal_slot.Signal) emitted when a script
        # is removed.
        #
        # \param script_object a [SheriffScript](\ref procman.sheriff_script.SheriffScript) object.
        self.script_removed = Signal()

        ## [Signal](\ref procman.signal_slot.Signal) emitted when a script
        # begins executing.
        # `script_started(script_object)`
        #
        # \param script_object a [SheriffScript](\ref procman.sheriff_script.SheriffScript) object.
        self.script_started = Signal()

        ## [Signal](\ref procman.signal_slot.Signal) emitted when a single
        # action in a script begins to run.  (e.g., start a command)
        # `script_action_executing(script_object, action)`
        #
        # \param script_object a [SheriffScript](\ref procman.sheriff_script.SheriffScript) object
        # \param action one of: [StartStopRestartAction](\ref procman.sheriff_script.StartStopRestartAction),
        # [WaitMsAction](\ref procman.sheriff_script.WaitMsAction),
        # [WaitStatusAction](\ref procman.sheriff_script.WaitStatusAction),
        # [RunScriptAction](\ref procman.sheriff_script.RunScriptAction)
        self.script_action_executing = Signal()

        ## [Signal](\ref procman.signal_slot.Signal) emitted when a script
        # finishes execution.
        # `script_finished(script_object)`
        #
        # \param script_object a SheriffScript object
        self.script_finished = Signal()

    def get_active_script(self):
        """Retrieve the currently executing script

        @return the SheriffScript object corresponding to the active script, or
        None if there is no active script.
        """
        with self._lock:
            if self._active_script_context:
                return self._active_script_context.script
            return None

    def _get_script(self, name):
        for script in self._scripts:
            if script.name == name:
                return script
        return None

    def get_script(self, name):
        """Look up a script by name

        @param name the name of the script

        @return a SheriffScript object, or None if no such script is found.
        """
        with self._lock:
            return self._get_script(name)

    def get_scripts(self):
        """Retrieve a list of all scripts

        @return a list of SheriffScript objects
        """
        with self._lock:
            return self._scripts

    def _add_script(self, new_script):
        for script in self._scripts:
            if script.name == new_script.name:
                raise ValueError("Script [%s] already exists", script.name)
        self._scripts.append(new_script)
        self._schedule_emit(self.script_added, new_script)

    def add_script(self, script):
        """Add a new script to the sheriff.

        @param script a SheriffScript object.
        """
        with self._lock:
            self._add_script(script)

    def _remove_script(self, script):
        if self._active_script_context is not None:
            raise RuntimeError("Script removal is not allowed while a script is running.")

        if script in self._scripts:
            self._scripts.remove(script)
            self._schedule_emit(self.script_removed, script)
        else:
            raise ValueError("Unknown script [%s]", script.name)

    def remove_script(self, script):
        """Remove a script.

        @param script the SheriffScript object to remove.
        """
        with self._lock:
            self._remove_script(script)

    def _get_action_commands(self, ident_type, ident):
        if ident_type == "cmd":
            return [ self._sheriff.get_command_by_id(ident) ]
        elif ident_type == "group":
            return self._sheriff.get_commands_by_group(ident)
        elif ident_type == "everything":
            return self._sheriff.get_all_commands()
        else:
            raise ValueError("Invalid ident_type %s" % ident_type)

    def _check_script_for_errors(self, script, path_to_root=None):
        if path_to_root is None:
            path_to_root = []
        err_msgs = []
        check_subscripts = True
        if path_to_root and script in path_to_root:
            err_msgs.append("Infinite loop: script %s eventually calls itself" % script.name)
            check_subscripts = False

        for action in script.actions:
            if action.action_type in \
                    [ "start", "stop", "restart", "wait_status" ]:
                if action.ident_type == "cmd":
                    if not self._sheriff.get_command_by_id(action.ident):
                        err_msgs.append("No such command: %s" % action.ident)
                elif action.ident_type == "group":
                    if not self._sheriff.get_commands_by_group(action.ident):
                        err_msgs.append("No such group: %s" % action.ident)
            elif action.action_type == "wait_ms":
                if action.delay_ms < 0:
                    err_msgs.append("Wait times must be nonnegative")
            elif action.action_type == "run_script":
                # action is to run another script.
                subscript = self._get_script(action.script_name)
                if subscript is None:
                    # couldn't find that script.  error out
                    err_msgs.append("Unknown script \"%s\"" % \
                            action.script_name)
                elif check_subscripts:
                    # Recursively check the caleld script for errors.
                    path = path_to_root + [script]
                    sub_messages = self._check_script_for_errors(subscript,
                            path)
                    parstr = "->".join([s.name for s in (path + [subscript])])
                    for msg in sub_messages:
                        err_msgs.append("%s - %s" % (parstr, msg))
            else:
                err_msgs.append("Unrecognized action %s" % action.action_type)
        return err_msgs

    def check_script_for_errors(self, script, path_to_root=None):
        """Check a script object for errors that would prevent its
        execution.  Possible errors include a command or group mentioned in the
        script not being found by the sheriff.

        @param script a SheriffScript object to inspect

        @return a list of error messages.  If the list is not empty, then each
        error message indicates a problem with the script.  Otherwise, the script
        can be executed.
        """
        with self._lock:
            return self._check_script_for_errors(script, path_to_root)

    def _finish_script_execution(self):
        _dbg("script finished")
        # self._lock should already be acquired
        script = self._active_script_context.script
        self._active_script_context = None
        self._waiting_on_commands = []
        self._waiting_for_status = None
        self._next_action_time = 0
        if script:
            self._schedule_emit(self.script_finished, script)

    def _check_wait_action_status(self):
        _dbg("_check_wait_action_status")
        # self._lock should already be acquired
        if not self._waiting_on_commands:
            _dbg("not waiting on command")
            return

        if self._waiting_for_status == "running":
            for cmd in self._waiting_on_commands:
                cmd_status = cmd.status()
                if not (cmd_status == RUNNING or
                        ((cmd_status in (STOPPED_OK, STOPPED_ERROR)) and
                            cmd.desired_runid == cmd.actual_runid)):
                    _dbg("cmd [%s] not ready (%s)" % (cmd.command_id, cmd_status))
                    return
        elif self._waiting_on_commands == "stopped":
            for cmd in self._waiting_on_commands:
                if cmd.status() not in (STOPPED_OK, STOPPED_ERROR):
                    return
        else:
            raise ValueError("Invalid desired status %s" % \
                    self._waiting_for_status)

        # all commands passed the status check.  schedule the next action
        self._waiting_on_commands = []
        self._waiting_for_status = None
        self._next_action_time = max(time.time(),
                self._last_script_action_time + 0.1)
        self._condvar.notify()

    def on_command_status_changed(self, cmd, old_status, new_status):
        _dbg("on_command_status_changed: [%s] : %s -> %s" % (cmd.command_id,
            old_status, new_status))
        with self._lock:
            self._check_wait_action_status()

    def _execute_next_script_action(self):
        # self._lock should already be acquired

        # make sure there's an active script
        if not self._active_script_context:
            return

        self._next_action_time = 0

        action = self._active_script_context.get_next_action()

        if action is None:
            # no more actions, script is done.
            _dbg("script done?")
            self._finish_script_execution()
            return

        _dbg("_execute_next_script_action: %s" % str(action))

        assert action.action_type != "run_script"

        self._schedule_emit(self.script_action_executing,
                self._active_script_context.script, action)

        # fixed time wait
        if action.action_type == "wait_ms":
            self._next_action_time = time.time() + action.delay_ms / 1000.
            return

        # find the commands that we're operating on
        cmds = self._get_action_commands(action.ident_type, action.ident)

        self._last_script_action_time = time.time()

        # execute an immediate action if applicable
        if action.action_type == "start":
            for cmd in cmds:
                self._sheriff.start_command(cmd)
        elif action.action_type == "stop":
            for cmd in cmds:
                self._sheriff.stop_command(cmd)
        elif action.action_type == "restart":
            for cmd in cmds:
                self._sheriff.restart_command(cmd)

        # do we need to wait for the commands to achieve a desired status?
        if action.wait_status:
            # yes
            self._waiting_on_commands = cmds
            self._waiting_for_status = action.wait_status
            self._check_wait_action_status()
        else:
            # no.  Just move on
            self._next_action_time = time.time()

    def execute_script(self, script):
        """Starts executing a script.  If another script is executing, then
        that script is aborted first.

        @param script a sheriff_script.SheriffScript object to execute
        @sa get_script()

        @return a list of error messages.  If the list is not empty, then each
        error message indicates a problem with the script.  Otherwise, the script
        has successfully started execution if the returned list is empty.
        """
        _dbg("execute_script")
        with self._lock:
            if self._active_script_context:
                self._finish_script_execution()

            errors = self._check_script_for_errors(script)
            if errors:
                return errors

            self._active_script_context = ScriptExecutionContext(self, script)

            self._next_action_time = time.time()
            self._condvar.notify()

            self._schedule_emit(self.script_started, script)

    def abort_script(self):
        """Cancels execution of the active script."""
        with self._lock:
            self._finish_script_execution()

    def load_config(self, config_node):
        with self._lock:
            # always replace scripts.
            for script in self._scripts[:]:
                self._remove_script(script)

            for script_node in config_node.scripts.values():
                self._add_script(SheriffScript.from_script_node(script_node))

    def save_config(self, config_node):
        with self._lock:
            for script in self._scripts:
                config_node.add_script(script.toScriptNode())
#        file_obj.write(str(config_node))

    def shutdown(self):
        # signal worker thread
        with self._lock:
            self._exiting = True
            self._condvar.notify()

        # wait for worker thread to exit
        self._worker_thread.join()

    def _worker_thread(self):
        to_emit = []
        while True:
            with self._lock:
                if self._exiting:
                    return

                # Calculate how long to wait on the condition variable.
                if self._next_action_time:
                    wait_time = self._next_action_time - time.time()
                    self._condvar.wait(wait_time)
                else:
                    self._condvar.wait()

                # Is a script action ready for execution?
                if self._next_action_time and \
                        time.time() > self._next_action_time:
                    self._execute_next_script_action()

                to_emit[:] = self._to_emit[:]
                del self._to_emit[:]

            # Emit any queued up signals
            for signal, args in to_emit:
                signal(*args)

    def _schedule_emit(self, signal, *args):
        # _lock should already be acquired
        self._to_emit.append((signal, args))
        self._condvar.notify()

