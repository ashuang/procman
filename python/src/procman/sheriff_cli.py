import getopt
import os
import signal
import subprocess
import sys
import time

import lcm
from procman_lcm.cmd_t import cmd_t
from procman_lcm.deputy_info_t import deputy_info_t
from procman_lcm.orders_t import orders_t
from procman_lcm.cmd_desired_t import cmd_desired_t
from procman_lcm.cmd_status_t import cmd_status_t
from procman_lcm.discovery_t import discovery_t

from procman.sheriff_script import ScriptManager, ScriptListener
from procman.sheriff import Sheriff
import procman.sheriff as sheriff
import procman.sheriff_config as sheriff_config

try:
    from procman.build_prefix import BUILD_PREFIX
except ImportError:
    BUILD_PREFIX = None

def find_procman_deputy_cmd():
    search_path = []
    if BUILD_PREFIX is not None:
        search_path.append("%s/bin" % BUILD_PREFIX)
    search_path.extend(os.getenv("PATH").split(":"))
    for dirname in search_path:
        fname = "%s/procman-deputy" % dirname
        if os.path.exists(fname) and os.path.isfile(fname):
            return fname
    return None

class SheriffHeadless(ScriptListener):
    def __init__(self, lcm_obj, config, spawn_deputy, script_name, script_done_action):
        self.sheriff = Sheriff(lcm_obj)
        self.script_manager = ScriptManager(self.sheriff)
        self.spawn_deputy = spawn_deputy
        self.spawned_deputy = None
        self.config = config
        self.script_name = script_name
        self.script = None
        self.mainloop = None
        self.lcm_obj = lcm_obj
        self._should_exit = False
        if script_done_action is None:
            self.script_done_action = "exit"
        else:
            self.script_done_action = script_done_action

    def _shutdown(self):
        if self.spawned_deputy:
            print("Terminating local deputy..")
            try:
                self.spawned_deputy.terminate()
            except AttributeError:
                os.kill(self.spawned_deputy.pid, signal.SIGTERM)
                self.spawned_deputy.wait()
        self.spawned_deputy = None
        self.sheriff.shutdown()
        self.script_manager.shutdown()

    def _start_script(self):
        if not self.script:
            return False
        print("Running script %s" % self.script_name)
        errors = self.script_manager.execute_script(self.script)
        if errors:
            print("Script failed to run.  Errors detected:\n" + "\n".join(errors))
            self._shutdown()
            sys.exit(1)
        return False

    def script_finished(self, script_object):
        # Overriden from ScriptListener. Called by ScriptManager when a
        # script is finished.
        if self.script_done_action == "exit":
            self._request_exit()
        elif self.script_done_action == "observe":
            self.sheriff.set_observer(True)

    def _request_exit(self):
        self._should_exit = True

    def run(self):
        # parse the config file
        if self.config is not None:
            self.sheriff.load_config(self.config)
            self.script_manager.load_config(self.config)

        # start a local deputy?
        if self.spawn_deputy:
            procman_deputy_cmd = find_procman_deputy_cmd()
            args = [ procman_deputy_cmd, "-i", "localhost" ]
            if not procman_deputy_cmd:
                sys.stderr.write("Can't find procman-deputy.")
                sys.exit(1)
            self.spawned_deputy = subprocess.Popen(args)
        else:
            self.spawned_deputy = None

        # run a script
        if self.script_name:
            self.script = self.script_manager.get_script(self.script_name)
            if not self.script:
                print "No such script: %s" % self.script_name
                self._shutdown()
                sys.exit(1)
            errors = self.script_manager.check_script_for_errors(self.script)
            if errors:
                print "Unable to run script.  Errors were detected:\n\n"
                print "\n    ".join(errors)
                self._shutdown()
                sys.exit(1)

            self.script_manager.add_listener(self)

        signal.signal(signal.SIGINT, lambda *s: self._request_exit())
        signal.signal(signal.SIGTERM, lambda *s: self._request_exit())
        signal.signal(signal.SIGHUP, lambda *s: self._request_exit())

        try:
            if self.script:
                time.sleep(0.2)
                self._start_script()

            while not self._should_exit:
                self.lcm_obj.handle_timeout(200)
        except KeyboardInterrupt:
            pass
        except IOError:
            pass
        finally:
            print("Sheriff terminating..")
            self._shutdown()

        return 0

def usage():
    sys.stdout.write(
"""usage: %s [options] [<procman_config_file> [<script_name>]]

Process management operating console.

Options:
  -l, --lone-ranger   Automatically run a deputy within the sheriff process
                      This deputy terminates with the sheriff, along with
                      all the commands it hosts.

  -o, --observer      Runs in observer mode on startup.  This prevents the
                      sheriff from sending any commands, and is useful for
                      monitoring existing procman sheriff and/or deputy
                      instances. Using this option is currently useless.

  --on-script-complete <exit|observe>
                      Only valid if a script is specified.  If set to "exit",
                      then the sheriff exits when the script is done executing.
                      If set to "observe", then the sheriff self-demotes to
                      observer mode.

  -h, --help          Shows this help text

If <procman_config_file> is specified, then the sheriff tries to load
deputy commands from the file.

If <script_name> is additionally specified, then the sheriff executes the
named script once the config file is loaded.

""" % os.path.basename(sys.argv[0]))
    sys.exit(1)

def main():
    try:
        opts, args = getopt.getopt( sys.argv[1:], 'hlon',
                ['help','lone-ranger', 'on-script-complete=', 'no-gui', 'observer'] )
    except getopt.GetoptError:
        usage()
        sys.exit(2)

    spawn_deputy = False
    use_gui = True
    script_done_action = None
    observer = False

    for optval, argval in opts:
        if optval in [ '-l', '--lone-ranger' ]:
            spawn_deputy = True
        elif optval in [ '-n', '--no-gui' ]:
            use_gui = False
        elif optval in [ '-o', '--observer' ]:
            observer = True
        elif optval in [ '--on-script-complete' ]:
            script_done_action = argval
            if argval not in [ "exit", "observe" ]:
                usage()
        elif optval in [ '-h', '--help' ]:
            usage()

    cfg = None
    script_name = None
    if len(args) > 0:
        try:
            cfg = sheriff.load_config_file(file(args[0]))
        except Exception, xcp:
            print "Unable to load config file."
            print xcp
            sys.exit(1)
    if len(args) > 1:
        script_name = args[1]

    if observer:
        if cfg:
            print "Loading a config file is not allowed when starting in observer mode."
            sys.exit(1)
        if spawn_deputy:
            print "Lone ranger mode and observer mode are mutually exclusive."
            sys.exit(1)

    lcm_obj = LCM()

    SheriffHeadless(lcm_obj, cfg, spawn_deputy, script_name, script_done_action).run()

if __name__ == "__main__":
    main()
