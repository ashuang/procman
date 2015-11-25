#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <string.h>
#include <ctype.h>
#include <time.h>
#include <errno.h>
#include <sys/time.h>

#ifdef __APPLE__
#include <util.h>
#else
#include <pty.h>
#endif

#include <sys/types.h>
#include <sys/wait.h>
#include <signal.h>

#include "exec_string_utils.hpp"
#include "procman.hpp"
#include "procinfo.hpp"

namespace procman {

static void dbgt (const char *fmt, ...) {
    va_list ap;
    va_start (ap, fmt);

    char timebuf[80];
    struct timeval now_tv;
    gettimeofday(&now_tv, NULL);
    struct tm now_tm;
    localtime_r(&now_tv.tv_sec, &now_tm);
    int pos = strftime(timebuf, sizeof(timebuf), "%FT%T", &now_tm);
    pos += snprintf(timebuf + pos, sizeof(timebuf)-pos, ".%03d", (int)(now_tv.tv_usec / 1000));
    strftime(timebuf + pos, sizeof(timebuf)-pos, "%z", &now_tm);

    char buf[4096];
    vsnprintf (buf, sizeof(buf), fmt, ap);

    va_end (ap);

    fprintf (stderr, "%s %s", timebuf, buf);
}

ProcmanOptions ProcmanOptions::Default() {
  ProcmanOptions result;
  result.verbose = false;
  return result;
}

Procman::Procman(const ProcmanOptions& options) :
  options_(options),
  variables_() {
}

int Procman::StartCommand(ProcmanCommandPtr cmd) {
    int status;

    if (0 != cmd->Pid()) {
        dbgt ("[%s] has non-zero PID.  not starting again\n", cmd->Id().c_str());
        return -1;
    } else {
        dbgt ("[%s] starting\n", cmd->Id().c_str());

        cmd->PrepareArgsAndEnvironment(variables_);

        // close existing fd's
        if (cmd->StdoutFd() >= 0) {
            close (cmd->StdoutFd());
            cmd->SetStdoutFd(-1);
        }
        cmd->SetStdinFd(-1);
        cmd->SetExitStatus(0);

        // make a backup of stderr, in case something bad happens during exec.
        // if exec succeeds, then we have a dangling file descriptor that
        // gets closed when the child exits... that's okay
        int stderr_backup = dup(STDERR_FILENO);

        printf("execv: \n");
        for (int i = 0; cmd->argv_[i]; ++i) {
          printf("  argv[%d]: %s\n", i, cmd->argv_[i]);
        }
        int stdin_fd;
        int pid = forkpty(&stdin_fd, NULL, NULL, NULL);
        cmd->SetStdinFd(stdin_fd);
        if (0 == pid) {
            // set environment variables from the beginning of the command
          for (auto& item : cmd->environment_) {
            setenv(item.first.c_str(), item.second.c_str(), 1);
          }

            // go!
            execvp(cmd->argv_[0], cmd->argv_);

            char ebuf[1024];
            snprintf (ebuf, sizeof(ebuf), "%s", strerror(errno));
            dbgt("[%s] ERRROR executing [%s]\n", cmd->Id().c_str(), cmd->ExecStr().c_str());
            dbgt("[%s] execv: %s\n", cmd->Id().c_str(), ebuf);

            // if execv returns, the command did not execute successfully
            // (e.g. permission denied or bad path or something)

            // restore stderr so we can barf a real error message
            close(STDERR_FILENO);
            dup2(stderr_backup, STDERR_FILENO);
            dbgt("[%s] ERROR executing [%s]\n", cmd->Id().c_str(), cmd->ExecStr().c_str());
            dbgt("[%s] execv: %s\n", cmd->Id().c_str(), ebuf);
            close(stderr_backup);

            exit(-1);
        } else if (pid < 0) {
            perror("forkpty");
            close(stderr_backup);
            return -1;
        } else {
            cmd->SetPid(pid);
            cmd->SetStdoutFd(cmd->StdinFd());
            close(stderr_backup);
        }
    }
    return 0;
}

int Procman::KillCommmand(ProcmanCommandPtr cmd, int signum) {
  if (0 == cmd->Pid()) {
    dbgt ("[%s] has no PID.  not stopping (already dead)\n", cmd->Id().c_str());
    return -EINVAL;
  }
  // get a list of the process's descendants
  std::vector<int> descendants = procinfo_get_descendants(cmd->Pid());

  dbgt ("[%s] stop (signal %d)\n", cmd->Id().c_str(), signum);
  if (0 != kill (cmd->Pid(), signum)) {
    return -errno;
  }

  // send the same signal to all of the process's descendants
  for (int child_pid : descendants) {
    dbgt("signal %d to descendant %d\n", signum, child_pid);
    kill(child_pid, signum);

    auto iter = std::find(cmd->descendants_to_kill_.begin(),
        cmd->descendants_to_kill_.end(), child_pid);
    if (iter ==  cmd->descendants_to_kill_.end()) {
      cmd->descendants_to_kill_.push_back(child_pid);
    }
  }
  return 0;
}

int Procman::StopAllCommands() {
  int ret = 0;

  // loop through each managed process and try to stop it
  for (ProcmanCommandPtr cmd : commands_) {
    int status = KillCommmand(cmd, SIGINT);
    if (0 != status) {
      ret = status;
      // If something bad happened, try to stop the other processes, but
      // still return an error
    }
  }
  return ret;
}

ProcmanCommandPtr Procman::CheckForDeadChildren() {
  int status;

  // check for dead children
  ProcmanCommandPtr dead_child = NULL;
  int pid = waitpid (-1, &status, WNOHANG);
  if(pid <= 0)
    return 0;

  for (ProcmanCommandPtr cmd : commands_) {
    if(cmd->Pid() == 0 || pid != cmd->Pid())
      continue;
    dead_child = cmd;
    cmd->SetPid(0);
    cmd->SetExitStatus(status);

    if (WIFSIGNALED (status)) {
      int signum = WTERMSIG (status);
      dbgt ("[%s] terminated by signal %d (%s)\n",
          cmd->Id().c_str(), signum, strsignal (signum));
    } else if (status != 0) {
      dbgt ("[%s] exited with status %d\n",
          cmd->Id().c_str(), WEXITSTATUS (status));
    } else {
      dbgt ("[%s] exited\n", cmd->Id().c_str());
    }

    // check for and kill orphaned children.
    for (int child_pid : cmd->descendants_to_kill_) {
      if(procinfo_is_orphaned_child_of(child_pid, pid)) {
        dbgt("sending SIGKILL to orphan process %d\n", child_pid);
        kill(child_pid, SIGKILL);
      }
    }

    return dead_child;
  }

  dbgt ("reaped [%d] but couldn't find process\n", pid);
  return dead_child;
}

int Procman::CloseDeadPipes(ProcmanCommandPtr cmd) {
  if (cmd->StdoutFd() < 0 && cmd->StdinFd() < 0)
    return 0;

  if (cmd->Pid()) {
    dbgt ("refusing to close pipes for command "
        "with nonzero pid [%s] [%d]\n",
        cmd->Id().c_str(), cmd->Pid());
    return 0;
  }
  if (cmd->StdoutFd() >= 0) {
    close(cmd->StdoutFd());
  }
  cmd->SetStdinFd(-1);
  cmd->SetStdoutFd(-1);
  return 0;
}

void ProcmanCommand::PrepareArgsAndEnvironment(const StringStringMap& variables) {
  if (argv_) {
    Strfreev(argv_);
    argv_ = NULL;
  }
  environment_.clear();

  const std::vector<std::string> args = SeparateArgs(exec_str_);

  // Extract environment variables and expand variables
  argv_ = (char**) calloc(args.size() + 1, sizeof(char**));
  int num_env_vars = 0;
  for (int i = 0; i < args.size(); i++) {
    if (i == num_env_vars && strchr(args[i].c_str(), '=')) {
      const std::vector<std::string> parts = Split(args[i], "=", 2);
      environment_[parts[0]] = parts[1];
      printf("env: [%s]=[%s]\n", parts[0].c_str(), parts[1].c_str());
      ++num_env_vars;
    } else {
      // substitute variables
      const std::string arg = ExpandVariables(args[i], variables);
      printf("argv[%d]: %s\n", i - num_env_vars, arg.c_str());
      argv_[i - num_env_vars] = strdup(arg.c_str());
    }
  }
  argc_ = args.size() - num_env_vars;
}

ProcmanCommand::ProcmanCommand(const std::string& exec_str,
    const std::string& cmd_id) :
  exec_str_(exec_str),
  cmd_id_(cmd_id),
  pid_(0),
  stdin_fd_(-1),
  stdout_fd_(-1),
  exit_status_(0),
  argc_(0),
  argv_(nullptr) {}

ProcmanCommand::~ProcmanCommand() {
  if (argv_) {
    Strfreev (argv_);
  }
}


const std::vector<ProcmanCommandPtr>& Procman::GetCommands() {
  return commands_;
}

void Procman::RemoveAllVariables() {
  variables_.clear();
}

ProcmanCommandPtr Procman::AddCommand(const std::string& exec_str, const std::string& cmd_id) {
  ProcmanCommandPtr newcmd(new ProcmanCommand(exec_str, cmd_id));
  commands_.push_back(newcmd);

  dbgt ("[%s] new command [%s]\n", cmd_id.c_str(), exec_str.c_str());

  return newcmd;
}

bool Procman::RemoveCommand(ProcmanCommandPtr cmd) {
  CheckCommand(cmd);

  // stop the command (if it's running)
  if (cmd->Pid()) {
    dbgt ("procman ERROR: refusing to remove running command %s\n",
        cmd->ExecStr().c_str());
    return false;
  }

  CloseDeadPipes(cmd);

  // remove
  commands_.erase(std::find(commands_.begin(), commands_.end(), cmd));
  return true;
}

CommandStatus Procman::GetCommandStatus(ProcmanCommandPtr cmd) {
  if (cmd->Pid() > 0) {
    return PROCMAN_CMD_RUNNING;
  }
  if (cmd->Pid() == 0) {
    return PROCMAN_CMD_STOPPED;
  }
  return PROCMAN_CMD_INVALID;
}

void Procman::SetCommandExecStr(ProcmanCommandPtr cmd,
    const std::string& exec_str) {
  CheckCommand(cmd);
  cmd->exec_str_ = exec_str;
}

void Procman::SetCommandId(ProcmanCommandPtr cmd, const std::string& cmd_id) {
  CheckCommand(cmd);
  cmd->cmd_id_ = cmd_id;
}

void Procman::CheckCommand(ProcmanCommandPtr cmd) {
  if (std::find(commands_.begin(), commands_.end(), cmd) == commands_.end()) {
    throw std::invalid_argument("invalid command");
  }
}

}
