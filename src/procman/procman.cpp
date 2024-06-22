#include <assert.h>
#include <errno.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#ifdef __APPLE__
#include <util.h>
#else
#include <pty.h>
#endif

#include <sys/types.h>
#include <sys/wait.h>
#include <signal.h>

#include <algorithm>

#include "exec_string_utils.hpp"
#include "procman.hpp"
#include "procinfo.hpp"

namespace procman {

#if 1
#define dbg(...)
#else
#define dbg(...) do { fprintf(stderr, __VA_ARGS__); } while(0)
#endif

Procman::Procman() {
}

Procman::~Procman() {
  while (!commands_.empty()) {
    RemoveCommand(commands_.front());
  }
}

void Procman::StartCommand(ProcmanCommandPtr cmd) {
  if (0 != cmd->Pid()) {
    // Command is already running.
    return;
  }
  dbg("starting [%s]\n", cmd->ExecStr().c_str());

  cmd->PrepareArgsAndEnvironment();

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
  const int stderr_backup = dup(STDERR_FILENO);

  int stdin_fd;
  const int pid = forkpty(&stdin_fd, NULL, NULL, NULL);
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
    fprintf(stderr, "ERRROR executing [%s]\n", cmd->ExecStr().c_str());
    fprintf(stderr, "       execv: %s\n", ebuf);

    // if execv returns, the command did not execute successfully
    // (e.g. permission denied or bad path or something)

    // restore stderr so we can barf a real error message
    close(STDERR_FILENO);
    dup2(stderr_backup, STDERR_FILENO);
    fprintf(stderr, "ERROR executing [%s]\n", cmd->ExecStr().c_str());
    fprintf(stderr, "      execv: %s\n", ebuf);
    close(stderr_backup);

    exit(-1);
  } else if (pid < 0) {
    const std::string errmsg(strerror(errno));
    close(stderr_backup);
    // throw std::runtime_error("forkpty: " + errmsg);
  } else {
    cmd->SetPid(pid);
    cmd->SetStdoutFd(cmd->StdinFd());
    close(stderr_backup);
  }
}

bool Procman::KillCommand(ProcmanCommandPtr cmd, int signum) {
  if (0 == cmd->Pid()) {
    dbg ("[%s] has no PID.  not stopping (already dead)\n", cmd->ExecStr().c_str());
    return false;
  }
  // get a list of the process's descendants
  std::vector<int> descendants = GetDescendants(cmd->Pid());

  dbg ("[%s] stop (signal %d)\n", cmd->ExecStr().c_str(), signum);
  if (0 != kill (cmd->Pid(), signum)) {
    return false;
  }

  // send the same signal to all of the process's descendants
  for (int child_pid : descendants) {
    dbg("signal %d to descendant %d\n", signum, child_pid);
    kill(child_pid, signum);

    auto iter = std::find(cmd->descendants_to_kill_.begin(),
        cmd->descendants_to_kill_.end(), child_pid);
    if (iter ==  cmd->descendants_to_kill_.end()) {
      cmd->descendants_to_kill_.push_back(child_pid);
    }
  }
  return true;
}

ProcmanCommandPtr Procman::CheckForStoppedCommands() {
  int exit_status;
  for (int pid = waitpid(-1, &exit_status, WNOHANG);
       pid > 0;
       pid = waitpid(-1, &exit_status, WNOHANG)) {
    for (ProcmanCommandPtr cmd : commands_) {
      if(pid != cmd->Pid()) {
        continue;
      }
      cmd->SetPid(0);
      cmd->SetExitStatus(exit_status);

      if (WIFSIGNALED (exit_status)) {
        int signum = WTERMSIG (exit_status);
        dbg ("[%s] terminated by signal %d (%s)\n",
            cmd->ExecStr().c_str(), signum, strsignal (signum));
      } else if (exit_status != 0) {
        dbg ("[%s] exited with status %d\n",
            cmd->ExecStr().c_str(), WEXITSTATUS (exit_status));
      } else {
        dbg ("[%s] exited\n", cmd->ExecStr().c_str());
      }

      // check for and kill orphaned children.
      for (int child_pid : cmd->descendants_to_kill_) {
        if(IsOrphanedChildOf(child_pid, pid)) {
          dbg("sending SIGKILL to orphan process %d\n", child_pid);
          kill(child_pid, SIGKILL);
        }
      }

      dead_children_.push_back(cmd);
      break;
    }
  }

  if (!dead_children_.empty()) {
    return dead_children_.front();
  }
  return ProcmanCommandPtr();
}

void Procman::CleanupStoppedCommand(ProcmanCommandPtr cmd) {
  auto iter = std::find(dead_children_.begin(), dead_children_.end(), cmd);
  if (iter == dead_children_.end()) {
    return;
  }
  dead_children_.erase(iter);

  if (cmd->StdoutFd() < 0 && cmd->StdinFd() < 0) {
    return;
  }
  if (cmd->StdoutFd() >= 0) {
    close(cmd->StdoutFd());
  }
  cmd->SetStdinFd(-1);
  cmd->SetStdoutFd(-1);
  assert(!cmd->Pid());
}

void ProcmanCommand::PrepareArgsAndEnvironment() {
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
      ++num_env_vars;
    } else {
      // substitute variables
      const std::string arg = ExpandVariables(args[i]);
      argv_[i - num_env_vars] = strdup(arg.c_str());
    }
  }
  argc_ = args.size() - num_env_vars;
}

ProcmanCommand::ProcmanCommand(const std::string& exec_str) :
  exec_str_(exec_str),
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

ProcmanCommandPtr Procman::AddCommand(const std::string& exec_str) {
  ProcmanCommandPtr newcmd(new ProcmanCommand(exec_str));
  commands_.push_back(newcmd);
  dbg("new command [%s]\n", exec_str.c_str());
  return newcmd;
}

void Procman::RemoveCommand(ProcmanCommandPtr cmd) {
  CheckCommand(cmd);

  // Wait for the command to exit.
  while(cmd->Pid()) {
    usleep(1000);
    CheckForStoppedCommands();
  }

  CleanupStoppedCommand(cmd);

  // remove
  commands_.erase(std::find(commands_.begin(), commands_.end(), cmd));
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

void Procman::CheckCommand(ProcmanCommandPtr cmd) {
  if (std::find(commands_.begin(), commands_.end(), cmd) == commands_.end()) {
    // throw std::invalid_argument("invalid command");
  }
}

}
