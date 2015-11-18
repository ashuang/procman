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

#include <libgen.h>

#include <sstream>

#include <glib.h>

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

static std::vector<std::string> Split(const std::string& input,
    const std::string& delimeters,
    int max_items) {
  std::vector<std::string> result;

  int tok_begin = 0;
  int tok_end = 0;
  while (tok_begin < input.size()) {
    if (result.size() == max_items - 1) {
      result.emplace_back(&input[tok_begin]);
      return result;
    }

    for (tok_end = tok_begin;
        tok_end < input.size() &&
        !strchr(delimeters.c_str(), input[tok_end]);
        ++tok_end) {}

    result.emplace_back(&input[tok_begin], tok_end - tok_begin);

    tok_begin = tok_end + 1;
  }

  return result;
}

static void strfreev(char** vec) {
  for (char** ptr = vec; *ptr; ++ptr) {
    free(*ptr);
  }
}

class VariableExpander {
  public:
    /**
     * Do variable expansion on a command argument.  This searches the argument for
     * text of the form $VARNAME and ${VARNAME}.  For each discovered variable, it
     * then expands the variable.  Values defined in the hashtable vars are used
     * first, followed by environment variable values.  If a variable expansion
     * fails, then the corresponding text is left unchanged.
     */
    std::string ExpandVariables(const char* input,
        const StringStringMap& vars) {
      input_ = input;
      input_len_ = strlen(input_);
      pos_ = 0;
      variables_ = &vars;

      while(EatToken()) {
        const char c = cur_tok_;
        if('\\' == c) {
          if(EatToken()) {
            output_.put(c);
          } else {
            output_.put('\\');
          }
          continue;
        }
        // variable?
        if('$' == c) {
          ParseVariable();
        } else {
          output_.put(c);
        }
      }
      return output_.str();
    }

  private:
    bool HasToken() {
      return pos_ < input_len_;
    }

    char PeekToken() {
      return HasToken() ? input_[pos_] : 0;
    }

    bool EatToken() {
      if(HasToken()) {
        cur_tok_ = input_[pos_];
        pos_++;
        return true;
      } else {
        cur_tok_ = 0;
        return false;
      }
    }

    bool ParseVariable() {
      int start = pos_;
      if(!HasToken()) {
        output_.put('$');
        return false;
      }
      int has_braces = PeekToken() == '{';
      if(has_braces) {
        EatToken();
      }
      int varname_start = pos_;
      int varname_len = 0;
      while(HasToken() &&
          IsValidVariableCharacter(PeekToken(), varname_len)) {
        varname_len++;
        EatToken();
      }
      char* varname = strndup(&input_[varname_start], varname_len);
      bool braces_ok = true;
      if(has_braces && ((!EatToken()) || cur_tok_ != '}')) {
        braces_ok = false;
      }
      bool ok = varname_len && braces_ok;
      if (ok) {
        // first lookup the variable in our stored table
        const char* val = nullptr;
        auto iter = variables_->find(varname);
        if (iter != variables_->end()) {
          val = iter->second.c_str();
        } else {
          val = getenv(varname);
        }
        // if that fails, then check for a similar environment variable
        if (val) {
          output_ << val;
        } else {
          ok = false;
        }
      }
      if (!ok) {
        output_.write(&input_[start - 1], pos_ - start + 1);
      }
      free(varname);
      return ok;
    }

    static bool IsValidVariableCharacter(char c, int pos) {
      const char* valid_start = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_";
      const char* valid_follow = "1234567890";
      return (strchr(valid_start, c) != NULL ||
          ((0 == pos) && (strchr(valid_follow, c) != NULL)));
    }

    const char* input_;
    int input_len_;
    int pos_;
    char cur_tok_;
    std::stringstream output_;
    const StringStringMap* variables_;
};

ProcmanOptions ProcmanOptions::Default(int argc, char **argv) {
  ProcmanOptions result;

  // infer the path of procman.  This will be used with execv to start the
  // child processes, as it's assumed that child executables reside in same
  // directory as procman (or specified as a relative path or absolute path)
  if (argc <= 0) {
    fprintf (stderr, "procman: INVALID argc (%d)\n", argc);
    abort();
  }

  char* argv0 = strdup(argv[0]);
  result.bin_path = std::string(dirname(argv0)) + "/";
  free(argv0);
  return result;
}

Procman::Procman(const ProcmanOptions& options) :
  options_(options),
  variables_() {
  // add the bin path to the PATH environment variable
  //
  // TODO check and see if it's already there
  char *path = getenv ("PATH");
  std::string newpath = options_.bin_path + ":" + path;
  printf("setting PATH to %s\n", newpath.c_str());
  setenv("PATH", newpath.c_str(), 1);
}

int Procman::StartCommand(ProcmanCommandPtr cmd)
{
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

        int stdin_fd;
        int pid = forkpty(&stdin_fd, NULL, NULL, NULL);
        cmd->SetStdinFd(stdin_fd);
        if (0 == pid) {
//            // block SIGINT (only allow the procman to kill the process now)
//            sigset_t toblock;
//            sigemptyset (&toblock);
//            sigaddset (&toblock, SIGINT);
//            sigprocmask (SIG_BLOCK, &toblock, NULL);

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

int Procman::KillCommmand(ProcmanCommandPtr cmd, int signum)
{
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
    strfreev(argv_);
    argv_ = NULL;
  }
  environment_.clear();

  // TODO don't use g_shell_parse_argv... it's not good with escape characters
  char** argv=NULL;
  int argc = -1;
  GError *err = NULL;
  gboolean parsed = g_shell_parse_argv(exec_str_.c_str(), &argc,
      &argv, &err);

  if(!parsed || err) {
    // unable to parse the command string as a Bourne shell command.
    // Do the simple thing and split it on spaces.
    std::vector<std::string> args = Split(exec_str_, " \t\n", 0);
    args.erase(std::remove_if(args.begin(), args.end(),
          [](const std::string& v) { return v.empty(); }), args.end());
    argv_ = (char**) calloc(args.size() + 1, sizeof(char*));
    argc_ = args.size();
    for (int i = 0; i < argc_; ++i) {
      argv_[i] = strdup(args[i].c_str());
    }
    g_error_free(err);
    return;
  }

  // extract environment variables
  int envCount=0;
  char * equalSigns[512];
  while((equalSigns[envCount] = strchr(argv[envCount],'=')))
    envCount++;
  argc_ = argc - envCount;
  argv_ = (char**) calloc(argc_ + 1, sizeof(char**));
  for (int i = 0; i < argc; i++) {
    if (i < envCount) {
      std::vector<std::string> parts = Split(argv[i], "=", 2);
      environment_[parts[0]] = parts[1];
    } else {
      // substitute variables
      VariableExpander expander;
      const std::string arg = expander.ExpandVariables(argv[i], variables);
      argv_[i - envCount] = strdup(arg.c_str());
    }
  }
  strfreev(argv);
}

ProcmanCommand::ProcmanCommand(const std::string& exec_str,
    const std::string& cmd_id, int32_t sheriff_id) :
  exec_str_(exec_str),
  cmd_id_(cmd_id),
  sheriff_id_(sheriff_id),
  pid_(0),
  stdin_fd_(-1),
  stdout_fd_(-1),
  exit_status_(0),
  argc_(0),
  argv_(nullptr)
{}

ProcmanCommand::~ProcmanCommand() {
  if (argv_) {
    strfreev (argv_);
  }
}


const std::vector<ProcmanCommandPtr>& Procman::GetCommands() {
  return commands_;
}

void Procman::RemoveAllVariables() {
  variables_.clear();
}

ProcmanCommandPtr Procman::AddCommand(const std::string& exec_str, const std::string& cmd_id) {
  // pick a suitable ID
  int32_t sheriff_id;

  // TODO make this more efficient (i.e. sort the existing sheriff_ids)
  //      this implementation is O (n^2)
  for (sheriff_id=1; sheriff_id<INT_MAX; sheriff_id++) {
    auto iter = std::find_if(commands_.begin(), commands_.end(),
        [sheriff_id](ProcmanCommandPtr cmd) {
        return cmd->SheriffId() == sheriff_id;
        });
    if (iter != commands_.end()) {
      break;
    }
  }
  if (sheriff_id == INT_MAX) {
    dbgt ("way too many commands on the system....\n");
    return ProcmanCommandPtr();
  }

  ProcmanCommandPtr newcmd(new ProcmanCommand(exec_str, cmd_id, sheriff_id));
  commands_.push_back(newcmd);

  dbgt ("[%s] new command [%s]\n", newcmd->Id().c_str(), newcmd->ExecStr().c_str());
  return newcmd;
}

bool Procman::RemoveCommand(ProcmanCommandPtr cmd)
{
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

CommandStatus Procman::GetCommandStatus(ProcmanCommandPtr cmd)
{
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
