/*
 * process management core code
 */

#define _GNU_SOURCE

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

#include <glib.h>

#include <libgen.h>

#include "procman.hpp"
#include "procinfo.hpp"

namespace procman {

static void dbgt (const char *fmt, ...)
{
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

static ProcmanCommand * procman_cmd_create (const std::string& exec_str,
    const std::string& cmd_id, int32_t sheriff_id);
static void procman_cmd_split_str (ProcmanCommandPtr cmd, const StringStringMap& variables);

ProcmanOptions ProcmanOptions::Default(int argc, char **argv)
{
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

    if (0 != cmd->pid) {
        dbgt ("[%s] has non-zero PID.  not starting again\n", cmd->Id().c_str());
        return -1;
    } else {
        dbgt ("[%s] starting\n", cmd->Id().c_str());

        procman_cmd_split_str(cmd, variables_);

        // close existing fd's
        if (cmd->stdout_fd >= 0) {
            close (cmd->stdout_fd);
            cmd->stdout_fd = -1;
        }
        cmd->stdin_fd = -1;
        cmd->exit_status = 0;

        // make a backup of stderr, in case something bad happens during exec.
        // if exec succeeds, then we have a dangling file descriptor that
        // gets closed when the child exits... that's okay
        int stderr_backup = dup(STDERR_FILENO);

        int pid = forkpty(&cmd->stdin_fd, NULL, NULL, NULL);
        if (0 == pid) {
//            // block SIGINT (only allow the procman to kill the process now)
//            sigset_t toblock;
//            sigemptyset (&toblock);
//            sigaddset (&toblock, SIGINT);
//            sigprocmask (SIG_BLOCK, &toblock, NULL);

            // set environment variables from the beginning of the command
            for (int i=0;i<cmd->envc;i++){
                setenv(cmd->envp[i][0],cmd->envp[i][1],1);
            }

            // go!
            execvp (cmd->argv[0], cmd->argv);

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
            cmd->pid = pid;
            cmd->stdout_fd = cmd->stdin_fd;
            close(stderr_backup);
        }
    }
    return 0;
}

int Procman::KillCommmand(ProcmanCommandPtr cmd, int signum)
{
  if (0 == cmd->pid) {
    dbgt ("[%s] has no PID.  not stopping (already dead)\n", cmd->Id().c_str());
    return -EINVAL;
  }
  // get a list of the process's descendants
  std::vector<int> descendants = procinfo_get_descendants(cmd->pid);

  dbgt ("[%s] stop (signal %d)\n", cmd->Id().c_str(), signum);
  if (0 != kill (cmd->pid, signum)) {
    return -errno;
  }

  // send the same signal to all of the process's descendants
  for (int child_pid : descendants) {
    dbgt("signal %d to descendant %d\n", signum, child_pid);
    kill(child_pid, signum);

    auto iter = std::find(cmd->descendants_to_kill.begin(),
        cmd->descendants_to_kill.end(), child_pid);
    if (iter ==  cmd->descendants_to_kill.end()) {
      cmd->descendants_to_kill.push_back(child_pid);
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
    cmd->pid = 0;
    cmd->exit_status = status;

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
    for (int child_pid : cmd->descendants_to_kill) {
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
  cmd->stdin_fd = -1;
  cmd->stdout_fd = -1;
  return 0;
}

/**
 * same as g_strsplit_set, but removes empty tokens
 */
static char **
strsplit_set_packed(const char *tosplit, const char *delimiters, int max_tokens)
{
    char **tmp = g_strsplit_set(tosplit, delimiters, max_tokens);
    int i;
    int n=0;
    for(i=0; tmp[i]; i++) {
        if(strlen(tmp[i])) n++;
    }
    char **result = (char**) calloc(n+1, sizeof(char*));
    int c=0;
    for(i=0; tmp[i]; i++) {
        if(strlen(tmp[i])) {
            result[c] = g_strdup(tmp[i]);
            c++;
        }
    }
    g_strfreev(tmp);
    return result;
}

typedef struct {
    const char* w;
    int w_len;
    int pos;
    char cur_tok;
    GString* result;
    const StringStringMap* variables;
} subst_parse_context_t;

static int
is_valid_variable_char(char c, int pos)
{
    const char* valid_start = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_";
    const char* valid_follow = "1234567890";
    return (strchr(valid_start, c) != NULL ||
            ((0 == pos) && (strchr(valid_follow, c) != NULL)));
}

static char
subst_vars_has_token(subst_parse_context_t* ctx)
{
    return (ctx->pos < ctx->w_len);
}

static char
subst_vars_peek_token(subst_parse_context_t* ctx)
{
    return subst_vars_has_token(ctx) ? ctx->w[ctx->pos] : 0;
}

static int
subst_vars_eat_token(subst_parse_context_t* ctx)
{
    if(subst_vars_has_token(ctx)) {
        ctx->cur_tok = ctx->w[ctx->pos];
        ctx->pos++;
        return TRUE;
    } else {
        ctx->cur_tok = 0;
        return FALSE;
    }
}

static int
subst_vars_parse_variable(subst_parse_context_t* ctx)
{
    int start = ctx->pos;
    if(!subst_vars_has_token(ctx)) {
        g_string_append_c(ctx->result, '$');
        return 0;
    }
    int has_braces = subst_vars_peek_token(ctx) == '{';
    if(has_braces)
        subst_vars_eat_token(ctx);
    int varname_start = ctx->pos;
    int varname_len = 0;
    while(subst_vars_has_token(ctx) &&
          is_valid_variable_char(subst_vars_peek_token(ctx), varname_len)) {
        varname_len++;
        subst_vars_eat_token(ctx);
    }
    char* varname = g_strndup(&ctx->w[varname_start], varname_len);
    int braces_ok = TRUE;
    if(has_braces && ((!subst_vars_eat_token(ctx)) || ctx->cur_tok != '}'))
        braces_ok = FALSE;
    int ok = varname_len && braces_ok;
    if(ok) {
      // first lookup the variable in our stored table
      const char* val = nullptr;
      auto iter = ctx->variables->find(varname);
      if (iter != ctx->variables->end()) {
        val = iter->second.c_str();
      } else {
        val = getenv(varname);
      }
      // if that fails, then check for a similar environment variable
      if (val) {
        g_string_append(ctx->result, val);
      } else {
        ok = FALSE;
      }
    }
    if(!ok)
        g_string_append_len(ctx->result, &ctx->w[start - 1], ctx->pos - start + 1);
    g_free(varname);
    return ok;
}

/**
 * Do variable expansion on a command argument.  This searches the argument for
 * text of the form $VARNAME and ${VARNAME}.  For each discovered variable, it
 * then expands the variable.  Values defined in the hashtable vars are used
 * first, followed by environment variable values.  If a variable expansion
 * fails, then the corresponding text is left unchanged.
 */
static char*
subst_vars(const char* w, const StringStringMap& vars)
{
    subst_parse_context_t ctx;
    ctx.w = w;
    ctx.w_len = strlen(w);
    ctx.pos = 0;
    ctx.result = g_string_sized_new(ctx.w_len * 2);
    ctx.variables = &vars;

    while(subst_vars_eat_token(&ctx)) {
        char c = ctx.cur_tok;
        if('\\' == c) {
            if(subst_vars_eat_token(&ctx)) {
                g_string_append_c(ctx.result, c);
            } else {
                g_string_append_c(ctx.result, '\\');
            }
            continue;
        }
        // variable?
        if('$' == c) {
            subst_vars_parse_variable(&ctx);
        } else {
            g_string_append_c(ctx.result, c);
        }
    }
    char* result = g_strdup(ctx.result->str);
    g_string_free(ctx.result, TRUE);
    return result;
}

static void
procman_cmd_split_str (ProcmanCommandPtr cmd, const StringStringMap& variables)
{
    if (cmd->argv) {
        g_strfreev (cmd->argv);
        cmd->argv = NULL;
    }
    if (cmd->envp) {
        for(int i=0;i<cmd->envc;i++)
            g_strfreev (cmd->envp[i]);
        free(cmd->envp);
        cmd->envp = NULL;
    }

    // TODO don't use g_shell_parse_argv... it's not good with escape characters
    char ** argv=NULL;
    int argc = -1;
    GError *err = NULL;
    gboolean parsed = g_shell_parse_argv(cmd->ExecStr().c_str(), &argc,
            &argv, &err);

    if(!parsed || err) {
        // unable to parse the command string as a Bourne shell command.
        // Do the simple thing and split it on spaces.
        cmd->envp = (char***) calloc(1, sizeof(char***));
        cmd->envc = 0;
        cmd->argv = strsplit_set_packed(cmd->ExecStr().c_str(), " \t\n", 0);
        for(cmd->argc=0; cmd->argv[cmd->argc]; cmd->argc++);
        g_error_free(err);
        return;
    }

    // extract environment variables
    int envCount=0;
    char * equalSigns[512];
    while((equalSigns[envCount]=strchr(argv[envCount],'=')))
        envCount++;
    cmd->envc=envCount;
    cmd->argc=argc-envCount;
    cmd->envp = (char***) calloc(cmd->envc+1,sizeof(char***));
    cmd->argv = (char**) calloc(cmd->argc+1,sizeof(char**));
    for (int i=0;i<argc;i++) {
        if (i<envCount)
            cmd->envp[i]=g_strsplit(argv[i],"=",2);
        else {
            // substitute variables
            cmd->argv[i-envCount]=subst_vars(argv[i], variables);
        }
    }
    g_strfreev(argv);
}

ProcmanCommand::ProcmanCommand() :
  exec_str_(),
  descendants_to_kill() {
}

ProcmanCommand::~ProcmanCommand() {
  g_strfreev (argv);
  for (int i = 0; i<envc; i++) {
    g_strfreev(envp[i]);
  }
  free(envp);
}

static ProcmanCommand *
procman_cmd_create(const std::string& exec_str, const std::string& cmd_id, int32_t sheriff_id)
{
  ProcmanCommand* pcmd = new ProcmanCommand();
  pcmd->exec_str_ = exec_str;
  pcmd->cmd_id_ = cmd_id;
  pcmd->sheriff_id_ = sheriff_id;
  pcmd->stdout_fd = -1;
  pcmd->stdin_fd = -1;

  pcmd->argv = NULL;
  pcmd->argc = 0;
  pcmd->envp = NULL;
  pcmd->envc = 0;

  return pcmd;
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

  ProcmanCommandPtr newcmd(procman_cmd_create(exec_str, cmd_id, sheriff_id));
  if (newcmd) {
    commands_.push_back(newcmd);
  }

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
