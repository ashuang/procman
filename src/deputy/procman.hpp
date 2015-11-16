#ifndef PROCMAN_PROCMAN_HPP__
#define PROCMAN_PROCMAN_HPP__

#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include <map>

#include <glib.h>

namespace procman {

typedef std::map<std::string, std::string> StringStringMap;

enum CommandStatus {
    PROCMAN_CMD_STOPPED = 0,
    PROCMAN_CMD_RUNNING,
    PROCMAN_CMD_INVALID
};

struct ProcmanOptions {
  static ProcmanOptions Default(int argc, char** argv);

  std::string bin_path;  // commands that are not specified as an absolute
                          // path have this prepended to their path
  bool verbose;
};

struct procman_cmd_t {
  procman_cmd_t();

  const std::string& ExecStr() const { return exec_str_; }

  const std::string& Id() const { return cmd_id_; }

  int32_t sheriff_id;   // unique to the containing instance of procman_t

  std::string exec_str_; // the command to execute.  Do not modify directly

  std::string cmd_id_;  // a user-assigned name for the command.  Do not modify directly

  int pid;      // pid of process when running.  0 otherwise

  int stdin_fd;  // when the process is running, writing to this pipe
  // writes to stdin of the process
  int stdout_fd; // and reading from this pipe reads from stdout of the proc

  int exit_status;

  int envc;    //number of environment variables
  char ***envp; //environment variables to set

  int argc;    //number of arguments, shouldn't be needed
  char **argv; // don't touch this

  std::vector<int> descendants_to_kill; // Used internally when killing a process.

  void *user;  // use this for application-specific data
};

struct procman_t {
  procman_t();

  ProcmanOptions options;
  GList *commands;
  StringStringMap variables_;
};


// constructor
procman_t *procman_create (const ProcmanOptions& options);

// destructor
void procman_destroy (procman_t *pm);

// returns a doubly linked list, where each data element is a procman_cmd_t
//
// Do not modify this list, or it's contents!
const GList* procman_get_cmds (procman_t *pm);

/**
 * Removes all variables from the variable expansion table.
 */
void procman_remove_all_variables(procman_t* pm);

int procman_start_cmd (procman_t *pm, procman_cmd_t *cmd);
int procman_stop_cmd (procman_t *pm, procman_cmd_t *p);
int procman_kill_cmd (procman_t *pm, procman_cmd_t *cmd, int signum);

// convenience functions
int procman_start_all_cmds (procman_t *pm);
int procman_stop_all_cmds (procman_t *pm);

/* adds a command to be managed by procman.  returns a pointer to a newly
 * created procman_cmd_t, or NULL on failure
 *
 * The command is not started.  To start a command running, use
 * procman_start_cmd
 */
procman_cmd_t* procman_add_cmd (procman_t *pm, const char *cmd_str, const char* cmd_id);

/* Removes a command from management by procman.  The command must already be
 * stopped and reaped by procman_check_for_dead_children.  Otherwise, this
 * function will fail.  On success, the %cmd structure is destroyed and no
 * longer available for use.
 *
 * returns 0 on success, -1 on failure.
 */
int procman_remove_cmd (procman_t *pm, procman_cmd_t *cmd);

/* searches for a command.  returns a pointer to the corresponding
 * procman_cmd_t on success, NULL on failure
 */
procman_cmd_t *procman_find_cmd (procman_t *pm, const char *cmd_str);

/* searches for a command.  returns a pointer to the corresponding
 * procman_cmd_t on success, NULL on failure
 */
procman_cmd_t *procman_find_cmd_by_id (procman_t *pm, int32_t sheriff_id);

/* checks to see if any processes spawned by procman_start_cmd have died
 *
 * dead_child should point to an unused procman_cmd_t *
 *
 * on return, if a child process has died, then it is reaped and a pointer to
 * the procman_cmd_t is placed in dead_child.  If no children have died, then
 * dead_child points to NULL on return.
 *
 * This function does not block
 *
 * returns 0 on success, -1 on failure
 */
int procman_check_for_dead_children (procman_t *pm,
        procman_cmd_t **dead_child);

int procman_close_dead_pipes (procman_t *pm, procman_cmd_t *cmd);

/* returns 0  TODO
 */
CommandStatus procman_get_cmd_status (procman_t *pm, procman_cmd_t *cmd);

/* Changes the command that will be executed for a procman_cmd_t
 * no effect until the command is started again (if it's currently running)
 */
void procman_cmd_change_str (procman_cmd_t *cmd, const char *cmd_str);

/**
 * Sets the command id.
 */
void procman_cmd_set_id(procman_cmd_t* cmd, const char* cmd_id);

#define PROCMAN_MAX_MESSAGE_AGE_USEC 60000000LL

}  // namespace procman

#endif  // PROCMAN_PROCMAN_HPP__
