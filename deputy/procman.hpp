#ifndef PROCMAN_PROCMAN_HPP__
#define PROCMAN_PROCMAN_HPP__

#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include <map>

namespace procman {

typedef std::map<std::string, std::string> StringStringMap;

enum CommandStatus {
  PROCMAN_CMD_STOPPED = 0,
  PROCMAN_CMD_RUNNING,
  PROCMAN_CMD_INVALID
};

struct ProcmanOptions {
  static ProcmanOptions Default();
  bool verbose;
};

class ProcmanCommand {
  public:
    ~ProcmanCommand();

    const std::string& ExecStr() const { return exec_str_; }

    const std::string& Id() const { return cmd_id_; }

    int Pid() const { return pid_; }

    int StdoutFd() const { return stdout_fd_; }

    int StdinFd() const { return stdin_fd_; }

    int ExitStatus() const { return exit_status_; }

  private:
    ProcmanCommand(const std::string& exec_str,
        const std::string& cmd_id);

    void SetPid(int pid) { pid_ = pid; }

    void SetStdinFd(int fd) { stdin_fd_ = fd; }

    void SetStdoutFd(int fd) { stdout_fd_ = fd; }

    void SetExitStatus(int status) { exit_status_ = status; }

    void PrepareArgsAndEnvironment(const StringStringMap& variables);

    friend class Procman;

    // a user-assigned name for the command.
    std::string cmd_id_;

    // the command to execute.
    std::string exec_str_;

    // pid of process when running.  0 otherwise
    int pid_;

    // when the process is running, writing to this pipe
    // writes to stdin of the process
    int stdin_fd_;

    // and reading from this pipe reads from stdout of the proc
    int stdout_fd_;

    int exit_status_;

    // number of arguments
    int argc_;
    char **argv_;

    // environment variables to set
    StringStringMap environment_;

    std::vector<int> descendants_to_kill_; // Used internally when killing a process.
};

typedef std::shared_ptr<ProcmanCommand> ProcmanCommandPtr;

class Procman {
  public:
    Procman(const ProcmanOptions& options);

    /**
     * Removes all variables from the variable expansion table.
     */
    void RemoveAllVariables();

    // returns a doubly linked list, where each data element is a ProcmanCommand
    //
    // Do not modify this list, or it's contents!
    const std::vector<ProcmanCommandPtr>& GetCommands();

    int StartCommand(ProcmanCommandPtr cmd);
    int StopCommand(ProcmanCommandPtr cmd);
    int KillCommmand(ProcmanCommandPtr cmd, int signum);

    // convenience functions
    int StopAllCommands();

    /* adds a command to be managed by procman.  returns a pointer to a newly
     * created ProcmanCommand, or NULL on failure
     *
     * The command is not started.  To start a command running, use
     * procman_start_cmd
     */
    ProcmanCommandPtr AddCommand(const std::string& exec_str, const std::string& cmd_id);

    /* Removes a command from management by procman.  The command must already be
     * stopped and reaped by procman_check_for_dead_children.  Otherwise, this
     * function will fail.  On success, the %cmd structure is destroyed and no
     * longer available for use.
     */
    bool RemoveCommand(ProcmanCommandPtr cmd);

    /* checks to see if any processes spawned by procman_start_cmd have died
     *
     * dead_child should point to an unused ProcmanCommand *
     *
     * on return, if a child process has died, then it is reaped and a pointer to
     * the ProcmanCommand is placed in dead_child.  If no children have died, then
     * dead_child points to NULL on return.
     *
     * This function does not block
     *
     * returns 0 on success, -1 on failure
     */
    ProcmanCommandPtr CheckForDeadChildren();

    int CloseDeadPipes(ProcmanCommandPtr cmd);

    CommandStatus GetCommandStatus(ProcmanCommandPtr cmd);

    /* Changes the command that will be executed for a ProcmanCommand
     * no effect until the command is started again (if it's currently running)
     */
    void SetCommandExecStr(ProcmanCommandPtr cmd, const std::string& exec_str);

    /**
     * Sets the command id.
     */
    void SetCommandId(ProcmanCommandPtr cmd, const std::string& cmd_id);

  private:
    void CheckCommand(ProcmanCommandPtr cmd);
    ProcmanOptions options_;
    std::vector<ProcmanCommandPtr> commands_;
    StringStringMap variables_;
};

}  // namespace procman

#endif  // PROCMAN_PROCMAN_HPP__
