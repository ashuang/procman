#ifndef PROCMAN_PROCMAN_HPP__
#define PROCMAN_PROCMAN_HPP__

#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include <map>

#include <procman/procinfo.hpp>

namespace procman {

enum CommandStatus {
  PROCMAN_CMD_STOPPED = 0,
  PROCMAN_CMD_RUNNING,
  PROCMAN_CMD_INVALID
};

class ProcmanCommand {
  public:
    ~ProcmanCommand();

    const std::string& ExecStr() const { return exec_str_; }

    int Pid() const { return pid_; }

    int StdoutFd() const { return stdout_fd_; }

    int StdinFd() const { return stdin_fd_; }

    int ExitStatus() const { return exit_status_; }

  private:
    ProcmanCommand(const std::string& exec_str);

    void SetPid(int pid) { pid_ = pid; }

    void SetStdinFd(int fd) { stdin_fd_ = fd; }

    void SetStdoutFd(int fd) { stdout_fd_ = fd; }

    void SetExitStatus(int status) { exit_status_ = status; }

    void PrepareArgsAndEnvironment();

    friend class Procman;

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
    std::map<std::string, std::string> environment_;

    std::vector<int> descendants_to_kill_; // Used internally when killing a process.
};

typedef std::shared_ptr<ProcmanCommand> ProcmanCommandPtr;

class Procman {
  public:
    Procman();

    /**
     * Destructor.
     *
     * On desctruction, calls RemoveCommand() on all commands.
     */
    ~Procman();

    const std::vector<ProcmanCommandPtr>& GetCommands();

    /**
     * Starts a command running.
     */
    void StartCommand(ProcmanCommandPtr cmd);

    /**
     * Sends the specified POSIX signal to a command.
     */
    bool KillCommand(ProcmanCommandPtr cmd, int signum);

    /**
     * Adds a command to be managed by procman.  returns a pointer to a newly
     * created ProcmanCommand, or NULL on failure
     *
     * The command is not started.  To start a command running, use
     * procman_start_cmd
     */
    ProcmanCommandPtr AddCommand(const std::string& exec_str);

    /**
     * Removes a command from management by procman.
     *
     * If the command is not already stopped, then RemoveCommand() blocks and
     * waits for the command to stop running. RemoveCommand() does _not_ try to
     * actively stop the command by sending it a signal or anything like that.
     */
    void RemoveCommand(ProcmanCommandPtr cmd);

    /**
     * Checks to see if any processes have stopped running.
     *
     * If a child process has died, then a pointer to the dead command is
     * returned. Otherwise, an empty pointer is returned.
     *
     * This function does not block.
     */
    ProcmanCommandPtr CheckForStoppedCommands();

    /**
     * Cleans up resources used by the stopped command.
     *
     * Call this after a command has terminated but you don't want to remove
     * it (e.g., it might be started again later).
     *
     * This method is automatically called by RemoveCommand(), so if you're
     * removing a command then there's no need to explicitly call
     * CleanupStoppedCommand().
     */
    void CleanupStoppedCommand(ProcmanCommandPtr cmd);

    CommandStatus GetCommandStatus(ProcmanCommandPtr cmd);

    /* Changes the command that will be executed.
     *
     * This has no effect until the command is next started.
     */
    void SetCommandExecStr(ProcmanCommandPtr cmd, const std::string& exec_str);

  private:
    void CheckCommand(ProcmanCommandPtr cmd);
    std::vector<ProcmanCommandPtr> commands_;
    std::vector<ProcmanCommandPtr> dead_children_;
};

}  // namespace procman

#endif  // PROCMAN_PROCMAN_HPP__
