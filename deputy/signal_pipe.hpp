#ifndef PROCMAN_SIGNAL_PIPE_HPP__
#define PROCMAN_SIGNAL_PIPE_HPP__

namespace procman {

// initializes signal_pipe.  call this once per process.
int signal_pipe_init (void);

// cleans up resources used by the signal_pipe
int signal_pipe_cleanup (void);

// specifies that signal should be caught by signal_pipe and converted to a
// glib event
void signal_pipe_add_signal(int signum);

// When a POSIX signal is caught, the signal number is written to this fd as an
// integer.
int signal_pipe_fd(void);

}  // namespace procman

#endif  // PROCMAN_SIGNAL_PIPE_HPP__
