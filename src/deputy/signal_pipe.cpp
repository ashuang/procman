#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>
#include <signal.h>

#include "signal_pipe.hpp"

namespace procman {

#define dbg(args...) fprintf(stderr, args)
#undef dbg
#define dbg(args...)

typedef struct _signal_pipe {
  int fds[2];
} signal_pipe_t;

static signal_pipe_t g_sp;
static int g_sp_initialized = 0;

int signal_pipe_init () {
  if (g_sp_initialized) {
    fprintf(stderr, "signal_pipe already initialized!!\n");
    return -1;
  }

  if (0 != pipe (g_sp.fds)) {
    perror("signal_pipe");
    return -1;
  }

  int flags = fcntl (g_sp.fds[1], F_GETFL);
  fcntl (g_sp.fds[1], F_SETFL, flags | O_NONBLOCK);

  g_sp_initialized = 1;

  dbg("signal_pipe: initialized\n");
  return 0;
}

int signal_pipe_cleanup () {
  if (g_sp_initialized) {
    close (g_sp.fds[0]);
    close (g_sp.fds[1]);
    g_sp_initialized = 0;
    return 0;
  }

  dbg("signal_pipe: destroyed\n");
  return -1;
}

static void signal_handler (int signal) {
  dbg("signal_pipe: caught signal %d\n", signal);
  int wstatus = write (g_sp.fds[1], &signal, sizeof(int));
  (void) wstatus;
}

void signal_pipe_add_signal (int sig) {
  signal (sig, signal_handler);
}

int signal_pipe_fd(void) {
  return g_sp.fds[0];
}

}  // namespace procman
