#include <assert.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>
#include <signal.h>

#include "signal_pipe.hpp"

namespace procman {

#define dbg(args...)
//#define dbg(args...) fprintf(stderr, args)

static int g_fds[2] = { -1, -1 };
static bool g_sp_initialized = false;

int signal_pipe_init () {
  if (g_sp_initialized) {
    return 0;
  }

  if (0 != pipe(g_fds)) {
    perror("signal_pipe");
    return -1;
  }

  const int flags = fcntl (g_fds[1], F_GETFL);
  fcntl (g_fds[1], F_SETFL, flags | O_NONBLOCK);

  g_sp_initialized = true;

  dbg("signal_pipe: initialized\n");
  return 0;
}

int signal_pipe_cleanup () {
  if (g_sp_initialized) {
    close (g_fds[0]);
    close (g_fds[1]);
    g_sp_initialized = false;
    return 0;
  }

  dbg("signal_pipe: destroyed\n");
  return -1;
}

static void signal_handler (int signal) {
  dbg("signal_pipe: caught signal %d\n", signal);
  int wstatus = write(g_fds[1], &signal, sizeof(int));
  (void) wstatus;
}

void signal_pipe_add_signal (int sig) {
  signal (sig, signal_handler);
}

int signal_pipe_fd(void) {
  if (!g_sp_initialized) {
    signal_pipe_init();
  }
  return g_fds[0];
}

}  // namespace procman
