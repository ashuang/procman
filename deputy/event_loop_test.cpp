#include <assert.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "event_loop.hpp"

using procman::EventLoop;
using procman::TimerPtr;
using procman::Timer;
using procman::SocketNotifier;
using procman::SocketNotifierPtr;

void TestTimers() {
  EventLoop loop;

  int a_count = 0;
  int b_count = 0;
  int c_count = 0;

  TimerPtr timer_a = loop.AddTimer(10, EventLoop::kSingleShot, true,
      [&a_count]() { ++a_count; });

  TimerPtr timer_b = loop.AddTimer(40, EventLoop::kRepeating, true,
      [&b_count, &timer_b]() { ++b_count;
      });

  TimerPtr timer_c = loop.AddTimer(100, EventLoop::kSingleShot, true,
      [&c_count, &timer_b]() {
        ++c_count;
        timer_b.reset();
      });

  TimerPtr timer_d = loop.AddTimer(150, EventLoop::kSingleShot, true,
      [&loop]() { loop.Quit(); });

  loop.Run();

  assert(a_count == 1);
  assert(b_count == 2);
  assert(c_count == 1);
}

void TestSockets() {
  EventLoop loop;

  int fds[2];
  int pipe_status = pipe(fds);
  assert(0 == pipe_status);

  const int read_fd = fds[0];
  const int write_fd = fds[1];
  char read_buf[80];
  char write_buf[80];
  memset(read_buf, 0, sizeof(read_buf));
  memset(write_buf, 0, sizeof(write_buf));

  SocketNotifierPtr sock = loop.AddSocket(read_fd, EventLoop::kRead,
      [&read_fd, &read_buf, &loop]() {
        const int unused = read(read_fd, read_buf, sizeof(read_buf));
        (void) unused;
        loop.Quit();
      });

  const char* write_text = "hello";
  int num_write_bytes = strlen(write_text);
  strncpy(write_buf, write_text, sizeof(write_buf));
  const int unused = write(write_fd, write_buf, num_write_bytes);
  (void) unused;

  loop.Run();

  bool bufs_equal = 0 == memcmp(read_buf, write_buf, num_write_bytes);
  assert(bufs_equal);
}

void TestPosixSignals() {
  EventLoop loop;

  bool handled_signal = false;

  loop.SetPosixSignals({ SIGINT, SIGTERM },
      [&loop, &handled_signal](int signum) {
        handled_signal = true;
        loop.Quit();
      });

  TimerPtr kill_timer = loop.AddTimer(10, EventLoop::kSingleShot, true,
      [&loop]() {
        kill(getpid(), SIGINT);
      });

  loop.Run();
  assert(handled_signal);
}

int main(int argc, char** argv) {
  printf("TestTimers\n");
  TestTimers();
  printf("TestSockets\n");
  TestSockets();
  printf("TestPosixSignals\n");
  TestPosixSignals();

  printf("All tests passed\n");
}
