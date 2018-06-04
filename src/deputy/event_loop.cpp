#include "event_loop.hpp"

#include <assert.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <stdexcept>

//#define dbg(...) do { fprintf(stderr, __VA_ARGS__); fprintf(stderr, "\n"); } while(0)
#define dbg(...)

namespace procman {

static int g_signal_fds[2] = { -1, -1 };

static int64_t Now() {
    struct timeval tv;
    gettimeofday (&tv, NULL);
    return (int64_t) tv.tv_sec * 1000000 + tv.tv_usec;
}

bool EventLoop::TimerComparator::operator() (const Timer* lhs, const Timer* rhs) {
  return lhs->deadline_ < rhs->deadline_;
}

class SocketNotifier {
  public:
    ~SocketNotifier();

  private:
    SocketNotifier(int fd, EventLoop::EventType event_type,
        std::function<void()> callback,
        EventLoop* loop);

    friend class EventLoop;

    int fd_;
    EventLoop::EventType event_type_;
    std::function<void()> callback_;
    std::weak_ptr<SocketNotifier> weak_;
    EventLoop* loop_;
};

SocketNotifier::SocketNotifier(int fd, EventLoop::EventType event_type,
        std::function<void()> callback,
        EventLoop* loop) :
  fd_(fd),
  event_type_(event_type),
  callback_(callback),
  loop_(loop) {
}

SocketNotifier::~SocketNotifier() {
  dbg("Destroying socket notifier %p for %d\n", this, fd_);

  auto iter = std::find(loop_->sockets_.begin(), loop_->sockets_.end(), this);
  if (iter != loop_->sockets_.end()) {
    dbg("found in sockets_\n");
    loop_->sockets_.erase(iter);
  }

  // If the SocketNotifier being destroyed is a socket queued up for callback,
  // then zero out its place in the queue, but don't remove it to avoid messing
  // with queue iteration.
  auto ready_iter = std::find(loop_->sockets_ready_.begin(),
      loop_->sockets_ready_.end(), this);
  if (iter != loop_->sockets_ready_.end()) {
    *ready_iter = nullptr;
  }
  fd_ = -1;
}

Timer::Timer(int interval_ms,
        EventLoop::TimerType timer_type,
        bool active,
        std::function<void()> callback,
        EventLoop* loop) :
  interval_ms_(interval_ms),
  timer_type_(timer_type),
  active_(false),
  callback_(callback),
  deadline_(0),
  loop_(loop) {
  if (active) {
    Start();
  } else {
    loop_->inactive_timers_.insert(this);
  }
}

Timer::~Timer() {
  if (loop_) {
    loop_->active_timers_.erase(this);
    loop_->timers_to_reschedule_.erase(this);
    loop_->inactive_timers_.erase(this);
    loop_ = nullptr;
  }
  active_ = false;
}

void Timer::SetTimerType(EventLoop::TimerType timer_type) {
  timer_type_ = timer_type;
}

void Timer::SetInterval(int interval_ms) {
  interval_ms_ = interval_ms;
  if (active_) {
    Stop();
    Start();
  }
}

void Timer::Start() {
  dbg("start: %p (%d, %p)", this, interval_ms_, loop_);
  if (active_) {
    return;
  }
  if (!loop_) {
    throw std::runtime_error("Event loop is gone");
  }

  // Search for and remove the timer from the inactive timer set.
  loop_->inactive_timers_.erase(this);

  // Also remove the timer from the reschedule set.
  loop_->timers_to_reschedule_.erase(this);

  // Move the timer onto the active set.
  deadline_ = Now() + interval_ms_ * 1000;
  loop_->active_timers_.insert(this);
  active_ = true;
}

void Timer::Stop() {
  if (!active_) {
    return;
  }
  if (!loop_) {
    throw std::runtime_error("Event loop is gone");
  }

  // Search for and remove the timer from the active timer set
  loop_->active_timers_.erase(this);

  // Also remove the timer from the reschedule queue.
  loop_->timers_to_reschedule_.erase(this);

  // Move the timer onto the inactive timer set.
  loop_->inactive_timers_.insert(this);
  active_ = false;
}

EventLoop::EventLoop() :
  quit_(false) {}

EventLoop::~EventLoop() {
  // Detach all outstanding timers from the event loop
  for (Timer* timer : active_timers_) {
    timer->loop_ = nullptr;
  }
  for (Timer* timer : inactive_timers_) {
    timer->loop_ = nullptr;
  }

  if (g_signal_fds[0] != -1) {
    close(g_signal_fds[0]);
    close(g_signal_fds[1]);
    g_signal_fds[0] = -1;
    g_signal_fds[1] = -1;
  }
}

SocketNotifierPtr EventLoop::AddSocket(int fd,
    EventType event_type, std::function<void()> callback) {
  if (event_type != kRead &&
      event_type != kWrite &&
      event_type != kError) {
    throw std::invalid_argument("Invalid socket event type");
  }
  SocketNotifier* notifier = new SocketNotifier(fd, event_type, callback, this);
  sockets_.push_back(notifier);
  return SocketNotifierPtr(notifier);
}

TimerPtr EventLoop::AddTimer(int interval_ms, TimerType timer_type,
    bool active, std::function<void()> callback) {
  TimerPtr result(new Timer(interval_ms, timer_type, active, callback, this));
  return result;
}

static void signal_handler (int signum) {
  int wstatus = write(g_signal_fds[1], &signum, sizeof(int));
  (void) wstatus;
}

void EventLoop::SetPosixSignals(const std::vector<int>& signums,
    std::function<void(int signum)> callback) {
  if (g_signal_fds[0] != -1) {
    throw std::runtime_error("EventLoop POSIX signals already set");
  }

  if (0 != pipe(g_signal_fds)) {
    throw std::runtime_error("Error initializing internal pipe for POSIX signals");
  }

  const int flags = fcntl(g_signal_fds[1], F_GETFL);
  fcntl(g_signal_fds[1], F_SETFL, flags | O_NONBLOCK);

  for (int signum : signums) {
    signal(signum, signal_handler);
  }

  posix_signal_notifier_ = AddSocket(g_signal_fds[0], kRead,
      [this, callback]() {
        int signum;
        const int unused = read(g_signal_fds[0], &signum, sizeof(int));
        (void) unused;
        callback(signum);
      });
}

void EventLoop::Run() {
  while (!quit_) {
    IterateOnce();
  }
}

void EventLoop::Quit() {
  quit_ = true;
}

void EventLoop::IterateOnce() {
  dbg("IterateOnce - timers: %d / %d / %d sockets: %d",
      (int)active_timers_.size(),
      (int)timers_to_reschedule_.size(),
      (int)inactive_timers_.size(),
      (int)sockets_.size());
  // Calculate next timer wakeup time.
  Timer* first_timer =
    active_timers_.empty() ? nullptr : *active_timers_.begin();

  if (!sockets_.empty()) {
    // If there are sockets in the event loop, then check them
    int timeout_ms = -1;
    if (first_timer) {
      timeout_ms = std::max(0,
          static_cast<int>((first_timer->deadline_ - Now()) / 1000));
    }

    // Prepare pollfd structure
    const int num_sockets = sockets_.size();
    struct pollfd* pfds = new struct pollfd[num_sockets];
    for (int index = 0; index < num_sockets; ++index) {
      pfds[index].fd = sockets_[index]->fd_;
      switch (sockets_[index]->event_type_) {
        case kRead:
          pfds[index].events = POLLIN;
          break;
        case kWrite:
          pfds[index].events = POLLOUT;
          break;
        case kError:
          pfds[index].events = POLLERR;
          break;
        default:
          pfds[index].events = POLLIN;
          break;
      }
      pfds[index].revents = 0;
    }

    // poll sockets for the maximum wait time.
    dbg("poll timeout: %d", timeout_ms);
    const int num_sockets_ready = poll(pfds, num_sockets, timeout_ms);

    // Check which sockets are ready, and queue them up for invoking callbacks.
    if (num_sockets_ready) {
      for (int index = 0; index < num_sockets; ++index) {
        struct pollfd* pfd = &pfds[index];
        if (pfd->revents & pfd->events) {
          dbg("marking socket notifier %p (%d) for callback",
              sockets_[index], pfd->fd);
          sockets_ready_.push_back(sockets_[index]);
        }
      }
    }
    // Call callbacks for sockets that are ready
    for (int index = 0; index < sockets_ready_.size(); ++index) {
      SocketNotifier* notifier = sockets_ready_[index];
      if (!notifier) {
        continue;
      }
      notifier->callback_();
    }
    sockets_ready_.clear();

    delete[] pfds;
  } else if (first_timer) {
    // If there aren't any sockets to wait on, and there's at least one timer,
    // then wait for that timer.
    const int64_t wait_usec = first_timer->deadline_ - Now();
    if (wait_usec > 0) {
      usleep(wait_usec);
    }
  }

  ProcessReadyTimers();
}

void EventLoop::ProcessReadyTimers() {
  // Process timers that are ready - call their callback functions, and then
  // reschedule the timers if appropriate.
  Timer* first_timer =
    active_timers_.empty() ?  nullptr : *active_timers_.begin();

  const int64_t process_time = Now();
  while (first_timer && first_timer->deadline_ < process_time && !quit_) {
    active_timers_.erase(first_timer);
    timers_to_reschedule_.insert(first_timer);
    first_timer->callback_();
    first_timer = active_timers_.empty() ? nullptr : *active_timers_.begin();
  }

  // Move timers to either the active or inactive set.
  const int64_t reschedule_base = Now();
  for (Timer* timer : timers_to_reschedule_) {
    if (timer->timer_type_ == EventLoop::kSingleShot || (!timer->active_)) {
      timer->active_ = false;
      inactive_timers_.insert(timer);
    } else {
      timer->deadline_ = reschedule_base + timer->interval_ms_ * 1000;
      active_timers_.insert(timer);
    }
  }
  timers_to_reschedule_.clear();
}

}  // namespace procman
