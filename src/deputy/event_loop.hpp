#ifndef PROCMAN_EVENT_LOOP_HPP__
#define PROCMAN_EVENT_LOOP_HPP__

#include <memory>
#include <set>
#include <vector>
#include <functional>

namespace procman {

class EventLoop;

class SocketNotifier;

class Timer;

typedef std::shared_ptr<SocketNotifier> SocketNotifierPtr;

typedef std::shared_ptr<Timer> TimerPtr;

class EventLoop {
  public:
    enum EventType {
      kRead,
      kWrite,
      kError
    };

    enum TimerType {
      kSingleShot,
      kRepeating
    };

    EventLoop();

    ~EventLoop();

    SocketNotifierPtr AddSocket(int fd, EventType event_type,
        std::function<void()> callback);

    TimerPtr AddTimer(int interval_ms, TimerType timer_type,
        bool active, std::function<void()> callback);

    void SetPosixSignals(const std::vector<int>& signums,
        std::function<void(int signum)> callback);

    void Run();

    void Quit();

    void IterateOnce();

  private:
    void ProcessReadyTimers();

    friend class Timer;

    friend class SocketNotifier;

    struct TimerComparator {
      bool operator() (const Timer* lhs, const Timer* rhs);
    };

    bool quit_;

    std::set<Timer*, TimerComparator> active_timers_;

    std::set<Timer*> timers_to_reschedule_;

    std::set<Timer*> inactive_timers_;

    std::vector<SocketNotifier*> sockets_;

    std::vector<SocketNotifier*> sockets_ready_;

    SocketNotifierPtr posix_signal_notifier_;
};

class Timer {
  public:
    ~Timer();

    void SetTimerType(EventLoop::TimerType timer_type);

    void SetInterval(int interval_ms);

    void Start();

    void Stop();

    bool IsActive() const { return active_; }

  private:

    Timer(int interval_ms,
        EventLoop::TimerType timer_type,
        bool active,
        std::function<void()> callback,
        EventLoop* loop);

    friend class EventLoop;

    std::function<void()> callback_;
    EventLoop::TimerType timer_type_;
    bool active_;
    int interval_ms_;

    int64_t deadline_;
    EventLoop* loop_;
};

}  // namespace procman

#endif  // PROCMAN_EVENT_LOOP_HPP__
