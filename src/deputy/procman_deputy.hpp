#ifndef PROCMAN_PROCMAN_DEPUTY_HPP__
#define PROCMAN_PROCMAN_DEPUTY_HPP__

#include <set>
#include <string>

#include <QObject>
#include <QSocketNotifier>
#include <QTimer>

#include <lcm/lcm-cpp.hpp>

#include <lcmtypes/procman_lcm/orders_t.hpp>
#include <lcmtypes/procman_lcm/discovery_t.hpp>
#include <lcmtypes/procman_lcm/deputy_info_t.hpp>

#include "procman.hpp"
#include "procinfo.hpp"

namespace procman {

class ProcmanDeputy;

struct DeputyCommand {
  ProcmanDeputy *deputy;

  ProcmanCommandPtr cmd;

  QSocketNotifier* stdout_notifier;

  int32_t actual_runid;
  int32_t should_be_stopped;

  proc_cpu_mem_t cpu_time[2];
  float cpu_usage;

  std::string group_;
  int auto_respawn;

  QTimer respawn_timer_;

  int64_t last_start_time;
  int respawn_backoff;

  int stop_signal;
  float stop_time_allowed;

  int num_kills_sent;
  int64_t first_kill_time;
  int remove_requested;
};

class ProcmanDeputy : public QObject {
  Q_OBJECT

  public:
    ProcmanDeputy(QObject* parent = nullptr);

    void OrdersReceived(const lcm::ReceiveBuffer* rbuf, const std::string& channel,
        const procman_lcm::orders_t* orders);
    void DiscoveryReceived(const lcm::ReceiveBuffer* rbuf,
        const std::string& channel, const procman_lcm::discovery_t* msg);
    void InfoReceived(const lcm::ReceiveBuffer* rbuf,
        const std::string& channel, const procman_lcm::deputy_info_t* msg);

    void Prepare();

    void OnDiscoveryTimer();

    void OnOneSecondTimer();

    void OnIntrospectionTimer();

    void OnQuitTimer();

    void OnPosixSignal();

    void OnProcessOutputAvailable(DeputyCommand* mi);

    Procman *pm;

    lcm::LCM *lcm_;

    std::string hostname;

    int norders_slm;       // total procman_lcm_orders_t observed Since Last MARK
    int norders_forme_slm; // total procman_lcm_orders_t for this deputy slm
    int nstale_orders_slm; // total stale procman_lcm_orders_t for this deputy slm

    std::set<std::string> observed_sheriffs_slm; // names of observed sheriffs slm

    int64_t deputy_start_time;
    lcm::Subscription* discovery_subs_;

    lcm::Subscription* info2_subs_;
    lcm::Subscription* orders2_subs_;

    pid_t deputy_pid;
    sys_cpu_mem_t cpu_time[2];
    float cpu_load;

    int verbose;
    int exiting;

    QTimer discovery_timer_;
    QTimer one_second_timer_;
    QTimer introspection_timer_;

    QSocketNotifier* posix_signal_notifier_;
    QSocketNotifier* lcm_notifier_;

    std::map<ProcmanCommandPtr, DeputyCommand*> commands_;
};

}  // namespace procman

#endif  // PROCMAN_PROCMAN_DEPUTY_HPP__
