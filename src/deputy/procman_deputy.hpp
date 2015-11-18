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

struct DeputyOptions {
  static DeputyOptions Defaults();

  std::string name;
  bool verbose;
};

class ProcmanDeputy : public QObject {
  Q_OBJECT

  public:
    ProcmanDeputy(const DeputyOptions& options, QObject* parent = nullptr);

    ~ProcmanDeputy();

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

    lcm::LCM *lcm_;

    Procman *pm;

  private:
    void UpdateCpuTimes();

    void CheckForDeadChildren();

    void TransmitProcInfo();

    void MaybeScheduleRespawn(DeputyCommand *mi);

    int StartCommand(DeputyCommand* mi, int desired_runid);

    int StopCommand(DeputyCommand* mi);

    void TransmitStr(int sheriff_id, const char* str);

    void PrintfAndTransmit(int sheriff_id, const char *fmt, ...);

    DeputyOptions options_;

    std::string deputy_name_;

    sys_cpu_mem_t cpu_time_[2];
    float cpu_load_;

    int64_t deputy_start_time_;
    pid_t deputy_pid_;

    lcm::Subscription* discovery_subs_;
    lcm::Subscription* info2_subs_;
    lcm::Subscription* orders2_subs_;

    QTimer discovery_timer_;
    QTimer one_second_timer_;
    QTimer introspection_timer_;

    QSocketNotifier* posix_signal_notifier_;
    QSocketNotifier* lcm_notifier_;

    std::map<ProcmanCommandPtr, DeputyCommand*> commands_;

    bool exiting_;
};

}  // namespace procman

#endif  // PROCMAN_PROCMAN_DEPUTY_HPP__
