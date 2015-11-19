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

struct DeputyCommand;

struct DeputyOptions {
  static DeputyOptions Defaults();

  std::string name;
  std::string lcm_url;
  bool verbose;
};

class ProcmanDeputy : public QObject {
  Q_OBJECT

  public:
    ProcmanDeputy(const DeputyOptions& options, QObject* parent = nullptr);
    ~ProcmanDeputy();

  private:
    void OrdersReceived(const lcm::ReceiveBuffer* rbuf, const std::string& channel,
        const procman_lcm::orders_t* orders);
    void DiscoveryReceived(const lcm::ReceiveBuffer* rbuf,
        const std::string& channel, const procman_lcm::discovery_t* msg);
    void InfoReceived(const lcm::ReceiveBuffer* rbuf,
        const std::string& channel, const procman_lcm::deputy_info_t* msg);

    void OnDiscoveryTimer();

    void OnOneSecondTimer();

    void OnIntrospectionTimer();

    void OnQuitTimer();

    void OnPosixSignal();

    void OnProcessOutputAvailable(DeputyCommand* mi);

    void UpdateCpuTimes();

    void CheckForDeadChildren();

    void TransmitProcInfo();

    void MaybeScheduleRespawn(DeputyCommand *mi);

    int StartCommand(DeputyCommand* mi, int desired_runid);

    int StopCommand(DeputyCommand* mi);

    void TransmitStr(int sheriff_id, const char* str);

    void PrintfAndTransmit(int sheriff_id, const char *fmt, ...);

    DeputyOptions options_;

    Procman *pm_;

    lcm::LCM *lcm_;

    std::string deputy_name_;

    sys_cpu_mem_t cpu_time_[2];
    float cpu_load_;

    int64_t deputy_start_time_;
    pid_t deputy_pid_;

    lcm::Subscription* discovery_sub_;
    lcm::Subscription* info_sub_;
    lcm::Subscription* orders_sub_;

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
