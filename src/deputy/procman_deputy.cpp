#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <getopt.h>
#include <libgen.h>
#include <sys/poll.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <signal.h>
#include <stdarg.h>
#include <time.h>
#include <sys/time.h>
#include <inttypes.h>
#include <errno.h>

#include <map>
#include <set>

#include <QCoreApplication>
#include <QTimer>

#include <lcmtypes/procman_lcm/output_t.hpp>

#include "signal_pipe.hpp"

#include "procman_deputy.hpp"

using procman_lcm::cmd_desired_t;
using procman_lcm::deputy_info_t;
using procman_lcm::output_t;
using procman_lcm::discovery_t;
using procman_lcm::deputy_info_t;
using procman_lcm::orders_t;

namespace procman {

#define ESTIMATED_MAX_CLOCK_ERROR_RATE 1.001

#define MIN_RESPAWN_DELAY_MS 10
#define MAX_RESPAWN_DELAY_MS 1000
#define RESPAWN_BACKOFF_RATE 2
#define DISCOVERY_TIME_MS 1500

#define DEFAULT_STOP_SIGNAL 2
#define DEFAULT_STOP_TIME_ALLOWED 7

#define PROCMAN_MAX_MESSAGE_AGE_USEC 60000000LL

#define dbg(args...) fprintf(stderr, args)
//#undef dbg
//#define dbg(args...)

static int64_t timestamp_now() {
  struct timeval tv;
  gettimeofday (&tv, NULL);
  return (int64_t) tv.tv_sec * 1000000 + tv.tv_usec;
}

static void dbgt (const char *fmt, ...) {
  va_list ap;
  va_start (ap, fmt);

  char timebuf[80];
  struct timeval now_tv;
  gettimeofday(&now_tv, NULL);
  struct tm now_tm;
  localtime_r(&now_tv.tv_sec, &now_tm);
  int pos = strftime(timebuf, sizeof(timebuf), "%FT%T", &now_tm);
  pos += snprintf(timebuf + pos, sizeof(timebuf)-pos, ".%03d", (int)(now_tv.tv_usec / 1000));
  strftime(timebuf + pos, sizeof(timebuf)-pos, "%z", &now_tm);

  char buf[4096];
  vsnprintf (buf, sizeof(buf), fmt, ap);

  va_end (ap);

  fprintf (stderr, "%s %s", timebuf, buf);
}

DeputyOptions DeputyOptions::Defaults() {
  DeputyOptions result;

  char buf[256];
  memset(buf, 0, sizeof(buf));
  gethostname(buf, sizeof(buf) - 1);
  result.name = buf;

  result.verbose = false;
  return result;
}

ProcmanDeputy::ProcmanDeputy(const DeputyOptions& options, QObject* parent) :
  QObject(parent),
  options_(options),
  deputy_name_(options.name),
  discovery_subs_(nullptr),
  info2_subs_(nullptr),
  orders2_subs_(nullptr),
  discovery_timer_(),
  posix_signal_notifier_(nullptr),
  lcm_notifier_(nullptr) {
  deputy_start_time_ = timestamp_now();
  deputy_pid_ = getpid();
  pm_ = new Procman(ProcmanOptions::Default());

  // Initialize LCM
  lcm_ = new lcm::LCM(options.lcm_url);
  if (!lcm_) {
    throw std::runtime_error("error initializing LCM.");
  }

  // Setup initial LCM subscriptions
  info2_subs_ = lcm_->subscribe("PM_INFO", &ProcmanDeputy::InfoReceived, this);

  discovery_subs_ = lcm_->subscribe("PM_DISCOVER",
      &ProcmanDeputy::DiscoveryReceived, this);

  // Setup Qt timers

  // TODO
  discovery_timer_.setInterval(200);
  connect(&discovery_timer_, &QTimer::timeout,
      this, &ProcmanDeputy::OnDiscoveryTimer);
  discovery_timer_.start();
  OnDiscoveryTimer();

  // TODO
  one_second_timer_.setInterval(1000);
  connect(&one_second_timer_, &QTimer::timeout,
      this, &ProcmanDeputy::OnOneSecondTimer);

  // periodically check memory usage
  introspection_timer_.setInterval(120000);
  connect(&introspection_timer_, &QTimer::timeout,
      this, &ProcmanDeputy::OnIntrospectionTimer);

  posix_signal_notifier_ = new QSocketNotifier(signal_pipe_fd(),
      QSocketNotifier::Read, this);
  connect(posix_signal_notifier_, &QSocketNotifier::activated,
      this, &ProcmanDeputy::OnPosixSignal);

  lcm_notifier_ = new QSocketNotifier(lcm_->getFileno(),
      QSocketNotifier::Read, this);
  connect(lcm_notifier_, &QSocketNotifier::activated,
      [this]() { lcm_->handle(); });
}

ProcmanDeputy::~ProcmanDeputy() {
  // unsubscribe
  if(orders2_subs_) {
    lcm_->unsubscribe(orders2_subs_);
  }
  if(info2_subs_) {
    lcm_->unsubscribe(info2_subs_);
  }
  if(discovery_subs_) {
    lcm_->unsubscribe(discovery_subs_);
  }
  delete lcm_;
  delete pm_;
}

void ProcmanDeputy::TransmitStr(int sheriff_id, const char* str) {
  output_t msg;
  msg.deputy_name = deputy_name_;
  msg.sheriff_id = sheriff_id;
  msg.text = str;
  msg.utime = timestamp_now ();
  lcm_->publish("PM_OUTPUT", &msg);
}

void ProcmanDeputy::PrintfAndTransmit(int sheriff_id, const char *fmt, ...) {
  int len;
  char buf[256];
  va_list ap;
  va_start (ap, fmt);

  len = vsnprintf (buf, sizeof (buf), fmt, ap);
  if (options_.verbose)
    fputs (buf, stderr);

  if (len) {
    output_t msg;
    msg.deputy_name = deputy_name_;
    msg.sheriff_id = sheriff_id;
    msg.text = buf;
    msg.utime = timestamp_now ();
    lcm_->publish("PM_OUTPUT", &msg);
  } else {
    dbgt ("uh oh.  printf_and_transmit printed zero bytes\n");
  }
}

// invoked when a child process writes something to its stdout/stderr fd
void ProcmanDeputy::OnProcessOutputAvailable(DeputyCommand* mi) {
  ProcmanCommandPtr cmd = mi->cmd;
  int anycondition = 0;

  char buf[1024];
  int bytes_read = read(cmd->StdoutFd(), buf, sizeof (buf)-1);
  if (bytes_read < 0) {
    snprintf (buf, sizeof (buf), "procman [%s] read: %s (%d)\n",
        cmd->ExecStr().c_str(), strerror (errno), errno);
    dbgt (buf);
    TransmitStr(cmd->SheriffId(), buf);
  } else {
    // TODO buffer output
    buf[bytes_read] = '\0';
    TransmitStr(cmd->SheriffId(), buf);
  }
  anycondition = 1;
}

void ProcmanDeputy::MaybeScheduleRespawn(DeputyCommand *mi) {
  if(mi->auto_respawn && !mi->should_be_stopped && !exiting_) {
    mi->respawn_timer_.start(mi->respawn_backoff);
  }
}

int ProcmanDeputy::StartCommand(DeputyCommand* mi, int desired_runid) {
    if(exiting_) {
        return -1;
    }

    ProcmanCommandPtr cmd = mi->cmd;

    int status;
    mi->should_be_stopped = 0;
    mi->respawn_timer_.stop();

    // update the respawn backoff counter, to throttle how quickly a
    // process respawns
    int ms_since_started = (timestamp_now() - mi->last_start_time) / 1000;
    if(ms_since_started < MAX_RESPAWN_DELAY_MS) {
        mi->respawn_backoff = std::min(MAX_RESPAWN_DELAY_MS,
                mi->respawn_backoff * RESPAWN_BACKOFF_RATE);
    } else {
        int d = ms_since_started / MAX_RESPAWN_DELAY_MS;
        mi->respawn_backoff = std::max(MIN_RESPAWN_DELAY_MS,
                mi->respawn_backoff >> d);
    }
    mi->last_start_time = timestamp_now();

    status = pm_->StartCommand(cmd);
    if (0 != status) {
        PrintfAndTransmit(0, "[%s] couldn't start [%s]\n", cmd->Id().c_str(), cmd->ExecStr().c_str());
        dbgt ("[%s] couldn't start [%s]\n", cmd->Id().c_str(), cmd->ExecStr().c_str());
        MaybeScheduleRespawn(mi);
        PrintfAndTransmit(cmd->SheriffId(),
                "ERROR!  [%s] couldn't start [%s]\n", cmd->Id().c_str(), cmd->ExecStr().c_str());
        return -1;
    }

    mi->stdout_notifier = new QSocketNotifier(cmd->StdoutFd(),
        QSocketNotifier::Read, this);
    fcntl(cmd->StdoutFd(), F_SETFL, O_NONBLOCK);
    connect(mi->stdout_notifier, &QSocketNotifier::activated,
        std::bind(&ProcmanDeputy::OnProcessOutputAvailable, this, mi));

    mi->actual_runid = desired_runid;
    mi->num_kills_sent = 0;
    mi->first_kill_time = 0;
    return 0;
}

int ProcmanDeputy::StopCommand(DeputyCommand* mi) {
  ProcmanCommandPtr cmd = mi->cmd;

  if (!cmd->Pid()) return 0;

  mi->should_be_stopped = 1;

  if (mi->respawn_timer_.isActive()) {
    mi->respawn_timer_.stop();
  }

  int64_t now = timestamp_now();
  int64_t sigkill_time = mi->first_kill_time + (int64_t)(mi->stop_time_allowed * 1000000);
  int status;
  if(!mi->first_kill_time) {
    status = pm_->KillCommmand(cmd, mi->stop_signal);
    mi->first_kill_time = now;
    mi->num_kills_sent++;
  } else if(now > sigkill_time) {
    status = pm_->KillCommmand(cmd, SIGKILL);
  } else {
    return 0;
  }

  if (0 != status) {
    PrintfAndTransmit(cmd->SheriffId(),
        "kill: %s\n", strerror (-status));
  }
  return status;
}

void ProcmanDeputy::CheckForDeadChildren() {
  ProcmanCommandPtr cmd = pm_->CheckForDeadChildren();

  while (cmd) {
    int status;
    DeputyCommand *mi = commands_[cmd];

    // check the stdout pipes to see if there is anything from stdout /
    // stderr.
    struct pollfd pfd = {
      cmd->StdoutFd(),
      POLLIN,
      0
    };
    status = poll (&pfd, 1, 0);
    if (pfd.revents & POLLIN) {
      OnProcessOutputAvailable(mi);
    }

    // did the child terminate with a signal?
    int exit_status = cmd->ExitStatus();
    if (WIFSIGNALED (exit_status)) {
      int signum = WTERMSIG (exit_status);

      PrintfAndTransmit(cmd->SheriffId(),
          "%s\n",
          strsignal (signum), signum);
      if (WCOREDUMP (exit_status)) {
        PrintfAndTransmit(cmd->SheriffId(), "Core dumped.\n");
      }
    }

    if (mi->stdout_notifier) {
      delete mi->stdout_notifier;
      mi->stdout_notifier = nullptr;

      pm_->CloseDeadPipes(cmd);
    }

    // remove ?
    if (mi->remove_requested) {
      dbgt ("[%s] remove\n", cmd->Id().c_str());
      // cleanup the private data structure used
      commands_.erase(cmd);
      pm_->RemoveCommand(cmd);
      delete mi;
    } else {
      MaybeScheduleRespawn(mi);
    }

    cmd = pm_->CheckForDeadChildren();
    TransmitProcInfo();
  }
}

void ProcmanDeputy::OnQuitTimer() {
  for (auto& item : commands_) {
    DeputyCommand* mi = item.second;
    ProcmanCommandPtr cmd = item.first;
    if (cmd->Pid()) {
      pm_->KillCommmand(cmd, SIGKILL);
    }
    commands_.erase(cmd);
    pm_->RemoveCommand(cmd);
    delete mi;
  }

  dbgt ("stopping deputy main loop\n");
  QCoreApplication::instance()->quit();
}

void ProcmanDeputy::TransmitProcInfo() {
  // build a deputy info message
  deputy_info_t msg;
  msg.utime = timestamp_now();
  msg.host = deputy_name_;
  msg.cpu_load = cpu_load_;
  msg.phys_mem_total_bytes = cpu_time_[1].memtotal;
  msg.phys_mem_free_bytes = cpu_time_[1].memfree;
  msg.swap_total_bytes = cpu_time_[1].swaptotal;
  msg.swap_free_bytes = cpu_time_[1].swapfree;

  msg.ncmds = commands_.size();
  msg.cmds.resize(msg.ncmds);
  msg.num_options = 0;

  int cmd_index = 0;
  for (auto& item : commands_) {
    ProcmanCommandPtr cmd = item.first;
    DeputyCommand* mi = item.second;

    msg.cmds[cmd_index].cmd.exec_str = cmd->ExecStr();
    msg.cmds[cmd_index].cmd.command_id = cmd->Id();
    msg.cmds[cmd_index].cmd.group = mi->group_;
    msg.cmds[cmd_index].cmd.auto_respawn = mi->auto_respawn;
    msg.cmds[cmd_index].cmd.stop_signal = mi->stop_signal;
    msg.cmds[cmd_index].cmd.stop_time_allowed = mi->stop_time_allowed;
    msg.cmds[cmd_index].cmd.num_options = 0;
    msg.cmds[cmd_index].cmd.option_names.clear();
    msg.cmds[cmd_index].cmd.option_values.clear();
    msg.cmds[cmd_index].actual_runid = mi->actual_runid;
    msg.cmds[cmd_index].pid = cmd->Pid();
    msg.cmds[cmd_index].exit_code = cmd->ExitStatus();
    msg.cmds[cmd_index].sheriff_id = cmd->SheriffId();
    msg.cmds[cmd_index].cpu_usage = mi->cpu_usage;
    msg.cmds[cmd_index].mem_vsize_bytes = mi->cpu_time[1].vsize;
    msg.cmds[cmd_index].mem_rss_bytes = mi->cpu_time[1].rss;
    cmd_index++;
  }

  if (options_.verbose) {
    dbgt ("transmitting deputy info!\n");
  }
  lcm_->publish("PM_INFO", &msg);
}

void ProcmanDeputy::UpdateCpuTimes() {
  int status = procinfo_read_sys_cpu_mem (&cpu_time_[1]);
  if(0 != status) {
    perror("update_cpu_times - procinfo_read_sys_cpu_mem");
  }

  sys_cpu_mem_t *a = &cpu_time_[1];
  sys_cpu_mem_t *b = &cpu_time_[0];

  uint64_t elapsed_jiffies = a->user - b->user +
    a->user_low - b->user_low +
    a->system - b->system +
    a->idle - b->idle;
  uint64_t loaded_jiffies = a->user - b->user +
    a->user_low - b->user_low +
    a->system - b->system;
  if (! elapsed_jiffies || loaded_jiffies > elapsed_jiffies) {
    cpu_load_ = 0;
  } else {
    cpu_load_ = (double)loaded_jiffies / elapsed_jiffies;
  }

  for (auto& item : commands_) {
    ProcmanCommandPtr cmd = item.first;
    DeputyCommand* mi = item.second;

    if (cmd->Pid()) {
      status = procinfo_read_proc_cpu_mem (cmd->Pid(), &mi->cpu_time[1]);
      if (0 != status) {
        mi->cpu_usage = 0;
        mi->cpu_time[1].vsize = 0;
        mi->cpu_time[1].rss = 0;
        perror("update_cpu_times - procinfo_read_proc_cpu_mem");
        // TODO handle this error
      } else {
        proc_cpu_mem_t *pa = &mi->cpu_time[1];
        proc_cpu_mem_t *pb = &mi->cpu_time[0];

        uint64_t used_jiffies = pa->user - pb->user +
          pa->system - pb->system;

        if (! elapsed_jiffies || pb->user == 0 || pb->system == 0 ||
            used_jiffies > elapsed_jiffies) {
          mi->cpu_usage = 0;
        } else {
          mi->cpu_usage = (double)used_jiffies / elapsed_jiffies;
        }
      }
    } else {
      mi->cpu_usage = 0;
      mi->cpu_time[1].vsize = 0;
      mi->cpu_time[1].rss = 0;
    }

    memcpy (&mi->cpu_time[0], &mi->cpu_time[1], sizeof (proc_cpu_mem_t));
  }

  memcpy (&cpu_time_[0], &cpu_time_[1], sizeof (sys_cpu_mem_t));
}

void ProcmanDeputy::OnOneSecondTimer() {
  UpdateCpuTimes();
  TransmitProcInfo();
}

void ProcmanDeputy::OnIntrospectionTimer() {
  int mypid = getpid();
  proc_cpu_mem_t pinfo;
  int status = procinfo_read_proc_cpu_mem (mypid, &pinfo);
  if(0 != status)  {
    perror("introspection_timeout - procinfo_read_proc_cpu_mem");
  }

  int nrunning = 0;
  for (ProcmanCommandPtr cmd : pm_->GetCommands()) {
    if (cmd->Pid()) {
      nrunning++;
    }
  }

  dbgt ("MARK - rss: %" PRId64 " kB vsz: %" PRId64
      " kB procs: %d (%d alive)\n",
      pinfo.rss / 1024, pinfo.vsize / 1024,
      (int) commands_.size(),
      nrunning
      );
}

void ProcmanDeputy::OnPosixSignal() {
  int signum;
  int status = read(signal_pipe_fd(), &signum, sizeof(int));
  if (status != sizeof(int)) {
    return;
  }

  if (signum == SIGCHLD) {
    // a child process died.  check to see which one, and cleanup its
    // remains.
    CheckForDeadChildren();
  } else {
    // quit was requested.  kill all processes and quit
    dbgt ("received signal %d (%s).  stopping all processes\n", signum,
        strsignal (signum));

    float max_stop_time_allowed = 1;

    // first, send everything a SIGINT to give them a chance to exit
    // cleanly.
    for (auto& item : commands_) {
      DeputyCommand* mi = item.second;
      ProcmanCommandPtr cmd = item.first;
      if (cmd->Pid()) {
        pm_->KillCommmand(cmd, mi->stop_signal);
        if(mi->stop_time_allowed > max_stop_time_allowed)
          max_stop_time_allowed = mi->stop_time_allowed;
      }
    }
    exiting_ = true;

    // set a timer, after which everything will be more forcefully
    // terminated.
    QTimer* quit_timer = new QTimer(this);
    quit_timer->setSingleShot(true);
    quit_timer->start((int)(max_stop_time_allowed * 1000));
    connect(quit_timer, &QTimer::timeout, this, &ProcmanDeputy::OnQuitTimer);
  }

  if(exiting_) {
    // if we're exiting, and all child processes are dead, then exit.
    bool all_dead = true;
    for (ProcmanCommandPtr cmd : pm_->GetCommands()) {
      if (cmd->Pid()) {
        all_dead = false;
        break;
      }
    }
    if(all_dead) {
      dbg("all child processes are dead, exiting.\n");
      QCoreApplication::instance()->quit();
    }
  }
}

static const cmd_desired_t* OrdersFindCmd (const orders_t* orders,
    int32_t sheriff_id) {
  for (int i=0; i<orders->ncmds; i++) {
    if (sheriff_id == orders->cmds[i].sheriff_id) {
      return &orders->cmds[i];
    }
  }
  return nullptr;
}

void ProcmanDeputy::OrdersReceived(const lcm::ReceiveBuffer* rbuf, const std::string& channel,
    const orders_t* orders) {
  // ignore orders if we're exiting
  if (exiting_) {
    return;
  }

  // ignore orders for other deputies
  if (orders->host != deputy_name_) {
    if (options_.verbose)
      dbgt ("ignoring orders for other host %s\n", orders->host.c_str());
    return;
  }

  // ignore stale orders (where utime is too long ago)
  int64_t now = timestamp_now ();
  if (now - orders->utime > PROCMAN_MAX_MESSAGE_AGE_USEC) {
    for (int i=0; i<orders->ncmds; i++) {
      const cmd_desired_t *cmd_msg = &orders->cmds[i];
      PrintfAndTransmit(cmd_msg->sheriff_id,
          "ignoring stale orders (utime %d seconds ago). You may want to check the system clocks!\n",
          (int) ((now - orders->utime) / 1000000));
    }
    return;
  }

  // update variables
  pm_->RemoveAllVariables();
  //    for(int varind=0; varind<orders->nvars; varind++) {
  //        procman_set_variable(pm_, orders->varnames[varind], orders->varvals[varind]);
  //    }

  // attempt to carry out the orders
  int action_taken = 0;
  int i;
  if (options_.verbose)
    dbgt ("orders for me received with %d commands\n", orders->ncmds);
  for (i=0; i<orders->ncmds; i++) {
    const cmd_desired_t* cmd_msg = &orders->cmds[i];

    if (options_.verbose)
      dbgt ("order %d: %s (%d, %d)\n",
          i, cmd_msg->cmd.exec_str.c_str(),
          cmd_msg->desired_runid, cmd_msg->force_quit);

    // do we already have this command somewhere?
    DeputyCommand *mi = nullptr;
    for (auto& item : commands_) {
      if (item.first->Id() == cmd_msg->cmd.command_id) {
        mi = item.second;
        break;
      }
    }
    ProcmanCommandPtr cmd;

    if (mi) {
      cmd = mi->cmd;
    } else {
      // if not, then create it.
      if (options_.verbose) {
        dbgt ("adding new process (%s)\n", cmd_msg->cmd.exec_str.c_str());
      }
      cmd = pm_->AddCommand(cmd_msg->cmd.exec_str, cmd_msg->cmd.command_id);

      // allocate a private data structure
      mi = new DeputyCommand();
      mi->group_ = cmd_msg->cmd.group;
      mi->auto_respawn = cmd_msg->cmd.auto_respawn;
      mi->stop_signal = cmd_msg->cmd.stop_signal;
      mi->stop_time_allowed = cmd_msg->cmd.stop_time_allowed;
      mi->last_start_time = 0;
      mi->respawn_backoff = MIN_RESPAWN_DELAY_MS;
      mi->stdout_notifier = nullptr;
      pm_->SetCommandSheriffId(cmd, cmd_msg->sheriff_id);

      mi->respawn_timer_.setSingleShot(true);
      QObject::connect(&mi->respawn_timer_, &QTimer::timeout,
          [this, mi]() { 
          if(mi->auto_respawn && !mi->should_be_stopped && !exiting_) {
            StartCommand(mi, mi->actual_runid);
          }
          });

      mi->cmd = cmd;
      commands_[cmd] = mi;
      action_taken = 1;
    }

    // check if the command needs to be started or stopped
    CommandStatus cmd_status = pm_->GetCommandStatus(cmd);

    // rename a command?  does not kill a running command, so effect does
    // not apply until command is restarted.
    if (cmd->ExecStr() != cmd_msg->cmd.exec_str) {
      dbgt ("[%s] exec str -> [%s]\n", cmd->Id().c_str(),
          cmd_msg->cmd.exec_str.c_str());
      pm_->SetCommandExecStr(cmd, cmd_msg->cmd.exec_str);

      action_taken = 1;
    }

    // change a command's id?
    if (cmd_msg->cmd.command_id != cmd->Id()) {
      dbgt ("[%s] rename -> [%s]\n", cmd->Id().c_str(),
          cmd_msg->cmd.command_id.c_str());
      pm_->SetCommandId(cmd, cmd_msg->cmd.command_id);
      action_taken = 1;
    }

    // has auto-respawn changed?
    if (cmd_msg->cmd.auto_respawn != mi->auto_respawn) {
      dbgt ("[%s] auto-respawn -> %d\n", cmd->Id().c_str(),
          cmd_msg->cmd.auto_respawn);
      mi->auto_respawn = cmd_msg->cmd.auto_respawn;
    }

    // change the group of a command?
    if (cmd_msg->cmd.group != mi->group_) {
      dbgt ("[%s] group -> [%s]\n", cmd->Id().c_str(),
          cmd_msg->cmd.group.c_str());
      mi->group_ = cmd_msg->cmd.group;
      action_taken = 1;
    }

    // change the stop signal of a command?
    if(mi->stop_signal != cmd_msg->cmd.stop_signal) {
      dbg("[%s] stop signal -> [%d]\n", cmd->Id().c_str(),
          cmd_msg->cmd.stop_signal);
      mi->stop_signal = cmd_msg->cmd.stop_signal;
    }

    // change the stop time allowed of a command?
    if(mi->stop_time_allowed != cmd_msg->cmd.stop_time_allowed) {
      dbg("[%s] stop time allowed -> [%f]\n", cmd->Id().c_str(),
          cmd_msg->cmd.stop_time_allowed);
      mi->stop_time_allowed = cmd_msg->cmd.stop_time_allowed;
    }

    mi->should_be_stopped = cmd_msg->force_quit;

    if (PROCMAN_CMD_STOPPED == cmd_status &&
        (mi->actual_runid != cmd_msg->desired_runid) &&
        ! mi->should_be_stopped) {
      StartCommand(mi, cmd_msg->desired_runid);
      action_taken = 1;
    } else if (PROCMAN_CMD_RUNNING == cmd_status &&
        (mi->should_be_stopped || (cmd_msg->desired_runid != mi->actual_runid))) {
      StopCommand(mi);
      action_taken = 1;
    } else {
      mi->actual_runid = cmd_msg->desired_runid;
    }
  }

  // if there are any commands being managed that did not appear in the
  // orders, then stop and remove those commands
  std::vector<DeputyCommand*> toremove;
  for (auto& item : commands_) {
    DeputyCommand* mi = item.second;
    ProcmanCommandPtr cmd = item.first;
    const cmd_desired_t *cmd_msg = OrdersFindCmd (orders, cmd->SheriffId());

    if (! cmd_msg) {
      // push the orphaned command into a list first.  remove later, to
      // avoid corrupting the linked list (since this is a borrowed data
      // structure)
      toremove.push_back(mi);
    }
  }

  // cull orphaned commands
  for (DeputyCommand* mi : toremove) {
    ProcmanCommandPtr cmd = mi->cmd;

    if (cmd->Pid()) {
      dbgt ("[%s] scheduling removal\n", cmd->Id().c_str());
      mi->remove_requested = 1;
      StopCommand(mi);
    } else {
      dbgt ("[%s] remove\n", cmd->Id().c_str());
      // cleanup the private data structure used
      commands_.erase(cmd);
      pm_->RemoveCommand(cmd);
      delete mi;
    }

    action_taken = 1;
  }

  if (action_taken) {
    TransmitProcInfo();
  }
}

void ProcmanDeputy::DiscoveryReceived(const lcm::ReceiveBuffer* rbuf,
    const std::string& channel, const discovery_t* msg) {
  int64_t now = timestamp_now();
  if(now < deputy_start_time_ + DISCOVERY_TIME_MS * 1000) {
    // received a discovery message while still in discovery mode.  Check to
    // see if it's from a conflicting deputy.
    if(msg->host == deputy_name_ && msg->nonce != deputy_pid_) {
      dbgt("ERROR.  Detected another deputy named [%s].  Aborting to avoid conflicts.\n",
          msg->host.c_str());
      exit(1);
    }
  } else {
    // received a discovery message while not in discovery mode.  Respond by
    // transmitting deputy info.
    TransmitProcInfo();
  }
}

void ProcmanDeputy::InfoReceived(const lcm::ReceiveBuffer* rbuf,
    const std::string& channel, const deputy_info_t* msg) {
  int64_t now = timestamp_now();
  if(now < deputy_start_time_ + DISCOVERY_TIME_MS * 1000) {
    // A different deputy has reported while we're still in discovery mode.
    // Check to see if the deputy names are in conflict.
    if(msg->host == deputy_name_) {
      dbgt("ERROR.  Detected another deputy named [%s].  Aborting to avoid conflicts.\n",
          msg->host.c_str());
      exit(2);
    }
  } else {
    dbgt("WARNING:  Still processing info messages while not in discovery mode??\n");
  }
}

void ProcmanDeputy::OnDiscoveryTimer() {
  int64_t now = timestamp_now();
  if(now < deputy_start_time_ + DISCOVERY_TIME_MS * 1000) {
    // publish a discover message to check for conflicting deputies
    discovery_t msg;
    msg.utime = now;
    msg.host = deputy_name_;
    msg.nonce = deputy_pid_;
    lcm_->publish("PM_DISCOVER", &msg);
  } else {
    // discovery period is over.

    // Adjust subscriptions
    lcm_->unsubscribe(info2_subs_);
    info2_subs_ = NULL;

    orders2_subs_ = lcm_->subscribe("PM_ORDERS",
        &ProcmanDeputy::OrdersReceived, this);

    // start the timer to periodically transmit status information
    one_second_timer_.start();
    OnOneSecondTimer();

    discovery_timer_.stop();
  }
}

static void usage() {
    fprintf (stderr, "usage: procman-deputy [options]\n"
            "\n"
            "  -h, --help        shows this help text and exits\n"
            "  -v, --verbose     verbose output\n"
            "  -n, --name NAME   use deputy name NAME instead of hostname\n"
            "  -l, --log PATH    dump messages to PATH instead of stdout\n"
            "  -u, --lcmurl URL  use specified LCM URL for procman messages\n"
            "\n"
            "DEPUTY NAME\n"
            "  The deputy name must be unique from other deputies.  On startup,\n"
            "  if another deputy with the same name is detected, the newly started\n"
            "  deputy will self-terminate.\n"
            "\n"
            "EXIT STATUS\n"
            "  0   Clean exit on SIGINT, SIGTERM\n"
            "  1   OS or other networking error\n"
            "  2   Conflicting deputy detected on the network\n"
          );
}

}  // namespace procman

using namespace procman;

int main (int argc, char **argv) {
  const char *optstring = "hvfl:n:u:";
  int c;
  struct option long_opts[] = {
    { "help", no_argument, 0, 'h' },
    { "verbose", no_argument, 0, 'v' },
    { "log", required_argument, 0, 'l' },
    { "lcmurl", required_argument, 0, 'u' },
    { "name", required_argument, 0, 'n' },
    { 0, 0, 0, 0 }
  };

  QCoreApplication app(argc, argv);

  DeputyOptions dep_options = DeputyOptions::Defaults();
  char *logfilename = NULL;
  std::string hostname_override;

  while ((c = getopt_long (argc, argv, optstring, long_opts, 0)) >= 0) {
    switch (c) {
      case 'v':
        dep_options.verbose = true;
        break;
      case 'l':
        free(logfilename);
        logfilename = strdup (optarg);
        break;
      case 'u':
        dep_options.lcm_url = optarg;
        break;
      case 'n':
        hostname_override = optarg;
        break;
      case 'h':
      default:
        usage();
        return 1;
    }
  }

  // Add the directory containing procamn_deputy to PATH. This is mostly
  // for convenience.
  if (argc <= 0) {
    return 1;
  }
  char* argv0 = strdup(argv[0]);
  std::string newpath = std::string(dirname(argv0)) + ":" + getenv("PATH");
  printf("setting PATH to %s\n", newpath.c_str());
  setenv("PATH", newpath.c_str(), 1);
  free(argv0);

  // redirect stdout and stderr to a log file if the -l command line flag
  // was specified.
  if (logfilename) {
    int fd = open (logfilename, O_WRONLY | O_APPEND | O_CREAT, 0644);
    if (fd < 0) {
      perror ("open");
      fprintf (stderr, "couldn't open logfile %s\n", logfilename);
      return 1;
    }
    close(1);
    close(2);
    if (dup2(fd, 1) < 0) { return 1; }
    if (dup2(fd, 2) < 0) { return 1; }
    close (fd);
    setlinebuf (stdout);
    setlinebuf (stderr);
  }

  // set deputy hostname to the system hostname
  if (!hostname_override.empty()) {
    dep_options.name = hostname_override;
  }

  // convert Unix signals into file descriptor writes
  signal_pipe_init();
  signal_pipe_add_signal(SIGINT);
  signal_pipe_add_signal(SIGHUP);
  signal_pipe_add_signal(SIGQUIT);
  signal_pipe_add_signal(SIGTERM);
  signal_pipe_add_signal(SIGCHLD);

  ProcmanDeputy pmd(dep_options);

  int app_status = app.exec();

  // cleanup
  signal_pipe_cleanup();

  return app_status;
}
