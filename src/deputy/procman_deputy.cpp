#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <getopt.h>
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

static int64_t timestamp_now()
{
    struct timeval tv;
    gettimeofday (&tv, NULL);
    return (int64_t) tv.tv_sec * 1000000 + tv.tv_usec;
}

static void dbgt (const char *fmt, ...)
{
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

ProcmanDeputy::ProcmanDeputy(QObject* parent) :
  QObject(parent),
  discovery_timer_(),
  posix_signal_notifier_(nullptr),
  lcm_notifier_(nullptr) {

}

void ProcmanDeputy::Prepare() {
  discovery_timer_.setInterval(200);
  connect(&discovery_timer_, &QTimer::timeout,
      this, &ProcmanDeputy::OnDiscoveryTimer);
  discovery_timer_.start();
  OnDiscoveryTimer();

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

// make this global so that the signal handler can access it
static ProcmanDeputy* g_pmd;

static void
transmit_proc_info (ProcmanDeputy *s);

static void
transmit_str (ProcmanDeputy *pmd, int sheriff_id, const char * str)
{
  output_t msg;
  msg.deputy_name = pmd->hostname;
  msg.sheriff_id = sheriff_id;
  msg.text = str;
  msg.utime = timestamp_now ();
  pmd->lcm_->publish("PM_OUTPUT", &msg);
}

static void
printf_and_transmit (ProcmanDeputy *pmd, int sheriff_id, const char *fmt, ...) {
    int len;
    char buf[256];
    va_list ap;
    va_start (ap, fmt);

    len = vsnprintf (buf, sizeof (buf), fmt, ap);
    if (pmd->verbose)
        fputs (buf, stderr);

    if (len) {
        output_t msg;
        msg.deputy_name = pmd->hostname;
        msg.sheriff_id = sheriff_id;
        msg.text = buf;
        msg.utime = timestamp_now ();
        pmd->lcm_->publish("PM_OUTPUT", &msg);
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
    transmit_str (g_pmd, cmd->SheriffId(), buf);
  } else {
    // TODO buffer output
    buf[bytes_read] = '\0';
    transmit_str(this, cmd->SheriffId(), buf);
  }
  anycondition = 1;
}

static void
maybe_schedule_respawn(ProcmanDeputy *pmd, DeputyCommand *mi)
{
  if(mi->auto_respawn && !mi->should_be_stopped && !pmd->exiting) {
    mi->respawn_timer_.start(mi->respawn_backoff);
  }
}

static int
start_cmd (ProcmanDeputy *pmd, DeputyCommand* mi, int desired_runid)
{
    if(pmd->exiting) {
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

    status = pmd->pm->StartCommand(cmd);
    if (0 != status) {
        printf_and_transmit (pmd, 0, "[%s] couldn't start [%s]\n", cmd->Id().c_str(), cmd->ExecStr().c_str());
        dbgt ("[%s] couldn't start [%s]\n", cmd->Id().c_str(), cmd->ExecStr().c_str());
        maybe_schedule_respawn(pmd, mi);
        printf_and_transmit (pmd, cmd->SheriffId(),
                "ERROR!  [%s] couldn't start [%s]\n", cmd->Id().c_str(), cmd->ExecStr().c_str());
        return -1;
    }

    mi->stdout_notifier = new QSocketNotifier(cmd->StdoutFd(),
        QSocketNotifier::Read, pmd);
    fcntl(cmd->StdoutFd(), F_SETFL, O_NONBLOCK);
    pmd->connect(mi->stdout_notifier, &QSocketNotifier::activated,
        std::bind(&ProcmanDeputy::OnProcessOutputAvailable, pmd, mi));

    mi->actual_runid = desired_runid;
    mi->num_kills_sent = 0;
    mi->first_kill_time = 0;
    return 0;
}

static int
stop_cmd (ProcmanDeputy *pmd, DeputyCommand* mi)
{
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
        status = pmd->pm->KillCommmand(cmd, mi->stop_signal);
        mi->first_kill_time = now;
        mi->num_kills_sent++;
    } else if(now > sigkill_time) {
        status = pmd->pm->KillCommmand(cmd, SIGKILL);
    } else {
        return 0;
    }

    if (0 != status) {
        printf_and_transmit (pmd, cmd->SheriffId(),
                "kill: %s\n", strerror (-status));
    }
    return status;
}

static void
check_for_dead_children (ProcmanDeputy *pmd)
{
  ProcmanCommandPtr cmd = pmd->pm->CheckForDeadChildren();

  while (cmd) {
    int status;
    DeputyCommand *mi = pmd->commands_[cmd];

    // check the stdout pipes to see if there is anything from stdout /
    // stderr.
    struct pollfd pfd = {
      cmd->StdoutFd(),
      POLLIN,
      0
    };
    status = poll (&pfd, 1, 0);
    if (pfd.revents & POLLIN) {
      pmd->OnProcessOutputAvailable(mi);
    }

    // did the child terminate with a signal?
    int exit_status = cmd->ExitStatus();
    if (WIFSIGNALED (exit_status)) {
      int signum = WTERMSIG (exit_status);

      printf_and_transmit (pmd, cmd->SheriffId(),
          "%s\n",
          strsignal (signum), signum);
      if (WCOREDUMP (exit_status)) {
        printf_and_transmit (pmd, cmd->SheriffId(), "Core dumped.\n");
      }
    }

    if (mi->stdout_notifier) {
      delete mi->stdout_notifier;
      mi->stdout_notifier = nullptr;

      pmd->pm->CloseDeadPipes(cmd);
    }

    // remove ?
    if (mi->remove_requested) {
      dbgt ("[%s] remove\n", cmd->Id().c_str());
      // cleanup the private data structure used
      delete mi;
      pmd->pm->RemoveCommand(cmd);
    } else {
      maybe_schedule_respawn(pmd, mi);
    }

    cmd = pmd->pm->CheckForDeadChildren();
    transmit_proc_info (pmd);
  }
}

void ProcmanDeputy::OnQuitTimer() {
  for (auto& item : commands_) {
    DeputyCommand* mi = item.second;
    ProcmanCommandPtr cmd = item.first;
    if (cmd->Pid()) {
      pm->KillCommmand(cmd, SIGKILL);
    }
    delete mi;
    pm->RemoveCommand(cmd);
  }

  dbgt ("stopping deputy main loop\n");
  QCoreApplication::instance()->quit();
}

static void
transmit_proc_info (ProcmanDeputy *s)
{
  int i;
  // build a deputy info message
  deputy_info_t msg;
  msg.utime = timestamp_now ();
  msg.host = s->hostname;
  msg.cpu_load = s->cpu_load;
  msg.phys_mem_total_bytes = s->cpu_time[1].memtotal;
  msg.phys_mem_free_bytes = s->cpu_time[1].memfree;
  msg.swap_total_bytes = s->cpu_time[1].swaptotal;
  msg.swap_free_bytes = s->cpu_time[1].swapfree;

  msg.ncmds = s->commands_.size();
  msg.cmds.resize(msg.ncmds);

  int cmd_index = 0;
  for (auto& item : s->commands_) {
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

  if (s->verbose) dbgt ("transmitting deputy info!\n");
  s->lcm_->publish("PM_INFO", &msg);
}

static void
update_cpu_times (ProcmanDeputy *s)
{
    int status;

    status = procinfo_read_sys_cpu_mem (&s->cpu_time[1]);
    if(0 != status) {
        perror("update_cpu_times - procinfo_read_sys_cpu_mem");
    }

    sys_cpu_mem_t *a = &s->cpu_time[1];
    sys_cpu_mem_t *b = &s->cpu_time[0];

    uint64_t elapsed_jiffies = a->user - b->user +
                                a->user_low - b->user_low +
                                a->system - b->system +
                                a->idle - b->idle;
    uint64_t loaded_jiffies = a->user - b->user +
                              a->user_low - b->user_low +
                              a->system - b->system;
    if (! elapsed_jiffies || loaded_jiffies > elapsed_jiffies) {
        s->cpu_load = 0;
    } else {
        s->cpu_load = (double)loaded_jiffies / elapsed_jiffies;
    }

  for (auto& item : s->commands_) {
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

    memcpy (&s->cpu_time[0], &s->cpu_time[1], sizeof (sys_cpu_mem_t));
}

void ProcmanDeputy::OnOneSecondTimer() {
  update_cpu_times (this);
  transmit_proc_info (this);
}

void ProcmanDeputy::OnIntrospectionTimer() {
  int mypid = getpid();
  proc_cpu_mem_t pinfo;
  int status = procinfo_read_proc_cpu_mem (mypid, &pinfo);
  if(0 != status)  {
    perror("introspection_timeout - procinfo_read_proc_cpu_mem");
  }

  int nrunning = 0;
  for (ProcmanCommandPtr cmd : pm->GetCommands()) {
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
  //    dbgt ("       orders: %d forme: %d (%d stale) sheriffs: %d\n",
  //            pmd->norders_slm, pmd->norders_forme_slm, pmd->nstale_orders_slm,
  //            g_list_length (pmd->observed_sheriffs_slm));

  norders_slm = 0;
  norders_forme_slm = 0;
  nstale_orders_slm = 0;

  observed_sheriffs_slm.clear();
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
    check_for_dead_children(this);
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
        pm->KillCommmand(cmd, mi->stop_signal);
        if(mi->stop_time_allowed > max_stop_time_allowed)
          max_stop_time_allowed = mi->stop_time_allowed;
      }
    }
    exiting = 1;

    // set a timer, after which everything will be more forcefully
    // terminated.
    QTimer* quit_timer = new QTimer(this);
    quit_timer->setSingleShot(true);
    quit_timer->start((int)(max_stop_time_allowed * 1000));
    connect(quit_timer, &QTimer::timeout, this, &ProcmanDeputy::OnQuitTimer);
  }

  if(exiting) {
    // if we're exiting, and all child processes are dead, then exit.
    bool all_dead = true;
    for (ProcmanCommandPtr cmd : pm->GetCommands()) {
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

static const cmd_desired_t *
procmd_orders_find_cmd (const orders_t* orders, int32_t sheriff_id)
{
  int i;
  for (i=0; i<orders->ncmds; i++) {
    if (sheriff_id == orders->cmds[i].sheriff_id) {
      return &orders->cmds[i];
    }
  }
  return nullptr;
}

static DeputyCommand*
find_local_cmd (ProcmanDeputy *s, int32_t sheriff_id)
{
  for (auto& item : s->commands_) {
    DeputyCommand* mi = item.second;
    ProcmanCommandPtr cmd = item.first;
    if (cmd->SheriffId() == sheriff_id) {
      return mi;
    }
  }
  return nullptr;
}

static void
_set_command_stop_signal (DeputyCommand* mi, int stop_signal)
{
    mi->stop_signal = stop_signal;
}

static void
_set_command_stop_time_allowed (DeputyCommand* mi, float stop_time_allowed)
{
    mi->stop_time_allowed = stop_time_allowed;
}


static void
_handle_orders2(ProcmanDeputy* pmd, const orders_t* orders)
{
    pmd->norders_slm ++;

    // ignore orders if we're exiting
    if (pmd->exiting) {
        return;
    }

    // ignore orders for other deputies
    if (orders->host != pmd->hostname) {
        if (pmd->verbose)
            dbgt ("ignoring orders for other host %s\n", orders->host.c_str());
        return;
    }
    pmd->norders_forme_slm++;

    // ignore stale orders (where utime is too long ago)
    int64_t now = timestamp_now ();
    if (now - orders->utime > PROCMAN_MAX_MESSAGE_AGE_USEC) {
        for (int i=0; i<orders->ncmds; i++) {
               const cmd_desired_t *cmd_msg = &orders->cmds[i];
               printf_and_transmit (pmd, cmd_msg->sheriff_id,
                   "ignoring stale orders (utime %d seconds ago). You may want to check the system clocks!\n",
                   (int) ((now - orders->utime) / 1000000));
        }
         pmd->nstale_orders_slm++;
        return;
    }

    // check if we've seen this sheriff since the last MARK.
    pmd->observed_sheriffs_slm.insert(orders->sheriff_name);

    // update variables
    pmd->pm->RemoveAllVariables();
//    for(int varind=0; varind<orders->nvars; varind++) {
//        procman_set_variable(pmd->pm, orders->varnames[varind], orders->varvals[varind]);
//    }

    // attempt to carry out the orders
    int action_taken = 0;
    int i;
    if (pmd->verbose)
        dbgt ("orders for me received with %d commands\n", orders->ncmds);
    for (i=0; i<orders->ncmds; i++) {
        const cmd_desired_t* cmd_msg = &orders->cmds[i];

        if (pmd->verbose)
            dbgt ("order %d: %s (%d, %d)\n",
                    i, cmd_msg->cmd.exec_str.c_str(),
                    cmd_msg->desired_runid, cmd_msg->force_quit);

        // do we already have this command somewhere?
        DeputyCommand *mi = find_local_cmd(pmd, cmd_msg->sheriff_id);
        ProcmanCommandPtr cmd;

        if (mi) {
          cmd = mi->cmd;
        } else {
            // if not, then create it.
            if (pmd->verbose) dbgt ("adding new process (%s)\n", cmd_msg->cmd.exec_str.c_str());
            cmd = pmd->pm->AddCommand(cmd_msg->cmd.exec_str, cmd_msg->cmd.command_id);

            // allocate a private data structure
            mi = new DeputyCommand();
            mi->deputy = pmd;
            mi->group_ = cmd_msg->cmd.group;
            mi->auto_respawn = cmd_msg->cmd.auto_respawn;
            mi->stop_signal = cmd_msg->cmd.stop_signal;
            mi->stop_time_allowed = cmd_msg->cmd.stop_time_allowed;
            mi->last_start_time = 0;
            mi->respawn_backoff = MIN_RESPAWN_DELAY_MS;
            mi->stdout_notifier = nullptr;

            mi->respawn_timer_.setSingleShot(true);
            QObject::connect(&mi->respawn_timer_, &QTimer::timeout,
                [mi]() { 
                if(mi->auto_respawn && !mi->should_be_stopped && !mi->deputy->exiting) {
                start_cmd(mi->deputy, mi, mi->actual_runid);
                }
                });

            mi->cmd = cmd;
            pmd->commands_[cmd] = mi;
            action_taken = 1;
        }

        // check if the command needs to be started or stopped
        CommandStatus cmd_status = pmd->pm->GetCommandStatus(cmd);

        // rename a command?  does not kill a running command, so effect does
        // not apply until command is restarted.
        if (cmd->ExecStr() != cmd_msg->cmd.exec_str) {
            dbgt ("[%s] exec str -> [%s]\n", cmd->Id().c_str(),
                cmd_msg->cmd.exec_str.c_str());
            pmd->pm->SetCommandExecStr(cmd, cmd_msg->cmd.exec_str);

            action_taken = 1;
        }

        // change a command's id?
        if (cmd_msg->cmd.command_id != cmd->Id()) {
            dbgt ("[%s] rename -> [%s]\n", cmd->Id().c_str(),
                    cmd_msg->cmd.command_id.c_str());
            pmd->pm->SetCommandId(cmd, cmd_msg->cmd.command_id);
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
            _set_command_stop_signal(mi, cmd_msg->cmd.stop_signal);
        }

        // change the stop time allowed of a command?
        if(mi->stop_time_allowed != cmd_msg->cmd.stop_time_allowed) {
            dbg("[%s] stop time allowed -> [%f]\n", cmd->Id().c_str(),
                    cmd_msg->cmd.stop_time_allowed);
            _set_command_stop_time_allowed(mi, cmd_msg->cmd.stop_time_allowed);
        }

        mi->should_be_stopped = cmd_msg->force_quit;

        if (PROCMAN_CMD_STOPPED == cmd_status &&
            (mi->actual_runid != cmd_msg->desired_runid) &&
            ! mi->should_be_stopped) {
            start_cmd (pmd, mi, cmd_msg->desired_runid);
            action_taken = 1;
        } else if (PROCMAN_CMD_RUNNING == cmd_status &&
                (mi->should_be_stopped || (cmd_msg->desired_runid != mi->actual_runid))) {
            stop_cmd(pmd, mi);
            action_taken = 1;
        } else {
            mi->actual_runid = cmd_msg->desired_runid;
        }
    }

    // if there are any commands being managed that did not appear in the
    // orders, then stop and remove those commands
    std::vector<DeputyCommand*> toremove;
    for (auto& item : pmd->commands_) {
      DeputyCommand* mi = item.second;
      ProcmanCommandPtr cmd = item.first;
        const cmd_desired_t *cmd_msg = procmd_orders_find_cmd (orders, cmd->SheriffId());

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
            stop_cmd (pmd, mi);
        } else {
            dbgt ("[%s] remove\n", cmd->Id().c_str());
            // cleanup the private data structure used
            delete mi;
            pmd->pm->RemoveCommand(cmd);
        }

        action_taken = 1;
    }

    if (action_taken)
        transmit_proc_info (pmd);
}

void ProcmanDeputy::OrdersReceived(const lcm::ReceiveBuffer* rbuf, const std::string& channel,
    const orders_t* orders) {
  _handle_orders2(this, orders);
}

void
ProcmanDeputy::DiscoveryReceived(const lcm::ReceiveBuffer* rbuf,
    const std::string& channel, const discovery_t* msg) {
    int64_t now = timestamp_now();
    if(now < deputy_start_time + DISCOVERY_TIME_MS * 1000) {
      // received a discovery message while still in discovery mode.  Check to
      // see if it's from a conflicting deputy.
      if(msg->host == hostname && msg->nonce != deputy_pid) {
        dbgt("ERROR.  Detected another deputy named [%s].  Aborting to avoid conflicts.\n",
            msg->host.c_str());
        exit(1);
      }
    } else {
      // received a discovery message while not in discovery mode.  Respond by
      // transmitting deputy info.
      transmit_proc_info(this);
    }
}

void
ProcmanDeputy::InfoReceived(const lcm::ReceiveBuffer* rbuf,
    const std::string& channel, const deputy_info_t* msg) {
  int64_t now = timestamp_now();
  if(now < deputy_start_time + DISCOVERY_TIME_MS * 1000) {
    // A different deputy has reported while we're still in discovery mode.
    // Check to see if the deputy names are in conflict.
    if(msg->host == hostname) {
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
  if(now < deputy_start_time + DISCOVERY_TIME_MS * 1000) {
    // publish a discover message to check for conflicting deputies
    discovery_t msg;
    msg.utime = now;
    msg.host = hostname;
    msg.nonce = deputy_pid;
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

static void usage()
{
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

int main (int argc, char **argv)
{
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

    char *logfilename = NULL;
    int verbose = 0;
    std::string hostname_override;
    char *lcmurl = NULL;

    while ((c = getopt_long (argc, argv, optstring, long_opts, 0)) >= 0)
    {
        switch (c) {
            case 'v':
                verbose = 1;
                break;
            case 'l':
                free(logfilename);
                logfilename = strdup (optarg);
                break;
            case 'u':
                free(lcmurl);
                lcmurl = strdup(optarg);
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

     // create the lcm_t structure for doing IPC
    lcm::LCM* lcm_obj = new lcm::LCM(lcmurl);
     if (!lcm_obj) {
         fprintf (stderr, "error initializing LCM.  ");
         return 1;
     }

     // redirect stdout and stderr to a log file if the -l command line flag
     // was specified.
     if (logfilename) {
         int fd = open (logfilename, O_WRONLY | O_APPEND | O_CREAT, 0644);
         if (fd < 0) {
             perror ("open");
             fprintf (stderr, "couldn't open logfile %s\n", logfilename);
             return 1;
         }
         close(1); close(2);
         if (dup2(fd, 1) < 0) { return 1; }
         if (dup2(fd, 2) < 0) { return 1; }
         close (fd);
         setlinebuf (stdout);
         setlinebuf (stderr);
     }

     g_pmd = new ProcmanDeputy();
     ProcmanDeputy* pmd = g_pmd;

     pmd->lcm_ = lcm_obj;
     pmd->verbose = verbose;
     pmd->norders_slm = 0;
     pmd->nstale_orders_slm = 0;
     pmd->norders_forme_slm = 0;
     pmd->exiting = 0;
     pmd->deputy_start_time = timestamp_now();
     pmd->deputy_pid = getpid();

     QCoreApplication app(argc, argv);

     // set deputy hostname to the system hostname
     if (!hostname_override.empty()) {
       pmd->hostname = hostname_override;
     } else {
       char buf[256];
       memset(buf, 0, sizeof(buf));
       gethostname (buf, sizeof(buf) - 1);
       pmd->hostname = buf;
     }

     // load config file
     ProcmanOptions options = ProcmanOptions::Default(argc, argv);
     pmd->pm = new Procman(options);

     // convert Unix signals into file descriptor writes
     signal_pipe_init();
     signal_pipe_add_signal(SIGINT);
     signal_pipe_add_signal(SIGHUP);
     signal_pipe_add_signal(SIGQUIT);
     signal_pipe_add_signal(SIGTERM);
     signal_pipe_add_signal(SIGCHLD);

     pmd->info2_subs_ = pmd->lcm_->subscribe("PM_INFO",
         &ProcmanDeputy::InfoReceived, pmd);

     pmd->discovery_subs_ = pmd->lcm_->subscribe("PM_DISCOVER",
         &ProcmanDeputy::DiscoveryReceived, pmd);

     pmd->Prepare();

     int app_status = app.exec();

     // cleanup
     signal_pipe_cleanup();

     delete pmd->pm;

     // unsubscribe
     if(pmd->orders2_subs_) {
      pmd->lcm_->unsubscribe(pmd->orders2_subs_);
     }
     if(pmd->info2_subs_) {
       pmd->lcm_->unsubscribe(pmd->info2_subs_);
     }
     if(pmd->discovery_subs_) {
       pmd->lcm_->unsubscribe(pmd->discovery_subs_);
     }

     pmd->observed_sheriffs_slm.clear();

     delete pmd->lcm_;

     delete pmd;

     return app_status;
}
