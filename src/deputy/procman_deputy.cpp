/*
 * MIT DARPA Urban Challenge Team
 *
 * Module Name: procman_deputy
 *
 * Description:
 *
 * The procman_deputy module is a process-management daemon that manages a
 * collection of processes.  It listens for commands over LCM and starts and
 * stops processes according to the commands it receives.  Addditionally, the
 * procman_deputy periodically transmits the state of the processes that it is
 * managing.
 *
 * Maintainer: Albert Huang <albert@csail.mit.edu>
 */

#define _GNU_SOURCE

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

#include <glib.h>

#include <lcm/lcm.h>

#include <lcmtypes/procman_lcm_output_t.h>
#include <lcmtypes/procman_lcm_discovery_t.h>

#include <lcmtypes/procman_lcm_deputy_info_t.h>
#include <lcmtypes/procman_lcm_orders_t.h>

#include "procman.hpp"
#include "procinfo.hpp"
#include "signal_pipe.hpp"
#include "lcm_util.hpp"

namespace procman {

#define ESTIMATED_MAX_CLOCK_ERROR_RATE 1.001

#define MIN_RESPAWN_DELAY_MS 10
#define MAX_RESPAWN_DELAY_MS 1000
#define RESPAWN_BACKOFF_RATE 2
#define DISCOVERY_TIME_MS 1500

#define DEFAULT_STOP_SIGNAL 2
#define DEFAULT_STOP_TIME_ALLOWED 7

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

struct ProcmanDeputy;

struct DeputyCommand {
  ProcmanDeputy *deputy;

  ProcmanCommandPtr cmd;

  // glib handles for IO watches
  GIOChannel *stdout_ioc;
  guint stdout_sid;
  int32_t actual_runid;
  int32_t sheriff_id;
  int32_t should_be_stopped;

  proc_cpu_mem_t cpu_time[2];
  float cpu_usage;

  char *group;
  char *id;
  int auto_respawn;
  guint respawn_timeout_id;
  int64_t last_start_time;
  int respawn_backoff;

  int stop_signal;
  float stop_time_allowed;

  int num_kills_sent;
  int64_t first_kill_time;
  int remove_requested;
};

struct ProcmanDeputy {
  procman_t *pm;

  lcm_t *lcm;

  char hostname[1024];

  GMainLoop * mainloop;

  int norders_slm;       // total procman_lcm_orders_t observed Since Last MARK
  int norders_forme_slm; // total procman_lcm_orders_t for this deputy slm
  int nstale_orders_slm; // total stale procman_lcm_orders_t for this deputy slm

  std::set<std::string> observed_sheriffs_slm; // names of observed sheriffs slm
  char *last_sheriff_name;      // name of the most recently observed sheriff

  int64_t deputy_start_time;
  procman_lcm_discovery_t_subscription_t* discovery_subs;

  procman_lcm_deputy_info_t_subscription_t* info2_subs;
  procman_lcm_orders_t_subscription_t* orders2_subs;

  pid_t deputy_pid;
  sys_cpu_mem_t cpu_time[2];
  float cpu_load;

  int verbose;
  int exiting;

  std::map<ProcmanCommandPtr, DeputyCommand*> commands_;
};

// make this global so that the signal handler can access it
static ProcmanDeputy* g_pmd;

static void
transmit_proc_info (ProcmanDeputy *s);

static gboolean
on_scheduled_respawn(DeputyCommand* mi);

static void
transmit_str (ProcmanDeputy *pmd, int sheriff_id, const char * str)
{
    procman_lcm_output_t msg;
    msg.deputy_name = pmd->hostname;
    msg.sheriff_id = sheriff_id;
    msg.text = (char*) str;
    msg.utime = timestamp_now ();
    procman_lcm_output_t_publish (pmd->lcm, "PM_OUTPUT", &msg);
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
        procman_lcm_output_t msg;
        msg.deputy_name = pmd->hostname;
        msg.sheriff_id = sheriff_id;
        msg.text = buf;
        msg.utime = timestamp_now ();
        procman_lcm_output_t_publish (pmd->lcm, "PM_OUTPUT", &msg);
    } else {
        dbgt ("uh oh.  printf_and_transmit printed zero bytes\n");
    }
}

// invoked when a child process writes something to its stdout/stderr fd
static int
pipe_data_ready (GIOChannel *source, GIOCondition condition,
        DeputyCommand *mi)
{
  ProcmanCommandPtr cmd = mi->cmd;
    int result = TRUE;
    int anycondition = 0;

    if (condition & G_IO_IN) {
        char buf[1024];
        int bytes_read = read (cmd->stdout_fd, buf, sizeof (buf)-1);
        if (bytes_read < 0) {
            snprintf (buf, sizeof (buf), "procman [%s] read: %s (%d)\n",
                    cmd->ExecStr().c_str(), strerror (errno), errno);
            dbgt (buf);
            transmit_str (g_pmd, mi->sheriff_id, buf);
        } else if ( bytes_read == 0) {
            dbgt ("zero byte read\n");
        } else {
            buf[bytes_read] = '\0';
            transmit_str (g_pmd, mi->sheriff_id, buf);
        }
        anycondition = 1;
    }
    if (condition & G_IO_ERR) {
        transmit_str (g_pmd, mi->sheriff_id,
                "procman deputy: detected G_IO_ERR.\n");
        dbgt ("G_IO_ERR from [%s]\n", cmd->Id().c_str());
        anycondition = 1;
    }
    if (condition & G_IO_HUP) {
        transmit_str (g_pmd, mi->sheriff_id,
                "procman deputy: detected G_IO_HUP.  end of output\n");
        dbgt ("G_IO_HUP from [%s]\n", cmd->Id().c_str());
        result = FALSE;
        anycondition = 1;
    }
    if (condition & G_IO_NVAL) {
        transmit_str (g_pmd, mi->sheriff_id,
                "procman deputy: detected G_IO_NVAL.  end of output\n");
        dbgt ("G_IO_NVAL from [%s]\n", cmd->Id().c_str());
        result = FALSE;
        anycondition = 1;
    }
    if (condition & G_IO_PRI) {
        transmit_str (g_pmd, mi->sheriff_id,
                "procman deputy: unexpected G_IO_PRI... wtf?\n");
        dbgt ("G_IO_PRI from [%s]\n", cmd->Id().c_str());
        anycondition = 1;
    }
    if (condition & G_IO_OUT) {
        transmit_str (g_pmd, mi->sheriff_id,
                "procman deputy: unexpected G_IO_OUT... wtf?\n");
        dbgt ("G_IO_OUT from [%s]\n", cmd->Id().c_str());
        anycondition = 1;
    }
    if (!anycondition) {
        dbgt ("wtf??? [%s] pipe has condition 0x%X\n", cmd->ExecStr().c_str(),
                condition);
    }
    return result;
}

static void
maybe_schedule_respawn(ProcmanDeputy *pmd, DeputyCommand *mi)
{
    if(mi->auto_respawn && !mi->should_be_stopped && !pmd->exiting) {
        mi->respawn_timeout_id =
            g_timeout_add(mi->respawn_backoff, (GSourceFunc)on_scheduled_respawn, mi);
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
    mi->respawn_timeout_id = 0;
    // update the respawn backoff counter, to throttle how quickly a
    // process respawns
    int ms_since_started = (timestamp_now() - mi->last_start_time) / 1000;
    if(ms_since_started < MAX_RESPAWN_DELAY_MS) {
        mi->respawn_backoff = MIN(MAX_RESPAWN_DELAY_MS,
                mi->respawn_backoff * RESPAWN_BACKOFF_RATE);
    } else {
        int d = ms_since_started / MAX_RESPAWN_DELAY_MS;
        mi->respawn_backoff = MAX(MIN_RESPAWN_DELAY_MS,
                mi->respawn_backoff >> d);
    }
    mi->last_start_time = timestamp_now();

    status = procman_start_cmd (pmd->pm, cmd);
    if (0 != status) {
        printf_and_transmit (pmd, 0, "[%s] couldn't start [%s]\n", cmd->Id().c_str(), cmd->ExecStr().c_str());
        dbgt ("[%s] couldn't start [%s]\n", cmd->Id().c_str(), cmd->ExecStr().c_str());
        maybe_schedule_respawn(pmd, mi);
        printf_and_transmit (pmd, mi->sheriff_id,
                "ERROR!  [%s] couldn't start [%s]\n", cmd->Id().c_str(), cmd->ExecStr().c_str());
        return -1;
    }

    // add stdout for this process to IO watch list
    if (mi->stdout_ioc) {
        dbgt ("ERROR: [%s] expected mi->stdout_ioc to be NULL [%s]\n",
                cmd->Id().c_str(), cmd->ExecStr().c_str());
    }

    mi->stdout_ioc = g_io_channel_unix_new (cmd->stdout_fd);
    g_io_channel_set_encoding (mi->stdout_ioc, NULL, NULL);
    fcntl (cmd->stdout_fd, F_SETFL, O_NONBLOCK);
    mi->stdout_sid = g_io_add_watch (mi->stdout_ioc,
            G_IO_IN, (GIOFunc)pipe_data_ready, mi);

    mi->actual_runid = desired_runid;
    mi->num_kills_sent = 0;
    mi->first_kill_time = 0;
    return 0;
}

static int
stop_cmd (ProcmanDeputy *pmd, DeputyCommand* mi)
{
    ProcmanCommandPtr cmd = mi->cmd;

    if (!cmd->pid) return 0;

    mi->should_be_stopped = 1;

    if(mi->respawn_timeout_id)
        g_source_remove(mi->respawn_timeout_id);

    int64_t now = timestamp_now();
    int64_t sigkill_time = mi->first_kill_time + (int64_t)(mi->stop_time_allowed * 1000000);
    int status;
    if(!mi->first_kill_time) {
        status = procman_kill_cmd (pmd->pm, cmd, mi->stop_signal);
        mi->first_kill_time = now;
        mi->num_kills_sent++;
    } else if(now > sigkill_time) {
        status = procman_kill_cmd (pmd->pm, cmd, SIGKILL);
    } else {
        return 0;
    }

    if (0 != status) {
        printf_and_transmit (pmd, mi->sheriff_id,
                "kill: %s\n", strerror (-status));
    }
    return status;
}

static void
check_for_dead_children (ProcmanDeputy *pmd)
{
  ProcmanCommandPtr cmd = procman_check_for_dead_children(pmd->pm);

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
      pipe_data_ready (NULL, G_IO_IN, mi);
    }

    // did the child terminate with a signal?
    if (WIFSIGNALED (cmd->exit_status)) {
      int signum = WTERMSIG (cmd->exit_status);

      printf_and_transmit (pmd, mi->sheriff_id,
          "%s\n",
          strsignal (signum), signum);
      if (WCOREDUMP (cmd->exit_status)) {
        printf_and_transmit (pmd, mi->sheriff_id, "Core dumped.\n");
      }
    }

    // cleanup the glib hooks if necessary
    if (mi->stdout_ioc) {
      // detach from the glib event loop
      g_io_channel_unref (mi->stdout_ioc);
      g_source_remove (mi->stdout_sid);
      mi->stdout_ioc = NULL;
      mi->stdout_sid = 0;

      procman_close_dead_pipes (pmd->pm, cmd);
    }

    // remove ?
    if (mi->remove_requested) {
      dbgt ("[%s] remove\n", cmd->Id().c_str());
      // cleanup the private data structure used
      free(mi->group);
      free(mi->id);
      delete mi;
      procman_remove_cmd (pmd->pm, cmd);
    } else {
      maybe_schedule_respawn(pmd, mi);
    }

    cmd = procman_check_for_dead_children (pmd->pm);
    transmit_proc_info (pmd);
  }
}

static gboolean
on_quit_timeout(ProcmanDeputy* pmd)
{
  for (auto& item : pmd->commands_) {
    DeputyCommand* mi = item.second;
    ProcmanCommandPtr cmd = item.first;
    if (cmd->Pid()) {
      procman_kill_cmd (pmd->pm, cmd, SIGKILL);
    }
    free(mi->group);
    free(mi->id);
    delete mi;
    procman_remove_cmd (pmd->pm, cmd);
  }

  dbgt ("stopping deputy main loop\n");
  g_main_loop_quit (pmd->mainloop);
  return FALSE;
}

static void
glib_handle_signal (int signal, ProcmanDeputy *pmd) {
    if (signal == SIGCHLD) {
        // a child process died.  check to see which one, and cleanup its
        // remains.
        check_for_dead_children (pmd);
    } else {
        // quit was requested.  kill all processes and quit
        dbgt ("received signal %d (%s).  stopping all processes\n", signal,
                strsignal (signal));

        float max_stop_time_allowed = 1;

        // first, send everything a SIGINT to give them a chance to exit
        // cleanly.
        for (auto& item : pmd->commands_) {
          DeputyCommand* mi = item.second;
          ProcmanCommandPtr cmd = item.first;
          if (cmd->Pid()) {
            procman_kill_cmd (pmd->pm, cmd, mi->stop_signal);
            if(mi->stop_time_allowed > max_stop_time_allowed)
              max_stop_time_allowed = mi->stop_time_allowed;
          }
        }
        pmd->exiting = 1;

        // set a timer, after which everything will be more forcefully
        // terminated.
        g_timeout_add((int)(max_stop_time_allowed * 1000),
                (GSourceFunc)on_quit_timeout, pmd);
    }

    if(pmd->exiting) {
        // if we're exiting, and all child processes are dead, then exit.
        int all_dead = 1;
        for (ProcmanCommandPtr cmd : procman_get_cmds(pmd->pm)) {
            if (cmd->pid) {
                all_dead = 0;
                break;
            }
        }
        if(all_dead) {
            dbg("all child processes are dead, exiting.\n");
            g_main_loop_quit(pmd->mainloop);
        }
    }
}

static void
transmit_proc_info (ProcmanDeputy *s)
{
  int i;
  procman_lcm_deputy_info_t msg;

  // build a deputy info message
  memset (&msg, 0, sizeof (msg));

  msg.utime = timestamp_now ();
  msg.host = s->hostname;
  msg.cpu_load = s->cpu_load;
  msg.phys_mem_total_bytes = s->cpu_time[1].memtotal;
  msg.phys_mem_free_bytes = s->cpu_time[1].memfree;
  msg.swap_total_bytes = s->cpu_time[1].swaptotal;
  msg.swap_free_bytes = s->cpu_time[1].swapfree;

  msg.ncmds = s->commands_.size();
  msg.cmds =
    (procman_lcm_cmd_status_t *) calloc(msg.ncmds, sizeof(procman_lcm_cmd_status_t));

  int cmd_index = 0;
  for (auto& item : s->commands_) {
    ProcmanCommandPtr cmd = item.first;
    DeputyCommand* mi = item.second;

    msg.cmds[cmd_index].cmd.exec_str = (char*) cmd->ExecStr().c_str();
    msg.cmds[cmd_index].cmd.command_id = mi->id;
    msg.cmds[cmd_index].cmd.group = mi->group;
    msg.cmds[cmd_index].cmd.auto_respawn = mi->auto_respawn;
    msg.cmds[cmd_index].cmd.stop_signal = mi->stop_signal;
    msg.cmds[cmd_index].cmd.stop_time_allowed = mi->stop_time_allowed;
    msg.cmds[cmd_index].cmd.num_options = 0;
    msg.cmds[cmd_index].cmd.option_names = NULL;
    msg.cmds[cmd_index].cmd.option_values = NULL;
    msg.cmds[cmd_index].actual_runid = mi->actual_runid;
    msg.cmds[cmd_index].pid = cmd->pid;
    msg.cmds[cmd_index].exit_code = cmd->exit_status;
    msg.cmds[cmd_index].sheriff_id = mi->sheriff_id;
    msg.cmds[cmd_index].cpu_usage = mi->cpu_usage;
    msg.cmds[cmd_index].mem_vsize_bytes = mi->cpu_time[1].vsize;
    msg.cmds[cmd_index].mem_rss_bytes = mi->cpu_time[1].rss;
    cmd_index++;
  }

  if (s->verbose) dbgt ("transmitting deputy info!\n");
  procman_lcm_deputy_info_t_publish (s->lcm, "PM_INFO", &msg);

  // release memory
  free (msg.cmds);
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

        if (cmd->pid) {
            status = procinfo_read_proc_cpu_mem (cmd->pid, &mi->cpu_time[1]);
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

static gboolean
on_scheduled_respawn(DeputyCommand* mi)
{
    if(mi->auto_respawn && !mi->should_be_stopped && !mi->deputy->exiting) {
        start_cmd(mi->deputy, mi, mi->actual_runid);
    }
    return FALSE;
}

static gboolean
one_second_timeout (ProcmanDeputy *pmd)
{
    update_cpu_times (pmd);
    transmit_proc_info (pmd);
    return TRUE;
}

static gboolean
introspection_timeout (ProcmanDeputy *s)
{
    int mypid = getpid();
    proc_cpu_mem_t pinfo;
    int status = procinfo_read_proc_cpu_mem (mypid, &pinfo);
    if(0 != status)  {
        perror("introspection_timeout - procinfo_read_proc_cpu_mem");
    }

    int nrunning = 0;
    for (ProcmanCommandPtr cmd : procman_get_cmds (s->pm)) {
        if (cmd->pid) nrunning++;
    }

    dbgt ("MARK - rss: %" PRId64 " kB vsz: %" PRId64
            " kB procs: %d (%d alive)\n",
            pinfo.rss / 1024, pinfo.vsize / 1024,
            (int) procman_get_cmds(s->pm).size(),
            nrunning
           );
//    dbgt ("       orders: %d forme: %d (%d stale) sheriffs: %d\n",
//            s->norders_slm, s->norders_forme_slm, s->nstale_orders_slm,
//            g_list_length (s->observed_sheriffs_slm));

    s->norders_slm = 0;
    s->norders_forme_slm = 0;
    s->nstale_orders_slm = 0;

    s->observed_sheriffs_slm.clear();

    return TRUE;
}

static procman_lcm_cmd_desired_t *
procmd_orders_find_cmd (const procman_lcm_orders_t *a, int32_t sheriff_id)
{
    int i;
    for (i=0; i<a->ncmds; i++) {
        if (sheriff_id == a->cmds[i].sheriff_id) return &a->cmds[i];
    }
    return NULL;
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
_set_command_group (DeputyCommand* mi, const char *group)
{
    free (mi->group);
    mi->group = strdup (group);
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
_set_command_id (DeputyCommand* mi, const char *id)
{
    free (mi->id);
    mi->id = strdup (id);
}

static void
_handle_orders2(ProcmanDeputy* pmd, const procman_lcm_orders_t* orders)
{
    const GList *iter = NULL;
    pmd->norders_slm ++;

    // ignore orders if we're exiting
    if (pmd->exiting) {
        return;
    }

    // ignore orders for other deputies
    if (strcmp (orders->host, pmd->hostname)) {
        if (pmd->verbose)
            dbgt ("ignoring orders for other host %s\n", orders->host);
        return;
    }
    pmd->norders_forme_slm++;

    // ignore stale orders (where utime is too long ago)
    int64_t now = timestamp_now ();
    if (now - orders->utime > PROCMAN_MAX_MESSAGE_AGE_USEC) {
        for (int i=0; i<orders->ncmds; i++) {
               procman_lcm_cmd_desired_t *cmd_msg = &orders->cmds[i];
               printf_and_transmit (pmd, cmd_msg->sheriff_id,
                   "ignoring stale orders (utime %d seconds ago). You may want to check the system clocks!\n",
                   (int) ((now - orders->utime) / 1000000));
        }
         pmd->nstale_orders_slm++;
        return;
    }

    // check if we've seen this sheriff since the last MARK.
    pmd->observed_sheriffs_slm.insert(orders->sheriff_name);
    if (pmd->last_sheriff_name &&
            strcmp (orders->sheriff_name, pmd->last_sheriff_name)) {
        free (pmd->last_sheriff_name);
        pmd->last_sheriff_name = NULL;
    }

    if (!pmd->last_sheriff_name) {
        pmd->last_sheriff_name = strdup (orders->sheriff_name);
    }

    // update variables
    procman_remove_all_variables(pmd->pm);
//    for(int varind=0; varind<orders->nvars; varind++) {
//        procman_set_variable(pmd->pm, orders->varnames[varind], orders->varvals[varind]);
//    }

    // attempt to carry out the orders
    int action_taken = 0;
    int i;
    if (pmd->verbose)
        dbgt ("orders for me received with %d commands\n", orders->ncmds);
    for (i=0; i<orders->ncmds; i++) {

        procman_lcm_cmd_desired_t *cmd_msg = &orders->cmds[i];

        if (pmd->verbose)
            dbgt ("order %d: %s (%d, %d)\n",
                    i, cmd_msg->cmd.exec_str,
                    cmd_msg->desired_runid, cmd_msg->force_quit);

        // do we already have this command somewhere?
        DeputyCommand *mi = find_local_cmd(pmd, cmd_msg->sheriff_id);
        procman_cmd_t* cmd = nullptr;

        if (mi) {
          cmd = mi->cmd;
        } else {
            // if not, then create it.
            if (pmd->verbose) dbgt ("adding new process (%s)\n", cmd_msg->cmd.exec_str);
            cmd = procman_add_cmd (pmd->pm, cmd_msg->cmd.exec_str, cmd_msg->cmd.command_id);

            // allocate a private data structure for glib info
            mi = new DeputyCommand();
            mi->deputy = pmd;
            mi->sheriff_id = cmd_msg->sheriff_id;
            mi->group = strdup (cmd_msg->cmd.group);
            mi->id = strdup (cmd_msg->cmd.command_id);
            mi->auto_respawn = cmd_msg->cmd.auto_respawn;
            mi->stop_signal = cmd_msg->cmd.stop_signal;
            mi->stop_time_allowed = cmd_msg->cmd.stop_time_allowed;
            mi->last_start_time = 0;
            mi->respawn_backoff = MIN_RESPAWN_DELAY_MS;
            mi->respawn_timeout_id = 0;
            mi->cmd = cmd;
            pmd->commands_[cmd] = mi;
            action_taken = 1;
        }

        // check if the command needs to be started or stopped
        CommandStatus cmd_status = procman_get_cmd_status (pmd->pm, cmd);

        // rename a command?  does not kill a running command, so effect does
        // not apply until command is restarted.
        if (strcmp (cmd->ExecStr().c_str(), cmd_msg->cmd.exec_str)) {
            dbgt ("[%s] exec str -> [%s]\n", cmd->Id().c_str(), cmd_msg->cmd.exec_str);
            procman_cmd_change_str (cmd, cmd_msg->cmd.exec_str);

            action_taken = 1;
        }

        // change a command's id?
        if (strcmp (mi->id, cmd_msg->cmd.command_id)) {
            dbgt ("[%s] rename -> [%s]\n", mi->id,
                    cmd_msg->cmd.command_id);
            _set_command_id (mi, cmd_msg->cmd.command_id);
            procman_cmd_set_id(cmd, cmd_msg->cmd.command_id);
            action_taken = 1;
        }

        // has auto-respawn changed?
        if (cmd_msg->cmd.auto_respawn != mi->auto_respawn) {
            dbgt ("[%s] auto-respawn -> %d\n", cmd->Id().c_str(), cmd_msg->cmd.auto_respawn);
            mi->auto_respawn = cmd_msg->cmd.auto_respawn;
        }

        // change the group of a command?
        if (strcmp (mi->group, cmd_msg->cmd.group)) {
            dbgt ("[%s] group -> [%s]\n", cmd->Id().c_str(),
                    cmd_msg->cmd.group);
            _set_command_group (mi, cmd_msg->cmd.group);
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
        procman_lcm_cmd_desired_t *cmd_msg =
            procmd_orders_find_cmd (orders, mi->sheriff_id);

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

        if (cmd->pid) {
            dbgt ("[%s] scheduling removal\n", cmd->Id().c_str());
            mi->remove_requested = 1;
            stop_cmd (pmd, mi);
        } else {
            dbgt ("[%s] remove\n", cmd->Id().c_str());
            // cleanup the private data structure used
            free (mi->group);
            free (mi->id);
            free (mi);
            procman_remove_cmd (pmd->pm, cmd);
        }

        action_taken = 1;
    }

    if (action_taken)
        transmit_proc_info (pmd);
}

static void
procman_deputy_order2_received (const lcm_recv_buf_t *rbuf, const char *channel,
        const procman_lcm_orders_t *orders, void *user_data)
{
    ProcmanDeputy *deputy = (ProcmanDeputy*) user_data;
    _handle_orders2(deputy, orders);
}

static void
procman_deputy_discovery_received(const lcm_recv_buf_t* rbuf, const char* channel,
        const procman_lcm_discovery_t* msg, void* user_data)
{
    ProcmanDeputy *pmd = (ProcmanDeputy*)user_data;

    int64_t now = timestamp_now();
    if(now < pmd->deputy_start_time + DISCOVERY_TIME_MS * 1000) {
      // received a discovery message while still in discovery mode.  Check to
      // see if it's from a conflicting deputy.
      if(!strcmp(msg->host, pmd->hostname) && msg->nonce != pmd->deputy_pid) {
        dbgt("ERROR.  Detected another deputy named [%s].  Aborting to avoid conflicts.\n",
            msg->host);
        exit(1);
      }
    } else {
      // received a discovery message while not in discovery mode.  Respond by
      // transmitting deputy info.
      transmit_proc_info(pmd);
    }
}

static void
procman_deputy_info2_received(const lcm_recv_buf_t *rbuf, const char *channel,
        const procman_lcm_deputy_info_t *msg, void *user_data)
{
    ProcmanDeputy *pmd = (ProcmanDeputy*) user_data;

    int64_t now = timestamp_now();
    if(now < pmd->deputy_start_time + DISCOVERY_TIME_MS * 1000) {
      // A different deputy has reported while we're still in discovery mode.
      // Check to see if the deputy names are in conflict.
      if(!strcmp(msg->host, pmd->hostname)) {
        dbgt("ERROR.  Detected another deputy named [%s].  Aborting to avoid conflicts.\n",
            msg->host);
        exit(2);
      }
    } else {
      dbgt("WARNING:  Still processing info messages while not in discovery mode??\n");
    }
}

static gboolean
discovery_timeout(ProcmanDeputy* pmd)
{
    int64_t now = timestamp_now();
    if(now < pmd->deputy_start_time + DISCOVERY_TIME_MS * 1000)
    {
        // publish a discover message to check for conflicting deputies
        procman_lcm_discovery_t msg;
        msg.utime = now;
        msg.host = pmd->hostname;
        msg.nonce = pmd->deputy_pid;
        procman_lcm_discovery_t_publish(pmd->lcm, "PM_DISCOVER", &msg);
        return TRUE;
    } else {
        // discovery period is over.

        // Adjust subscriptions
        procman_lcm_deputy_info_t_unsubscribe(pmd->lcm, pmd->info2_subs);
        pmd->info2_subs = NULL;

        procman_lcm_discovery_t_unsubscribe(pmd->lcm, pmd->discovery_subs);
        pmd->discovery_subs = NULL;

        pmd->discovery_subs = procman_lcm_discovery_t_subscribe(pmd->lcm,
            "PM_DISCOVER", procman_deputy_discovery_received, pmd);

        pmd->orders2_subs = procman_lcm_orders_t_subscribe (pmd->lcm,
                "PM_ORDERS", procman_deputy_order2_received, pmd);

        // setup a timer to periodically transmit status information
        g_timeout_add(1000, (GSourceFunc) one_second_timeout, pmd);

        one_second_timeout(pmd);
        return FALSE;
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
    char *hostname_override = NULL;
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
                free(hostname_override);
                hostname_override = strdup (optarg);
                break;
            case 'h':
            default:
                usage();
                return 1;
        }
    }

     // create the lcm_t structure for doing IPC
     lcm_t *lcm = lcm_create(lcmurl);
     free(lcmurl);
     if (NULL == lcm) {
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

     pmd->lcm = lcm;
     pmd->verbose = verbose;
     pmd->norders_slm = 0;
     pmd->nstale_orders_slm = 0;
     pmd->norders_forme_slm = 0;
     pmd->last_sheriff_name = NULL;
     pmd->exiting = 0;
     pmd->deputy_start_time = timestamp_now();
     pmd->deputy_pid = getpid();

     pmd->mainloop = g_main_loop_new (NULL, FALSE);
     if (!pmd->mainloop) {
         fprintf (stderr, "Error: Failed to create glib mainloop\n");
         return 1;
     }

     // set deputy hostname to the system hostname
     if (hostname_override) {
         strcpy (pmd->hostname, hostname_override);
         free (hostname_override);
     } else {
         gethostname (pmd->hostname, sizeof (pmd->hostname));
     }
//     sprintf (pmd->hostname + strlen (pmd->hostname), "%d", getpid());

     // load config file
     ProcmanOptions options = ProcmanOptions::Default(argc, argv);
     pmd->pm = procman_create(options);

     // convert Unix signals into glib events
     signal_pipe_init();
     signal_pipe_add_signal (SIGINT);
     signal_pipe_add_signal (SIGHUP);
     signal_pipe_add_signal (SIGQUIT);
     signal_pipe_add_signal (SIGTERM);
     signal_pipe_add_signal (SIGCHLD);
     signal_pipe_attach_glib ((signal_pipe_glib_handler_t) glib_handle_signal,
             pmd);

     // setup LCM handler
     lcmu_glib_mainloop_attach_lcm (pmd->lcm);

     pmd->info2_subs =
         procman_lcm_deputy_info_t_subscribe(pmd->lcm, "PM_INFO",
                 procman_deputy_info2_received, pmd);

     pmd->discovery_subs =
         procman_lcm_discovery_t_subscribe(pmd->lcm, "PM_DISCOVER",
                 procman_deputy_discovery_received, pmd);

     // periodically publish a discovery message on startup.
     g_timeout_add(200, (GSourceFunc)discovery_timeout, pmd);
     discovery_timeout(pmd);

     // periodically check memory usage
     g_timeout_add (120000, (GSourceFunc) introspection_timeout, pmd);

     // go!
     g_main_loop_run (pmd->mainloop);

     lcmu_glib_mainloop_detach_lcm (pmd->lcm);

     // cleanup
     signal_pipe_cleanup();

     procman_destroy (pmd->pm);

     g_main_loop_unref (pmd->mainloop);

     // unsubscribe
     if(pmd->orders2_subs)
         procman_lcm_orders_t_unsubscribe(pmd->lcm, pmd->orders2_subs);
     if(pmd->info2_subs)
         procman_lcm_deputy_info_t_unsubscribe(pmd->lcm, pmd->info2_subs);
     if(pmd->discovery_subs)
         procman_lcm_discovery_t_unsubscribe(pmd->lcm, pmd->discovery_subs);

     pmd->observed_sheriffs_slm.clear();

     lcm_destroy (pmd->lcm);

     if (pmd->last_sheriff_name) free (pmd->last_sheriff_name);

     delete pmd;

     return 0;
}
