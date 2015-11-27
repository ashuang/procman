/*
 * code for reading detailed process information on a GNU/Llinux system
 */

#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <ctype.h>
#include <string.h>
#include <stdlib.h>
#include <limits.h>
#include <inttypes.h>
#include <assert.h>
#include <sys/types.h>
#include <dirent.h>

#include <map>

#include "procinfo.hpp"

namespace procman {

static void strsplit (char *buf, char **words, int maxwords) {
  int inword = 0;
  int i;
  int wordind = 0;
  for (i=0; buf[i] != 0; i++) {
    if (isspace (buf[i])) {
      inword = 0;
      buf[i] = 0;
    } else {
      if (! inword) {
        words[wordind] = buf + i;
        wordind++;
        if (wordind >= maxwords) break;
        inword = 1;
      }
    }
  }
  words[wordind] = NULL;
}

static int procinfo_read_proc_cpu_mem_linux(int pid, ProcessInfo *procinfo) {
  memset (procinfo, 0, sizeof (ProcessInfo));
  char fname[80];
  sprintf (fname, "/proc/%d/stat", pid);
  FILE *fp = fopen (fname, "r");
  if (! fp) { return -1; }

  char buf[4096];
  if(!fgets (buf, sizeof (buf), fp)) {
    return -1;
  }
  char *words[50];
  memset (words, 0, sizeof(words));
  strsplit (buf, words, 50);

  procinfo->user = atoi (words[13]);
  procinfo->system = atoi (words[14]);
  procinfo->vsize = strtoll (words[22], NULL, 10);
  procinfo->rss = strtoll (words[23], NULL, 10) * getpagesize();

  fclose (fp);

  sprintf (fname, "/proc/%d/statm", pid);
  fp = fopen (fname, "r");
  if (! fp) { return -1; }

  if(!fgets (buf, sizeof(buf), fp)) {
    return -1;
  }
  memset (words, 0, sizeof(words));
  strsplit (buf, words, 50);

  procinfo->shared = atoi (words[2]) * getpagesize();
  procinfo->text = atoi (words[3]) * getpagesize();
  procinfo->data = atoi (words[5]) * getpagesize();

  fclose (fp);

  return 0;
}

static int procinfo_read_sys_cpu_mem_linux(SystemInfo *sysinfo) {
  memset (sysinfo, 0, sizeof(SystemInfo));
  FILE *fp = fopen ("/proc/stat", "r");
  if (! fp) { return -1; }

  char buf[4096];
  char tmp[80];

  while (! feof (fp)) {
    if(!fgets (buf, sizeof (buf), fp)) {
      if(feof(fp))
        break;
      else
        return -1;
    }

    if (! strncmp (buf, "cpu ", 4)) {
      sscanf (buf, "%s %u %u %u %u",
          tmp,
          &sysinfo->user,
          &sysinfo->user_low,
          &sysinfo->system,
          &sysinfo->idle);
      break;
    }
  }
  fclose (fp);

  fp = fopen ("/proc/meminfo", "r");
  if (! fp) { return -1; }
  while (! feof (fp)) {
    char units[10];
    memset (units,0,sizeof(units));
    if(!fgets (buf, sizeof (buf), fp)) {
      if(feof(fp))
        break;
      else
        return -1;
    }

    if (! strncmp ("MemTotal:", buf, strlen ("MemTotal:"))) {
      sscanf (buf, "MemTotal: %" PRId64" %9s", &sysinfo->memtotal, units);
      sysinfo->memtotal *= 1024;
    } else if (! strncmp ("MemFree:", buf, strlen ("MemFree:"))) {
      sscanf (buf, "MemFree: %" PRId64" %9s", &sysinfo->memfree, units);
      sysinfo->memfree *= 1024;
    } else if (! strncmp ("SwapTotal:", buf, strlen("SwapTotal:"))) {
      sscanf (buf, "SwapTotal: %" PRId64" %9s", &sysinfo->swaptotal, units);
      sysinfo->swaptotal *= 1024;
    } else if (! strncmp ("SwapFree:", buf, strlen("SwapFree:"))) {
      sscanf (buf, "SwapFree: %" PRId64" %9s", &sysinfo->swapfree, units);
      sysinfo->swapfree *= 1024;
    } else {
      continue;
    }

    if (0 != strcmp (units, "kB")) {
      fprintf (stderr, "unknown units [%s] while reading "
          "/proc/meminfo!!!\n", units);
    }
  }

  fclose (fp);

  return 0;
}

struct PidInfo {
  int pid;
  int ppid;
  int pgrp;
  int session;
  char state;

  std::vector<PidInfo*> children;
};

bool GetPidInfo(int pid, PidInfo* result) {
  result->pid = pid;
  result->children.clear();

  char fname[40];
  snprintf(fname, 40, "/proc/%d/stat", pid);
  FILE* fp = fopen(fname, "r");
  if(!fp) {
    return false;
  }
  int read_pid;
  char exec_name[PATH_MAX + 1];
  int numwords = fscanf(fp, "%d %s %c %d %d %d", &read_pid, exec_name,
      &result->state, &result->ppid, &result->pgrp, &result->session);
  fclose(fp);
  if(6 != numwords) {
    return false;
  }
  return true;
}

bool procinfo_is_orphaned_child_of(int orphan, int parent) {
  PidInfo pinfo;
  if (!GetPidInfo(orphan, &pinfo)) {
    return false;
  }
  return (pinfo.ppid == 1 &&
      pinfo.pgrp == parent &&
      pinfo.session == parent);
}

static std::map<int, PidInfo> get_all_pids_and_ppids() {
  std::map<int, PidInfo> result;
  DIR* procdir = opendir("/proc");
  if (!procdir) {
    return result;
  }

  struct dirent* entry;
  for (entry = readdir(procdir); entry; entry = readdir(procdir)) {
    const int pid = atoi(entry->d_name);
    if(!pid) {
      continue;
    }
    result[pid] = PidInfo();
    PidInfo& pinfo = result[pid];
    if (!GetPidInfo(pid, &pinfo)) {
      continue;
    }
    auto iter = result.find(pinfo.ppid);
    if (iter != result.end()) {
      PidInfo& parent = iter->second;
      parent.children.push_back(&pinfo);
    }
  }
  closedir(procdir);
  return result;
}

static void pid_info_get_descendants(const PidInfo& pinfo,
    std::vector<int>* result) {
  for (PidInfo* child : pinfo.children) {
    assert(child->ppid == pinfo.pid);
    result->push_back(child->pid);
    pid_info_get_descendants(*child, result);
  }
}

std::vector<int> procinfo_get_descendants(int pid) {
  std::vector<int> result;

  std::map<int, PidInfo> pid_graph = get_all_pids_and_ppids();
  auto iter = pid_graph.find(pid);
  if (iter != pid_graph.end()) {
    PidInfo& root = iter->second;
    pid_info_get_descendants(root, &result);
  }
  return result;
}

int procinfo_read_proc_cpu_mem (int pid, ProcessInfo *s) {
  return procinfo_read_proc_cpu_mem_linux(pid, s);
}

int procinfo_read_sys_cpu_mem (SystemInfo *s) {
  return procinfo_read_sys_cpu_mem_linux(s);
}

}  // namespace procman
