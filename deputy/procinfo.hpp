#ifndef PROCMAN_PROCINFO_HPP__
#define PROCMAN_PROCINFO_HPP__

// functions for reading how much CPU and memory are being used by individual
// processes and the system as a whole

#include <cstdint>
#include <vector>

namespace procman {

struct ProcessInfo {
  int pid;

  // cpu usage time
  uint32_t user;
  uint32_t system;

  // memory usage

  // VSIZE bytes
  int64_t  vsize;

  // RSS bytes
  int64_t  rss;

  // SHR bytes
  int64_t  shared;

  int64_t  text;
  int64_t  data;
};

struct SystemInfo {
  uint32_t user;
  uint32_t user_low;
  uint32_t system;
  uint32_t idle;

  int64_t memtotal;
  int64_t memfree;
  int64_t swaptotal;
  int64_t swapfree;
};

bool ReadProcessInfo(int pid, ProcessInfo *s);

bool ReadSystemInfo(SystemInfo *s);

std::vector<int> GetDescendants(int pid);

bool IsOrphanedChildOf(int orphan, int parent);

}  // namespace procman

#endif   // PROCMAN_PROCINFO_HPP__
