#include "procinfo.hpp"

#include <string.h>

namespace procman {

std::vector<int> GetDescendants(int pid) {
  return std::vector<int>();
}

bool IsOrphanedChildOf(int orphan, int parent) {
  return false;
}

bool ReadProcessInfo(int pid, ProcessInfo *procinfo) {
  memset(procinfo, 0, sizeof(ProcessInfo));
  return false;
}

bool ReadSystemInfo(SystemInfo *sysinfo) {
  memset(sysinfo, 0, sizeof(SystemInfo));
  return false;
}

}  // namespace procman
