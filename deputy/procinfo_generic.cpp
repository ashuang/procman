#include "procinfo.hpp"

#include <string.h>

namespace procman {

std::vector<int> procinfo_get_descendants(int pid) {
  return std::vector<int>();
}

int procinfo_is_orphaned_child_of(int orphan, int parent) {
  return 0;
}

int procinfo_read_proc_cpu_mem (int pid, ProcessInfo *s) {
  memset(s, 0, sizeof(ProcessInfo));
  return 0;
}

int procinfo_read_sys_cpu_mem (SystemInfo *s) {
  memset(s, 0, sizeof(SystemInfo));
  return 0;
}

}  // namespace procman
