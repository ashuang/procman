#include "procinfo.hpp"

#include <string.h>

namespace procman {

std::vector<int> procinfo_get_descendants(int pid) {
  return std::vector<int>();
}

int procinfo_is_orphaned_child_of(int orphan, int parent) {
  return 0;
}

int procinfo_read_proc_cpu_mem (int pid, proc_cpu_mem_t *s) {
  memset(s, 0, sizeof(proc_cpu_mem_t));
  return 0;
}

int procinfo_read_sys_cpu_mem (sys_cpu_mem_t *s) {
  memset(s, 0, sizeof(sys_cpu_mem_t));
  return 0;
}

}  // namespace procman
