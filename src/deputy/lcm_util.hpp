#ifndef PROCMAN_LCM_UTIL_HPP__
#define PROCMAN_LCM_UTIL_HPP__

#include <glib.h>

#include <lcm/lcm.h>

namespace procman {

/**
 * lcu_mainloop_attach_lc (lc_t *lc)
 * attaches/detaches LC to/from the glib mainloop
 * When attached, lc_handle() is invoked "automatically" when a message is
 * received over LC.
 *
 * only one instance of lc_t can be attached per process
 *
 * returns 0 on success, -1 on failure
 */
int lcmu_glib_mainloop_attach_lcm (lcm_t *lcm);

int lcmu_glib_mainloop_detach_lcm (lcm_t *lcm);

}  // namespace procman

#endif  // PROCMAN_LCM_UTIL_HPP__
