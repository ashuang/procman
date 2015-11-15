cmake_minimum_required(VERSION 2.6.0)

project(procman)

# build LCM types.
include(cmake/lcmtypes.cmake)
lcmtypes_build(TYPES
               lcmtypes/procman_lcm_command2_t.lcm
               lcmtypes/procman_lcm_deputy_cmd2_t.lcm
               lcmtypes/procman_lcm_discovery_t.lcm
               lcmtypes/procman_lcm_info2_t.lcm
               lcmtypes/procman_lcm_orders2_t.lcm
               lcmtypes/procman_lcm_printf_t.lcm
               lcmtypes/procman_lcm_sheriff_cmd2_t.lcm)

add_subdirectory(src/deputy)
add_subdirectory(python)
