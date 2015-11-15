cmake_minimum_required(VERSION 2.6.0)

project(procman)

# build LCM types.
include(cmake/lcmtypes.cmake)
lcmtypes_build(TYPES
               lcmtypes/procman_command2_t.lcm
               lcmtypes/procman_deputy_cmd2_t.lcm
               lcmtypes/procman_discovery_t.lcm
               lcmtypes/procman_info2_t.lcm
               lcmtypes/procman_orders2_t.lcm
               lcmtypes/procman_printf_t.lcm
               lcmtypes/procman_sheriff_cmd2_t.lcm)

add_subdirectory(src/deputy)
add_subdirectory(python)
