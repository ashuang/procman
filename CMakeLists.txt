cmake_minimum_required(VERSION 2.8.11)

project(procman)

find_package(Qt5Core)

# build LCM types.
include(cmake/lcmtypes.cmake)
lcmtypes_build(TYPES
               lcmtypes/procman_lcm_cmd_t.lcm
               lcmtypes/procman_lcm_cmd_status_t.lcm
               lcmtypes/procman_lcm_discovery_t.lcm
               lcmtypes/procman_lcm_deputy_info_t.lcm
               lcmtypes/procman_lcm_orders_t.lcm
               lcmtypes/procman_lcm_output_t.lcm
               lcmtypes/procman_lcm_cmd_desired_t.lcm)

add_subdirectory(src/deputy)
add_subdirectory(python)
