cmake_minimum_required(VERSION 2.8.11)

project(procman)

set(CMAKE_CXX_STANDARD 11)

set(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR}/bin)
set(LIBRARY_OUTPUT_PATH ${CMAKE_BINARY_DIR}/lib)

# Create a directory for build-generated header files
set(INCLUDE_OUTPUT_PATH ${CMAKE_CURRENT_BINARY_DIR}/include)
execute_process(COMMAND mkdir -p ${INCLUDE_OUTPUT_PATH})

# build LCM types.
include(cmake/lcmtypes.cmake)
lcmtypes_build(INCLUDE_BUILD_PATH ${INCLUDE_OUTPUT_PATH}
               TYPES
               lcmtypes/procman_lcm_cmd_t.lcm
               lcmtypes/procman_lcm_cmd_status_t.lcm
               lcmtypes/procman_lcm_discovery_t.lcm
               lcmtypes/procman_lcm_deputy_info_t.lcm
               lcmtypes/procman_lcm_orders_t.lcm
               lcmtypes/procman_lcm_output_t.lcm
               lcmtypes/procman_lcm_cmd_desired_t.lcm)

add_subdirectory(src)
add_subdirectory(python)
