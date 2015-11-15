add_definitions(-std=c++11)

add_executable(procman-deputy
    lcm_util.cpp
    procinfo.cpp
    procman.cpp
    procman_deputy.cpp
    signal_pipe.cpp)

find_package(PkgConfig REQUIRED)

pkg_check_modules(DEPS REQUIRED glib-2.0 gthread-2.0 lcm)

include_directories(${DEPS_INCLUDE_DIRS})

target_link_libraries(procman-deputy
                      util
                      procman_lcmtypes
                      ${DEPS_LDFLAGS})

install(TARGETS procman-deputy DESTINATION bin)
