add_definitions(-std=gnu99)

add_executable(procman-deputy
    lcm_util.c
    procinfo.c
    procman.c
    procman_deputy.c
    signal_pipe.c)

find_package(PkgConfig REQUIRED)

pkg_check_modules(DEPS REQUIRED glib-2.0 gthread-2.0 lcm)

include_directories(${DEPS_INCLUDE_DIRS})

target_link_libraries(procman-deputy
                      util
                      procman_lcmtypes
                      ${DEPS_LDFLAGS})

install(TARGETS procman-deputy DESTINATION bin)
