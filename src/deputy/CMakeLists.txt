add_definitions(-std=gnu99)

add_executable(procman-deputy
    lcm_util.c
    procinfo.c
    procman.c
    procman_deputy.c
    signal_pipe.c)

pods_use_pkg_config_packages(procman-deputy
    glib-2.0
    gthread-2.0
    lcm
    lcmtypes_procman)
target_link_libraries(procman-deputy util)

pods_install_executables(procman-deputy)
