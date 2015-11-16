add_definitions(-std=c++11)

if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  set(procinfo_cpp procinfo_linux.cpp)
else()
  set(procinfo_cpp procinfo_generic.cpp)
endif()

add_executable(procman-deputy
    lcm_util.cpp
    ${procinfo_cpp}
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
