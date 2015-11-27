set(CMAKE_CXX_FLAGS "-std=c++11 ${CMAKE_CXX_FLAGS}")

if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  set(procinfo_cpp procinfo_linux.cpp)
else()
  set(procinfo_cpp procinfo_generic.cpp)
endif()

add_executable(procman-deputy
    ${procinfo_cpp}
    event_loop.cpp
    exec_string_utils.cpp
    procman.cpp
    procman_deputy.cpp)

find_package(PkgConfig REQUIRED)
pkg_check_modules(LCM REQUIRED lcm)
include_directories(${LCM_INCLUDE_DIRS})

target_link_libraries(procman-deputy
                      util
                      procman_lcmtypes
                      ${LCM_LDFLAGS})

install(TARGETS procman-deputy DESTINATION bin)

add_executable(event_loop_test event_loop_test.cpp event_loop.cpp)
