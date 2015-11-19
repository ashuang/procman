set(CMAKE_INCLUDE_CURRENT_DIR ON)
set(CMAKE_AUTOMOC ON)

add_definitions(-std=c++11 -fsanitize=address -fno-optimize-sibling-calls -fno-omit-frame-pointer)
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")

if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  set(procinfo_cpp procinfo_linux.cpp)
else()
  set(procinfo_cpp procinfo_generic.cpp)
endif()

add_executable(procman-deputy
    ${procinfo_cpp}
    procman.cpp
    procman_deputy.cpp
    signal_pipe.cpp)

find_package(PkgConfig REQUIRED)
pkg_check_modules(LCM REQUIRED lcm)
include_directories(${LCM_INCLUDE_DIRS})

target_link_libraries(procman-deputy
                      util
                      procman_lcmtypes
                      Qt5::Core
                      ${LCM_LDFLAGS})

install(TARGETS procman-deputy DESTINATION bin)
