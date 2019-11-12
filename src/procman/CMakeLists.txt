if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  set(procinfo_cpp procinfo_linux.cpp)
else()
  set(procinfo_cpp procinfo_generic.cpp)
endif()

add_library(procman SHARED
    exec_string_utils.cpp
    ${procinfo_cpp}
    procman.cpp)

target_link_libraries(procman util)

install(TARGETS procman LIBRARY DESTINATION lib)

install(FILES procman.hpp procinfo.hpp DESTINATION include/procman)

set(PROCMAN_VERSION 0.0.1)

# Create a .pc (pkg-config) file to install
set(pc_fname ${CMAKE_CURRENT_BINARY_DIR}/procman.pc)
file(WRITE ${pc_fname}
     "prefix=${CMAKE_INSTALL_PREFIX}\n"
     "exec_prefix=\${prefix}\n"
     "libdir=${CMAKE_INSTALL_PREFIX}/lib\n"
     "includedir=${CMAKE_INSTALL_PREFIX}/include\n"
     "\n"
     "Name: procman\n"
     "Description: Procman\n"
     "Version: ${PROCMAN_VERSION}\n"
     "Libs: -L\${libdir} -Wl,-R\${libdir} -lprocman\n"
     "Cflags: -I\${includedir}\n")
install(FILES ${pc_fname} DESTINATION lib/pkgconfig)

