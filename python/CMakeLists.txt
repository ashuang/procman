find_package(PythonInterp REQUIRED)

execute_process(COMMAND
    ${PYTHON_EXECUTABLE} -c "import sys; sys.stdout.write(sys.version[:3])"
    OUTPUT_VARIABLE pyversion)

set(py_install_dir lib/python${pyversion}/dist-packages)

set(build_prefix_py ${CMAKE_CURRENT_BINARY_DIR}/src/procman/build_prefix.py)
file(WRITE ${build_prefix_py}
     "BUILD_PREFIX='${CMAKE_INSTALL_PREFIX}'\n")
install(FILES ${build_prefix_py} DESTINATION ${py_install_dir}/procman)

# install python package
install(DIRECTORY src/procman
       DESTINATION ${py_install_dir}
       FILES_MATCHING PATTERN "*.py")

# Install glade file
install(FILES procman-sheriff.glade DESTINATION share/procman)

# procman-sheriff executable script
set(procman_sheriff ${CMAKE_CURRENT_BINARY_DIR}/procman-sheriff)
file(WRITE ${procman_sheriff}
           "#!/usr/bin/env python2\n"
           "import sys\n"
           "sys.path.append('${CMAKE_INSTALL_PREFIX}/${py_install_dir}')\n"
           "import procman.sheriff_gtk.sheriff_gtk\n"
           "procman.sheriff_gtk.sheriff_gtk.main()\n")
install(PROGRAMS ${procman_sheriff} DESTINATION bin)
