# Procman

Procman is a tool for managing many processes distributed over one or more
computers. There are several ways to use procman:

1. Sheriff / Deputy GUI mode

  In this mode, every workstation runs a "deputy" process:

  ```
  procman-deputy
  ```

  One workstation runs a "sheriff" process, which provides a GUI to command and
  communicate with the deputies:

  ```
  procman-sheriff
  ```

  Using the GUI, you can:
  -  create/edit/remove processes
  -  start/stop/restart processes
  -  aggregate processes together into logical groups (e.g., "Planning")
  -  view the console output of each process
  -  save and load process configuration files
  -  view process statistics (memory, CPU usage)

  For the special case where you only want to run processes on the local
  computer, the sheriff can act as its own deputy.  To operate in lone ranger
  mode, run

  ```
  procman-sheriff --lone-ranger
  ```

2. C++ API

  Procman also provides a C++ API for spawning and managing child processes,
  comparable to the Python subprocess module.

## Build Instructions

### Dependencies

 * [LCM](http://lcm-proj.github.io)
 * Python
 * PyGTK  (procman-sheriff is written in Python with PyGTK)

Currently only tested on GNU/Linux.  Some stuff will definitely only work on
Linux (e.g., the process memory, CPU statistics).

### Local Install

After cloning procman, create and move a build directory 

```sh
mkdir build
cd build
```

Now run cmake (optionally use `-DCMAKE_INSTALL_PREFIX` to change the local install directory from `/usr/local` to a different directory):

```sh
cmake ..
make
```

### Documentation

  Documentation is built with Doxygen.

  ```
  cd doc
  doxygen
  ```


