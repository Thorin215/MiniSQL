# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.16

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:


#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:


# Remove some rules from gmake that .SUFFIXES does not remove.
SUFFIXES =

.SUFFIXES: .hpux_make_needs_suffix_list


# Suppress display of executed commands.
$(VERBOSE).SILENT:


# A target that is always out of date.
cmake_force:

.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /usr/bin/cmake

# The command to remove a file.
RM = /usr/bin/cmake -E remove -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /mnt/d/CS/SQL-studying/MiniSQL

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /mnt/d/CS/SQL-studying/MiniSQL/cmake-build-debug-wsl

# Utility rule file for NightlyMemoryCheck.

# Include the progress variables for this target.
include glog-build/CMakeFiles/NightlyMemoryCheck.dir/progress.make

glog-build/CMakeFiles/NightlyMemoryCheck:
	cd /mnt/d/CS/SQL-studying/MiniSQL/cmake-build-debug-wsl/glog-build && /usr/bin/ctest -D NightlyMemoryCheck

NightlyMemoryCheck: glog-build/CMakeFiles/NightlyMemoryCheck
NightlyMemoryCheck: glog-build/CMakeFiles/NightlyMemoryCheck.dir/build.make

.PHONY : NightlyMemoryCheck

# Rule to build all files generated by this target.
glog-build/CMakeFiles/NightlyMemoryCheck.dir/build: NightlyMemoryCheck

.PHONY : glog-build/CMakeFiles/NightlyMemoryCheck.dir/build

glog-build/CMakeFiles/NightlyMemoryCheck.dir/clean:
	cd /mnt/d/CS/SQL-studying/MiniSQL/cmake-build-debug-wsl/glog-build && $(CMAKE_COMMAND) -P CMakeFiles/NightlyMemoryCheck.dir/cmake_clean.cmake
.PHONY : glog-build/CMakeFiles/NightlyMemoryCheck.dir/clean

glog-build/CMakeFiles/NightlyMemoryCheck.dir/depend:
	cd /mnt/d/CS/SQL-studying/MiniSQL/cmake-build-debug-wsl && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /mnt/d/CS/SQL-studying/MiniSQL /mnt/d/CS/SQL-studying/MiniSQL/thirdparty/glog /mnt/d/CS/SQL-studying/MiniSQL/cmake-build-debug-wsl /mnt/d/CS/SQL-studying/MiniSQL/cmake-build-debug-wsl/glog-build /mnt/d/CS/SQL-studying/MiniSQL/cmake-build-debug-wsl/glog-build/CMakeFiles/NightlyMemoryCheck.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : glog-build/CMakeFiles/NightlyMemoryCheck.dir/depend

