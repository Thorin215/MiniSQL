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

# Include any dependencies generated for this target.
include test/CMakeFiles/lru_replacer_test.dir/depend.make

# Include the progress variables for this target.
include test/CMakeFiles/lru_replacer_test.dir/progress.make

# Include the compile flags for this target's objects.
include test/CMakeFiles/lru_replacer_test.dir/flags.make

test/CMakeFiles/lru_replacer_test.dir/buffer/lru_replacer_test.cpp.o: test/CMakeFiles/lru_replacer_test.dir/flags.make
test/CMakeFiles/lru_replacer_test.dir/buffer/lru_replacer_test.cpp.o: ../test/buffer/lru_replacer_test.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/mnt/d/CS/SQL-studying/MiniSQL/cmake-build-debug-wsl/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object test/CMakeFiles/lru_replacer_test.dir/buffer/lru_replacer_test.cpp.o"
	cd /mnt/d/CS/SQL-studying/MiniSQL/cmake-build-debug-wsl/test && /usr/bin/g++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/lru_replacer_test.dir/buffer/lru_replacer_test.cpp.o -c /mnt/d/CS/SQL-studying/MiniSQL/test/buffer/lru_replacer_test.cpp

test/CMakeFiles/lru_replacer_test.dir/buffer/lru_replacer_test.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/lru_replacer_test.dir/buffer/lru_replacer_test.cpp.i"
	cd /mnt/d/CS/SQL-studying/MiniSQL/cmake-build-debug-wsl/test && /usr/bin/g++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /mnt/d/CS/SQL-studying/MiniSQL/test/buffer/lru_replacer_test.cpp > CMakeFiles/lru_replacer_test.dir/buffer/lru_replacer_test.cpp.i

test/CMakeFiles/lru_replacer_test.dir/buffer/lru_replacer_test.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/lru_replacer_test.dir/buffer/lru_replacer_test.cpp.s"
	cd /mnt/d/CS/SQL-studying/MiniSQL/cmake-build-debug-wsl/test && /usr/bin/g++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /mnt/d/CS/SQL-studying/MiniSQL/test/buffer/lru_replacer_test.cpp -o CMakeFiles/lru_replacer_test.dir/buffer/lru_replacer_test.cpp.s

# Object files for target lru_replacer_test
lru_replacer_test_OBJECTS = \
"CMakeFiles/lru_replacer_test.dir/buffer/lru_replacer_test.cpp.o"

# External object files for target lru_replacer_test
lru_replacer_test_EXTERNAL_OBJECTS =

test/lru_replacer_test: test/CMakeFiles/lru_replacer_test.dir/buffer/lru_replacer_test.cpp.o
test/lru_replacer_test: test/CMakeFiles/lru_replacer_test.dir/build.make
test/lru_replacer_test: bin/libzSql.so
test/lru_replacer_test: test/libminisql_test_main.so
test/lru_replacer_test: glog-build/libglogd.so.0.6.0
test/lru_replacer_test: lib/libgtestd.so.1.11.0
test/lru_replacer_test: test/CMakeFiles/lru_replacer_test.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/mnt/d/CS/SQL-studying/MiniSQL/cmake-build-debug-wsl/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable lru_replacer_test"
	cd /mnt/d/CS/SQL-studying/MiniSQL/cmake-build-debug-wsl/test && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/lru_replacer_test.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
test/CMakeFiles/lru_replacer_test.dir/build: test/lru_replacer_test

.PHONY : test/CMakeFiles/lru_replacer_test.dir/build

test/CMakeFiles/lru_replacer_test.dir/clean:
	cd /mnt/d/CS/SQL-studying/MiniSQL/cmake-build-debug-wsl/test && $(CMAKE_COMMAND) -P CMakeFiles/lru_replacer_test.dir/cmake_clean.cmake
.PHONY : test/CMakeFiles/lru_replacer_test.dir/clean

test/CMakeFiles/lru_replacer_test.dir/depend:
	cd /mnt/d/CS/SQL-studying/MiniSQL/cmake-build-debug-wsl && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /mnt/d/CS/SQL-studying/MiniSQL /mnt/d/CS/SQL-studying/MiniSQL/test /mnt/d/CS/SQL-studying/MiniSQL/cmake-build-debug-wsl /mnt/d/CS/SQL-studying/MiniSQL/cmake-build-debug-wsl/test /mnt/d/CS/SQL-studying/MiniSQL/cmake-build-debug-wsl/test/CMakeFiles/lru_replacer_test.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : test/CMakeFiles/lru_replacer_test.dir/depend

