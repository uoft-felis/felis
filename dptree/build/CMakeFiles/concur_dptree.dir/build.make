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


# Produce verbose output by default.
VERBOSE = 1

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
CMAKE_SOURCE_DIR = /home/shirley/DPTree-code

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/shirley/DPTree-code/build

# Include any dependencies generated for this target.
include CMakeFiles/concur_dptree.dir/depend.make

# Include the progress variables for this target.
include CMakeFiles/concur_dptree.dir/progress.make

# Include the compile flags for this target's objects.
include CMakeFiles/concur_dptree.dir/flags.make

CMakeFiles/concur_dptree.dir/test/concur_dptree_test.cxx.o: CMakeFiles/concur_dptree.dir/flags.make
CMakeFiles/concur_dptree.dir/test/concur_dptree_test.cxx.o: ../test/concur_dptree_test.cxx
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/shirley/DPTree-code/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object CMakeFiles/concur_dptree.dir/test/concur_dptree_test.cxx.o"
	/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/concur_dptree.dir/test/concur_dptree_test.cxx.o -c /home/shirley/DPTree-code/test/concur_dptree_test.cxx

CMakeFiles/concur_dptree.dir/test/concur_dptree_test.cxx.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/concur_dptree.dir/test/concur_dptree_test.cxx.i"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/shirley/DPTree-code/test/concur_dptree_test.cxx > CMakeFiles/concur_dptree.dir/test/concur_dptree_test.cxx.i

CMakeFiles/concur_dptree.dir/test/concur_dptree_test.cxx.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/concur_dptree.dir/test/concur_dptree_test.cxx.s"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/shirley/DPTree-code/test/concur_dptree_test.cxx -o CMakeFiles/concur_dptree.dir/test/concur_dptree_test.cxx.s

CMakeFiles/concur_dptree.dir/src/util.cpp.o: CMakeFiles/concur_dptree.dir/flags.make
CMakeFiles/concur_dptree.dir/src/util.cpp.o: ../src/util.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/shirley/DPTree-code/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Building CXX object CMakeFiles/concur_dptree.dir/src/util.cpp.o"
	/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/concur_dptree.dir/src/util.cpp.o -c /home/shirley/DPTree-code/src/util.cpp

CMakeFiles/concur_dptree.dir/src/util.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/concur_dptree.dir/src/util.cpp.i"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/shirley/DPTree-code/src/util.cpp > CMakeFiles/concur_dptree.dir/src/util.cpp.i

CMakeFiles/concur_dptree.dir/src/util.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/concur_dptree.dir/src/util.cpp.s"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/shirley/DPTree-code/src/util.cpp -o CMakeFiles/concur_dptree.dir/src/util.cpp.s

CMakeFiles/concur_dptree.dir/src/ART.cpp.o: CMakeFiles/concur_dptree.dir/flags.make
CMakeFiles/concur_dptree.dir/src/ART.cpp.o: ../src/ART.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/shirley/DPTree-code/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_3) "Building CXX object CMakeFiles/concur_dptree.dir/src/ART.cpp.o"
	/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/concur_dptree.dir/src/ART.cpp.o -c /home/shirley/DPTree-code/src/ART.cpp

CMakeFiles/concur_dptree.dir/src/ART.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/concur_dptree.dir/src/ART.cpp.i"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/shirley/DPTree-code/src/ART.cpp > CMakeFiles/concur_dptree.dir/src/ART.cpp.i

CMakeFiles/concur_dptree.dir/src/ART.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/concur_dptree.dir/src/ART.cpp.s"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/shirley/DPTree-code/src/ART.cpp -o CMakeFiles/concur_dptree.dir/src/ART.cpp.s

CMakeFiles/concur_dptree.dir/misc/ARTOLC/Epoche.cpp.o: CMakeFiles/concur_dptree.dir/flags.make
CMakeFiles/concur_dptree.dir/misc/ARTOLC/Epoche.cpp.o: ../misc/ARTOLC/Epoche.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/shirley/DPTree-code/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_4) "Building CXX object CMakeFiles/concur_dptree.dir/misc/ARTOLC/Epoche.cpp.o"
	/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/concur_dptree.dir/misc/ARTOLC/Epoche.cpp.o -c /home/shirley/DPTree-code/misc/ARTOLC/Epoche.cpp

CMakeFiles/concur_dptree.dir/misc/ARTOLC/Epoche.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/concur_dptree.dir/misc/ARTOLC/Epoche.cpp.i"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/shirley/DPTree-code/misc/ARTOLC/Epoche.cpp > CMakeFiles/concur_dptree.dir/misc/ARTOLC/Epoche.cpp.i

CMakeFiles/concur_dptree.dir/misc/ARTOLC/Epoche.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/concur_dptree.dir/misc/ARTOLC/Epoche.cpp.s"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/shirley/DPTree-code/misc/ARTOLC/Epoche.cpp -o CMakeFiles/concur_dptree.dir/misc/ARTOLC/Epoche.cpp.s

CMakeFiles/concur_dptree.dir/misc/ARTOLC/Tree.cpp.o: CMakeFiles/concur_dptree.dir/flags.make
CMakeFiles/concur_dptree.dir/misc/ARTOLC/Tree.cpp.o: ../misc/ARTOLC/Tree.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/shirley/DPTree-code/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_5) "Building CXX object CMakeFiles/concur_dptree.dir/misc/ARTOLC/Tree.cpp.o"
	/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/concur_dptree.dir/misc/ARTOLC/Tree.cpp.o -c /home/shirley/DPTree-code/misc/ARTOLC/Tree.cpp

CMakeFiles/concur_dptree.dir/misc/ARTOLC/Tree.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/concur_dptree.dir/misc/ARTOLC/Tree.cpp.i"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/shirley/DPTree-code/misc/ARTOLC/Tree.cpp > CMakeFiles/concur_dptree.dir/misc/ARTOLC/Tree.cpp.i

CMakeFiles/concur_dptree.dir/misc/ARTOLC/Tree.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/concur_dptree.dir/misc/ARTOLC/Tree.cpp.s"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/shirley/DPTree-code/misc/ARTOLC/Tree.cpp -o CMakeFiles/concur_dptree.dir/misc/ARTOLC/Tree.cpp.s

CMakeFiles/concur_dptree.dir/src/art_idx.cpp.o: CMakeFiles/concur_dptree.dir/flags.make
CMakeFiles/concur_dptree.dir/src/art_idx.cpp.o: ../src/art_idx.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/shirley/DPTree-code/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_6) "Building CXX object CMakeFiles/concur_dptree.dir/src/art_idx.cpp.o"
	/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/concur_dptree.dir/src/art_idx.cpp.o -c /home/shirley/DPTree-code/src/art_idx.cpp

CMakeFiles/concur_dptree.dir/src/art_idx.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/concur_dptree.dir/src/art_idx.cpp.i"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/shirley/DPTree-code/src/art_idx.cpp > CMakeFiles/concur_dptree.dir/src/art_idx.cpp.i

CMakeFiles/concur_dptree.dir/src/art_idx.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/concur_dptree.dir/src/art_idx.cpp.s"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/shirley/DPTree-code/src/art_idx.cpp -o CMakeFiles/concur_dptree.dir/src/art_idx.cpp.s

CMakeFiles/concur_dptree.dir/src/MurmurHash2.cpp.o: CMakeFiles/concur_dptree.dir/flags.make
CMakeFiles/concur_dptree.dir/src/MurmurHash2.cpp.o: ../src/MurmurHash2.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/shirley/DPTree-code/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_7) "Building CXX object CMakeFiles/concur_dptree.dir/src/MurmurHash2.cpp.o"
	/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/concur_dptree.dir/src/MurmurHash2.cpp.o -c /home/shirley/DPTree-code/src/MurmurHash2.cpp

CMakeFiles/concur_dptree.dir/src/MurmurHash2.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/concur_dptree.dir/src/MurmurHash2.cpp.i"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/shirley/DPTree-code/src/MurmurHash2.cpp > CMakeFiles/concur_dptree.dir/src/MurmurHash2.cpp.i

CMakeFiles/concur_dptree.dir/src/MurmurHash2.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/concur_dptree.dir/src/MurmurHash2.cpp.s"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/shirley/DPTree-code/src/MurmurHash2.cpp -o CMakeFiles/concur_dptree.dir/src/MurmurHash2.cpp.s

CMakeFiles/concur_dptree.dir/src/bloom.c.o: CMakeFiles/concur_dptree.dir/flags.make
CMakeFiles/concur_dptree.dir/src/bloom.c.o: ../src/bloom.c
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/shirley/DPTree-code/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_8) "Building C object CMakeFiles/concur_dptree.dir/src/bloom.c.o"
	/usr/bin/cc $(C_DEFINES) $(C_INCLUDES) $(C_FLAGS) -o CMakeFiles/concur_dptree.dir/src/bloom.c.o   -c /home/shirley/DPTree-code/src/bloom.c

CMakeFiles/concur_dptree.dir/src/bloom.c.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing C source to CMakeFiles/concur_dptree.dir/src/bloom.c.i"
	/usr/bin/cc $(C_DEFINES) $(C_INCLUDES) $(C_FLAGS) -E /home/shirley/DPTree-code/src/bloom.c > CMakeFiles/concur_dptree.dir/src/bloom.c.i

CMakeFiles/concur_dptree.dir/src/bloom.c.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling C source to assembly CMakeFiles/concur_dptree.dir/src/bloom.c.s"
	/usr/bin/cc $(C_DEFINES) $(C_INCLUDES) $(C_FLAGS) -S /home/shirley/DPTree-code/src/bloom.c -o CMakeFiles/concur_dptree.dir/src/bloom.c.s

# Object files for target concur_dptree
concur_dptree_OBJECTS = \
"CMakeFiles/concur_dptree.dir/test/concur_dptree_test.cxx.o" \
"CMakeFiles/concur_dptree.dir/src/util.cpp.o" \
"CMakeFiles/concur_dptree.dir/src/ART.cpp.o" \
"CMakeFiles/concur_dptree.dir/misc/ARTOLC/Epoche.cpp.o" \
"CMakeFiles/concur_dptree.dir/misc/ARTOLC/Tree.cpp.o" \
"CMakeFiles/concur_dptree.dir/src/art_idx.cpp.o" \
"CMakeFiles/concur_dptree.dir/src/MurmurHash2.cpp.o" \
"CMakeFiles/concur_dptree.dir/src/bloom.c.o"

# External object files for target concur_dptree
concur_dptree_EXTERNAL_OBJECTS =

concur_dptree: CMakeFiles/concur_dptree.dir/test/concur_dptree_test.cxx.o
concur_dptree: CMakeFiles/concur_dptree.dir/src/util.cpp.o
concur_dptree: CMakeFiles/concur_dptree.dir/src/ART.cpp.o
concur_dptree: CMakeFiles/concur_dptree.dir/misc/ARTOLC/Epoche.cpp.o
concur_dptree: CMakeFiles/concur_dptree.dir/misc/ARTOLC/Tree.cpp.o
concur_dptree: CMakeFiles/concur_dptree.dir/src/art_idx.cpp.o
concur_dptree: CMakeFiles/concur_dptree.dir/src/MurmurHash2.cpp.o
concur_dptree: CMakeFiles/concur_dptree.dir/src/bloom.c.o
concur_dptree: CMakeFiles/concur_dptree.dir/build.make
concur_dptree: CMakeFiles/concur_dptree.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/home/shirley/DPTree-code/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_9) "Linking CXX executable concur_dptree"
	$(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/concur_dptree.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
CMakeFiles/concur_dptree.dir/build: concur_dptree

.PHONY : CMakeFiles/concur_dptree.dir/build

CMakeFiles/concur_dptree.dir/clean:
	$(CMAKE_COMMAND) -P CMakeFiles/concur_dptree.dir/cmake_clean.cmake
.PHONY : CMakeFiles/concur_dptree.dir/clean

CMakeFiles/concur_dptree.dir/depend:
	cd /home/shirley/DPTree-code/build && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/shirley/DPTree-code /home/shirley/DPTree-code /home/shirley/DPTree-code/build /home/shirley/DPTree-code/build /home/shirley/DPTree-code/build/CMakeFiles/concur_dptree.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : CMakeFiles/concur_dptree.dir/depend

