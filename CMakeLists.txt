cmake_minimum_required(VERSION 3.0)

option(ENABLE_TEST "Build tests." OFF)
option(REPLAY_OPT "Replay Style" "")

project(dolly C CXX ASM)

add_definitions(-pthread -DNR_THREADS=${NR_THREADS} -DCACHE_LINE_SIZE=64)

if (REPLAY_OPT STREQUAL "calvin")
  add_definitions(-DCALVIN_REPLAY=1)
elseif (REPLAY_OPT STREQUAL "ll")
  add_definitions(-DLL_REPLAY=1)
endif ()

set(CMAKE_C_FLAGS "-Wstrict-aliasing")
set(CMAKE_CXX_FLAGS "-Wstrict-aliasing -std=c++11")
set(CMAKE_EXE_LINKER_FLAGS "-static-libstdc++")
set(CMAKE_C_FLAGS_RELEASE "-Ofast -g -flto -fno-omit-frame-pointer -mno-omit-leaf-frame-pointer -DNDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "-Ofast -g -flto -fno-omit-frame-pointer -mno-omit-leaf-frame-pointer -DNDEBUG")
set(CMAKE_EXE_LINKER_FLAGS_RELEASE "-Ofast -g -flto -fwhole-program -mno-omit-leaf-frame-pointer -fno-omit-frame-pointer")

# set(CMAKE_CXX_FLAGS_DEBUG "-fsanitize=address -fno-omit-frame-pointer")
# set(CMAKE_EXE_LINKER_FLAGS_DEBUG "-fsanitize=address")

set_source_files_properties(gopp/asm.S PROPERTIES COMPILE_FLAGS "-x assembler-with-cpp")

set_source_files_properties(masstree/kvthread.cc masstree/string.cc masstree/straccum.cc PROPERTIES COMPILE_FLAGS "-include masstree/build/config.h")

set(SRCS epoch.cc txn.cc log.cc vhandle.cc index.cc mem.cc masstree_index_impl.cc
  dolly_probes.cc
  fpcore.cc 
  json11/json11.cpp
  xxHash/xxhash.c
  gopp/gopp.cc gopp/channels.cc gopp/asm.S
  masstree/kvthread.cc masstree/string.cc masstree/straccum.cc)

set(LIBS numa pthread libjemalloc.a)

add_executable(db-backup-node client.cc main.cc
  ${SRCS}
)

add_subdirectory(benchmark/tpcc/)
add_subdirectory(chkpt/)

target_link_libraries(db-backup-node ${LIBS})
target_include_directories(db-backup-node PRIVATE ${CMAKE_SOURCE_DIR} ${CMAKE_SOURCE_DIR}/spdlog/include/)

if (ENABLE_TEST)
  enable_testing()
  find_library(GTEST_LIBS NAMES gtest)

  add_executable(test-epoch test/epoch_test.cc ${SRCS})
  target_include_directories(test-epoch PRIVATE ${CMAKE_SOURCE_DIR})
  target_link_libraries(test-epoch ${GTEST_LIBS} gtest_main ${LIBS})

  add_test(NAME test-epoch COMMAND test-epoch)
endif (ENABLE_TEST)
