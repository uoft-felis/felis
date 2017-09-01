cmake_minimum_required(VERSION 3.0)

option(ENABLE_TEST "Build tests." OFF)
option(CALVIN_REPLAY "Calvin Style replay" OFF)

project(dolly C CXX ASM)

find_library(JEMALLOC_LIBS NAMES jemalloc)

add_definitions(-std=c++11 -pthread -Wstrict-aliasing -DNR_THREADS=${NR_THREADS} -DCACHE_LINE_SIZE=64)

if (CALVIN_REPLAY)
  add_definitions(-DCALVIN_REPLAY=1)
endif (CALVIN_REPLAY)

set(CMAKE_C_FLAGS "-Wstrict-aliasing")
set(CMAKE_CXX_FLAGS "-Wstrict-aliasing")
set(CMAKE_EXE_LINKER_FLAGS "-static-libstdc++ -static-libgcc")
set(CMAKE_C_FLAGS_RELEASE "-Ofast -g -flto -fno-omit-frame-pointer -mno-omit-leaf-frame-pointer -DNDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "-Ofast -g -flto -fno-omit-frame-pointer -mno-omit-leaf-frame-pointer -DNDEBUG")
set(CMAKE_EXE_LINKER_FLAGS_RELEASE "-Ofast -g -flto -fwhole-program -mno-omit-leaf-frame-pointer -fno-omit-frame-pointer")

# set(CMAKE_CXX_FLAGS_DEBUG "-fsanitize=address -fno-omit-frame-pointer")
# set(CMAKE_EXE_LINKER_FLAGS_DEBUG "-fsanitize=address")

set_source_files_properties(gopp/asm.S PROPERTIES COMPILE_FLAGS "-x assembler-with-cpp")

set_source_files_properties(masstree/kvthread.cc masstree/string.cc masstree/straccum.cc PROPERTIES COMPILE_FLAGS "-include masstree/build/config.h")

set(SRCS epoch.cc txn.cc log.cc vhandle.cc index.cc mem.cc masstree_index_impl.cc probe.cc
  dolly_probes.cc
  fpcore.cc 
  json11/json11.cpp
  gopp/gopp.cc gopp/channels.cc gopp/asm.S
  masstree/kvthread.cc masstree/string.cc masstree/straccum.cc)

set(LIBS numa pthread ${JEMALLOC_LIBS})

add_executable(db-backup-node client.cc main.cc
  ${SRCS}
)

add_subdirectory(benchmark/tpcc/)
add_subdirectory(chkpt/)

target_link_libraries(db-backup-node ${LIBS})
target_include_directories(db-backup-node PRIVATE ${CMAKE_SOURCE_DIR})

if (ENABLE_TEST)
  enable_testing()
  find_library(GTEST_LIBS NAMES gtest)

  add_executable(test-epoch test/epoch_test.cc ${SRCS})
  target_include_directories(test-epoch PRIVATE ${CMAKE_SOURCE_DIR})
  target_link_libraries(test-epoch ${GTEST_LIBS} gtest_main ${LIBS})

  add_test(NAME test-epoch COMMAND test-epoch)
endif (ENABLE_TEST)
