cmake_minimum_required(VERSION 2.8)

project(dolly C CXX ASM)

add_definitions(-std=c++11 -pthread -Wstrict-aliasing -DNR_THREADS=${NR_THREADS})

if (${CALVIN_REPLAY})
  add_definitions(-DCALVIN_REPLAY=1)
endif (${CALVIN_REPLAY})

set(CMAKE_CXX_FLAGS_RELEASE "-Ofast -g -flto -fno-omit-frame-pointer -DNDEBUG")
set(CMAKE_EXE_LINKER_FLAGS_RELEASE "-Ofast -flto -fwhole-program -g -fno-omit-frame-pointer")

# set(CMAKE_CXX_FLAGS_DEBUG "-fsanitize=address -fno-omit-frame-pointer")
# set(CMAKE_EXE_LINKER_FLAGS_DEBUG "-fsanitize=address")

set_source_files_properties(gopp/asm.S PROPERTIES COMPILE_FLAGS "-x assembler-with-cpp")

add_library(dolly SHARED epoch.cc log.cc vhandle.cc index.cc mem.cc json11/json11.cpp masstree_index_impl.cc gopp/gopp.cc gopp/channels.cc gopp/asm.S)
target_link_libraries(dolly numa pthread masstree)
target_include_directories(dolly PRIVATE ${CMAKE_SOURCE_DIR})

add_executable(db-backup-node client.cc main.cc)
target_link_libraries(db-backup-node dl dolly masstree pthread tcmalloc)
target_include_directories(db-backup-node PRIVATE ${CMAKE_SOURCE_DIR})

add_subdirectory(benchmark/tpcc/)
add_subdirectory(chkpt/)
