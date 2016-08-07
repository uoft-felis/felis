cmake_minimum_required(VERSION 2.8)

project(dolly C CXX ASM)

add_definitions(-std=c++11 -pthread)

# set(CMAKE_CXX_COMPILER "g++")
# set(CMAKE_C_COMPILER "gcc")

set(CMAKE_CXX_FLAGS_RELEASE "-Ofast -g -flto -fno-omit-frame-pointer -DNDEBUG")
set(CMAKE_EXE_LINKER_FLAGS_RELEASE "-flto -fwhole-program -g -fno-omit-frame-pointer")

#set(CMAKE_CXX_FLAGS "-fsanitize=address -fno-omit-frame-pointer")
#set(CMAKE_EXE_LINKER_FLAGS "-fsanitize=address")

set_source_files_properties(goplusplus/asm.S PROPERTIES COMPILE_FLAGS "-x assembler-with-cpp")

add_library(dolly SHARED net-io.cc epoch.cc log.cc index.cc mem.cc json11/json11.cpp masstree_index_impl.cc goplusplus/gopp.cc goplusplus/epoll-channel.cc goplusplus/asm.S)
target_link_libraries(dolly numa pthread masstree)
target_include_directories(dolly PRIVATE ${CMAKE_SOURCE_DIR})

add_executable(db-backup-node client.cc main.cc)
target_link_libraries(db-backup-node dl dolly masstree pthread tcmalloc)
target_include_directories(db-backup-node PRIVATE ${CMAKE_SOURCE_DIR})

add_subdirectory(benchmark/tpcc/)
