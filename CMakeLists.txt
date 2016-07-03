cmake_minimum_required(VERSION 2.8)

project(dolly)

add_definitions(-std=c++11 -pthread)

set(CMAKE_CXX_FLAGS_RELEASE "-Ofast -g -fno-omit-frame-pointer -flto -DNDEBUG")
set(CMAKE_EXE_LINKER_FLAGS_RELEASE "-flto -fwhole-program -g -fno-omit-frame-pointer")

# set(CMAKE_CXX_FLAGS "-fsanitize=address -fno-omit-frame-pointer")
# set(CMAKE_EXE_LINKER_FLAGS "-fsanitize=address")

add_library(dolly SHARED net-io.cc epoch.cc log.cc index.cc worker.cc json11/json11.cpp masstree_index_impl.cc goplusplus/gopp.cc goplusplus/epoll-channel.cc)
target_link_libraries(dolly numa pthread masstree)
target_include_directories(dolly PRIVATE ${CMAKE_SOURCE_DIR})

add_executable(db-backup-node main.cc)
target_link_libraries(db-backup-node dl dolly masstree pthread tcmalloc)
target_include_directories(db-backup-node PRIVATE ${CMAKE_SOURCE_DIR})

add_subdirectory(benchmark/tpcc/)
