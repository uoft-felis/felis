cmake_minimum_required(VERSION 2.8)

project(dolly)

add_definitions(-std=c++11 -pthread)

add_library(dolly SHARED net-io.cc epoch.cc log.cc index.cc worker.cc json11/json11.cpp)
target_link_libraries(dolly numa pthread)

add_executable(db-backup-node main.cc)
target_link_libraries(db-backup-node dl dolly pthread ${JSON11_LIBRARIES})

add_subdirectory(benchmark/tpcc/)
