add_library(tpcc SHARED tpcc.cc tpcc_workload.cc)
include_directories(${PROJECT_SOURCE_DIR})
target_link_libraries(tpcc dolly)
