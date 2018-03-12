add_library(tpcc STATIC ${CMAKE_CURRENT_LIST_DIR}/tpcc.cc ${CMAKE_CURRENT_LIST_DIR}/tpcc_workload.cc)
target_include_directories(tpcc PRIVATE ${PROJECT_SOURCE_DIR} ${CMAKE_SOURCE_DIR}/spdlog/include/)

