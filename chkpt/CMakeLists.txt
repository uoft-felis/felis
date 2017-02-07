add_library(chkpt_ermia SHARED ermia.cc ermia_size_encode.cc)
include_directories(${PROJECT_SOURCE_DIR})
target_link_libraries(chkpt_ermia dolly)
