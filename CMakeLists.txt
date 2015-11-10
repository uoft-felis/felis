project (dolly)

add_definitions(-std=c++11)
add_executable(db-backup-node main.cc net-io.cc request.cc epoch.cc log.cc index.cc)
