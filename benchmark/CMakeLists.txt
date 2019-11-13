cmake_minimum_required(VERSION 2.8.12)
project(wwsearch_ben) 
enable_language(CXX) 
enable_language(C) 
enable_language(ASM)

#configure
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -m64 -g -Wno-deprecated -Wall -pthread -lsnappy " "-lz -std=c++11 ") 
set(CMAKE_C_FLAGS "${CMAKE_CXX_FLAGS} -m64 -g -Wno-deprecated " "-Wall -pthread -lsnappy -lz ")

set(paxossearch_STATIC_LIB paxossearch ${ARTIFACT_SUFFIX})
#set(paxossearch_SHARED_LIB paxossearch - shared${ARTIFACT_SUFFIX})
#set(paxossearch_IMPORT_LIB ${paxossearch_SHARED_LIB})

option(WITH_ROCKSDB "build with rocksdb" ON)
option(WITH_SNAPPY "build with snappy" ON)

if (WITH_ROCKSDB) 
set(ROCKSDB_INCLUDE_DIR "/data/store/rocksdb/rocksdb-master/include")
set(ROCKSDB_LIBRARIES "/data/store/rocksdb/rocksdb-master/librocksdb.a")
include_directories(${ROCKSDB_INCLUDE_DIR}) 
list(APPEND THIRDPARTY_LIBS ${ROCKSDB_LIBRARIES}) 
endif()

include_directories(${PROJECT_SOURCE_DIR})

#source
set(CXXSOURCES main.cpp bench_index.cpp)

#target
add_library(${paxossearch_SHARED_LIB} SHARED ${CXXSOURCES}) 
add_library(${paxossearch_STATIC_LIB} STATIC ${CXXSOURCES})

add_executable(rockssearch_benchmark main.cpp) 
target_link_libraries(rockssearch_benchmark ${paxossearch_STATIC_LIB} ${THIRDPARTY_LIBS})
