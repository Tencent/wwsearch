cmake_minimum_required(VERSION 2.8.12)
project(wwsearch)

option(WITH_ROCKSDB "build with rocksdb" ON)
option(WITH_SNAPPY "build with snappy" ON)
option(WITH_TESTS "build with ut" OFF)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake/modules/")

set(CMAKE_COMMON_FLAGS "-DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX  -DOS_LINUX -fno-builtin-memcmp -DZLIB -DROCKSDB_MALLOC_USABLE_SIZE  -DROCKSDB_PTHREAD_ADAPTIVE_MUTEX -DROCKSDB_BACKTRACE -DROCKSDB_RANGESYNC_PRESENT  -DROCKSDB_SCHED_GETCPU_PRESENT -march=native  -DHAVE_SSE42 -DHAVE_PCLMUL  -DROCKSDB_SUPPORT_THREAD_LOCAL -DWITH_TESTS=OFF")
set(CMAKE_C_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_COMMON_FLAGS} -lpthread -lrt -lz -g")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_COMMON_FLAGS} -lpthread -std=c++11 -lrt -lz -g")
set(CMAKE_CXX_LINK_EXECUTABLE "${CMAKE_CXX_LINK_EXECUTABLE} -ldl -lrt")
#set(CMAKE_EXE_LINKER_FLAGS "-static-libstdc++ -static-libgcc")
set(WWSEARCH_DIR ${CMAKE_CURRENT_SOURCE_DIR})

# add search path for build rocksdb
set(CMAKE_INCLUDE_PATH "${WWSEARCH_DIR}/deps/snappy/include")
set(CMAKE_LIBRARY_PATH "${WWSEARCH_DIR}/deps/snappy/lib")


add_subdirectory(${WWSEARCH_DIR}/deps/rocksdb)
add_subdirectory(${WWSEARCH_DIR}/deps/tokenizer)

link_libraries(rocksdb)
link_libraries(tokenizer)
link_libraries(gtest)

include_directories(
	${WWSEARCH_DIR}
	${WWSEARCH_DIR}/include
	${WWSEARCH_DIR}/deps/rocksdb/
	${WWSEARCH_DIR}/deps/rocksdb/include
    ${WWSEARCH_DIR}/deps/rocksdb/third-party/gtest-1.7.0/fused-src
	${WWSEARCH_DIR}/deps/tokenizer/include
    ${WWSEARCH_DIR}/unittest
)

file(GLOB WWSEARCH_SRC_DIR ./src/*.cpp ./include/*.cc)
ADD_LIBRARY(wwsearch ${WWSEARCH_SRC_DIR}) 

include_directories(${WWSEARCH_DIR}/deps/protobuf/include/)
FIND_LIBRARY(PROTOBUF_LIB NAMES libprotobuf.a HINTS ${WWSEARCH_DIR}/deps/protobuf/lib NO_DEFAULT_PATH)
#link_libraries(protobuf)

include_directories(${WWSEARCH_DIR}/deps/snappy/include)
FIND_LIBRARY(SNAPPY_LIB NAMES libsnappy.a HINTS ${WWSEARCH_DIR}/deps/snappy/lib NO_DEFAULT_PATH)

target_link_libraries(wwsearch rocksdb tokenizer -Lprotobuf ${PROTOBUF_LIB} -Lsnappy ${SNAPPY_LIB})

file(GLOB WWSEARCH_UT_DIR ./unittest/*.cpp)

add_executable(wwsearch_example example/example.cpp)
target_link_libraries(wwsearch_example wwsearch)

include_directories(${WWSEARCH_DIR}/benchmark)
file(GLOB WWSEARCH_BEN_DIR ./benchmark/*.cpp)
add_executable(wwsearch_ben ${WWSEARCH_BEN_DIR})
target_link_libraries(wwsearch_ben wwsearch)

set(CMAKE_C_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_COMMON_FLAGS} -lpthread -lrt -lz -g -Dprivate=public")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_COMMON_FLAGS} -lpthread -std=c++11 -lrt -lz -g -Dprivate=public")
add_executable(wwsearch_ut ${WWSEARCH_UT_DIR})
target_link_libraries(wwsearch_ut wwsearch gtest)
