cmake_minimum_required(VERSION 2.8.12)
project(TOKENIZER_DIR)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -std=c++11")
set(CMAKE_CXX_LINK_EXECUTABLE "${CMAKE_CXX_LINK_EXECUTABLE}")
set(TOKENIZER_DIR ${CMAKE_CURRENT_SOURCE_DIR})

include_directories(
	${TOKENIZER_DIR}
	${TOKENIZER_DIR}/include
)

file(GLOB TOKENIZER_SRC_PATH ./src/*.cpp ./src/*.c)

add_library(tokenizer STATIC ${TOKENIZER_SRC_PATH})

target_link_libraries(tokenizer)

install(
  TARGETS tokenizer
  EXPORT tokenizer
  COMPONENT devel
  ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
)