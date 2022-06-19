project(arrow_examples)

cmake_minimum_required(VERSION 3.5)

set (CMAKE_CXX_STANDARD 11)

set (CMAKE_BUILD_TYPE "DEBUG")
set (ARROW_BUILD_SHARED OFF)
set (ARROW_BUILD_STATIC ON)
set (ARROW_IPC ON)
set (ARROW_SIMD_LEVEL "SSE4_2")

add_subdirectory("arrow/cpp")
include_directories("arrow/cpp/src")


aux_source_directory(. SRC_LIST)

add_executable(${PROJECT_NAME} ${SRC_LIST})

target_link_libraries(${PROJECT_NAME} arrow_static)