project(arrow_examples)

cmake_minimum_required(VERSION 3.16)

find_package(Arrow REQUIRED)

aux_source_directory(. SRC_LIST)
add_executable(${PROJECT_NAME} ${SRC_LIST})
target_link_libraries(${PROJECT_NAME} PRIVATE Arrow::arrow_shared)
install(TARGETS ${PROJECT_NAME} DESTINATION bin)
