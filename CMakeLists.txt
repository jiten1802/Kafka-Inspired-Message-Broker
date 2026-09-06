cmake_minimum_required(VERSION 3.13)
set(CMAKE_TRY_COMPILE_TARGET_TYPE "STATIC_LIBRARY")

project(codecrafters-kafka)

set(CMAKE_CXX_STANDARD 23) # Enable the C++23 standard
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
set(CMAKE_COMPILE_WARNING_AS_ERROR ON)

file(GLOB_RECURSE SOURCE_FILES src/*.cc src/*.cpp)

add_executable(kafka ${SOURCE_FILES})