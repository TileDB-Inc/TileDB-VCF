#
# CMakeLists.txt
#
#
# The MIT License
#
# Copyright (c) 2019 TileDB, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#

############################################################
# CMake setup
############################################################

cmake_minimum_required(VERSION 3.3)
project(LibTileDBVCF-JNI)

# Release build by default
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# Set C++11 as required standard for all C++ targets.
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF) # Don't use GNU extensions

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF) # Don't use GNU extensions

message(STATUS "Starting LibTileDBVCF-JNI regular build.")
message(STATUS "Resource install path is ${RESOURCE_INSTALL_DIR}")
message(STATUS "Install prefix is ${CMAKE_INSTALL_PREFIX}")

############################################################
# LibTileDBVCF-JNI shared library target
############################################################

# Find JNI
find_package(JNI REQUIRED)

# Find native TileDB-VCF library.
# You can set CMAKE_PREFIX_PATH manually for a custom TileDB-VCF location.
find_library(TILEDBVCF_LIBRARY
  NAMES
    tiledbvcf
  PATH_SUFFIXES lib
)
find_path(TILEDBVCF_INCLUDE_DIR
  NAMES
    tiledbvcf/tiledbvcf.h
  PATH_SUFFIXES include
)
include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(TileDB-VCF
  REQUIRED_VARS TILEDBVCF_LIBRARIES TILEDBVCF_INCLUDE_DIR
)

set(LIBTILEDBVCF_JNI_SOURCES
  ${CMAKE_CURRENT_SOURCE_DIR}/src/main/java/io/tiledb/libvcfnative/LibVCFNative.c
)

add_library(LIBTILEDBVCF_JNI_OBJECTS OBJECT 
  ${LIBTILEDBVCF_JNI_SOURCES}
)

set_property(TARGET LIBTILEDBVCF_JNI_OBJECTS PROPERTY POSITION_INDEPENDENT_CODE ON)

target_include_directories(LIBTILEDBVCF_JNI_OBJECTS
  PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/src/main/java/io/tiledb/libvcf/"
    "${TILEDBVCF_INCLUDE_DIR}"
    "${JAVA_INCLUDE_PATH}"
    "${JAVA_INCLUDE_PATH2}"
)

add_library(tiledbvcfjni SHARED
  $<TARGET_OBJECTS:LIBTILEDBVCF_JNI_OBJECTS>
)

target_link_libraries(tiledbvcfjni
  PRIVATE
    ${TILEDBVCF_LIBRARY}
    ${JAVA_JVM_LIBRARY}
)

# Set rpath to be relative to the .so.
if (APPLE)
  set_target_properties(tiledbvcfjni PROPERTIES INSTALL_RPATH "@loader_path/")
else()
  set_target_properties(tiledbvcfjni PROPERTIES INSTALL_RPATH "$ORIGIN/")
endif()

install(
  TARGETS tiledbvcfjni
  LIBRARY DESTINATION lib
)
