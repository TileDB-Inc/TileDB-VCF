#
# The MIT License
#
# Copyright (c) 2024 TileDB, Inc.
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

cmake_minimum_required(VERSION 3.21)

project(
    ${SKBUILD_PROJECT_NAME}
    VERSION ${SKBUILD_PROJECT_VERSION}
    LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(pybind11 REQUIRED)

find_library(
    TILEDBVCF_LIB
    NAMES tiledbvcf
    PATHS "${CMAKE_CURRENT_SOURCE_DIR}/../../dist/lib"
)

pybind11_add_module(
    libtiledbvcf
    src/tiledbvcf/binding/libtiledbvcf.cc
    src/tiledbvcf/binding/reader.cc
    src/tiledbvcf/binding/writer.cc
    src/tiledbvcf/binding/vcf_arrow.cc
)

target_compile_options(libtiledbvcf PRIVATE
    -Wno-deprecated-declarations
)

target_include_directories(
    libtiledbvcf PRIVATE
    ${pybind11_INCLUDE_DIR}
    "${CMAKE_CURRENT_SOURCE_DIR}/../../dist/include/"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../libtiledbvcf/build/externals/install/include"
)

target_link_libraries(
    libtiledbvcf PRIVATE
    ${TILEDBVCF_LIB}
)

set_target_properties(
    libtiledbvcf PROPERTIES
    INSTALL_RPATH "$ORIGIN"
)

install(
    TARGETS libtiledbvcf
    DESTINATION ${SKBUILD_PROJECT_NAME}
)

file(GLOB SHARED_LIBS "${CMAKE_CURRENT_SOURCE_DIR}/../../dist/lib*/*")
install(
    FILES ${SHARED_LIBS}
    DESTINATION ${SKBUILD_PROJECT_NAME}
)
