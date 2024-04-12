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

find_package(pybind11 REQUIRED)

find_library(
    TILEDBVCF_LIB
    NAMES tiledbvcf
    PATHS "${CMAKE_CURRENT_SOURCE_DIR}/../../dist/lib"
)

if(NOT TILEDBVCF_LIB)
    message(STATUS "Building libtiledbvcf from source")

    file(MAKE_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/../../libtiledbvcf/build")

    execute_process(
        # Force Unix Makefiles to avoid issues with Ninja and htslib, which uses a custom build system
        COMMAND bash -c "${CMAKE_COMMAND} -G 'Unix Makefiles' .. && make -j && make -j install-libtiledbvcf"
        WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/../../libtiledbvcf/build"
    )

    find_library(
        TILEDBVCF_LIB
        NAMES tiledbvcf
        PATHS "${CMAKE_CURRENT_SOURCE_DIR}/../../dist/lib"
    )
endif()

set(VCF_TARGET_NAME libtiledbvcf)

pybind11_add_module(
    ${VCF_TARGET_NAME}
    src/tiledbvcf/binding/libtiledbvcf.cc
    src/tiledbvcf/binding/reader.cc
    src/tiledbvcf/binding/writer.cc
    src/tiledbvcf/binding/vcf_arrow.cc
)

target_compile_features(${VCF_TARGET_NAME} PRIVATE cxx_std_20)

target_include_directories(
    ${VCF_TARGET_NAME} PRIVATE
    ${pybind11_INCLUDE_DIR}
    "${CMAKE_CURRENT_SOURCE_DIR}/../../dist/include/"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../libtiledbvcf/build/externals/install/include"
)

target_link_libraries(
    ${VCF_TARGET_NAME} PRIVATE
    ${TILEDBVCF_LIB}
)

# Search for shared libraries in the same directory as the target
if (APPLE)
    set_target_properties(${VCF_TARGET_NAME} PROPERTIES INSTALL_RPATH "@loader_path")
else()
    set_target_properties(${VCF_TARGET_NAME} PROPERTIES INSTALL_RPATH "$ORIGIN")
endif()

# Install the extension module
install(
    TARGETS ${VCF_TARGET_NAME}
    DESTINATION ${SKBUILD_PROJECT_NAME}
)

# Install required shared libraries
file(GLOB SHARED_LIBS "${CMAKE_CURRENT_SOURCE_DIR}/../../dist/lib*/*")
install(
    FILES ${SHARED_LIBS}
    DESTINATION ${SKBUILD_PROJECT_NAME}
)

# Install the command line tool
install(
    PROGRAMS "${CMAKE_CURRENT_SOURCE_DIR}/../../dist/bin/tiledbvcf"
    DESTINATION ${SKBUILD_SCRIPTS_DIR}
)
