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

if (NOT TARGET tiledbvcf)
    message(STATUS "DEBUG: tiledbvcf_DIR: ${tiledbvcf_DIR}")
    # If tiledbvcf is not present it means that this script was not included as a
    # subdirectory of the tiledbvcf library, thus we need to find it

    find_library(
            TILEDBVCF_LIBRARIES
            REQUIRED
            NAMES libtiledbvcf tiledbvcf
            PATHS ${tiledbvcf_DIR}
            PATH_SUFFIXES tiledbvcf/lib
    )
    find_path(
            TILEDBVCF_INCLUDE_DIR
            REQUIRED
            NAMES tiledbvcf.h
            PATHS ${tiledbvcf_DIR}
            PATH_SUFFIXES tiledbvcf/include
    )
    add_library(tiledbvcf SHARED IMPORTED)
    set_target_properties(tiledbvcf PROPERTIES
            IMPORTED_IMPLIB "${TILEDBVCF_LIBRARIES}"
            INTERFACE_INCLUDE_DIRECTORIES "${TILEDBVCF_INCLUDE_DIR}"
    )
endif()

if (NOT TARGET TileDB::tiledb_shared)
    find_package(TileDB REQUIRED)
endif()

set(VCF_TARGET_NAME libtiledbvcf)

# NOTE: If you remove the line below the Windows build will fail
find_package(Python COMPONENTS Interpreter Development.Module REQUIRED)
find_package(pybind11 CONFIG REQUIRED)

pybind11_add_module(
    ${VCF_TARGET_NAME}
    src/tiledbvcf/binding/libtiledbvcf.cc
    src/tiledbvcf/binding/reader.cc
    src/tiledbvcf/binding/writer.cc
    src/tiledbvcf/binding/vcf_arrow.cc
)

target_compile_features(${VCF_TARGET_NAME} PRIVATE cxx_std_20)

if (NOT WIN32)
    target_compile_options(${VCF_TARGET_NAME} PRIVATE -Wno-deprecated-declarations)
endif()

target_link_libraries(
        ${VCF_TARGET_NAME}
        PRIVATE
        tiledbvcf
        TileDB::tiledb_shared
)

if (TILEDBVCF_SET_RPATH)
# Search for shared libraries in the same directory as the target
  if (APPLE)
      set_target_properties(${VCF_TARGET_NAME} PROPERTIES INSTALL_RPATH "@loader_path/lib")
  else()
      set_target_properties(${VCF_TARGET_NAME} PROPERTIES INSTALL_RPATH "$ORIGIN/lib")
  endif()
endif()

# Install the extension module
install(
        TARGETS ${VCF_TARGET_NAME}
        DESTINATION .
)
