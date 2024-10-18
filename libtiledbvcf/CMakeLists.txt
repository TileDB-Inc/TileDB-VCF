#
# CMakeLists.txt
#
#
# The MIT License
#
# Copyright (c) 2018 TileDB, Inc.
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

cmake_minimum_required(VERSION 3.5)

option(TILEDBVCF_ENABLE_TESTS "TODO" OFF)
option(TILEDBVCF_ENABLE_PYTHON "TODO" OFF)

include(cmake/TileDBToolchain.cmake)
include(cmake/DownloadTileDB.cmake)

fetch_prebuilt_tiledb(
        VERSION 2.26.1
        RELLIST_HASH SHA256=256216aa989015397f4efbbd319ebeccfead568baa73611aa0c1c0fcea35f8d5
)

project(TileDB-VCF)

############################################################
# Find required dependencies
############################################################

find_package(TileDB REQUIRED)
find_package(CLI11 REQUIRED)
find_package(spdlog REQUIRED)

if (WIN32)
  FetchContent_Declare(
          htslib
          URL https://github.com/TileDB-Inc/m2w64-htslib-build/releases/download/1.20-0/m2w64-htslib-1.20-0.tar.gz
          # TODO: URL_HASH SHA256=
  )
  FetchContent_MakeAvailable(htslib)
  find_library(
          HTSLIB_LIBRARIES
          REQUIRED
          NAMES hts hts-3
          PATHS ${htslib_SOURCE_DIR}
          PATH_SUFFIXES lib
          NO_DEFAULT_PATH
  )
  find_path(
          HTSLIB_INCLUDE_DIR
          NAMES htslib/hts.h
          PATHS ${htslib_SOURCE_DIR}
          PATH_SUFFIXES include
          NO_DEFAULT_PATH
  )
  add_library(HTSlib::HTSlib UNKNOWN IMPORTED)
  set_target_properties(HTSlib::HTSlib PROPERTIES
          IMPORTED_IMPLIB "${HTSLIB_LIBRARIES}"
          IMPORTED_LOCATION "${HTSLIB_LIBRARIES}"
          INTERFACE_INCLUDE_DIRECTORIES "${HTSLIB_INCLUDE_DIR}"
  )
else()
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(HTSlib REQUIRED IMPORTED_TARGET htslib)
  add_library(HTSlib::HTSlib ALIAS PkgConfig::HTSlib)
endif()

add_subdirectory(src)

if (TILEDBVCF_ENABLE_TESTS)
  enable_testing()
  add_subdirectory(test)
endif()

if (TILEDBVCF_ENABLE_PYTHON)
  add_subdirectory(../apis/python apis/python)
endif()
