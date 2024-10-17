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

add_subdirectory(src)

if (TILEDBVCF_ENABLE_TESTS)
  enable_testing()
  add_subdirectory(test)
endif()

if (TILEDBVCF_ENABLE_PYTHON)
  add_subdirectory(../apis/python apis/python)
endif()
