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

############################################################
# CMake setup
############################################################

cmake_minimum_required(VERSION 3.3)
if (CMAKE_VERSION VERSION_GREATER_EQUAL "3.24.0")
  cmake_policy(SET CMP0135 NEW)
endif()
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/")

# Superbuild option must be on by default.
option(SUPERBUILD "If true, perform a superbuild (builds all missing dependencies)." ON)
option(CMAKE_IDE "(Used for CLion builds). Disables superbuild and sets the EP install dir." OFF)
option(FORCE_EXTERNAL_HTSLIB "Forces a local build of HTSlib instead of searching system paths." ON)
option(FORCE_EXTERNAL_TILEDB "Forces a local build of TileDB instead of searching system paths." OFF)
option(DOWNLOAD_TILEDB_PREBUILT "If tiledb is being super built, this controls downloading prebuilt artifacts or building from source" ON)
option(TILEDB_S3 "Enables S3/minio support using aws-cpp-sdk" ON)
option(OVERRIDE_INSTALL_PREFIX "Ignores the setting of CMAKE_INSTALL_PREFIX and sets a default prefix" ON)
option(ENABLE_ARROW_EXPORT "Installs an extra header for exporting in-memory results with Apache Arrow" ON)

# Set C++17 as required standard for all C++ targets (required to use the TileDB
# C++ API).
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF) # Don't use GNU extensions

# Release builds by default.
if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RELEASE)
endif()

# Root directory default installation prefix
if (OVERRIDE_INSTALL_PREFIX)
  set(PREFIX_REL_PATH "${CMAKE_SOURCE_DIR}/../dist")
  get_filename_component(DEFAULT_PREFIX "${PREFIX_REL_PATH}" ABSOLUTE)
  set(CMAKE_INSTALL_PREFIX "${DEFAULT_PREFIX}" CACHE PATH "Default install prefix" FORCE)
  message(STATUS "Using default install prefix ${CMAKE_INSTALL_PREFIX}. To control CMAKE_INSTALL_PREFIX, set OVERRIDE_INSTALL_PREFIX=OFF")
endif()
message(STATUS "Install prefix is ${CMAKE_INSTALL_PREFIX}.")

if (FORCE_EXTERNAL_HTSLIB)
  message(STATUS "Skipping search for htslib, building it as an external project. To use system htslib, set FORCE_EXTERNAL_HTSLIB=OFF")
endif()

if (FORCE_EXTERNAL_TILEDB)
  message(STATUS "Skipping search for TileDB, building it as an external project. To use system TileDB, set FORCE_EXTERNAL_TILEDB=OFF")
endif()

# Use @rpath on macOS for building shared libraries.
if (APPLE)
  set(CMAKE_MACOSX_RPATH ON)
endif()

# Add some global compiler flags
string( TOUPPER "${CMAKE_BUILD_TYPE}" BUILD_TYPE)
if (BUILD_TYPE STREQUAL "DEBUG")
  add_compile_options(-DDEBUG -O0 -g3 -ggdb3 -gdwarf-3 -Wall)
elseif (BUILD_TYPE STREQUAL "RELEASE")
  add_compile_options(-DNDEBUG -O3 -Wall -Werror)
endif()
message(WARNING "Building without deprecation warnings")
add_compile_options(-Wno-deprecated-declarations)
add_definitions(-D_FILE_OFFSET_BITS=64)

############################################################
# Superbuild setup
############################################################

# Search the externals install directory for dependencies.
list(APPEND CMAKE_PREFIX_PATH "${EP_INSTALL_PREFIX}")

# If this is an in-IDE build, we need to disable the superbuild and explicitly
# set the EP base dir. The normal 'cmake && make' process won't need this step,
# it is for better CLion support of the superbuild architecture.
if (CMAKE_IDE)
  set(SUPERBUILD OFF)
  set(EP_BASE "${CMAKE_CURRENT_BINARY_DIR}/externals")
endif()

if (SUPERBUILD)
  project(TileDB-VCF-Superbuild)
  message(STATUS "Starting TileDB-VCF superbuild.")
  include("cmake/Superbuild.cmake")
  return()
endif()

project(TileDB-VCF)
message(STATUS "Starting TileDB-VCF regular build.")

# Paths to locate the installed external projects.
set(EP_SOURCE_DIR "${EP_BASE}/src")
set(EP_INSTALL_PREFIX "${EP_BASE}/install")

############################################################
# Regular build
############################################################

# Enable testing
enable_testing()

add_subdirectory(src)
add_subdirectory(test)
