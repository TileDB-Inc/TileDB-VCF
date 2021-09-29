#
# FindCLI11_EP.cmake
#
#
# The MIT License
#
# Copyright (c) 2021 TileDB, Inc.
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
# Finds the CLI11 library, installing with an ExternalProject as necessary.
#
# This module defines:
#   - CLI11_INCLUDE_DIR, directory containing headers
#   - CLI11_FOUND, whether CLI11 has been found
#   - The CLI11::CLI11 imported target

# Search the path set during the superbuild for the EP.
set(CLI11_PATHS ${EP_INSTALL_PREFIX})

find_path(CLI11_INCLUDE_DIR
  NAMES CLI11.hpp
  PATHS ${CLI11_PATHS}
  PATH_SUFFIXES include
)

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(CLI11
  REQUIRED_VARS CLI11_INCLUDE_DIR
)

if (NOT CLI11_FOUND)
  if (SUPERBUILD)
    message(STATUS "Adding CLI11 as an external project")
    ExternalProject_Add(ep_cli11
      PREFIX "externals"
      URL "https://github.com/CLIUtils/CLI11/releases/download/v2.1.0/CLI11.hpp"
      URL_HASH SHA1=35fd46dfdcee03f13f0cd63d2b6d64c80a2668bb
      DOWNLOAD_NO_EXTRACT TRUE
      DOWNLOAD_DIR ${EP_BASE}/src/ep_cli11
      UPDATE_COMMAND ""
      CONFIGURE_COMMAND ""
      BUILD_COMMAND ""
      INSTALL_COMMAND
        ${CMAKE_COMMAND} -E make_directory ${EP_INSTALL_PREFIX}/include
        COMMAND
          ${CMAKE_COMMAND} -E copy_if_different
            ${EP_BASE}/src/ep_cli11/CLI11.hpp
            ${EP_INSTALL_PREFIX}/include/
      PATCH_COMMAND
        patch -N -p1 < ${CMAKE_CURRENT_SOURCE_DIR}/cmake/patches/cli11-2.1.0.patch
      LOG_DOWNLOAD TRUE
      LOG_CONFIGURE TRUE
      LOG_BUILD TRUE
      LOG_INSTALL TRUE
    )

    list(APPEND EXTERNAL_PROJECTS ep_cli11)
  else()
    message(FATAL_ERROR "Unable to find CLI11")
  endif()
endif()

# Create the imported target for CLI11
if (CLI11_FOUND AND NOT TARGET CLI11::CLI11)
  add_library(CLI11::CLI11 INTERFACE IMPORTED)
  set_target_properties(CLI11::CLI11 PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${CLI11_INCLUDE_DIR}"
  )
endif()