#
# FindHTSlib_EP.cmake
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
# Finds the HTSlib library, installing with an ExternalProject as necessary.
#
# This module defines:
#   - HTSLIB_INCLUDE_DIR, directory containing headers
#   - HTSLIB_LIBRARIES, the HTSlib library path
#   - HTSLIB_FOUND, whether HTSlib has been found
#   - The HTSlib::HTSlib imported target

# Search the path set during the superbuild for the EP.
set(HTSLIB_PATHS ${EP_INSTALL_PREFIX})

if (FORCE_EXTERNAL_HTSLIB)
  set(HTSLIB_NO_DEFAULT_PATH NO_DEFAULT_PATH)
else()
  set(HTSLIB_NO_DEFAULT_PATH)
endif()

find_path(HTSLIB_INCLUDE_DIR
  NAMES htslib/hts.h
  PATHS ${HTSLIB_PATHS}
  PATH_SUFFIXES include
  ${HTSLIB_NO_DEFAULT_PATH}
)

find_library(HTSLIB_LIBRARIES
  NAMES hts hts-3
  PATHS ${HTSLIB_PATHS}
  PATH_SUFFIXES lib
  ${HTSLIB_NO_DEFAULT_PATH}
)

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(HTSlib
  REQUIRED_VARS HTSLIB_LIBRARIES HTSLIB_INCLUDE_DIR
)


if (NOT HTSLIB_FOUND)
  if (SUPERBUILD)
    message(STATUS "Adding HTSlib as an external project")
    # Use explicit soname to avoid having to use library symlinks (easier for embedding).
    if (APPLE)
      set(EXTRA_LDFLAGS "-Wl,-install_name,@rpath/libhts.1.15.1.dylib")
    else()
      set(EXTRA_LDFLAGS "-Wl,-soname,libhts.so.1.15.1")
    endif()
    SET(CFLAGS "")
    string( TOUPPER "${CMAKE_BUILD_TYPE}" BUILD_TYPE)
    if (BUILD_TYPE STREQUAL "DEBUG")
      SET(CFLAGS "-g")
    endif()

    # required to updated htslib configure.ac with autoconf 2.70
    #   - see https://github.com/samtools/htslib/commit/680c0b8ef0ff133d3b572abc80fe66fc2ea965f0
    #   - and https://github.com/samtools/htslib/pull/1198/commits/6821fc8ed88706e9282b561e74dfa45dac4d74c8
    if(NOT WIN32)
    find_program(AUTORECONF NAMES autoreconf REQUIRED)
    find_program(AUTOHEADER NAMES autoheader REQUIRED)
    find_program(BASH_PATH NAMES bash REQUIRED)
    endif()

    if(WIN32)
      if(0)
       set(DRIVE_LETTERS_TO_SEARCH "E")
       include (${CMAKE_SOURCE_DIR}/cmake/Modules/FindInstalledMsysEnvCmd.cmake)
       # returns MSYS2_ENV_CMD containing path to executable if found.
       if(NOT MSYS2_ENV_CMD)
         message(FATAL_ERROR "Failed to find needed 'env.exe' to invoke msys2 build of htslib!")
       endif()
      find_package(Git REQUIRED)
      set(MSYS_INVOKE ${MSYS2_ENV_CMD} MSYSTEM=MINGW64 CHERE_INVOKING=1 /usr/bin/bash -li -x -c )
      set(CONDITIONAL_PATCH ${MSYS_INVOKE} "cd ${CMAKE_SOURCE_DIR}/.. && git apply --no-index --ignore-whitespace -p1 --unsafe-paths --verbose --directory=${EP_SOURCE_DIR}/ep_htslib ${CMAKE_CURRENT_SOURCE_DIR}/cmake/patches/htslib.1.15.1-win.patch && git apply --no-index --ignore-whitespace -p1 --unsafe-paths --verbose --directory=${EP_SOURCE_DIR}/ep_htslib ${CMAKE_CURRENT_SOURCE_DIR}/cmake/patches/htslib.1.15.1.hts_defs.h.patch && git apply --no-index --ignore-whitespace -p1 --unsafe-paths --verbose --directory=${EP_SOURCE_DIR}/ep_htslib ${CMAKE_CURRENT_SOURCE_DIR}/cmake/patches/htslib.1.15.1.vcf.h.patch "
        )
      ExternalProject_Add(ep_htslib
        PREFIX "externals"
        URL "https://github.com/samtools/htslib/releases/download/1.15.1/htslib-1.15.1.tar.bz2"
        URL_HASH SHA1=e7cbd4bb059020c9486facc028f750ec0fb2e182
        #URL "https://github.com/samtools/htslib/releases/download/1.16/htslib-1.16.tar.bz2"
        #URL_HASH SHA1=36b16f462384af257d292ebeed766f299ec205f5
        UPDATE_COMMAND ""
        PATCH_COMMAND
          ${MSYS_INVOKE} "cmake -E copy ${EP_BASE}/src/ep_htslib/Makefile ${EP_BASE}/src/ep_htslib/Makefile.orig"
        COMMAND
          ${CONDITIONAL_PATCH}
        CONFIGURE_COMMAND
            ${MSYS_INVOKE} "autoheader --verbose"
          COMMAND
            ${MSYS_INVOKE} "autoreconf -i --verbose"
          COMMAND
            ${MSYS_INVOKE} "./configure --prefix=${EP_INSTALL_PREFIX} LDFLAGS=${EXTRA_LDFLAGS} CFLAGS=-DCURL_STATICLIB "
          COMMAND
            ${MSYS_INVOKE} "cmake -E copy ${EP_BASE}/src/ep_htslib/config.mk ${EP_BASE}/src/ep_htslib/config.mk.orig"
#          COMMAND
#            ${MSYS_INVOKE} "cd ${CMAKE_SOURCE_DIR}/.. && git apply --no-index --ignore-whitespace -p1 --unsafe-paths --verbose --directory=${EP_SOURCE_DIR}/ep_htslib ${CMAKE_CURRENT_SOURCE_DIR}/cmake/patches/htslib.1.15.1.Makefile.patch"
#          COMMAND
#            ${MSYS_INVOKE} "cd ${CMAKE_SOURCE_DIR}/.. && git apply --no-index --ignore-whitespace -p1 --unsafe-paths --verbose --directory=${EP_SOURCE_DIR}/ep_htslib ${CMAKE_CURRENT_SOURCE_DIR}/cmake/patches/htslib.1.15.1.config.mk.patch"
#          COMMAND
#            ${MSYS_INVOKE} "cmake -P ${CMAKE_SOURCE_DIR}/cmake/Modules/ModHTSConfigLibs.cmake ${EP_BASE}/src/ep_htslib/config.mk ${MSYS2_ENV_CMD}"
          COMMAND
            ${MSYS_INVOKE} "printenv"
        BUILD_COMMAND
          #$(MAKE)
          ${MSYS_INVOKE} make
        INSTALL_COMMAND
          #$(MAKE) install
          ${MSYS_INVOKE} "make install"
        BUILD_IN_SOURCE TRUE
        LOG_DOWNLOAD TRUE
        LOG_CONFIGURE TRUE
        LOG_BUILD TRUE
        LOG_INSTALL TRUE
        LOG_PATCH TRUE
      )
      endif() # for WIN32 section active or not
    else()
      # required to updated htslib configure.ac with autoconf 2.70
      #   - see https://github.com/samtools/htslib/commit/680c0b8ef0ff133d3b572abc80fe66fc2ea965f0
      #   - and https://github.com/samtools/htslib/pull/1198/commits/6821fc8ed88706e9282b561e74dfa45dac4d74c8
      find_program(AUTORECONF NAMES autoreconf REQUIRED)

      ExternalProject_Add(ep_htslib
        PREFIX "externals"
        URL "https://github.com/samtools/htslib/releases/download/1.15.1/htslib-1.15.1.tar.bz2"
        URL_HASH SHA1=e7cbd4bb059020c9486facc028f750ec0fb2e182
        #URL "https://github.com/samtools/htslib/releases/download/1.16/htslib-1.16.tar.bz2"
        #URL_HASH SHA1=36b16f462384af257d292ebeed766f299ec205f5
        UPDATE_COMMAND ""
        CONFIGURE_COMMAND
            autoheader
          COMMAND
            ${AUTORECONF} -i
          COMMAND
            ./configure --prefix=${EP_INSTALL_PREFIX} LDFLAGS=${EXTRA_LDFLAGS} CFLAGS=${CFLAGS}
        BUILD_COMMAND
          $(MAKE)
        INSTALL_COMMAND
          $(MAKE) install
        PATCH_COMMAND
        BUILD_IN_SOURCE TRUE
        LOG_DOWNLOAD TRUE
        LOG_CONFIGURE TRUE
        LOG_BUILD TRUE
        LOG_INSTALL TRUE
      )
    endif()
    list(APPEND FORWARD_EP_CMAKE_ARGS -DEP_HTSLIB_BUILT=TRUE)
    list(APPEND EXTERNAL_PROJECTS ep_htslib)
  else()
    message(FATAL_ERROR "Unable to find HTSlib")
  endif()
endif()

# Create the imported target for HTSlib
if (HTSLIB_FOUND AND NOT TARGET HTSlib::HTSlib)
  add_library(HTSlib::HTSlib UNKNOWN IMPORTED)
  set_target_properties(HTSlib::HTSlib PROPERTIES
    IMPORTED_LOCATION "${HTSLIB_LIBRARIES}"
    INTERFACE_INCLUDE_DIRECTORIES "${HTSLIB_INCLUDE_DIR}"
  )
endif()

if (EP_HTSLIB_BUILT AND TARGET HTSlib::HTSlib)
  include(TileDBCommon)
  install_target_libs(HTSlib::HTSlib)
endif()
