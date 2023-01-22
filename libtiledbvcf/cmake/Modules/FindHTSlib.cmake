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
  NAMES hts
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
    find_program(AUTORECONF NAMES autoreconf REQUIRED)
    find_program(AUTOHEADER NAMES autoheader REQUIRED)
    find_program(BASH_PATH NAMES bash REQUIRED)
    set(MSYS_INVOKE e:/msys64/usr/bin/env MSYSTEM=MINGW64 CHERE_INVOKING=1 /usr/bin/bash -li -x -c )

    ExternalProject_Add(ep_htslib
      PREFIX "externals"
      URL "https://github.com/samtools/htslib/releases/download/1.15.1/htslib-1.15.1.tar.bz2"
      URL_HASH SHA1=e7cbd4bb059020c9486facc028f750ec0fb2e182
      UPDATE_COMMAND ""
      CONFIGURE_COMMAND
          echo "hello from ep_htslib configure attempt... \n$PATH"
        COMMAND
          printenv
        COMMAND
          ps
        #~ COMMAND
          #~ #echo "MSYSTEM is ${MSYSTEM}"
          #~ #echo "MSYSTEM is $MSYSTEM or %MSYSTEM% or \\\$\{MSYSTEM}"
          #~ #echo "MSYSTEM is $MSYSTEM or %MSYSTEM% or \\\\\$\\\{MSYSTEM}"
          #~ echo "MSYSTEM is $MSYSTEM or %MSYSTEM% or $\\\{MSYSTEM}"
        ##COMMAND
        ##  set
        #COMMAND
          #which autoheader
        #COMMAND
          #ls -l /usr/bin
        #COMMAND
          #pwd
        #COMMAND
          #mount
        #COMMAND
          #ls -l /usr/share
        #COMMAND
          #ls -l /usr/share/autoconf-2.71
        #COMMAND
          #ls -l /usr/share/autoconf-2.71/Autom4te
        #COMMAND
          #bash -c 'which autoheader & ls -l `which autoheader`'
        ##~ COMMAND
          ##~ bash -c 'echo backticks & `which autoheader`'
        ##~ COMMAND
          ##~ `which autoheader`
        ##~ COMMAND
          ##~ "`which autoheader`"
        ##~ COMMAND
          ##~ cat "`which autoheader`"
        ##~ COMMAND
          ##~ cat '`which autoheader`'
        #COMMAND
          ##echo trying bare cygpath... & cygpath -w /
          #echo trying bare cygpath...
        #COMMAND
          #which cygpath
        #COMMAND
          #cygpath -w /
        #COMMAND
          #whoami
        ##~ COMMAND
          ##~ #autoheader --verbose
          ##~ bash -c autoheader --verbose
        ##~ COMMAND
          ##~ #bash -c \""whoami & cygpath -w / & ls -l /"\"
          ##~ #bash -c "\"whoami & cygpath -w / & ls -l /\""
          ##~ #bash -c "\\\"whoami & cygpath -w / & ls -l /\\\""
          ##~ #bash -c "\\\"which whoami & which cygpath & which ls\\\""
          ##~ #which whoami & which cygpath & which ls
          ##~ 'bash -c "whoami & which cygpath & which ls"'
        ##~ COMMAND
          ##~ bash -c "whoami & cygpath -w / & ls -l /"
        ##~ COMMAND
          ##~ whoami & ls -l /
        #COMMAND
          #which autoheader
        ##~ COMMAND
          ##~ #ls -l `which autoheader`
          ##~ #ls -l "`which autoheader`"
          ##~ bash -c 'ls -l \"`which autoheader`\"'
        ##~ COMMAND
          ##~ cat /usr/bin/dlhhack.sh
        ##~ COMMAND
          ##~ #. /usr/bin/dlhhack.sh
          ##~ #/usr/bin/dlhhack.sh
          ##~ bash -c /usr/bin/dlhhack.sh
        #COMMAND
          #bash -c "autoheader --verbose"
        #COMMAND
          #autoheader --verbose
          ##. /usr/bin/autoheader
        #COMMAND
          #. /dlhhack.sh
        ##~ COMMAND
          ##~ bash -c ". /dlhhack.sh"
        #COMMAND
          #set -x & cygpath -w /
        #COMMAND
          #set -x & /usr/bin/autoheader
        #COMMAND
          #bash -c pwd
        #COMMAND
          #bash -c "pwd"
        #COMMAND
          #bash -c "pwd & ps -a & ls -l /"
        #COMMAND
          #bash -c "echo / & ls -l /"
        #COMMAND
          #bash -c "set -x & cygpath -w /"
        #COMMAND
          #bash -c "set -x & cygpath -w /usr"
        #COMMAND
          #bash -c "set -x & cygpath -w /usr/bin"
        #COMMAND
          #bash -c "echo /usr & ls -l /usr"
        #COMMAND
          #bash -c "echo /usr/bin & ls -l /usr/bin"
        #COMMAND
          #bash -c "echo /usr/bin/*uto* & ls -l /usr/bin/*uto*"
        #COMMAND
          #bash -c "echo /usr/bin/*4* & ls -l /usr/bin/*4*"
        #COMMAND
          #bash -c "echo /usr/bin/*uto* & ls -ld /usr/bin/*uto*/"
        #COMMAND
          #bash -c "/mnt/e/msys64/usr/bin/autoheader --help"
        #COMMAND
          #bash -c "/usr/bin/autoheader --help"
        #COMMAND
          #echo "-1 not about to run autoheader..."
        ##~ COMMAND
          ##~ "echo -1 not about to run autoheader..."
        ##~ COMMAND
          ##~ bash -c \"echo 0 not about to run autoheader...\"
        #COMMAND
          #bash -c "\\\"echo 0 not about to run autoheader...\\\""
        #COMMAND
          #bash -c "\\\"echo A not about to run autoheader... & echo $PATH \\\""
        #COMMAND
          #bash -c "\\\"echo B not about to run autoheader... & echo $PATH & set -x \\\""
        #COMMAND
          ##autoheader
          ##$(which autoheader)
          ##/usr/bin/autoheader
          ##bash -c "/usr/bin/autoheader"
          ##bash -c "\"echo about to run autoheader... & /usr/bin/autoheader\""
          #bash -c "\\\"echo about to run autoheader... & echo $PATH & set -x & /usr/bin/autoheader\\\""
          ##bash -c '"echo about to run autoheader... & /usr/bin/autoheader"'
          ##. /usr/bin/autoheader
          ##"(/usr/bin/autoheader)"
        #~ COMMAND
          #~ which bash
        #~ COMMAND
          #~ ls -l /bin/bash
        #~ COMMAND
          #~ cygpath -w /bin/bash
        #~ COMMAND
          #~ cygpath -w bash
        #~ COMMAND
          #~ #/bin/bash "echo will this hang around, let me exec autoheader from it..."
          #~ #bash "echo will this hang around, let me exec autoheader from it..."
          #~ #bash -c "/usr/bin/autoheader --verbose"
          #~ bash -c "E:/msys64/usr/bin/autoheader --verbose"
        #COMMAND
        # TBD: curious, this was invoking wsl bash, suddenly after I add the BASH_PATH, it
        # ceases working, complains about not finding the output path...?
        # ... seems I was probably -almost- out of disk space, as I ran low
        # ... from something else running very soon after this latest issue encountered...
        #  bash -c "printenv > /mnt/e/msys64/dlh1.printenv.out.txt"
        COMMAND
          #${AUTOHEADER}
          #bash -c "${AUTOHEADER}"
          #bash -x -c "${AUTOHEADER} --verbose"
          #bash -x -c "/mnt/e/msys64/usr/bin/dlhhack.sh"
          #bash 
          #${BASH_PATH} -x -c "/mnt/e/msys64/usr/bin/autoheader"
          ${BASH_PATH} -x -c "${AUTOHEADER} --verbose"
          # try again after renaming WSL bash...
          #still failed... autoheader --verbose
          #bash -c "autoheader --verbose"
        COMMAND
          #${AUTORECONF} -i
          #bash -x -c "${AUTORECONF} -i"
          #${BASH_PATH} -x -c "${AUTORECONF} -i"
          #${BASH_PATH} -x -c "${AUTORECONF} -i --verbose"
          #from above (msys2), got 
          #~ + E:/msys64/usr/bin/autoreconf -i
          #~ aclocal-1.16: error: aclocal: file '/msys64/usr/share/aclocal/tcl-tea.m4' does not exist
          #~ autoreconf-2.71: error: aclocal failed with exit status: 1
          # of course, even if this works for msys2/mingw64, negates whatever 'they' were
          # trying to do when the imp'd the find_program/variable usage...
          # seems to fail similarly whether 'bare' here, or path in ${AUTORECONF} above...
          #${BASH_PATH} -x -c "autoreconf -i"
          #${BASH_PATH} -x -c "autoreconf -i --verbose"
          #e:/msys64/mingw64 -x -c "autoreconf -i --verbose"
          #autoreconf -i --verbose
          #considering info @ https://www.msys2.org/wiki/Launchers/
          e:/msys64/usr/bin/env MSYSTEM=MINGW64 CHERE_INVOKING=1 /usr/bin/bash -li -x -c "autoreconf -i --verbose"
        COMMAND
          #./configure --prefix=${EP_INSTALL_PREFIX} LDFLAGS=${EXTRA_LDFLAGS} CFLAGS=${CFLAGS}
          ${MSYS_INVOKE} "./configure --prefix=${EP_INSTALL_PREFIX} LDFLAGS=${EXTRA_LDFLAGS} CFLAGS=${CFLAGS}"
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
