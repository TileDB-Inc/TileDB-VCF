#
# FindTileDB_EP.cmake
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
# Finds the TileDB library, installing with an ExternalProject as necessary.

# If TileDB was installed as an EP, need to search the EP install path also.
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} "${EP_INSTALL_PREFIX}")

if (FORCE_EXTERNAL_TILEDB)
  find_package(TileDB CONFIG PATHS ${EP_INSTALL_PREFIX} NO_DEFAULT_PATH)
else()
  find_package(TileDB CONFIG)
endif()

if (TILEDB_FOUND)
    if(CMAKE_BUILD_TYPE MATCHES "[Dd][Ee][Bb][Uu][Gg]")
        get_target_property(TILEDB_LIB TileDB::tiledb_shared IMPORTED_LOCATION_DEBUG)
    else()
        get_target_property(TILEDB_LIB TileDB::tiledb_shared IMPORTED_LOCATION_RELEASE)
    endif()
  message(STATUS "Found TileDB: ${TILEDB_LIB}")
else()
  if (SUPERBUILD)
    message(STATUS "Adding TileDB as an external project")
    if (TILEDB_S3 STREQUAL "OFF")
      message(STATUS "TileDB will be built WITHOUT S3 support")
    endif()

    # Try to download prebuilt artifacts unless the user specifies to build from source
    if(DOWNLOAD_TILEDB_PREBUILT)
        if (WIN32) # Windows
          SET(DOWNLOAD_URL "https://github.com/TileDB-Inc/TileDB/releases/download/2.22.0/tiledb-windows-x86_64-2.22.0-52e981e.zip")
          SET(DOWNLOAD_SHA1 "7c1852c1c916be650cf7f75b238844104370cb1f")
        elseif(APPLE) # OSX

          if (CMAKE_OSX_ARCHITECTURES STREQUAL x86_64 OR CMAKE_SYSTEM_PROCESSOR MATCHES "(x86_64)|(AMD64|amd64)|(^i.86$)")
            SET(DOWNLOAD_URL "https://github.com/TileDB-Inc/TileDB/releases/download/2.22.0/tiledb-macos-x86_64-2.22.0-52e981e.tar.gz")
            SET(DOWNLOAD_SHA1 "a0b6b91a448aefae351cbe96c5cc2c8867eb2b07")
          elseif (CMAKE_OSX_ARCHITECTURES STREQUAL arm64 OR CMAKE_SYSTEM_PROCESSOR MATCHES "^aarch64" OR CMAKE_SYSTEM_PROCESSOR MATCHES "^arm")
            SET(DOWNLOAD_URL "https://github.com/TileDB-Inc/TileDB/releases/download/2.22.0/tiledb-macos-arm64-2.22.0-52e981e.tar.gz")
            SET(DOWNLOAD_SHA1 "838ab13090f0832e3858b88aa3aa9b38bde7c7cb")
          endif()
        else() # Linux
          SET(DOWNLOAD_URL "https://github.com/TileDB-Inc/TileDB/releases/download/2.22.0/tiledb-linux-x86_64-2.22.0-52e981e.tar.gz")
          SET(DOWNLOAD_SHA1 "292fdd4d4034ef7e4686da04b1ffc9e7976f1601")
        endif()

        ExternalProject_Add(ep_tiledb
                PREFIX "externals"
                URL ${DOWNLOAD_URL}
                URL_HASH SHA1=${DOWNLOAD_SHA1}
                CONFIGURE_COMMAND ""
                BUILD_COMMAND ""
                UPDATE_COMMAND ""
                PATCH_COMMAND ""
                TEST_COMMAND ""
                INSTALL_COMMAND
                    ${CMAKE_COMMAND} -E copy_directory ${EP_BASE}/src/ep_tiledb ${EP_INSTALL_PREFIX}
                LOG_DOWNLOAD TRUE
                LOG_CONFIGURE FALSE
                LOG_BUILD FALSE
                LOG_INSTALL FALSE
                )
    else() # Build from source
        ExternalProject_Add(ep_tiledb
          PREFIX "externals"
          URL "https://github.com/TileDB-Inc/TileDB/archive/2.22.0.zip"
          URL_HASH SHA1=edce787da60547b8a3043248f1f44e86fcffc3eb
          DOWNLOAD_NAME "tiledb.zip"
          CMAKE_ARGS
            -DCMAKE_INSTALL_PREFIX=${EP_INSTALL_PREFIX}
            -DCMAKE_PREFIX_PATH=${EP_INSTALL_PREFIX}
            -DTILEDB_S3=${TILEDB_S3}
            -DTILEDB_SKIP_S3AWSSDK_DIR_LENGTH_CHECK=ON # for windows build
            -DTILEDB_VERBOSE=ON
            -DTILEDB_SERIALIZATION=ON
            -DTILEDB_TESTS=OFF
            -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
          UPDATE_COMMAND ""
          INSTALL_COMMAND
            ${CMAKE_COMMAND} --build . --target install-tiledb
          LOG_DOWNLOAD TRUE
          LOG_CONFIGURE TRUE
          LOG_BUILD TRUE
          LOG_INSTALL TRUE
        )
    endif()

    list(APPEND FORWARD_EP_CMAKE_ARGS -DEP_TILEDB_BUILT=TRUE)
    list(APPEND EXTERNAL_PROJECTS ep_tiledb)
  else()
    message(FATAL_ERROR "Unable to find TileDB library.")
  endif()
endif()

if (EP_TILEDB_BUILT AND TARGET TileDB::tiledb_shared)
  include(TileDBCommon)
  install_target_libs(TileDB::tiledb_shared)
endif()
