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
# Search for libtilebdvcf in the default install directory and in the directory
# specified by the LIBTILEDBVCF_PATH environment variable.
set(LIBTILEDBVCF_SEARCH_PATHS 
    "${CMAKE_CURRENT_SOURCE_DIR}/../../dist/lib"
    "${CMAKE_CURRENT_SOURCE_DIR}/dist_links/dist/lib"
    "$ENV{LIBTILEDBVCF_PATH}"
)

message(STATUS "Searching for libtiledbvcf in ${LIBTILEDBVCF_SEARCH_PATHS}")

# TODO: Use find_package() instead of find_library() to find libtiledbvcf.
find_library(
    TILEDBVCF_LIB
    NAMES tiledbvcf
    PATHS ${LIBTILEDBVCF_SEARCH_PATHS}
)

if(NOT TILEDBVCF_LIB)
    message(STATUS "Building libtiledbvcf from source")

    file(MAKE_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/dist_links/libtiledbvcf/build")

    execute_process(
        COMMAND bash -c "\
            ${CMAKE_COMMAND} .. && \
            ${CMAKE_COMMAND} --build . && \
            ${CMAKE_COMMAND} --build . --target install-libtiledbvcf"
        WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/dist_links/libtiledbvcf/build"
    )

    find_library(
        TILEDBVCF_LIB
        NAMES tiledbvcf
        PATHS ${LIBTILEDBVCF_SEARCH_PATHS}
    )
endif()

if(NOT TILEDBVCF_LIB)
    message(FATAL_ERROR "Could not find libtiledbvcf")
else()
    message(STATUS "Found libtiledbvcf: ${TILEDBVCF_LIB}")
    get_filename_component(TILEDBVCF_LIB_DIR ${TILEDBVCF_LIB} DIRECTORY)
    cmake_path(APPEND TILEDBVCF_LIB_DIR "../include/tiledbvcf" OUTPUT_VARIABLE TILEDBVCF_INCLUDE_DIR)
    cmake_path(APPEND TILEDBVCF_LIB_DIR "../bin" OUTPUT_VARIABLE TILEDBVCF_BIN_DIR)

    # Find required shared libs for the current platform
    if (APPLE)
        set(TILEDBVCF_SHARED_LIB_NAMES
            "libtiledb.dylib" "libtiledbvcf.dylib" "libhts.*dylib"
        )
    elseif(WIN32)
        set(TILEDBVCF_SHARED_LIB_NAMES
            "tiledb.lib" "tiledbvcf.lib" "hts-3.lib"
        )
    else()
        set(TILEDBVCF_SHARED_LIB_NAMES
            "libtiledb.so*" "libtiledbvcf.so" "libhts.so*"
        )
    endif()

    set(TILEDBVCF_SHARED_LIBS "")
    foreach(ITEM ${TILEDBVCF_SHARED_LIB_NAMES})
        file(GLOB LIB_FILE "${TILEDBVCF_LIB_DIR}/${ITEM}")
        if(LIB_FILE)
            list(APPEND TILEDBVCF_SHARED_LIBS "${LIB_FILE}")
        endif()
    endforeach()

    file(GLOB TILEDBVCF_PROGRAMS "${TILEDBVCF_BIN_DIR}/tiledbvcf*")
endif()
    set(TILEDBVCF_LIB tiledbvcf)
endif()

# Search for TileDB in the EP install directory
# list(PREPEND CMAKE_PREFIX_PATH 
#     "${CMAKE_CURRENT_SOURCE_DIR}/dist_links/libtiledbvcf/build/externals/install"
# )

# Search TileDB_DIR first if it is defined
if (DEFINED TileDB_DIR)
    list(PREPEND CMAKE_PREFIX_PATH "${TileDB_DIR}")
endif()

message(STATUS "CMAKE_MODULE_PATH: ${CMAKE_MODULE_PATH}")
message(STATUS "CMAKE_PREFIX_PATH: ${CMAKE_PREFIX_PATH}")

find_package(TileDB REQUIRED)

if (TILEDB_FOUND)
    get_target_property(TILEDB_LIB TileDB::tiledb_shared IMPORTED_LOCATION_RELEASE)
    message(STATUS "Found TileDB: ${TILEDB_LIB}")
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

target_include_directories(
    ${VCF_TARGET_NAME} PRIVATE
    ${TILEDBVCF_INCLUDE_DIR}
    "${CMAKE_CURRENT_SOURCE_DIR}/dist_links/libtiledbvcf/src"
    $<TARGET_PROPERTY:TileDB::tiledb_shared,INTERFACE_INCLUDE_DIRECTORIES>
)

target_link_libraries(
    ${VCF_TARGET_NAME} PRIVATE
        tiledbvcf
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
    LIBRARY DESTINATION .
)

# Install required shared libraries
#install(
#    FILES ${TILEDBVCF_SHARED_LIBS}
#    LIBRARY DESTINATION .
#)

# Install the command line tool
install(
    PROGRAMS ${TILEDBVCF_PROGRAMS}
    DESTINATION .
)
