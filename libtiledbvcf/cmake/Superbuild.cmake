#
# Superbuild.cmake
#
#
# The MIT License
#
# Copyright (c) 2018-2021 TileDB, Inc.
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

include(ExternalProject)

############################################################
# Common variables
############################################################

# Build paths for external projects
set(EP_BASE "${CMAKE_CURRENT_BINARY_DIR}/externals")
set(EP_SOURCE_DIR "${EP_BASE}/src")
set(EP_INSTALL_PREFIX "${EP_BASE}/install")

# A variable that will hold extra variables to pass to the regular
# non-superbuild build process as CMake arguments.
set(FORWARD_EP_CMAKE_ARGS)

# Variable that will hold a list of all the external projects added
# as a part of the superbuild.
set(EXTERNAL_PROJECTS)

message(STATUS "CMAKE_MODULE_PATH: ${CMAKE_MODULE_PATH}")
message(STATUS "CMAKE_PREFIX_PATH: ${CMAKE_PREFIX_PATH}")

list(JOIN CMAKE_PREFIX_PATH "|" CMAKE_PREFIX_PATH_STR)

# Forward any additional CMake args to the non-superbuild.
set(INHERITED_CMAKE_ARGS
  -DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX}
  -DCMAKE_PREFIX_PATH=${CMAKE_PREFIX_PATH_STR}
  -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
  -DCMAKE_MODULE_PATH=${CMAKE_MODULE_PATH}
  -DEP_BASE=${EP_BASE}
  -DFORCE_EXTERNAL_HTSLIB=${FORCE_EXTERNAL_HTSLIB}
  -DFORCE_EXTERNAL_TILEDB=${FORCE_EXTERNAL_TILEDB}
  -DTILEDB_S3=${TILEDB_S3}
  -DTILEDB_WERROR=${TILEDB_WERROR}
  -DTILEDBVCF_ENABLE_PYTHON=${TILEDBVCF_ENABLE_PYTHON}
  -DTILEDBVCF_ENABLE_TESTS=${TILEDBVCF_ENABLE_TESTS}
  -DTileDB_DIR=${TileDB_DIR}
  -DSANITIZER=${SANITIZER}
  -DOVERRIDE_INSTALL_PREFIX=${OVERRIDE_INSTALL_PREFIX}
  -DPython_FIND_REGISTRY=${Python_FIND_REGISTRY}
  -DPYTHON_VERSION_STRING=${PYTHON_VERSION_STRING}
  -DPYTHON_LIBRARY=${PYTHON_LIBRARY}
  -DPYTHON_INCLUDE_DIR=${PYTHON_INCLUDE_DIR}
  -DPython_EXECUTABLE=${Python_EXECUTABLE}
  -DPython_ROOT_DIR=${Python_ROOT_DIR}
  -DPython_INCLUDE_DIR=${Python_INCLUDE_DIR}
  -DPython3_FIND_REGISTRY=${Python3_FIND_REGISTRY}
  -DPython3_INCLUDE_DIR=${Python3_INCLUDE_DIR}
  -Dpybind11_DIR=${pybind11_DIR}
)

############################################################
# Set up external projects for dependencies
############################################################

# These includes modify the EXTERNAL_PROJECTS variable.

include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/FindCLI11.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/FindHTSlib.cmake)
# need spdlog to set up any of its needed targets...
include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/FindSpdlog.cmake)
#... before tiledb sets up only half of them...
include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/FindTileDB_EP.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/FindCatch2.cmake)

############################################################
# 'make format' target
############################################################

set(SCRIPTS_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../ci")

find_package(ClangTools)
if (${CLANG_FORMAT_FOUND})
  # Runs clang-format and updates files in place.
  add_custom_target(format ${SCRIPTS_DIR}/run-clang-format.sh ${CMAKE_CURRENT_SOURCE_DIR}/src ${CLANG_FORMAT_BIN} 1
    `find ${CMAKE_CURRENT_SOURCE_DIR}/src ${CMAKE_CURRENT_SOURCE_DIR}/test
    -name \\*.cc -or -name \\*.c -or -name \\*.h`)

  # Runs clang-format and exits with a non-zero exit code# if any files need to
  # be reformatted
  add_custom_target(check-format ${SCRIPTS_DIR}/run-clang-format.sh ${CMAKE_CURRENT_SOURCE_DIR}/src ${CLANG_FORMAT_BIN} 0
    `find ${CMAKE_CURRENT_SOURCE_DIR}/src ${CMAKE_CURRENT_SOURCE_DIR}/test
    -name \\*.cc -or -name \\*.c -or -name \\*.h`)
endif()

############################################################
# Set up the regular build (i.e. non-superbuild).
############################################################

ExternalProject_Add(libtiledbvcf
  SOURCE_DIR ${PROJECT_SOURCE_DIR}
  CMAKE_ARGS
    -DSUPERBUILD=OFF
    ${INHERITED_CMAKE_ARGS}
    ${FORWARD_EP_CMAKE_ARGS}
  INSTALL_COMMAND ""
  BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/libtiledbvcf
  DEPENDS ${EXTERNAL_PROJECTS}
  LIST_SEPARATOR "|"
)

# make install-libtiledbvcf
add_custom_target(install-libtiledbvcf
  COMMAND ${CMAKE_COMMAND} --build . --target install --config ${CMAKE_BUILD_TYPE}
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/libtiledbvcf
)

add_custom_target(
  skbuild-export
  COMMAND
  ${CMAKE_COMMAND} --install . --prefix ${SKBUILD_PLATLIB_DIR}/tiledbvcf --config $<CONFIG>
  WORKING_DIRECTORY
  ${CMAKE_CURRENT_BINARY_DIR}/libtiledbvcf
  DEPENDS
  libtiledbvcf
)

# make check
add_custom_target(check
  COMMAND ${CMAKE_COMMAND} --build . --target check --config ${CMAKE_BUILD_TYPE}
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/libtiledbvcf
)
