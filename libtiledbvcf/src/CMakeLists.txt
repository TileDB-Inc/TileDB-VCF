############################################################
# Get source commit hash
############################################################

# TODO: This won't work in sdist

find_package(Git REQUIRED)

execute_process(
        COMMAND "${GIT_EXECUTABLE}" describe --exact-match --tags HEAD
        WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
        RESULT_VARIABLE res
        OUTPUT_VARIABLE BUILD_COMMIT_HASH
        ERROR_QUIET
        OUTPUT_STRIP_TRAILING_WHITESPACE)

# If we didn't find a tag name let's grab the SHA
if (res)
  execute_process(
          COMMAND "${GIT_EXECUTABLE}" describe --dirty=-modified --always
          WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
          RESULT_VARIABLE res
          OUTPUT_VARIABLE BUILD_COMMIT_HASH
          ERROR_QUIET
          OUTPUT_STRIP_TRAILING_WHITESPACE)
endif()

set_property(GLOBAL APPEND
        PROPERTY CMAKE_CONFIGURE_DEPENDS
        "${CMAKE_SOURCE_DIR}/.git/index")

message(STATUS "Building with commit hash ${BUILD_COMMIT_HASH}")

############################################################
# Common object library
############################################################

set(TILEDB_VCF_SOURCES
        ${CMAKE_CURRENT_SOURCE_DIR}/c_api/tiledbvcf.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/dataset/attribute_buffer_set.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/dataset/tiledbvcfdataset.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/htslib_plugin/hfile_tiledb_vfs.c
        ${CMAKE_CURRENT_SOURCE_DIR}/read/bcf_exporter.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/read/delete_exporter.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/read/exporter.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/read/in_memory_exporter.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/read/pvcf_exporter.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/read/read_query_results.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/read/reader.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/read/tsv_exporter.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/stats/array_buffers.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/stats/arrow_adapter.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/stats/allele_count.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/stats/column_buffer.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/stats/managed_query.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/stats/sample_stats.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/stats/variant_stats.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/stats/variant_stats_reader.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/utils/bitmap.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/utils/buffer.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/utils/logger.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/utils/normalize.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/utils/sample_utils.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/utils/utils.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/vcf/bed_file.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/vcf/region.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/vcf/vcf_merger.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/vcf/vcf_utils.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/vcf/vcf_v2.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/vcf/vcf_v3.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/vcf/vcf_v4.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/write/record_heap_v2.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/write/record_heap_v3.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/write/record_heap_v4.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/write/writer_worker_v2.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/write/writer_worker_v3.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/write/writer_worker_v4.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/write/writer.cc
)

set(TILEDB_VCF_EXTERNAL_SOURCES
        ${CMAKE_CURRENT_SOURCE_DIR}/../external/base64/base64.cc
)

add_library(tiledbvcf
        ${TILEDB_VCF_SOURCES}
        ${TILEDB_VCF_EXTERNAL_SOURCES}
)

if (TILEDBVCF_ENABLE_PYTHON)
  set_target_properties(
          tiledbvcf
          PROPERTIES
          POSITION_INDEPENDENT_CODE ON
  )
endif()

target_compile_features(tiledbvcf PUBLIC cxx_std_20)

#set_property(TARGET tiledbvcf PROPERTY POSITION_INDEPENDENT_CODE ON)

target_include_directories(tiledbvcf
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../external
        ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(
        tiledbvcf
        PUBLIC
        HTSlib::HTSlib
        TileDB::tiledb_shared
        spdlog::spdlog
)

target_compile_definitions(tiledbvcf PRIVATE
        -DBUILD_COMMIT_HASH="${BUILD_COMMIT_HASH}"
)
############################################################
# Compile options/definitions
############################################################

if (SANITIZER)
  string( TOLOWER "${CMAKE_BUILD_TYPE}" CMAKE_BUILD_TYPE_LOWER)
  if (NOT CMAKE_BUILD_TYPE_LOWER MATCHES "debug")
    message(FATAL_ERROR "Sanitizers only enabled for Debug build")
  endif()
  string(TOLOWER ${SANITIZER} SANITIZER)
  if (NOT SANITIZER MATCHES "^(address|memory|leak|thread|undefined)$")
    message(FATAL_ERROR "Unknown clang sanitizer: ${SANITIZER})")
  else()
    message(STATUS "The TileDB-VCF library is compiled with sanitizer ${SANITIZER} enabled")
  endif()
  target_compile_options(tiledbvcf
          PRIVATE
          -g -fno-omit-frame-pointer -fno-optimize-sibling-calls -fsanitize=${SANITIZER}
  )
endif()

target_include_directories(
        tiledbvcf
        PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/c_api
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_BINARY_DIR}
)

# Sanitizer linker flags
if (SANITIZER)
  target_link_libraries(tiledbvcf
          INTERFACE
          -fsanitize=${SANITIZER}
  )
  if (WIN32)
    target_link_libraries(tiledbvcf4test
            INTERFACE
            -fsanitize=${SANITIZER}
    )
  endif()
endif()

# List of API headers (to be installed)
set(TILEDB_VCF_PUBLIC_HEADERS
        ${CMAKE_CURRENT_SOURCE_DIR}/c_api/tiledbvcf.h
        ${CMAKE_CURRENT_SOURCE_DIR}/c_api/tiledbvcf_enum.h
)

############################################################
# CLI executable target
############################################################

add_executable(tiledbvcf-bin
        ${CMAKE_CURRENT_SOURCE_DIR}/cli/tiledbvcf.cc
)

target_link_libraries(
        tiledbvcf-bin
        PUBLIC
        tiledbvcf
        CLI11::CLI11
)

if (WIN32)
  set_target_properties(tiledbvcf-bin PROPERTIES OUTPUT_NAME tiledbvcfcli)
else()
  set_target_properties(tiledbvcf-bin PROPERTIES OUTPUT_NAME tiledbvcf)
endif()

# Sanitizer linker flags
if (SANITIZER)
  target_link_libraries(tiledbvcf-bin
          INTERFACE
          -fsanitize=${SANITIZER}
  )
endif()

if (NOT APPLE AND NOT WIN32)
  target_link_libraries(tiledbvcf-bin PRIVATE pthread)
endif()

target_include_directories(tiledbvcf-bin
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/../external
)

############################################################
# API symbol exports (and public headers for install)
############################################################

include(GenerateExportHeader)

# Generates the file 'tiledbvcf_export.h' suitable for the current compiler.
generate_export_header(tiledbvcf
        BASE_NAME tiledbvcf
)

# Set variables in the parent scope so the tests can reference it.
set(TILEDB_VCF_EXPORT_HEADER_NAME "tiledbvcf_export.h" PARENT_SCOPE)
set(TILEDB_VCF_EXPORT_HEADER_DIR "${CMAKE_CURRENT_BINARY_DIR}" PARENT_SCOPE)
set(TILEDB_VCF_EXPORT_HEADER "${CMAKE_CURRENT_BINARY_DIR}/tiledbvcf_export.h")
set(TILEDB_VCF_EXPORT_HEADER "${TILEDB_VCF_EXPORT_HEADER}" PARENT_SCOPE)

# Set related compiler settings
#target_compile_definitions(TILEDB_VCF_OBJECTS PRIVATE -DTILEDB_VCF_OBJECTS_EXPORTS)
#target_include_directories(TILEDB_VCF_OBJECTS PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
target_include_directories(tiledbvcf-bin PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

# Add the generated header to the public headers list
list(APPEND TILEDB_VCF_PUBLIC_HEADERS
        "${TILEDB_VCF_EXPORT_HEADER}"
)

# Set the public headers, which are the ones that get installed.
set_target_properties(tiledbvcf
        PROPERTIES
        PUBLIC_HEADER "${TILEDB_VCF_PUBLIC_HEADERS}"
)

############################################################
# Installation
############################################################

include(GNUInstallDirs)

# Set rpath to be relative to the .so.
if (APPLE)
  set_target_properties(tiledbvcf PROPERTIES INSTALL_RPATH "@loader_path/")
else()
  set_target_properties(tiledbvcf PROPERTIES INSTALL_RPATH "$ORIGIN/")
endif()

set_property(
        TARGET tiledbvcf-bin
        PROPERTY INSTALL_RPATH
        "${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}"
        "${CMAKE_INSTALL_PREFIX}/lib"
)

if(WIN32)
  # Having the .dll and the .exe with the same name was resulting in a .exp
  # file for the .exe being 'last out' (replacing one of same name for the .dll)
  # and was causing the python api extension
  # to link to the .exe, which was not functional.
  # So, tiledbvcf-bin now sets an output (above somewhere) of tiledbvcfcli.exe
  # and we install it here RENAMEing it to match the known name in pre-existing
  # *nix world.
  install(PROGRAMS $<TARGET_FILE:tiledbvcf-bin>
          RENAME tiledbvcf.exe
          DESTINATION ${CMAKE_INSTALL_BINDIR}
  )
else()
  install(
          TARGETS tiledbvcf-bin
          RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  )
endif()

#if(TILEDBVCF_INSTALL_TILEDB)
#  install(
#          IMPORTED_RUNTIME_ARTIFACTS
#          TileDB::tiledb_shared
#          RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
#          LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
#  )
#endif()

install(
        TARGETS tiledbvcf
        RUNTIME DESTINATION tiledbvcf
        LIBRARY DESTINATION tiledbvcf
        ARCHIVE DESTINATION tiledbvcf
        INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
        PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/tiledbvcf
)

if (TILEDBVCF_ENABLE_PYTHON)
  if (APPLE)
    set_target_properties(tiledbvcf-bin PROPERTIES INSTALL_RPATH "@loader_path/../lib")
    set_target_properties(tiledbvcf PROPERTIES INSTALL_RPATH "@loader_path")
  else()
    set_target_properties(tiledbvcf-bin PROPERTIES INSTALL_RPATH "\$ORIGIN/../lib")
    set_target_properties(tiledbvcf PROPERTIES INSTALL_RPATH "\$ORIGIN")
  endif()
endif()
