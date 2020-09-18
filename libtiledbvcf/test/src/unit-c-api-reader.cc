/**
 * @file   unit-c-api-reader.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2019 TileDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 */

#include "c_api/tiledbvcf.h"
#include "catch.hpp"

#include <cstring>
#include <iostream>

static std::string INPUT_ARRAYS_DIR_V4 =
    TILEDB_VCF_TEST_INPUT_DIR + std::string("/arrays/v4");
static std::string INPUT_ARRAYS_DIR_V3 =
    TILEDB_VCF_TEST_INPUT_DIR + std::string("/arrays/v3");
static std::string INPUT_ARRAYS_DIR_V2 =
    TILEDB_VCF_TEST_INPUT_DIR + std::string("/arrays/v2");

/* ********************************* */
/*           HELPER MACROS           */
/* ********************************* */

#define SET_BUFF_POS_START(r, nr)                               \
  uint32_t pos_start[(nr)];                                     \
  REQUIRE(                                                      \
      tiledb_vcf_reader_set_buffer_values(                      \
          reader, "pos_start", sizeof(pos_start), pos_start) == \
      TILEDB_VCF_OK);

#define SET_BUFF_POS_END(r, nr)            \
  uint32_t pos_end[(nr)];                  \
  REQUIRE(                                 \
      tiledb_vcf_reader_set_buffer_values( \
          reader, "pos_end", sizeof(pos_end), pos_end) == TILEDB_VCF_OK);

#define SET_BUFF_QUERY_BED_START(r, nr)    \
  uint32_t query_bed_start[(nr)];          \
  REQUIRE(                                 \
      tiledb_vcf_reader_set_buffer_values( \
          reader,                          \
          "query_bed_start",               \
          sizeof(query_bed_start),         \
          query_bed_start) == TILEDB_VCF_OK);

#define SET_BUFF_QUERY_BED_END(r, nr)                                       \
  uint32_t query_bed_end[(nr)];                                             \
  REQUIRE(                                                                  \
      tiledb_vcf_reader_set_buffer_values(                                  \
          reader, "query_bed_end", sizeof(query_bed_end), query_bed_end) == \
      TILEDB_VCF_OK);

#define SET_BUFF_SAMPLE_NAME(r, nr)                                     \
  int32_t sample_name_offsets[(nr) + 1];                                \
  char sample_name[(nr)*10];                                            \
  REQUIRE(                                                              \
      tiledb_vcf_reader_set_buffer_values(                              \
          (reader), "sample_name", sizeof(sample_name), sample_name) == \
      TILEDB_VCF_OK);                                                   \
  REQUIRE(                                                              \
      tiledb_vcf_reader_set_buffer_offsets(                             \
          (reader),                                                     \
          "sample_name",                                                \
          sizeof(sample_name_offsets),                                  \
          sample_name_offsets) == TILEDB_VCF_OK);

#define SET_BUFF_CONTIG(r, nr)                                           \
  int32_t contig_offsets[(nr) + 1];                                      \
  char contig[(nr)*10];                                                  \
  REQUIRE(                                                               \
      tiledb_vcf_reader_set_buffer_values(                               \
          (reader), "contig", sizeof(contig), contig) == TILEDB_VCF_OK); \
  REQUIRE(                                                               \
      tiledb_vcf_reader_set_buffer_offsets(                              \
          (reader), "contig", sizeof(contig_offsets), contig_offsets) == \
      TILEDB_VCF_OK);

#define SET_BUFF_ALLELES(r, nr)                                             \
  int32_t alleles_offsets[2 * (nr) + 1];                                    \
  int32_t alleles_list_offsets[(nr) + 1];                                   \
  char alleles[(nr)*20];                                                    \
  REQUIRE(                                                                  \
      tiledb_vcf_reader_set_buffer_values(                                  \
          (reader), "alleles", sizeof(alleles), alleles) == TILEDB_VCF_OK); \
  REQUIRE(                                                                  \
      tiledb_vcf_reader_set_buffer_offsets(                                 \
          (reader), "alleles", sizeof(alleles_offsets), alleles_offsets) == \
      TILEDB_VCF_OK);                                                       \
  REQUIRE(                                                                  \
      tiledb_vcf_reader_set_buffer_list_offsets(                            \
          (reader),                                                         \
          "alleles",                                                        \
          sizeof(alleles_list_offsets),                                     \
          alleles_list_offsets) == TILEDB_VCF_OK);

#define SET_BUFF_FILTERS(r, nr)                                             \
  int32_t filters_offsets[2 * (nr) + 1];                                    \
  int32_t filters_list_offsets[(nr) + 1];                                   \
  uint8_t filters_bitmap[(nr) / 8 + 1];                                     \
  char filters[(nr)*20];                                                    \
  REQUIRE(                                                                  \
      tiledb_vcf_reader_set_buffer_values(                                  \
          (reader), "filters", sizeof(filters), filters) == TILEDB_VCF_OK); \
  REQUIRE(                                                                  \
      tiledb_vcf_reader_set_buffer_offsets(                                 \
          (reader), "filters", sizeof(filters_offsets), filters_offsets) == \
      TILEDB_VCF_OK);                                                       \
  REQUIRE(                                                                  \
      tiledb_vcf_reader_set_buffer_list_offsets(                            \
          (reader),                                                         \
          "filters",                                                        \
          sizeof(filters_list_offsets),                                     \
          filters_list_offsets) == TILEDB_VCF_OK);                          \
  REQUIRE(                                                                  \
      tiledb_vcf_reader_set_buffer_validity_bitmap(                         \
          reader, "filters", sizeof(filters_bitmap), filters_bitmap) ==     \
      TILEDB_VCF_OK);

#define SET_BUFF_INFO(r, nr)                                       \
  int32_t info_offsets[(nr) + 1];                                  \
  char info[(nr)*100];                                             \
  REQUIRE(                                                         \
      tiledb_vcf_reader_set_buffer_values(                         \
          (reader), "info", sizeof(info), info) == TILEDB_VCF_OK); \
  REQUIRE(                                                         \
      tiledb_vcf_reader_set_buffer_offsets(                        \
          (reader), "info", sizeof(info_offsets), info_offsets) == \
      TILEDB_VCF_OK);

#define SET_BUFF_FORMAT(r, nr)                                        \
  int32_t format_offsets[(nr) + 1];                                   \
  char format[(nr)*100];                                              \
  REQUIRE(                                                            \
      tiledb_vcf_reader_set_buffer_values(                            \
          (reader), "fmt", sizeof(format), format) == TILEDB_VCF_OK); \
  REQUIRE(                                                            \
      tiledb_vcf_reader_set_buffer_offsets(                           \
          (reader), "fmt", sizeof(format_offsets), format_offsets) == \
      TILEDB_VCF_OK);

#define SET_BUFF_FMT_GT(r, nr)                                           \
  int32_t fmt_GT_offsets[(nr) + 1];                                      \
  int fmt_GT[(nr)*2];                                                    \
  REQUIRE(                                                               \
      tiledb_vcf_reader_set_buffer_values(                               \
          (reader), "fmt_GT", sizeof(fmt_GT), fmt_GT) == TILEDB_VCF_OK); \
  REQUIRE(                                                               \
      tiledb_vcf_reader_set_buffer_offsets(                              \
          (reader), "fmt_GT", sizeof(fmt_GT_offsets), fmt_GT_offsets) == \
      TILEDB_VCF_OK);

#define SET_BUFF_FMT_DP(r, nr)             \
  int fmt_DP[(nr)];                        \
  REQUIRE(                                 \
      tiledb_vcf_reader_set_buffer_values( \
          (reader), "fmt_DP", sizeof(fmt_DP), fmt_DP) == TILEDB_VCF_OK);

/* ********************************* */
/*               TESTS               */
/* ********************************* */

TEST_CASE("C API: Reader allocation", "[capi][reader]") {
  tiledb_vcf_reader_t* reader = nullptr;
  REQUIRE(tiledb_vcf_reader_alloc(&reader) == TILEDB_VCF_OK);
  REQUIRE(reader != nullptr);
  tiledb_vcf_reader_free(&reader);
  REQUIRE(reader == nullptr);
}

TEST_CASE("C API: Reader initialization", "[capi][reader]") {
  tiledb_vcf_reader_t* reader = nullptr;
  REQUIRE(tiledb_vcf_reader_alloc(&reader) == TILEDB_VCF_OK);

  SECTION("- Nonexistent array") {
    REQUIRE(tiledb_vcf_reader_init(reader, "abc") == TILEDB_VCF_ERR);
  }

  SECTION("- Existing sample") {
    std::string dataset_uri;
    SECTION("- V2") {
      dataset_uri = INPUT_ARRAYS_DIR_V2 + "/ingested_2samples";
    }

    SECTION("- V3") {
      dataset_uri = INPUT_ARRAYS_DIR_V3 + "/ingested_2samples";
    }

    REQUIRE(
        tiledb_vcf_reader_init(reader, dataset_uri.c_str()) == TILEDB_VCF_OK);
  }

  tiledb_vcf_reader_free(&reader);
}

TEST_CASE("C API: Reader set config", "[capi][query]") {
  tiledb_vcf_reader_t* reader = nullptr;
  REQUIRE(tiledb_vcf_reader_alloc(&reader) == TILEDB_VCF_OK);

  SECTION("- Empty string") {
    const char* config = "";
    REQUIRE(
        tiledb_vcf_reader_set_tiledb_config(reader, config) == TILEDB_VCF_OK);
  }

  SECTION("- Valid options") {
    const char* config =
        "sm.num_reader_threads=4, vfs.s3.proxy_host=abc.def.ghi";
    REQUIRE(
        tiledb_vcf_reader_set_tiledb_config(reader, config) == TILEDB_VCF_OK);
  }

  SECTION("- Invalid format") {
    const char* config =
        "sm.num_reader_threads=4 vfs.s3.proxy_host=abc.def.ghi";
    REQUIRE(
        tiledb_vcf_reader_set_tiledb_config(reader, config) == TILEDB_VCF_ERR);

    const char* config2 = "sm.num_reader_threads 4";
    REQUIRE(
        tiledb_vcf_reader_set_tiledb_config(reader, config2) == TILEDB_VCF_ERR);
  }

  SECTION("- TBB options") {
    const char* config = "sm.num_tbb_threads=4";
    REQUIRE(
        tiledb_vcf_reader_set_tiledb_config(reader, config) == TILEDB_VCF_OK);
  }

  tiledb_vcf_reader_free(&reader);
}

TEST_CASE("C API: Reader set tbb and memory", "[capi][query]") {
  tiledb_vcf_reader_t* reader = nullptr;
  REQUIRE(tiledb_vcf_reader_alloc(&reader) == TILEDB_VCF_OK);

  // Setting the memory budget first should not cause any error
  REQUIRE(tiledb_vcf_reader_set_memory_budget(reader, 100) == TILEDB_VCF_OK);

  const char* config = "sm.num_tbb_threads=4";
  REQUIRE(tiledb_vcf_reader_set_tiledb_config(reader, config) == TILEDB_VCF_OK);

  tiledb_vcf_reader_free(&reader);
}

TEST_CASE("C API: Reader get attributes", "[capi][reader]") {
  tiledb_vcf_reader_t* reader = nullptr;
  REQUIRE(tiledb_vcf_reader_alloc(&reader) == TILEDB_VCF_OK);

  std::string dataset_uri;
  SECTION("- V2") {
    dataset_uri = INPUT_ARRAYS_DIR_V2 + "/ingested_2samples";
  }

  SECTION("- V3") {
    dataset_uri = INPUT_ARRAYS_DIR_V3 + "/ingested_2samples";
  }

  REQUIRE(tiledb_vcf_reader_init(reader, dataset_uri.c_str()) == TILEDB_VCF_OK);

  int32_t count = 0;
  REQUIRE(
      tiledb_vcf_reader_get_queryable_attribute_count(reader, &count) ==
      TILEDB_VCF_OK);

  REQUIRE(count > 0);

  for (int32_t i = 0; i < count; i++) {
    char* attribute_name;
    REQUIRE(
        tiledb_vcf_reader_get_queryable_attribute_name(
            reader, i, &attribute_name) == TILEDB_VCF_OK);
    REQUIRE(attribute_name != nullptr);
  }

  count = 0;
  REQUIRE(
      tiledb_vcf_reader_get_fmt_attribute_count(reader, &count) ==
      TILEDB_VCF_OK);

  REQUIRE(count > 0);

  for (int32_t i = 0; i < count; i++) {
    char* attribute_name;
    REQUIRE(
        tiledb_vcf_reader_get_fmt_attribute_name(reader, i, &attribute_name) ==
        TILEDB_VCF_OK);
    REQUIRE(attribute_name != nullptr);
  }

  count = 0;
  REQUIRE(
      tiledb_vcf_reader_get_info_attribute_count(reader, &count) ==
      TILEDB_VCF_OK);

  REQUIRE(count > 0);

  for (int32_t i = 0; i < count; i++) {
    char* attribute_name;
    REQUIRE(
        tiledb_vcf_reader_get_info_attribute_name(reader, i, &attribute_name) ==
        TILEDB_VCF_OK);
    REQUIRE(attribute_name != nullptr);
  }

  tiledb_vcf_reader_free(&reader);
}

TEST_CASE("C API: Reader set regions", "[capi][reader]") {
  tiledb_vcf_reader_t* reader = nullptr;
  REQUIRE(tiledb_vcf_reader_alloc(&reader) == TILEDB_VCF_OK);

  std::string dataset_uri;
  SECTION("- V2") {
    dataset_uri = INPUT_ARRAYS_DIR_V2 + "/ingested_2samples";
  }

  SECTION("- V3") {
    dataset_uri = INPUT_ARRAYS_DIR_V3 + "/ingested_2samples";
  }

  REQUIRE(tiledb_vcf_reader_init(reader, dataset_uri.c_str()) == TILEDB_VCF_OK);

  // Empty string is ok
  const char* regions = "";
  REQUIRE(tiledb_vcf_reader_set_regions(reader, regions) == TILEDB_VCF_OK);

  // CSV list
  const char* regions2 = "chr1:100-200,chr2:1-100";
  REQUIRE(tiledb_vcf_reader_set_regions(reader, regions2) == TILEDB_VCF_OK);

  tiledb_vcf_reader_free(&reader);
}

TEST_CASE("C API: Reader set BED file", "[capi][reader]") {
  tiledb_vcf_reader_t* reader = nullptr;
  REQUIRE(tiledb_vcf_reader_alloc(&reader) == TILEDB_VCF_OK);

  std::string dataset_uri;
  SECTION("- V2") {
    dataset_uri = INPUT_ARRAYS_DIR_V2 + "/ingested_2samples";
  }

  SECTION("- V3") {
    dataset_uri = INPUT_ARRAYS_DIR_V3 + "/ingested_2samples";
  }

  const char* all_samples = "HG01762,HG00280";
  REQUIRE(tiledb_vcf_reader_init(reader, dataset_uri.c_str()) == TILEDB_VCF_OK);

  REQUIRE(
      tiledb_vcf_reader_set_bed_file(reader, "file:///does/not/exist.bed") ==
      TILEDB_VCF_ERR);

  auto bed_uri = TILEDB_VCF_TEST_INPUT_DIR + std::string("/simple.bed");
  REQUIRE(
      tiledb_vcf_reader_set_bed_file(reader, bed_uri.c_str()) == TILEDB_VCF_OK);

  tiledb_vcf_reader_free(&reader);
}

TEST_CASE("C API: Reader set buffers", "[capi][reader]") {
  tiledb_vcf_reader_t* reader = nullptr;
  REQUIRE(tiledb_vcf_reader_alloc(&reader) == TILEDB_VCF_OK);

  std::string dataset_uri;
  SECTION("- V2") {
    dataset_uri = INPUT_ARRAYS_DIR_V2 + "/ingested_2samples";
  }

  SECTION("- V3") {
    dataset_uri = INPUT_ARRAYS_DIR_V3 + "/ingested_2samples";
  }

  const char* all_samples = "HG01762,HG00280";
  REQUIRE(tiledb_vcf_reader_init(reader, dataset_uri.c_str()) == TILEDB_VCF_OK);

  int32_t offsets[10];
  uint32_t pos_start[10];

  // Error for offsets on fixed-len attr
  REQUIRE(
      tiledb_vcf_reader_set_buffer_offsets(
          reader, "pos_start", sizeof(offsets), offsets) == TILEDB_VCF_ERR);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_values(
          reader, "pos_start", sizeof(pos_start), pos_start) == TILEDB_VCF_OK);

  int32_t contig_offsets[10];
  char contigs[100];

  // Error for null offsets on var-len attr
  REQUIRE(
      tiledb_vcf_reader_set_buffer_offsets(reader, "contig", 0, nullptr) ==
      TILEDB_VCF_ERR);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_values(
          reader, "contig", sizeof(contigs), contigs) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_offsets(
          reader, "contig", sizeof(contig_offsets), contig_offsets) ==
      TILEDB_VCF_OK);

  // Error on null data buffer
  REQUIRE(
      tiledb_vcf_reader_set_buffer_values(reader, "pos_start", 0, nullptr) ==
      TILEDB_VCF_ERR);

  tiledb_vcf_reader_free(&reader);
}

TEST_CASE("C API: Reader submit (default attributes)", "[capi][reader]") {
  tiledb_vcf_reader_t* reader = nullptr;
  REQUIRE(tiledb_vcf_reader_alloc(&reader) == TILEDB_VCF_OK);

  unsigned version;
  SECTION("- V2") {
    version = 2;
  }

  SECTION("- V3") {
    version = 3;
  }

  std::string dataset_uri = version == 2 ?
                                INPUT_ARRAYS_DIR_V2 + "/ingested_2samples" :
                                INPUT_ARRAYS_DIR_V3 + "/ingested_2samples";

  REQUIRE(tiledb_vcf_reader_init(reader, dataset_uri.c_str()) == TILEDB_VCF_OK);

  // Set up samples and ranges
  const char* all_samples = "HG01762,HG00280";
  REQUIRE(tiledb_vcf_reader_set_samples(reader, all_samples) == TILEDB_VCF_OK);
  const char* ranges = "1:12100-13360,1:13500-17350";
  REQUIRE(tiledb_vcf_reader_set_regions(reader, ranges) == TILEDB_VCF_OK);

  // Allocate and set buffers
  const unsigned expected_num_records = 10;
  SET_BUFF_POS_START(reader, expected_num_records);
  SET_BUFF_POS_END(reader, expected_num_records);
  SET_BUFF_QUERY_BED_START(reader, expected_num_records);
  SET_BUFF_QUERY_BED_END(reader, expected_num_records);
  SET_BUFF_SAMPLE_NAME(reader, expected_num_records);
  SET_BUFF_CONTIG(reader, expected_num_records);
  SET_BUFF_ALLELES(reader, expected_num_records);
  SET_BUFF_FILTERS(reader, expected_num_records);
  SET_BUFF_INFO(reader, expected_num_records);
  SET_BUFF_FORMAT(reader, expected_num_records);
  SET_BUFF_FMT_GT(reader, expected_num_records);

  int64_t num_records = ~0;
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 0);

  tiledb_vcf_read_status_t status;
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_UNINITIALIZED);

  // Submit query
  REQUIRE(tiledb_vcf_reader_read(reader) == TILEDB_VCF_OK);

  // Check status
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_COMPLETED);

  // Check result size
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == expected_num_records);

  // Check a few buffer sizes
  int64_t num_offsets, num_data_elements, num_data_bytes;
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "pos_start",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_data_bytes == expected_num_records * sizeof(uint32_t));
  REQUIRE(num_offsets == 0);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "sample_name",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_data_bytes == 70);
  REQUIRE(num_offsets == (expected_num_records + 1));
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "alleles",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_data_bytes == 100);
  REQUIRE(num_offsets == (2 * expected_num_records + 1));

  // Check results
  REQUIRE(pos_start[0] == 12141);
  REQUIRE(pos_end[0] == 12277);
  REQUIRE(sample_name_offsets[0] == 0);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[0]], 7) == 0);
  REQUIRE(strncmp("1", &contig[contig_offsets[0]], 1) == 0);
  REQUIRE(
      strncmp("C", &alleles[alleles_offsets[alleles_list_offsets[0]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[0] + 1]],
          9) == 0);
  REQUIRE((filters_bitmap[0] & ((uint8_t)1 << 0)) == 0);
  REQUIRE(filters_offsets[0] == 0);
  REQUIRE(fmt_GT_offsets[0] == 0);
  REQUIRE(fmt_GT[fmt_GT_offsets[0]] == 0);
  REQUIRE(fmt_GT[fmt_GT_offsets[0] + 1] == 0);
  REQUIRE(info_offsets[0] == 0);
  REQUIRE(format_offsets[0] == 0);
  REQUIRE(query_bed_start[0] == 12099);
  REQUIRE(query_bed_end[0] == 13360);

  REQUIRE(pos_start[1] == 12141);
  REQUIRE(pos_end[1] == 12277);
  REQUIRE(sample_name_offsets[1] == 7);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[1]], 7) == 0);
  REQUIRE(strncmp("1", &contig[contig_offsets[1]], 1) == 0);
  REQUIRE(
      strncmp("C", &alleles[alleles_offsets[alleles_list_offsets[1]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[1] + 1]],
          9) == 0);
  REQUIRE((filters_bitmap[0] & ((uint8_t)1 << 1)) == 0);
  REQUIRE(filters_offsets[1] == 0);
  REQUIRE(fmt_GT_offsets[1] == 2);
  REQUIRE(fmt_GT[fmt_GT_offsets[1]] == 0);
  REQUIRE(fmt_GT[fmt_GT_offsets[1] + 1] == 0);
  REQUIRE(info_offsets[1] == 4);
  REQUIRE(format_offsets[1] == 95);
  REQUIRE(query_bed_start[1] == 12099);
  REQUIRE(query_bed_end[1] == 13360);

  REQUIRE(pos_start[2] == 12546);
  REQUIRE(pos_end[2] == 12771);
  REQUIRE(sample_name_offsets[2] == 14);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[2]], 7) == 0);
  REQUIRE(strncmp("1", &contig[contig_offsets[2]], 1) == 0);
  REQUIRE(
      strncmp("G", &alleles[alleles_offsets[alleles_list_offsets[2]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[2] + 1]],
          9) == 0);
  REQUIRE((filters_bitmap[0] & ((uint8_t)1 << 2)) == 0);
  REQUIRE(filters_offsets[2] == 0);
  REQUIRE(fmt_GT_offsets[2] == 4);
  REQUIRE(fmt_GT[fmt_GT_offsets[2]] == 0);
  REQUIRE(fmt_GT[fmt_GT_offsets[2] + 1] == 0);
  REQUIRE(info_offsets[2] == 8);
  REQUIRE(format_offsets[2] == 190);
  REQUIRE(query_bed_start[2] == 12099);
  REQUIRE(query_bed_end[2] == 13360);

  REQUIRE(pos_start[3] == 12546);
  REQUIRE(pos_end[3] == 12771);
  REQUIRE(sample_name_offsets[3] == 21);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[3]], 7) == 0);
  REQUIRE(strncmp("1", &contig[contig_offsets[3]], 1) == 0);
  REQUIRE(
      strncmp("G", &alleles[alleles_offsets[alleles_list_offsets[3]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[3] + 1]],
          9) == 0);
  REQUIRE((filters_bitmap[0] & ((uint8_t)1 << 3)) == 0);
  REQUIRE(filters_offsets[3] == 0);
  REQUIRE(fmt_GT_offsets[3] == 6);
  REQUIRE(fmt_GT[fmt_GT_offsets[3]] == 0);
  REQUIRE(fmt_GT[fmt_GT_offsets[3] + 1] == 0);
  REQUIRE(info_offsets[3] == 12);
  REQUIRE(format_offsets[3] == 285);
  REQUIRE(query_bed_start[3] == 12099);
  REQUIRE(query_bed_end[3] == 13360);

  REQUIRE(pos_start[4] == 13354);
  REQUIRE(pos_end[4] == 13374);
  REQUIRE(sample_name_offsets[4] == 28);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[4]], 7) == 0);
  REQUIRE(strncmp("1", &contig[contig_offsets[4]], 1) == 0);
  REQUIRE(
      strncmp("T", &alleles[alleles_offsets[alleles_list_offsets[4]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[4] + 1]],
          9) == 0);

  REQUIRE((filters_bitmap[0] & ((uint8_t)1 << 4)) != 0);
  REQUIRE(filters_offsets[4] == 0);
  REQUIRE(fmt_GT_offsets[4] == 8);
  REQUIRE(fmt_GT[fmt_GT_offsets[4]] == 0);
  REQUIRE(fmt_GT[fmt_GT_offsets[4] + 1] == 0);
  REQUIRE(info_offsets[4] == 16);
  REQUIRE(format_offsets[4] == 380);
  REQUIRE(query_bed_start[4] == 12099);
  REQUIRE(query_bed_end[4] == 13360);

  REQUIRE(pos_start[5] == 13354);
  REQUIRE(pos_end[5] == 13389);
  REQUIRE(sample_name_offsets[5] == 35);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[5]], 7) == 0);
  REQUIRE(strncmp("1", &contig[contig_offsets[5]], 1) == 0);
  REQUIRE(
      strncmp("T", &alleles[alleles_offsets[alleles_list_offsets[5]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[5] + 1]],
          9) == 0);
  REQUIRE((filters_bitmap[0] & ((uint8_t)1 << 5)) == 0);
  REQUIRE(filters_offsets[5] == 7);
  REQUIRE(fmt_GT_offsets[5] == 10);
  REQUIRE(fmt_GT[fmt_GT_offsets[5]] == 0);
  REQUIRE(fmt_GT[fmt_GT_offsets[5] + 1] == 0);
  REQUIRE(info_offsets[5] == 20);
  REQUIRE(format_offsets[5] == 475);
  REQUIRE(query_bed_start[5] == 12099);
  REQUIRE(query_bed_end[5] == 13360);

  REQUIRE(pos_start[6] == 13452);
  REQUIRE(pos_end[6] == 13519);
  REQUIRE(sample_name_offsets[6] == 42);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[6]], 7) == 0);
  REQUIRE(strncmp("1", &contig[contig_offsets[6]], 1) == 0);
  REQUIRE(
      strncmp("G", &alleles[alleles_offsets[alleles_list_offsets[6]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[6] + 1]],
          9) == 0);
  REQUIRE((filters_bitmap[0] & ((uint8_t)1 << 6)) == 0);
  REQUIRE(filters_offsets[6] == 7);
  REQUIRE(fmt_GT_offsets[6] == 12);
  REQUIRE(fmt_GT[fmt_GT_offsets[6]] == 0);
  REQUIRE(fmt_GT[fmt_GT_offsets[6] + 1] == 0);
  REQUIRE(info_offsets[6] == 24);
  REQUIRE(format_offsets[6] == 570);
  REQUIRE(query_bed_start[6] == 13499);
  REQUIRE(query_bed_end[6] == 17350);

  REQUIRE(pos_start[7] == 13520);
  REQUIRE(pos_end[7] == 13544);
  REQUIRE(sample_name_offsets[7] == 49);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[7]], 7) == 0);
  REQUIRE(strncmp("1", &contig[contig_offsets[7]], 1) == 0);
  REQUIRE(
      strncmp("G", &alleles[alleles_offsets[alleles_list_offsets[7]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[7] + 1]],
          9) == 0);
  REQUIRE((filters_bitmap[0] & ((uint8_t)1 << 7)) == 0);
  REQUIRE(filters_offsets[7] == 7);
  REQUIRE(fmt_GT_offsets[7] == 14);
  REQUIRE(fmt_GT[fmt_GT_offsets[7]] == 0);
  REQUIRE(fmt_GT[fmt_GT_offsets[7] + 1] == 0);
  REQUIRE(info_offsets[7] == 28);
  REQUIRE(format_offsets[7] == 665);
  REQUIRE(query_bed_start[7] == 13499);
  REQUIRE(query_bed_end[7] == 17350);

  REQUIRE(pos_start[8] == 13545);
  REQUIRE(pos_end[8] == 13689);
  REQUIRE(sample_name_offsets[8] == 56);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[8]], 7) == 0);
  REQUIRE(strncmp("1", &contig[contig_offsets[8]], 1) == 0);
  REQUIRE(
      strncmp("G", &alleles[alleles_offsets[alleles_list_offsets[8]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[8] + 1]],
          9) == 0);
  REQUIRE((filters_bitmap[1] & ((uint8_t)1 << 0)) == 0);
  REQUIRE(filters_offsets[8] == 7);
  REQUIRE(fmt_GT_offsets[8] == 16);
  REQUIRE(fmt_GT[fmt_GT_offsets[8]] == 0);
  REQUIRE(fmt_GT[fmt_GT_offsets[8] + 1] == 0);
  REQUIRE(info_offsets[8] == 32);
  REQUIRE(format_offsets[8] == 760);
  REQUIRE(query_bed_start[8] == 13499);
  REQUIRE(query_bed_end[8] == 17350);

  REQUIRE(pos_start[9] == 17319);
  REQUIRE(pos_end[9] == 17479);
  REQUIRE(sample_name_offsets[9] == 63);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[9]], 7) == 0);
  REQUIRE(strncmp("1", &contig[contig_offsets[9]], 1) == 0);
  REQUIRE(
      strncmp("T", &alleles[alleles_offsets[alleles_list_offsets[9]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[9] + 1]],
          9) == 0);
  REQUIRE((filters_bitmap[1] & ((uint8_t)1 << 1)) == 0);
  REQUIRE(filters_offsets[9] == 7);
  REQUIRE(fmt_GT_offsets[9] == 18);
  REQUIRE(fmt_GT[fmt_GT_offsets[9]] == 0);
  REQUIRE(fmt_GT[fmt_GT_offsets[9] + 1] == 0);
  REQUIRE(info_offsets[9] == 36);
  REQUIRE(format_offsets[9] == 855);
  REQUIRE(query_bed_start[9] == 13499);
  REQUIRE(query_bed_end[9] == 17350);

  // Check final offsets are equal to data size
  REQUIRE(sample_name_offsets[10] == 70);
  REQUIRE(filters_offsets[10] == 7);
  REQUIRE(fmt_GT_offsets[10] == 20);
  REQUIRE(info_offsets[10] == 40);
  REQUIRE(format_offsets[10] == 950);

  tiledb_vcf_reader_free(&reader);
}

TEST_CASE("C API: Reader submit (optional attributes)", "[capi][reader]") {
  tiledb_vcf_reader_t* reader = nullptr;
  REQUIRE(tiledb_vcf_reader_alloc(&reader) == TILEDB_VCF_OK);

  unsigned version;
  SECTION("- V2") {
    version = 2;
  }

  SECTION("- V3") {
    version = 3;
  }

  std::string dataset_uri =
      version == 2 ? INPUT_ARRAYS_DIR_V2 + "/ingested_2samples_GT_DP_PL" :
                     INPUT_ARRAYS_DIR_V3 + "/ingested_2samples_GT_DP_PL";

  REQUIRE(tiledb_vcf_reader_init(reader, dataset_uri.c_str()) == TILEDB_VCF_OK);

  // Set up samples and ranges
  const char* all_samples = "HG01762,HG00280";
  REQUIRE(tiledb_vcf_reader_set_samples(reader, all_samples) == TILEDB_VCF_OK);
  const char* regions = "1:12100-13360,1:13500-17350";
  REQUIRE(tiledb_vcf_reader_set_regions(reader, regions) == TILEDB_VCF_OK);

  // Allocate and set buffers
  const unsigned expected_num_records = 10;
  SET_BUFF_POS_START(reader, expected_num_records);
  SET_BUFF_POS_END(reader, expected_num_records);
  SET_BUFF_QUERY_BED_START(reader, expected_num_records);
  SET_BUFF_QUERY_BED_END(reader, expected_num_records);
  SET_BUFF_SAMPLE_NAME(reader, expected_num_records);
  SET_BUFF_CONTIG(reader, expected_num_records);
  SET_BUFF_ALLELES(reader, expected_num_records);
  SET_BUFF_FILTERS(reader, expected_num_records);
  SET_BUFF_INFO(reader, expected_num_records);
  SET_BUFF_FORMAT(reader, expected_num_records);
  SET_BUFF_FMT_GT(reader, expected_num_records);
  SET_BUFF_FMT_DP(reader, expected_num_records);

  int32_t pl_offsets[expected_num_records + 1];
  int pl[expected_num_records * 3];
  REQUIRE(
      tiledb_vcf_reader_set_buffer_values(reader, "fmt_PL", sizeof(pl), pl) ==
      TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_offsets(
          reader, "fmt_PL", sizeof(pl_offsets), pl_offsets) == TILEDB_VCF_OK);

  int64_t num_records = ~0;
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 0);

  tiledb_vcf_read_status_t status;
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_UNINITIALIZED);

  // Submit query
  REQUIRE(tiledb_vcf_reader_read(reader) == TILEDB_VCF_OK);

  // Check status
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_COMPLETED);

  // Check result size
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == expected_num_records);

  // Check results
  REQUIRE(pos_start[0] == 12141);
  REQUIRE(pos_end[0] == 12277);
  REQUIRE(sample_name_offsets[0] == 0);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[0]], 7) == 0);
  REQUIRE(strncmp("1", &contig[contig_offsets[0]], 1) == 0);
  REQUIRE(
      strncmp("C", &alleles[alleles_offsets[alleles_list_offsets[0]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[0] + 1]],
          9) == 0);
  REQUIRE((filters_bitmap[0] & ((uint8_t)1 << 4)) != 0);
  REQUIRE(filters_offsets[0] == 0);
  REQUIRE(fmt_GT_offsets[0] == 0);
  REQUIRE(fmt_GT[fmt_GT_offsets[0]] == 0);
  REQUIRE(fmt_GT[fmt_GT_offsets[0] + 1] == 0);
  REQUIRE(info_offsets[0] == 0);
  REQUIRE(format_offsets[0] == 0);
  REQUIRE(query_bed_start[0] == 12099);
  REQUIRE(query_bed_end[0] == 13360);
  REQUIRE(fmt_DP[0] == 0);
  REQUIRE(pl[pl_offsets[0]] == 0);
  REQUIRE(pl[pl_offsets[0] + 1] == 0);
  REQUIRE(pl[pl_offsets[0] + 2] == 0);

  REQUIRE(pos_start[1] == 12141);
  REQUIRE(pos_end[1] == 12277);
  REQUIRE(sample_name_offsets[1] == 7);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[1]], 7) == 0);
  REQUIRE(strncmp("1", &contig[contig_offsets[1]], 1) == 0);
  REQUIRE(
      strncmp("C", &alleles[alleles_offsets[alleles_list_offsets[1]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[1] + 1]],
          9) == 0);
  REQUIRE((filters_bitmap[0] & ((uint8_t)1 << 1)) == 0);
  REQUIRE(filters_offsets[1] == 0);
  REQUIRE(fmt_GT_offsets[1] == 2);
  REQUIRE(fmt_GT[fmt_GT_offsets[1]] == 0);
  REQUIRE(fmt_GT[fmt_GT_offsets[1] + 1] == 0);
  REQUIRE(info_offsets[1] == 4);
  REQUIRE(format_offsets[1] == 38);
  REQUIRE(query_bed_start[1] == 12099);
  REQUIRE(query_bed_end[1] == 13360);
  REQUIRE(fmt_DP[1] == 0);
  REQUIRE(pl[pl_offsets[1]] == 0);
  REQUIRE(pl[pl_offsets[1] + 1] == 0);
  REQUIRE(pl[pl_offsets[1] + 2] == 0);

  REQUIRE(pos_start[2] == 12546);
  REQUIRE(pos_end[2] == 12771);
  REQUIRE(sample_name_offsets[2] == 14);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[2]], 7) == 0);
  REQUIRE(strncmp("1", &contig[contig_offsets[2]], 1) == 0);
  REQUIRE(
      strncmp("G", &alleles[alleles_offsets[alleles_list_offsets[2]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[2] + 1]],
          9) == 0);
  REQUIRE((filters_bitmap[0] & ((uint8_t)1 << 2)) == 0);
  REQUIRE(filters_offsets[2] == 0);
  REQUIRE(fmt_GT_offsets[2] == 4);
  REQUIRE(fmt_GT[fmt_GT_offsets[2]] == 0);
  REQUIRE(fmt_GT[fmt_GT_offsets[2] + 1] == 0);
  REQUIRE(info_offsets[2] == 8);
  REQUIRE(format_offsets[2] == 76);
  REQUIRE(query_bed_start[2] == 12099);
  REQUIRE(query_bed_end[2] == 13360);
  REQUIRE(fmt_DP[2] == 0);
  REQUIRE(pl[pl_offsets[2]] == 0);
  REQUIRE(pl[pl_offsets[2] + 1] == 0);
  REQUIRE(pl[pl_offsets[2] + 2] == 0);

  REQUIRE(pos_start[3] == 12546);
  REQUIRE(pos_end[3] == 12771);
  REQUIRE(sample_name_offsets[3] == 21);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[3]], 7) == 0);
  REQUIRE(strncmp("1", &contig[contig_offsets[3]], 1) == 0);
  REQUIRE(
      strncmp("G", &alleles[alleles_offsets[alleles_list_offsets[3]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[3] + 1]],
          9) == 0);
  REQUIRE((filters_bitmap[0] & ((uint8_t)1 << 3)) == 0);
  REQUIRE(filters_offsets[3] == 0);
  REQUIRE(fmt_GT_offsets[3] == 6);
  REQUIRE(fmt_GT[fmt_GT_offsets[3]] == 0);
  REQUIRE(fmt_GT[fmt_GT_offsets[3] + 1] == 0);
  REQUIRE(info_offsets[3] == 12);
  REQUIRE(format_offsets[3] == 114);
  REQUIRE(query_bed_start[3] == 12099);
  REQUIRE(query_bed_end[3] == 13360);
  REQUIRE(fmt_DP[3] == 0);
  REQUIRE(pl[pl_offsets[3]] == 0);
  REQUIRE(pl[pl_offsets[3] + 1] == 0);
  REQUIRE(pl[pl_offsets[3] + 2] == 0);

  REQUIRE(pos_start[4] == 13354);
  REQUIRE(pos_end[4] == 13374);
  REQUIRE(sample_name_offsets[4] == 28);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[4]], 7) == 0);
  REQUIRE(strncmp("1", &contig[contig_offsets[4]], 1) == 0);
  REQUIRE(
      strncmp("T", &alleles[alleles_offsets[alleles_list_offsets[4]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[4] + 1]],
          9) == 0);
  REQUIRE((filters_bitmap[0] & ((uint8_t)1 << 4)) != 0);
  REQUIRE(filters_offsets[4] == 0);
  REQUIRE(fmt_GT_offsets[4] == 8);
  REQUIRE(fmt_GT[fmt_GT_offsets[4]] == 0);
  REQUIRE(fmt_GT[fmt_GT_offsets[4] + 1] == 0);
  REQUIRE(info_offsets[4] == 16);
  REQUIRE(format_offsets[4] == 152);
  REQUIRE(query_bed_start[4] == 12099);
  REQUIRE(query_bed_end[4] == 13360);
  REQUIRE(fmt_DP[4] == 15);
  REQUIRE(pl[pl_offsets[4]] == 0);
  REQUIRE(pl[pl_offsets[4] + 1] == 24);
  REQUIRE(pl[pl_offsets[4] + 2] == 360);

  REQUIRE(pos_start[5] == 13354);
  REQUIRE(pos_end[5] == 13389);
  REQUIRE(sample_name_offsets[5] == 35);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[5]], 7) == 0);
  REQUIRE(strncmp("1", &contig[contig_offsets[5]], 1) == 0);
  REQUIRE(
      strncmp("T", &alleles[alleles_offsets[alleles_list_offsets[5]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[5] + 1]],
          9) == 0);
  REQUIRE((filters_bitmap[0] & ((uint8_t)1 << 5)) == 0);
  REQUIRE(filters_offsets[5] == 7);
  REQUIRE(fmt_GT_offsets[5] == 10);
  REQUIRE(fmt_GT[fmt_GT_offsets[5]] == 0);
  REQUIRE(fmt_GT[fmt_GT_offsets[5] + 1] == 0);
  REQUIRE(info_offsets[5] == 20);
  REQUIRE(format_offsets[5] == 190);
  REQUIRE(query_bed_start[5] == 12099);
  REQUIRE(query_bed_end[5] == 13360);
  REQUIRE(fmt_DP[5] == 64);
  REQUIRE(pl[pl_offsets[5]] == 0);
  REQUIRE(pl[pl_offsets[5] + 1] == 66);
  REQUIRE(pl[pl_offsets[5] + 2] == 990);

  REQUIRE(pos_start[6] == 13452);
  REQUIRE(pos_end[6] == 13519);
  REQUIRE(sample_name_offsets[6] == 42);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[6]], 7) == 0);
  REQUIRE(strncmp("1", &contig[contig_offsets[6]], 1) == 0);
  REQUIRE(
      strncmp("G", &alleles[alleles_offsets[alleles_list_offsets[6]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[6] + 1]],
          9) == 0);
  REQUIRE((filters_bitmap[0] & ((uint8_t)1 << 6)) == 0);
  REQUIRE(filters_offsets[6] == 7);
  REQUIRE(fmt_GT_offsets[6] == 12);
  REQUIRE(fmt_GT[fmt_GT_offsets[6]] == 0);
  REQUIRE(fmt_GT[fmt_GT_offsets[6] + 1] == 0);
  REQUIRE(info_offsets[6] == 24);
  REQUIRE(format_offsets[6] == 228);
  REQUIRE(query_bed_start[6] == 13499);
  REQUIRE(query_bed_end[6] == 17350);
  REQUIRE(fmt_DP[6] == 10);
  REQUIRE(pl[pl_offsets[6]] == 0);
  REQUIRE(pl[pl_offsets[6] + 1] == 21);
  REQUIRE(pl[pl_offsets[6] + 2] == 210);

  REQUIRE(pos_start[7] == 13520);
  REQUIRE(pos_end[7] == 13544);
  REQUIRE(sample_name_offsets[7] == 49);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[7]], 7) == 0);
  REQUIRE(strncmp("1", &contig[contig_offsets[7]], 1) == 0);
  REQUIRE(
      strncmp("G", &alleles[alleles_offsets[alleles_list_offsets[7]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[7] + 1]],
          9) == 0);
  REQUIRE((filters_bitmap[0] & ((uint8_t)1 << 7)) == 0);
  REQUIRE(filters_offsets[7] == 7);
  REQUIRE(fmt_GT_offsets[7] == 14);
  REQUIRE(fmt_GT[fmt_GT_offsets[7]] == 0);
  REQUIRE(fmt_GT[fmt_GT_offsets[7] + 1] == 0);
  REQUIRE(info_offsets[7] == 28);
  REQUIRE(format_offsets[7] == 266);
  REQUIRE(query_bed_start[7] == 13499);
  REQUIRE(query_bed_end[7] == 17350);
  REQUIRE(fmt_DP[7] == 6);
  REQUIRE(pl[pl_offsets[7]] == 0);
  REQUIRE(pl[pl_offsets[7] + 1] == 6);
  REQUIRE(pl[pl_offsets[7] + 2] == 90);

  REQUIRE(pos_start[8] == 13545);
  REQUIRE(pos_end[8] == 13689);
  REQUIRE(sample_name_offsets[8] == 56);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[8]], 7) == 0);
  REQUIRE(strncmp("1", &contig[contig_offsets[8]], 1) == 0);
  REQUIRE(
      strncmp("G", &alleles[alleles_offsets[alleles_list_offsets[8]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[8] + 1]],
          9) == 0);
  REQUIRE((filters_bitmap[1] & ((uint8_t)1 << 0)) == 0);
  REQUIRE(filters_offsets[8] == 7);
  REQUIRE(fmt_GT_offsets[8] == 16);
  REQUIRE(fmt_GT[fmt_GT_offsets[8]] == 0);
  REQUIRE(fmt_GT[fmt_GT_offsets[8] + 1] == 0);
  REQUIRE(info_offsets[8] == 32);
  REQUIRE(format_offsets[8] == 304);
  REQUIRE(query_bed_start[8] == 13499);
  REQUIRE(query_bed_end[8] == 17350);
  REQUIRE(fmt_DP[8] == 0);
  REQUIRE(pl[pl_offsets[8]] == 0);
  REQUIRE(pl[pl_offsets[8] + 1] == 0);
  REQUIRE(pl[pl_offsets[8] + 2] == 0);

  REQUIRE(pos_start[9] == 17319);
  REQUIRE(pos_end[9] == 17479);
  REQUIRE(sample_name_offsets[9] == 63);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[9]], 7) == 0);
  REQUIRE(strncmp("1", &contig[contig_offsets[9]], 1) == 0);
  REQUIRE(
      strncmp("T", &alleles[alleles_offsets[alleles_list_offsets[9]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[9] + 1]],
          9) == 0);
  REQUIRE((filters_bitmap[1] & ((uint8_t)1 << 1)) == 0);
  REQUIRE(filters_offsets[9] == 7);
  REQUIRE(fmt_GT_offsets[9] == 18);
  REQUIRE(fmt_GT[fmt_GT_offsets[9]] == 0);
  REQUIRE(fmt_GT[fmt_GT_offsets[9] + 1] == 0);
  REQUIRE(info_offsets[9] == 36);
  REQUIRE(format_offsets[9] == 342);
  REQUIRE(query_bed_start[9] == 13499);
  REQUIRE(query_bed_end[9] == 17350);
  REQUIRE(fmt_DP[9] == 0);
  REQUIRE(pl[pl_offsets[9]] == 0);
  REQUIRE(pl[pl_offsets[9] + 1] == 0);
  REQUIRE(pl[pl_offsets[9] + 2] == 0);

  tiledb_vcf_reader_free(&reader);
}

TEST_CASE("C API: Reader submit (subselect attributes)", "[capi][reader]") {
  tiledb_vcf_reader_t* reader = nullptr;
  REQUIRE(tiledb_vcf_reader_alloc(&reader) == TILEDB_VCF_OK);

  std::string dataset_uri;
  SECTION("- V2") {
    dataset_uri = INPUT_ARRAYS_DIR_V2 + "/ingested_2samples_GT_DP_PL";
  }

  SECTION("- V3") {
    dataset_uri = INPUT_ARRAYS_DIR_V3 + "/ingested_2samples_GT_DP_PL";
  }

  REQUIRE(tiledb_vcf_reader_init(reader, dataset_uri.c_str()) == TILEDB_VCF_OK);

  // Set up samples and ranges
  const char* samples = "HG01762,HG00280";
  REQUIRE(tiledb_vcf_reader_set_samples(reader, samples) == TILEDB_VCF_OK);
  const char* regions = "1:12100-13360,1:13500-17350";
  REQUIRE(tiledb_vcf_reader_set_regions(reader, regions) == TILEDB_VCF_OK);

  // Allocate and set buffers
  const unsigned expected_num_records = 10;
  SET_BUFF_POS_END(reader, expected_num_records);
  SET_BUFF_SAMPLE_NAME(reader, expected_num_records);
  SET_BUFF_ALLELES(reader, expected_num_records);
  SET_BUFF_FMT_DP(reader, expected_num_records);

  int64_t num_records = ~0;
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 0);

  tiledb_vcf_read_status_t status;
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_UNINITIALIZED);

  // Submit query
  REQUIRE(tiledb_vcf_reader_read(reader) == TILEDB_VCF_OK);

  // Check status
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_COMPLETED);

  // Check result size
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == expected_num_records);

  // Check results
  REQUIRE(pos_end[0] == 12277);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[0]], 7) == 0);
  REQUIRE(
      strncmp("C", &alleles[alleles_offsets[alleles_list_offsets[0]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[0] + 1]],
          9) == 0);
  REQUIRE(fmt_DP[0] == 0);

  REQUIRE(pos_end[1] == 12277);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[1]], 7) == 0);
  REQUIRE(
      strncmp("C", &alleles[alleles_offsets[alleles_list_offsets[1]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[1] + 1]],
          9) == 0);
  REQUIRE(fmt_DP[1] == 0);

  REQUIRE(pos_end[2] == 12771);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[2]], 7) == 0);
  REQUIRE(
      strncmp("G", &alleles[alleles_offsets[alleles_list_offsets[2]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[2] + 1]],
          9) == 0);
  REQUIRE(fmt_DP[2] == 0);

  REQUIRE(pos_end[3] == 12771);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[3]], 7) == 0);
  REQUIRE(
      strncmp("G", &alleles[alleles_offsets[alleles_list_offsets[3]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[3] + 1]],
          9) == 0);
  REQUIRE(fmt_DP[3] == 0);

  REQUIRE(pos_end[4] == 13374);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[4]], 7) == 0);
  REQUIRE(
      strncmp("T", &alleles[alleles_offsets[alleles_list_offsets[4]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[4] + 1]],
          9) == 0);
  REQUIRE(fmt_DP[4] == 15);

  REQUIRE(pos_end[5] == 13389);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[5]], 7) == 0);
  REQUIRE(
      strncmp("T", &alleles[alleles_offsets[alleles_list_offsets[5]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[5] + 1]],
          9) == 0);
  REQUIRE(fmt_DP[5] == 64);

  REQUIRE(pos_end[6] == 13519);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[6]], 7) == 0);
  REQUIRE(
      strncmp("G", &alleles[alleles_offsets[alleles_list_offsets[6]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[6] + 1]],
          9) == 0);
  REQUIRE(fmt_DP[6] == 10);

  REQUIRE(pos_end[7] == 13544);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[7]], 7) == 0);
  REQUIRE(
      strncmp("G", &alleles[alleles_offsets[alleles_list_offsets[7]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[7] + 1]],
          9) == 0);
  REQUIRE(fmt_DP[7] == 6);

  REQUIRE(pos_end[8] == 13689);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[8]], 7) == 0);
  REQUIRE(
      strncmp("G", &alleles[alleles_offsets[alleles_list_offsets[8]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[8] + 1]],
          9) == 0);
  REQUIRE(fmt_DP[8] == 0);

  REQUIRE(pos_end[9] == 17479);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[9]], 7) == 0);
  REQUIRE(
      strncmp("T", &alleles[alleles_offsets[alleles_list_offsets[9]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[9] + 1]],
          9) == 0);
  REQUIRE(fmt_DP[9] == 0);

  tiledb_vcf_reader_free(&reader);
}

TEST_CASE("C API: Reader submit (all samples)", "[capi][reader]") {
  tiledb_vcf_reader_t* reader = nullptr;
  REQUIRE(tiledb_vcf_reader_alloc(&reader) == TILEDB_VCF_OK);
  std::string dataset_uri;
  SECTION("- V2") {
    dataset_uri = INPUT_ARRAYS_DIR_V2 + "/ingested_2samples_GT_DP_PL";
  }

  SECTION("- V3") {
    dataset_uri = INPUT_ARRAYS_DIR_V3 + "/ingested_2samples_GT_DP_PL";
  }
  REQUIRE(tiledb_vcf_reader_init(reader, dataset_uri.c_str()) == TILEDB_VCF_OK);

  // Set up ranges (defaulting to all samples)
  const char* regions = "1:12100-13100";
  REQUIRE(tiledb_vcf_reader_set_regions(reader, regions) == TILEDB_VCF_OK);

  // Allocate and set buffers
  const unsigned expected_num_records = 4;
  SET_BUFF_POS_END(reader, expected_num_records);
  SET_BUFF_SAMPLE_NAME(reader, expected_num_records);
  SET_BUFF_ALLELES(reader, expected_num_records);
  SET_BUFF_FMT_DP(reader, expected_num_records);

  int64_t num_records = ~0;
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 0);

  tiledb_vcf_read_status_t status;
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_UNINITIALIZED);

  // Submit query
  REQUIRE(tiledb_vcf_reader_read(reader) == TILEDB_VCF_OK);

  // Check status
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_COMPLETED);

  // Check result size
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == expected_num_records);

  // Check results
  REQUIRE(pos_end[0] == 12277);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[0]], 7) == 0);
  REQUIRE(
      strncmp("C", &alleles[alleles_offsets[alleles_list_offsets[0]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[0] + 1]],
          9) == 0);
  REQUIRE(fmt_DP[0] == 0);

  REQUIRE(pos_end[1] == 12277);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[1]], 7) == 0);
  REQUIRE(
      strncmp("C", &alleles[alleles_offsets[alleles_list_offsets[1]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[1] + 1]],
          9) == 0);
  REQUIRE(fmt_DP[1] == 0);

  REQUIRE(pos_end[2] == 12771);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[2]], 7) == 0);
  REQUIRE(
      strncmp("G", &alleles[alleles_offsets[alleles_list_offsets[2]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[2] + 1]],
          9) == 0);
  REQUIRE(fmt_DP[2] == 0);

  REQUIRE(pos_end[3] == 12771);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[3]], 7) == 0);
  REQUIRE(
      strncmp("G", &alleles[alleles_offsets[alleles_list_offsets[3]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[3] + 1]],
          9) == 0);
  REQUIRE(fmt_DP[3] == 0);

  tiledb_vcf_reader_free(&reader);
}

TEST_CASE("C API: Reader submit (BED file)", "[capi][reader]") {
  tiledb_vcf_reader_t* reader = nullptr;
  REQUIRE(tiledb_vcf_reader_alloc(&reader) == TILEDB_VCF_OK);
  std::string dataset_uri;
  SECTION("- V2") {
    dataset_uri = INPUT_ARRAYS_DIR_V2 + "/ingested_2samples_GT_DP_PL";
  }

  SECTION("- V3") {
    dataset_uri = INPUT_ARRAYS_DIR_V3 + "/ingested_2samples_GT_DP_PL";
  }
  REQUIRE(tiledb_vcf_reader_init(reader, dataset_uri.c_str()) == TILEDB_VCF_OK);

  // Set up samples and ranges
  auto bed_uri = TILEDB_VCF_TEST_INPUT_DIR + std::string("/simple.bed");
  REQUIRE(
      tiledb_vcf_reader_set_bed_file(reader, bed_uri.c_str()) == TILEDB_VCF_OK);

  // Allocate and set buffers
  const unsigned expected_num_records = 10;
  SET_BUFF_POS_END(reader, expected_num_records);
  SET_BUFF_SAMPLE_NAME(reader, expected_num_records);
  SET_BUFF_ALLELES(reader, expected_num_records);
  SET_BUFF_FMT_DP(reader, expected_num_records);

  int64_t num_records = ~0;
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 0);

  tiledb_vcf_read_status_t status;
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_UNINITIALIZED);

  // Submit query
  REQUIRE(tiledb_vcf_reader_read(reader) == TILEDB_VCF_OK);

  // Check status
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_COMPLETED);

  // Check result size
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == expected_num_records);

  // Check results
  REQUIRE(pos_end[0] == 12277);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[0]], 7) == 0);
  REQUIRE(
      strncmp("C", &alleles[alleles_offsets[alleles_list_offsets[0]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[0] + 1]],
          9) == 0);
  REQUIRE(fmt_DP[0] == 0);

  REQUIRE(pos_end[1] == 12277);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[1]], 7) == 0);
  REQUIRE(
      strncmp("C", &alleles[alleles_offsets[alleles_list_offsets[1]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[1] + 1]],
          9) == 0);
  REQUIRE(fmt_DP[1] == 0);

  REQUIRE(pos_end[2] == 12771);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[2]], 7) == 0);
  REQUIRE(
      strncmp("G", &alleles[alleles_offsets[alleles_list_offsets[2]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[2] + 1]],
          9) == 0);
  REQUIRE(fmt_DP[2] == 0);

  REQUIRE(pos_end[3] == 12771);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[3]], 7) == 0);
  REQUIRE(
      strncmp("G", &alleles[alleles_offsets[alleles_list_offsets[3]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[3] + 1]],
          9) == 0);
  REQUIRE(fmt_DP[3] == 0);

  REQUIRE(pos_end[4] == 13374);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[4]], 7) == 0);
  REQUIRE(
      strncmp("T", &alleles[alleles_offsets[alleles_list_offsets[4]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[4] + 1]],
          9) == 0);
  REQUIRE(fmt_DP[4] == 15);

  REQUIRE(pos_end[5] == 13389);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[5]], 7) == 0);
  REQUIRE(
      strncmp("T", &alleles[alleles_offsets[alleles_list_offsets[5]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[5] + 1]],
          9) == 0);
  REQUIRE(fmt_DP[5] == 64);

  REQUIRE(pos_end[6] == 13519);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[6]], 7) == 0);
  REQUIRE(
      strncmp("G", &alleles[alleles_offsets[alleles_list_offsets[6]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[6] + 1]],
          9) == 0);
  REQUIRE(fmt_DP[6] == 10);

  REQUIRE(pos_end[7] == 13544);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[7]], 7) == 0);
  REQUIRE(
      strncmp("G", &alleles[alleles_offsets[alleles_list_offsets[7]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[7] + 1]],
          9) == 0);
  REQUIRE(fmt_DP[7] == 6);

  REQUIRE(pos_end[8] == 13689);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[8]], 7) == 0);
  REQUIRE(
      strncmp("G", &alleles[alleles_offsets[alleles_list_offsets[8]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[8] + 1]],
          9) == 0);
  REQUIRE(fmt_DP[8] == 0);

  REQUIRE(pos_end[9] == 17479);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[9]], 7) == 0);
  REQUIRE(
      strncmp("T", &alleles[alleles_offsets[alleles_list_offsets[9]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[9] + 1]],
          9) == 0);
  REQUIRE(fmt_DP[9] == 0);

  tiledb_vcf_reader_free(&reader);
}

TEST_CASE("C API: Reader submit (samples file)", "[capi][reader]") {
  tiledb_vcf_reader_t* reader = nullptr;
  REQUIRE(tiledb_vcf_reader_alloc(&reader) == TILEDB_VCF_OK);
  std::string dataset_uri;
  SECTION("- V2") {
    dataset_uri = INPUT_ARRAYS_DIR_V2 + "/ingested_2samples_GT_DP_PL";
  }

  SECTION("- V3") {
    dataset_uri = INPUT_ARRAYS_DIR_V3 + "/ingested_2samples_GT_DP_PL";
  }
  auto sample_uri =
      TILEDB_VCF_TEST_INPUT_DIR + std::string("/sample_names.txt");
  REQUIRE(
      tiledb_vcf_reader_set_samples_file(reader, sample_uri.c_str()) ==
      TILEDB_VCF_OK);
  REQUIRE(tiledb_vcf_reader_init(reader, dataset_uri.c_str()) == TILEDB_VCF_OK);

  // Set up samples and ranges
  auto bed_uri = TILEDB_VCF_TEST_INPUT_DIR + std::string("/simple.bed");
  REQUIRE(
      tiledb_vcf_reader_set_bed_file(reader, bed_uri.c_str()) == TILEDB_VCF_OK);

  // Allocate and set buffers
  const unsigned expected_num_records = 10;
  SET_BUFF_POS_END(reader, expected_num_records);
  SET_BUFF_SAMPLE_NAME(reader, expected_num_records);
  SET_BUFF_ALLELES(reader, expected_num_records);
  SET_BUFF_FMT_DP(reader, expected_num_records);

  int64_t num_records = ~0;
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 0);

  tiledb_vcf_read_status_t status;
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_UNINITIALIZED);

  // Submit query
  REQUIRE(tiledb_vcf_reader_read(reader) == TILEDB_VCF_OK);

  // Check status
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_COMPLETED);

  // Check result size
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == expected_num_records);

  // Check results
  REQUIRE(pos_end[0] == 12277);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[0]], 7) == 0);
  REQUIRE(
      strncmp("C", &alleles[alleles_offsets[alleles_list_offsets[0]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[0] + 1]],
          9) == 0);
  REQUIRE(fmt_DP[0] == 0);

  REQUIRE(pos_end[1] == 12277);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[1]], 7) == 0);
  REQUIRE(
      strncmp("C", &alleles[alleles_offsets[alleles_list_offsets[1]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[1] + 1]],
          9) == 0);
  REQUIRE(fmt_DP[1] == 0);

  REQUIRE(pos_end[2] == 12771);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[2]], 7) == 0);
  REQUIRE(
      strncmp("G", &alleles[alleles_offsets[alleles_list_offsets[2]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[2] + 1]],
          9) == 0);
  REQUIRE(fmt_DP[2] == 0);

  REQUIRE(pos_end[3] == 12771);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[3]], 7) == 0);
  REQUIRE(
      strncmp("G", &alleles[alleles_offsets[alleles_list_offsets[3]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[3] + 1]],
          9) == 0);
  REQUIRE(fmt_DP[3] == 0);

  REQUIRE(pos_end[4] == 13374);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[4]], 7) == 0);
  REQUIRE(
      strncmp("T", &alleles[alleles_offsets[alleles_list_offsets[4]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[4] + 1]],
          9) == 0);
  REQUIRE(fmt_DP[4] == 15);

  REQUIRE(pos_end[5] == 13389);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[5]], 7) == 0);
  REQUIRE(
      strncmp("T", &alleles[alleles_offsets[alleles_list_offsets[5]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[5] + 1]],
          9) == 0);
  REQUIRE(fmt_DP[5] == 64);

  REQUIRE(pos_end[6] == 13519);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[6]], 7) == 0);
  REQUIRE(
      strncmp("G", &alleles[alleles_offsets[alleles_list_offsets[6]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[6] + 1]],
          9) == 0);
  REQUIRE(fmt_DP[6] == 10);

  REQUIRE(pos_end[7] == 13544);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[7]], 7) == 0);
  REQUIRE(
      strncmp("G", &alleles[alleles_offsets[alleles_list_offsets[7]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[7] + 1]],
          9) == 0);
  REQUIRE(fmt_DP[7] == 6);

  REQUIRE(pos_end[8] == 13689);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[8]], 7) == 0);
  REQUIRE(
      strncmp("G", &alleles[alleles_offsets[alleles_list_offsets[8]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[8] + 1]],
          9) == 0);
  REQUIRE(fmt_DP[8] == 0);

  REQUIRE(pos_end[9] == 17479);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[9]], 7) == 0);
  REQUIRE(
      strncmp("T", &alleles[alleles_offsets[alleles_list_offsets[9]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[9] + 1]],
          9) == 0);
  REQUIRE(fmt_DP[9] == 0);

  tiledb_vcf_reader_free(&reader);
}

TEST_CASE("C API: Reader submit (empty result set)", "[capi][reader]") {
  tiledb_vcf_reader_t* reader = nullptr;
  REQUIRE(tiledb_vcf_reader_alloc(&reader) == TILEDB_VCF_OK);
  std::string dataset_uri;
  SECTION("- V2") {
    dataset_uri = INPUT_ARRAYS_DIR_V2 + "/ingested_2samples_GT_DP_PL";
  }

  SECTION("- V3") {
    dataset_uri = INPUT_ARRAYS_DIR_V3 + "/ingested_2samples_GT_DP_PL";
  }
  REQUIRE(tiledb_vcf_reader_init(reader, dataset_uri.c_str()) == TILEDB_VCF_OK);

  // Set up samples and ranges
  const char* regions = "1:100-1000,1:18000-19000,2:100-1000";
  REQUIRE(tiledb_vcf_reader_set_regions(reader, regions) == TILEDB_VCF_OK);

  int64_t num_records = ~0;
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 0);

  tiledb_vcf_read_status_t status;
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_UNINITIALIZED);

  // Submit query
  REQUIRE(tiledb_vcf_reader_read(reader) == TILEDB_VCF_OK);

  // Check status
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_COMPLETED);

  // Check result size
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 0);

  tiledb_vcf_reader_free(&reader);
}

TEST_CASE(
    "C API: Reader submit (incomplete query)", "[capi][reader][incomplete]") {
  tiledb_vcf_reader_t* reader = nullptr;
  REQUIRE(tiledb_vcf_reader_alloc(&reader) == TILEDB_VCF_OK);
  std::string dataset_uri;
  SECTION("- V2") {
    dataset_uri = INPUT_ARRAYS_DIR_V2 + "/ingested_2samples_GT_DP_PL";
  }

  SECTION("- V3") {
    dataset_uri = INPUT_ARRAYS_DIR_V3 + "/ingested_2samples_GT_DP_PL";
  }
  REQUIRE(tiledb_vcf_reader_init(reader, dataset_uri.c_str()) == TILEDB_VCF_OK);

  // Set up samples and ranges
  const char* regions = "1:12100-13360,1:13500-16000";
  REQUIRE(tiledb_vcf_reader_set_regions(reader, regions) == TILEDB_VCF_OK);

  // Allocate and set buffers
  const unsigned expected_num_records = 9;
  const unsigned alloced_num_records = 2;

  SET_BUFF_POS_START(reader, alloced_num_records);
  SET_BUFF_POS_END(reader, alloced_num_records);
  SET_BUFF_SAMPLE_NAME(reader, alloced_num_records);
  SET_BUFF_QUERY_BED_START(reader, alloced_num_records);
  SET_BUFF_QUERY_BED_END(reader, alloced_num_records);
  SET_BUFF_FMT_DP(reader, alloced_num_records);

  uint64_t tot_num_records = 0;
  int64_t num_records = ~0;
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 0);

  tiledb_vcf_read_status_t status;
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_UNINITIALIZED);

  // Submit initial query
  REQUIRE(tiledb_vcf_reader_read(reader) == TILEDB_VCF_OK);

  // Check status
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_INCOMPLETE);

  // Check incomplete result size
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == alloced_num_records);
  tot_num_records += num_records;

  // Check a few buffer sizes
  int64_t num_offsets, num_data_elements, num_data_bytes;
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "pos_start",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_data_bytes == alloced_num_records * sizeof(uint32_t));
  REQUIRE(num_offsets == 0);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "sample_name",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_data_bytes == 14);
  REQUIRE(num_offsets == (alloced_num_records + 1));

  // Check first results
  REQUIRE(pos_start[0] == 12141);
  REQUIRE(pos_end[0] == 12277);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[0]], 7) == 0);
  REQUIRE(query_bed_start[0] == 12099);
  REQUIRE(query_bed_end[0] == 13360);

  REQUIRE(pos_start[1] == 12141);
  REQUIRE(pos_end[1] == 12277);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[1]], 7) == 0);
  REQUIRE(query_bed_start[1] == 12099);
  REQUIRE(query_bed_end[1] == 13360);

  // Resubmit query
  REQUIRE(tiledb_vcf_reader_read(reader) == TILEDB_VCF_OK);

  // Check status
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_INCOMPLETE);

  // Check incomplete result size
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == alloced_num_records);
  tot_num_records += num_records;

  // Check next results
  REQUIRE(pos_start[0] == 12546);
  REQUIRE(pos_end[0] == 12771);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[0]], 7) == 0);
  REQUIRE(query_bed_start[0] == 12099);
  REQUIRE(query_bed_end[0] == 13360);

  REQUIRE(pos_start[1] == 12546);
  REQUIRE(pos_end[1] == 12771);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[1]], 7) == 0);
  REQUIRE(query_bed_start[1] == 12099);
  REQUIRE(query_bed_end[1] == 13360);

  // Resubmit query
  REQUIRE(tiledb_vcf_reader_read(reader) == TILEDB_VCF_OK);

  // Check status
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_INCOMPLETE);

  // Check incomplete result size
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == alloced_num_records);
  tot_num_records += num_records;

  // Check buffer sizes again
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "pos_start",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_data_bytes == alloced_num_records * sizeof(uint32_t));
  REQUIRE(num_offsets == 0);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "sample_name",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_data_bytes == 14);
  REQUIRE(num_offsets == (alloced_num_records + 1));

  // Check next results
  REQUIRE(pos_start[0] == 13354);
  REQUIRE(pos_end[0] == 13374);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[0]], 7) == 0);
  REQUIRE(query_bed_start[0] == 12099);
  REQUIRE(query_bed_end[0] == 13360);

  REQUIRE(pos_start[1] == 13354);
  REQUIRE(pos_end[1] == 13389);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[1]], 7) == 0);
  REQUIRE(query_bed_start[1] == 12099);
  REQUIRE(query_bed_end[1] == 13360);

  // Resubmit query
  REQUIRE(tiledb_vcf_reader_read(reader) == TILEDB_VCF_OK);

  // Check status
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_INCOMPLETE);

  // Check incomplete result size
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == alloced_num_records);
  tot_num_records += num_records;

  // Check next results
  REQUIRE(pos_start[0] == 13452);
  REQUIRE(pos_end[0] == 13519);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[0]], 7) == 0);
  REQUIRE(query_bed_start[0] == 13499);
  REQUIRE(query_bed_end[0] == 16000);

  REQUIRE(pos_start[1] == 13520);
  REQUIRE(pos_end[1] == 13544);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[1]], 7) == 0);
  REQUIRE(query_bed_start[1] == 13499);
  REQUIRE(query_bed_end[1] == 16000);

  // Resubmit query
  REQUIRE(tiledb_vcf_reader_read(reader) == TILEDB_VCF_OK);

  // Check status (now finished)
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_COMPLETED);

  // Check incomplete result size
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 1);
  tot_num_records += num_records;

  // Check last results
  REQUIRE(pos_start[0] == 13545);
  REQUIRE(pos_end[0] == 13689);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[0]], 7) == 0);
  REQUIRE(query_bed_start[0] == 13499);
  REQUIRE(query_bed_end[0] == 16000);

  // Check total num records
  REQUIRE(tot_num_records == expected_num_records);

  // Check last buffer sizes
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "pos_start",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_data_bytes == 1 * sizeof(uint32_t));
  REQUIRE(num_offsets == 0);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "sample_name",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_data_bytes == 7);
  REQUIRE(num_offsets == 2);

  // Check resubmit completed query is ok
  REQUIRE(tiledb_vcf_reader_read(reader) == TILEDB_VCF_OK);
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_COMPLETED);
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 0);

  tiledb_vcf_reader_free(&reader);
}

TEST_CASE(
    "C API: Reader submit (incomplete query 2)", "[capi][reader][incomplete]") {
  tiledb_vcf_reader_t* reader = nullptr;
  REQUIRE(tiledb_vcf_reader_alloc(&reader) == TILEDB_VCF_OK);
  std::string dataset_uri;
  SECTION("- V2") {
    dataset_uri = INPUT_ARRAYS_DIR_V2 + "/ingested_2samples_GT_DP_PL";
  }

  SECTION("- V3") {
    dataset_uri = INPUT_ARRAYS_DIR_V3 + "/ingested_2samples_GT_DP_PL";
  }
  REQUIRE(tiledb_vcf_reader_init(reader, dataset_uri.c_str()) == TILEDB_VCF_OK);

  // Set up samples and ranges
  const char* regions = "1:12277-13400,1:17000-18000";
  REQUIRE(tiledb_vcf_reader_set_regions(reader, regions) == TILEDB_VCF_OK);

  // Allocate and set buffers
  const unsigned expected_num_records = 10;
  const unsigned alloced_num_records = 2;
  SET_BUFF_POS_START(reader, alloced_num_records);
  SET_BUFF_POS_END(reader, alloced_num_records);
  SET_BUFF_SAMPLE_NAME(reader, alloced_num_records);
  SET_BUFF_QUERY_BED_START(reader, alloced_num_records);
  SET_BUFF_QUERY_BED_END(reader, alloced_num_records);
  SET_BUFF_FMT_DP(reader, alloced_num_records);

  uint64_t tot_num_records = 0;
  int64_t num_records = ~0;
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 0);

  tiledb_vcf_read_status_t status;
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_UNINITIALIZED);

  // Submit initial query
  REQUIRE(tiledb_vcf_reader_read(reader) == TILEDB_VCF_OK);

  // Check status
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_INCOMPLETE);

  // Check incomplete result size
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == alloced_num_records);
  tot_num_records += num_records;

  // Check a few buffer sizes
  int64_t num_offsets, num_data_elements, num_data_bytes;
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "pos_start",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_data_bytes == alloced_num_records * sizeof(uint32_t));
  REQUIRE(num_offsets == 0);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "sample_name",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_data_bytes == 14);
  REQUIRE(num_offsets == (alloced_num_records + 1));

  // Check first results
  REQUIRE(pos_start[0] == 12141);
  REQUIRE(pos_end[0] == 12277);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[0]], 7) == 0);
  REQUIRE(query_bed_start[0] == 12276);
  REQUIRE(query_bed_end[0] == 13400);

  REQUIRE(pos_start[1] == 12141);
  REQUIRE(pos_end[1] == 12277);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[1]], 7) == 0);
  REQUIRE(query_bed_start[1] == 12276);
  REQUIRE(query_bed_end[1] == 13400);

  // Resubmit query
  REQUIRE(tiledb_vcf_reader_read(reader) == TILEDB_VCF_OK);

  // Check status
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_INCOMPLETE);

  // Check incomplete result size
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == alloced_num_records);
  tot_num_records += num_records;

  // Check next results
  REQUIRE(pos_start[0] == 12546);
  REQUIRE(pos_end[0] == 12771);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[0]], 7) == 0);
  REQUIRE(query_bed_start[0] == 12276);
  REQUIRE(query_bed_end[0] == 13400);

  REQUIRE(pos_start[1] == 12546);
  REQUIRE(pos_end[1] == 12771);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[1]], 7) == 0);
  REQUIRE(query_bed_start[1] == 12276);
  REQUIRE(query_bed_end[1] == 13400);

  // Resubmit query
  REQUIRE(tiledb_vcf_reader_read(reader) == TILEDB_VCF_OK);

  // Check status
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_INCOMPLETE);

  // Check incomplete result size
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == alloced_num_records);
  tot_num_records += num_records;

  // Check buffer sizes again
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "pos_start",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_data_bytes == alloced_num_records * sizeof(uint32_t));
  REQUIRE(num_offsets == 0);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "sample_name",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_data_bytes == 14);
  REQUIRE(num_offsets == (alloced_num_records + 1));

  // Check next results
  REQUIRE(pos_start[0] == 13354);
  REQUIRE(pos_end[0] == 13374);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[0]], 7) == 0);
  REQUIRE(query_bed_start[0] == 12276);
  REQUIRE(query_bed_end[0] == 13400);

  REQUIRE(pos_start[1] == 13354);
  REQUIRE(pos_end[1] == 13389);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[1]], 7) == 0);
  REQUIRE(query_bed_start[1] == 12276);
  REQUIRE(query_bed_end[1] == 13400);

  // Resubmit query
  REQUIRE(tiledb_vcf_reader_read(reader) == TILEDB_VCF_OK);

  // Check status
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_INCOMPLETE);

  // Check incomplete result size
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == alloced_num_records);
  tot_num_records += num_records;

  // Check next results
  REQUIRE(pos_start[0] == 13375);
  REQUIRE(pos_end[0] == 13395);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[0]], 7) == 0);
  REQUIRE(query_bed_start[0] == 12276);
  REQUIRE(query_bed_end[0] == 13400);

  REQUIRE(pos_start[1] == 13396);
  REQUIRE(pos_end[1] == 13413);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[1]], 7) == 0);
  REQUIRE(query_bed_start[1] == 12276);
  REQUIRE(query_bed_end[1] == 13400);

  // Resubmit query
  REQUIRE(tiledb_vcf_reader_read(reader) == TILEDB_VCF_OK);

  // Check status (now finished)
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_COMPLETED);

  // Check result size
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 2);
  tot_num_records += num_records;

  // Check last results
  REQUIRE(pos_start[0] == 17319);
  REQUIRE(pos_end[0] == 17479);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[0]], 7) == 0);
  REQUIRE(query_bed_start[0] == 16999);
  REQUIRE(query_bed_end[0] == 18000);

  REQUIRE(pos_start[1] == 17480);
  REQUIRE(pos_end[1] == 17486);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[1]], 7) == 0);
  REQUIRE(query_bed_start[1] == 16999);
  REQUIRE(query_bed_end[1] == 18000);

  // Check total num records
  REQUIRE(tot_num_records == expected_num_records);

  // Check last buffer sizes
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "pos_start",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_data_bytes == 2 * sizeof(uint32_t));
  REQUIRE(num_offsets == 0);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "sample_name",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_data_bytes == 14);
  REQUIRE(num_offsets == 3);

  // Check resubmit completed query is ok
  REQUIRE(tiledb_vcf_reader_read(reader) == TILEDB_VCF_OK);
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_COMPLETED);
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 0);

  tiledb_vcf_reader_free(&reader);
}

TEST_CASE("C API: Reader get error message", "[capi][reader]") {
  tiledb_vcf_reader_t* reader = nullptr;
  REQUIRE(tiledb_vcf_reader_alloc(&reader) == TILEDB_VCF_OK);

  // Error with nonexistent array
  const char* all_samples = "HG01762,HG00280";
  REQUIRE(tiledb_vcf_reader_init(reader, "abc") == TILEDB_VCF_ERR);

  // Get error
  tiledb_vcf_error_t* error = nullptr;
  REQUIRE(tiledb_vcf_reader_get_last_error(reader, &error) == TILEDB_VCF_OK);
  const char* msg = nullptr;
  std::string expected_msg =
      "TileDB-VCF exception: Cannot open TileDB-VCF dataset; dataset 'abc' "
      "or its metadata does not exist.";
  REQUIRE(tiledb_vcf_error_get_message(error, &msg) == TILEDB_VCF_OK);
  REQUIRE(std::string(msg).find(expected_msg) != std::string::npos);
  tiledb_vcf_error_free(&error);

  // Check OK operation clears error message
  std::string dataset_uri;
  SECTION("- V2") {
    dataset_uri = INPUT_ARRAYS_DIR_V2 + "/ingested_2samples";
  }

  SECTION("- V3") {
    dataset_uri = INPUT_ARRAYS_DIR_V3 + "/ingested_2samples";
  }
  REQUIRE(tiledb_vcf_reader_init(reader, dataset_uri.c_str()) == TILEDB_VCF_OK);
  REQUIRE(tiledb_vcf_reader_get_last_error(reader, &error) == TILEDB_VCF_OK);
  REQUIRE(tiledb_vcf_error_get_message(error, &msg) == TILEDB_VCF_OK);
  REQUIRE(msg == nullptr);
  tiledb_vcf_error_free(&error);

  tiledb_vcf_reader_free(&reader);
}

TEST_CASE("C API: Reader submit (max num records)", "[capi][reader]") {
  tiledb_vcf_reader_t* reader = nullptr;
  REQUIRE(tiledb_vcf_reader_alloc(&reader) == TILEDB_VCF_OK);
  std::string dataset_uri;
  SECTION("- V2") {
    dataset_uri = INPUT_ARRAYS_DIR_V2 + "/ingested_2samples_GT_DP_PL";
  }

  SECTION("- V3") {
    dataset_uri = INPUT_ARRAYS_DIR_V3 + "/ingested_2samples_GT_DP_PL";
  }
  REQUIRE(tiledb_vcf_reader_init(reader, dataset_uri.c_str()) == TILEDB_VCF_OK);

  // Set up samples and ranges
  const char* regions = "1:12100-13360,1:13500-16000";
  REQUIRE(tiledb_vcf_reader_set_regions(reader, regions) == TILEDB_VCF_OK);

  // Note that this value is less than the actual number of records that
  // intersect the above ranges.
  const unsigned expected_num_records = 3;
  REQUIRE(
      tiledb_vcf_reader_set_max_num_records(reader, expected_num_records) ==
      TILEDB_VCF_OK);

  // Allocate and set buffers such that we need to submit 2 queries.
  const unsigned alloced_num_records = 2;
  SET_BUFF_POS_START(reader, alloced_num_records);
  SET_BUFF_POS_END(reader, alloced_num_records);
  SET_BUFF_SAMPLE_NAME(reader, alloced_num_records);
  SET_BUFF_QUERY_BED_START(reader, alloced_num_records);
  SET_BUFF_QUERY_BED_END(reader, alloced_num_records);
  SET_BUFF_FMT_DP(reader, alloced_num_records);

  uint64_t tot_num_records = 0;
  int64_t num_records = ~0;
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 0);

  tiledb_vcf_read_status_t status;
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_UNINITIALIZED);

  // Submit initial query
  REQUIRE(tiledb_vcf_reader_read(reader) == TILEDB_VCF_OK);

  // Check status
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_INCOMPLETE);

  // Check incomplete result size
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == alloced_num_records);
  tot_num_records += num_records;

  // Check a few buffer sizes
  int64_t num_offsets, num_data_elements, num_data_bytes;
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "pos_start",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_data_bytes == alloced_num_records * sizeof(uint32_t));
  REQUIRE(num_offsets == 0);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "sample_name",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_data_bytes == 14);
  REQUIRE(num_offsets == (alloced_num_records + 1));

  // Check first results
  REQUIRE(pos_start[0] == 12141);
  REQUIRE(pos_end[0] == 12277);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[0]], 7) == 0);
  REQUIRE(query_bed_start[0] == 12099);
  REQUIRE(query_bed_end[0] == 13360);

  REQUIRE(pos_start[1] == 12141);
  REQUIRE(pos_end[1] == 12277);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[1]], 7) == 0);
  REQUIRE(query_bed_start[1] == 12099);
  REQUIRE(query_bed_end[1] == 13360);

  // Resubmit query
  REQUIRE(tiledb_vcf_reader_read(reader) == TILEDB_VCF_OK);

  // Check status
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_COMPLETED);

  // Check complete result size
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 1);
  tot_num_records += num_records;

  // Check results
  REQUIRE(pos_start[0] == 12546);
  REQUIRE(pos_end[0] == 12771);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[0]], 7) == 0);
  REQUIRE(query_bed_start[0] == 12099);
  REQUIRE(query_bed_end[0] == 13360);

  // Check total num records
  REQUIRE(tot_num_records == expected_num_records);

  // Check last buffer sizes
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "pos_start",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_data_bytes == 1 * sizeof(uint32_t));
  REQUIRE(num_offsets == 0);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "sample_name",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_data_bytes == 7);
  REQUIRE(num_offsets == 2);

  // Check resubmit completed query is ok
  REQUIRE(tiledb_vcf_reader_read(reader) == TILEDB_VCF_OK);
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_COMPLETED);
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 0);

  tiledb_vcf_reader_free(&reader);
}

TEST_CASE("C API: Reader submit (partitioned)", "[capi][reader]") {
  tiledb_vcf_reader_t *reader0 = nullptr, *reader1 = nullptr;
  REQUIRE(tiledb_vcf_reader_alloc(&reader0) == TILEDB_VCF_OK);
  REQUIRE(tiledb_vcf_reader_alloc(&reader1) == TILEDB_VCF_OK);
  std::string dataset_uri;
  SECTION("- V2") {
    dataset_uri = INPUT_ARRAYS_DIR_V2 + "/ingested_2samples_GT_DP_PL";
  }

  SECTION("- V3") {
    dataset_uri = INPUT_ARRAYS_DIR_V3 + "/ingested_2samples_GT_DP_PL";
  }
  REQUIRE(
      tiledb_vcf_reader_init(reader0, dataset_uri.c_str()) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_init(reader1, dataset_uri.c_str()) == TILEDB_VCF_OK);

  // Set up samples and ranges
  const char* regions = "1:12100-13360,1:13500-17350,1:17485-17485";
  REQUIRE(tiledb_vcf_reader_set_regions(reader0, regions) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_region_partition(reader0, 0, 2) == TILEDB_VCF_OK);
  REQUIRE(tiledb_vcf_reader_set_regions(reader1, regions) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_region_partition(reader1, 1, 2) == TILEDB_VCF_OK);

  // Allocate and set buffers
  const unsigned allocated_num_records = 10;
  uint32_t pos_end0[allocated_num_records];
  int32_t sample_name_offsets0[allocated_num_records + 1];
  char sample_name0[allocated_num_records * 10];
  uint32_t pos_end1[allocated_num_records];
  int32_t sample_name_offsets1[allocated_num_records + 1];
  char sample_name1[allocated_num_records * 10];
  REQUIRE(
      tiledb_vcf_reader_set_buffer_values(
          reader0, "pos_end", sizeof(pos_end0), pos_end0) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_values(
          reader0, "sample_name", sizeof(sample_name0), sample_name0) ==
      TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_offsets(
          reader0,
          "sample_name",
          sizeof(sample_name_offsets0),
          sample_name_offsets0) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_values(
          reader1, "pos_end", sizeof(pos_end1), pos_end1) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_values(
          reader1, "sample_name", sizeof(sample_name1), sample_name1) ==
      TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_offsets(
          reader1,
          "sample_name",
          sizeof(sample_name_offsets1),
          sample_name_offsets1) == TILEDB_VCF_OK);

  int64_t num_records = ~0;
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader0, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 0);

  tiledb_vcf_read_status_t status;
  REQUIRE(tiledb_vcf_reader_get_status(reader0, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_UNINITIALIZED);

  // Submit queries
  REQUIRE(tiledb_vcf_reader_read(reader0) == TILEDB_VCF_OK);
  REQUIRE(tiledb_vcf_reader_read(reader1) == TILEDB_VCF_OK);

  // Check status
  REQUIRE(tiledb_vcf_reader_get_status(reader0, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_COMPLETED);
  REQUIRE(tiledb_vcf_reader_get_status(reader1, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_COMPLETED);

  // Check result size
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader0, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 10);
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader1, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 1);

  // Check results
  REQUIRE(pos_end0[0] == 12277);
  REQUIRE(strncmp("HG00280", &sample_name0[sample_name_offsets0[0]], 7) == 0);

  REQUIRE(pos_end0[1] == 12277);
  REQUIRE(strncmp("HG01762", &sample_name0[sample_name_offsets0[1]], 7) == 0);

  REQUIRE(pos_end0[2] == 12771);
  REQUIRE(strncmp("HG00280", &sample_name0[sample_name_offsets0[2]], 7) == 0);

  REQUIRE(pos_end0[3] == 12771);
  REQUIRE(strncmp("HG01762", &sample_name0[sample_name_offsets0[3]], 7) == 0);

  REQUIRE(pos_end0[4] == 13374);
  REQUIRE(strncmp("HG00280", &sample_name0[sample_name_offsets0[4]], 7) == 0);

  REQUIRE(pos_end0[5] == 13389);
  REQUIRE(strncmp("HG01762", &sample_name0[sample_name_offsets0[5]], 7) == 0);

  REQUIRE(pos_end0[6] == 13519);
  REQUIRE(strncmp("HG00280", &sample_name0[sample_name_offsets0[6]], 7) == 0);

  REQUIRE(pos_end0[7] == 13544);
  REQUIRE(strncmp("HG00280", &sample_name0[sample_name_offsets0[7]], 7) == 0);

  REQUIRE(pos_end0[8] == 13689);
  REQUIRE(strncmp("HG00280", &sample_name0[sample_name_offsets0[8]], 7) == 0);

  REQUIRE(pos_end0[9] == 17479);
  REQUIRE(strncmp("HG00280", &sample_name0[sample_name_offsets0[9]], 7) == 0);

  REQUIRE(pos_end1[0] == 17486);
  REQUIRE(strncmp("HG00280", &sample_name1[sample_name_offsets1[0]], 7) == 0);

  tiledb_vcf_reader_free(&reader0);
  tiledb_vcf_reader_free(&reader1);
}

TEST_CASE("C API: Reader submit (partitioned samples)", "[capi][reader]") {
  tiledb_vcf_reader_t *reader0 = nullptr, *reader1 = nullptr;
  REQUIRE(tiledb_vcf_reader_alloc(&reader0) == TILEDB_VCF_OK);
  REQUIRE(tiledb_vcf_reader_alloc(&reader1) == TILEDB_VCF_OK);
  std::string dataset_uri;
  SECTION("- V2") {
    dataset_uri = INPUT_ARRAYS_DIR_V2 + "/ingested_2samples_GT_DP_PL";
  }

  SECTION("- V3") {
    dataset_uri = INPUT_ARRAYS_DIR_V3 + "/ingested_2samples_GT_DP_PL";
  }
  const char* all_samples = "HG01762,HG00280";
  REQUIRE(
      tiledb_vcf_reader_init(reader0, dataset_uri.c_str()) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_init(reader1, dataset_uri.c_str()) == TILEDB_VCF_OK);

  // Set up samples and ranges
  const char* regions = "1:12100-13360,1:13500-17350,1:17485-17485";
  REQUIRE(tiledb_vcf_reader_set_regions(reader0, regions) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_sample_partition(reader0, 0, 2) == TILEDB_VCF_OK);
  REQUIRE(tiledb_vcf_reader_set_regions(reader1, regions) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_sample_partition(reader1, 1, 2) == TILEDB_VCF_OK);

  // Allocate and set buffers
  const unsigned allocated_num_records = 10;
  uint32_t pos_end0[allocated_num_records];
  int32_t sample_name_offsets0[allocated_num_records];
  char sample_name0[allocated_num_records * 10];
  uint32_t pos_end1[allocated_num_records];
  int32_t sample_name_offsets1[allocated_num_records];
  char sample_name1[allocated_num_records * 10];
  REQUIRE(
      tiledb_vcf_reader_set_buffer_values(
          reader0, "pos_end", sizeof(pos_end0), pos_end0) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_values(
          reader0, "sample_name", sizeof(sample_name0), sample_name0) ==
      TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_offsets(
          reader0,
          "sample_name",
          sizeof(sample_name_offsets0),
          sample_name_offsets0) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_values(
          reader1, "pos_end", sizeof(pos_end1), pos_end1) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_values(
          reader1, "sample_name", sizeof(sample_name1), sample_name1) ==
      TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_offsets(
          reader1,
          "sample_name",
          sizeof(sample_name_offsets1),
          sample_name_offsets1) == TILEDB_VCF_OK);

  int64_t num_records = ~0;
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader0, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 0);

  tiledb_vcf_read_status_t status;
  REQUIRE(tiledb_vcf_reader_get_status(reader0, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_UNINITIALIZED);

  // Submit queries
  REQUIRE(tiledb_vcf_reader_read(reader0) == TILEDB_VCF_OK);
  REQUIRE(tiledb_vcf_reader_read(reader1) == TILEDB_VCF_OK);

  // Check status
  REQUIRE(tiledb_vcf_reader_get_status(reader0, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_COMPLETED);
  REQUIRE(tiledb_vcf_reader_get_status(reader1, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_COMPLETED);

  // Check result size
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader0, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 8);
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader1, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 3);

  // Check results
  REQUIRE(pos_end0[0] == 12277);
  REQUIRE(strncmp("HG00280", &sample_name0[sample_name_offsets0[0]], 7) == 0);

  REQUIRE(pos_end0[1] == 12771);
  REQUIRE(strncmp("HG00280", &sample_name0[sample_name_offsets0[1]], 7) == 0);

  REQUIRE(pos_end0[2] == 13374);
  REQUIRE(strncmp("HG00280", &sample_name0[sample_name_offsets0[2]], 7) == 0);

  REQUIRE(pos_end0[3] == 13519);
  REQUIRE(strncmp("HG00280", &sample_name0[sample_name_offsets0[3]], 7) == 0);

  REQUIRE(pos_end0[4] == 13544);
  REQUIRE(strncmp("HG00280", &sample_name0[sample_name_offsets0[4]], 7) == 0);

  REQUIRE(pos_end0[5] == 13689);
  REQUIRE(strncmp("HG00280", &sample_name0[sample_name_offsets0[5]], 7) == 0);

  REQUIRE(pos_end0[6] == 17479);
  REQUIRE(strncmp("HG00280", &sample_name0[sample_name_offsets0[6]], 7) == 0);

  REQUIRE(pos_end0[7] == 17486);
  REQUIRE(strncmp("HG00280", &sample_name0[sample_name_offsets0[7]], 7) == 0);

  REQUIRE(pos_end1[0] == 12277);
  REQUIRE(strncmp("HG01762", &sample_name1[sample_name_offsets1[0]], 7) == 0);

  REQUIRE(pos_end1[1] == 12771);
  REQUIRE(strncmp("HG01762", &sample_name1[sample_name_offsets1[1]], 7) == 0);

  REQUIRE(pos_end1[2] == 13389);
  REQUIRE(strncmp("HG01762", &sample_name1[sample_name_offsets1[2]], 7) == 0);

  tiledb_vcf_reader_free(&reader0);
  tiledb_vcf_reader_free(&reader1);
}

TEST_CASE("C API: Reader submit (ranges will overlap)", "[capi][reader]") {
  tiledb_vcf_reader_t* reader = nullptr;
  REQUIRE(tiledb_vcf_reader_alloc(&reader) == TILEDB_VCF_OK);
  std::string dataset_uri;
  SECTION("- V2") {
    dataset_uri = INPUT_ARRAYS_DIR_V2 + "/ingested_2samples_GT_DP_PL";
  }

  SECTION("- V3") {
    dataset_uri = INPUT_ARRAYS_DIR_V3 + "/ingested_2samples_GT_DP_PL";
  }
  REQUIRE(tiledb_vcf_reader_init(reader, dataset_uri.c_str()) == TILEDB_VCF_OK);

  // Set up samples and ranges
  const char* regions = "1:13000-13360,1:13500-16000";
  REQUIRE(tiledb_vcf_reader_set_regions(reader, regions) == TILEDB_VCF_OK);

  // Allocate and set buffers
  const unsigned expected_num_records = 5;
  SET_BUFF_POS_END(reader, expected_num_records);
  SET_BUFF_SAMPLE_NAME(reader, expected_num_records);
  SET_BUFF_ALLELES(reader, expected_num_records);
  SET_BUFF_FMT_DP(reader, expected_num_records);

  int64_t num_records = ~0;
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 0);

  tiledb_vcf_read_status_t status;
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_UNINITIALIZED);

  // Submit query
  REQUIRE(tiledb_vcf_reader_read(reader) == TILEDB_VCF_OK);

  // Check status
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_COMPLETED);

  // Check result size
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == expected_num_records);

  // Check results
  REQUIRE(pos_end[0] == 13374);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[0]], 7) == 0);
  REQUIRE(
      strncmp("T", &alleles[alleles_offsets[alleles_list_offsets[0]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[0] + 1]],
          9) == 0);
  REQUIRE(fmt_DP[0] == 15);

  REQUIRE(pos_end[1] == 13389);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[1]], 7) == 0);
  REQUIRE(
      strncmp("T", &alleles[alleles_offsets[alleles_list_offsets[1]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[1] + 1]],
          9) == 0);
  REQUIRE(fmt_DP[1] == 64);

  REQUIRE(pos_end[2] == 13519);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[2]], 7) == 0);
  REQUIRE(
      strncmp("G", &alleles[alleles_offsets[alleles_list_offsets[2]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[2] + 1]],
          9) == 0);
  REQUIRE(fmt_DP[2] == 10);

  REQUIRE(pos_end[3] == 13544);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[3]], 7) == 0);
  REQUIRE(
      strncmp("G", &alleles[alleles_offsets[alleles_list_offsets[3]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[3] + 1]],
          9) == 0);
  REQUIRE(fmt_DP[3] == 6);

  REQUIRE(pos_end[4] == 13689);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[4]], 7) == 0);
  REQUIRE(
      strncmp("G", &alleles[alleles_offsets[alleles_list_offsets[4]]], 1) == 0);
  REQUIRE(
      strncmp(
          "<NON_REF>",
          &alleles[alleles_offsets[alleles_list_offsets[4] + 1]],
          9) == 0);
  REQUIRE(fmt_DP[4] == 0);

  tiledb_vcf_reader_free(&reader);
}

TEST_CASE(
    "C API: Reader submit (incomplete, uneven attributes)",
    "[capi][reader][incomplete]") {
  tiledb_vcf_reader_t* reader = nullptr;
  REQUIRE(tiledb_vcf_reader_alloc(&reader) == TILEDB_VCF_OK);
  std::string dataset_uri;
  SECTION("- V2") {
    dataset_uri = INPUT_ARRAYS_DIR_V2 + "/ingested_2samples";
  }

  SECTION("- V3") {
    dataset_uri = INPUT_ARRAYS_DIR_V3 + "/ingested_2samples";
  }
  REQUIRE(tiledb_vcf_reader_init(reader, dataset_uri.c_str()) == TILEDB_VCF_OK);

  // Set up samples and ranges
  const char* regions = "1:12100-13360,1:13500-17350";
  REQUIRE(tiledb_vcf_reader_set_regions(reader, regions) == TILEDB_VCF_OK);

  // Allocate and set buffers. Note due to the varying result size per
  // attribute, a different number of cells fits for the different attributes,
  // which used to trigger a bug.
  int32_t sample_name_offsets[3];
  char sample_name[17];
  int32_t contig_offsets[3];
  char contig[17];
  int32_t alleles_offsets[2 * 3];
  int32_t alleles_list_offsets[3];
  char alleles[17];
  REQUIRE(
      tiledb_vcf_reader_set_buffer_values(
          reader, "sample_name", sizeof(sample_name), sample_name) ==
      TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_offsets(
          reader,
          "sample_name",
          sizeof(sample_name_offsets),
          sample_name_offsets) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_values(
          reader, "contig", sizeof(contig), contig) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_offsets(
          reader, "contig", sizeof(contig_offsets), contig_offsets) ==
      TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_values(
          reader, "alleles", sizeof(alleles), alleles) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_offsets(
          reader, "alleles", sizeof(alleles_offsets), alleles_offsets) ==
      TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_list_offsets(
          reader,
          "alleles",
          sizeof(alleles_list_offsets),
          alleles_list_offsets) == TILEDB_VCF_OK);

  int64_t num_records = ~0;
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 0);

  tiledb_vcf_read_status_t status;
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_UNINITIALIZED);

  // Submit query
  REQUIRE(tiledb_vcf_reader_read(reader) == TILEDB_VCF_OK);

  // Check status
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_INCOMPLETE);

  // Check result size
  int64_t num_offsets, num_data_elements, num_data_bytes;
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 1);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "sample_name",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_offsets == 2);
  REQUIRE(num_data_bytes == 7);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "contig",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_offsets == 2);
  REQUIRE(num_data_bytes == 1);

  // Resubmit query
  REQUIRE(tiledb_vcf_reader_read(reader) == TILEDB_VCF_OK);
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_INCOMPLETE);
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 1);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "sample_name",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_offsets == 2);
  REQUIRE(num_data_bytes == 7);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "contig",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_offsets == 2);
  REQUIRE(num_data_bytes == 1);

  // Resubmit query
  REQUIRE(tiledb_vcf_reader_read(reader) == TILEDB_VCF_OK);
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_INCOMPLETE);
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 1);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "sample_name",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_offsets == 2);
  REQUIRE(num_data_bytes == 7);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "contig",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_offsets == 2);
  REQUIRE(num_data_bytes == 1);

  // Resubmit query
  REQUIRE(tiledb_vcf_reader_read(reader) == TILEDB_VCF_OK);
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_INCOMPLETE);
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 1);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "sample_name",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_offsets == 2);
  REQUIRE(num_data_bytes == 7);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "contig",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_offsets == 2);
  REQUIRE(num_data_bytes == 1);

  // Resubmit query
  REQUIRE(tiledb_vcf_reader_read(reader) == TILEDB_VCF_OK);
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_INCOMPLETE);
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 1);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "sample_name",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_offsets == 2);
  REQUIRE(num_data_bytes == 7);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "contig",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_offsets == 2);
  REQUIRE(num_data_bytes == 1);

  // Resubmit query
  REQUIRE(tiledb_vcf_reader_read(reader) == TILEDB_VCF_OK);
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_INCOMPLETE);
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 1);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "sample_name",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_offsets == 2);
  REQUIRE(num_data_bytes == 7);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "contig",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_offsets == 2);
  REQUIRE(num_data_bytes == 1);

  // Resubmit query
  REQUIRE(tiledb_vcf_reader_read(reader) == TILEDB_VCF_OK);
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_INCOMPLETE);
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 1);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "sample_name",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_offsets == 2);
  REQUIRE(num_data_bytes == 7);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "contig",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_offsets == 2);
  REQUIRE(num_data_bytes == 1);

  // Resubmit query
  REQUIRE(tiledb_vcf_reader_read(reader) == TILEDB_VCF_OK);
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_INCOMPLETE);
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 1);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "sample_name",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_offsets == 2);
  REQUIRE(num_data_bytes == 7);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "contig",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_offsets == 2);
  REQUIRE(num_data_bytes == 1);

  // Resubmit query
  REQUIRE(tiledb_vcf_reader_read(reader) == TILEDB_VCF_OK);
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_INCOMPLETE);
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 1);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "sample_name",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_offsets == 2);
  REQUIRE(num_data_bytes == 7);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "contig",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_offsets == 2);
  REQUIRE(num_data_bytes == 1);

  // Resubmit query
  REQUIRE(tiledb_vcf_reader_read(reader) == TILEDB_VCF_OK);
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_COMPLETED);
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 1);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "sample_name",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_offsets == 2);
  REQUIRE(num_data_bytes == 7);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "contig",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_offsets == 2);
  REQUIRE(num_data_bytes == 1);

  tiledb_vcf_reader_free(&reader);
}

TEST_CASE("C API: Reader submit (BED file Parallelism)", "[capi][reader]") {
  tiledb_vcf_reader_t* reader = nullptr;
  REQUIRE(tiledb_vcf_reader_alloc(&reader) == TILEDB_VCF_OK);

  std::string bed_uri;
  SECTION("- Compressed") {
    bed_uri = std::string(TILEDB_VCF_TEST_INPUT_DIR) +
              "/E001_15_coreMarks_dense.bed.gz";
  }

  SECTION("- Uncompressed") {
    bed_uri =
        std::string(TILEDB_VCF_TEST_INPUT_DIR) + "/E001_15_coreMarks_dense.bed";
  }

  std::string dataset_uri = INPUT_ARRAYS_DIR_V3 + "/synth-array";

  REQUIRE(tiledb_vcf_reader_init(reader, dataset_uri.c_str()) == TILEDB_VCF_OK);

  // Set ranges
  REQUIRE(
      tiledb_vcf_reader_set_bed_file(reader, bed_uri.c_str()) == TILEDB_VCF_OK);

  // Allocate and set buffers, only set small buffers for a few records
  const unsigned expected_num_records = 10;
  SET_BUFF_POS_START(reader, expected_num_records);
  SET_BUFF_POS_END(reader, expected_num_records);
  SET_BUFF_SAMPLE_NAME(reader, expected_num_records);
  SET_BUFF_CONTIG(reader, expected_num_records);
  SET_BUFF_FMT_GT(reader, expected_num_records);

  int64_t num_records = ~0;
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 0);

  tiledb_vcf_read_status_t status;
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_UNINITIALIZED);

  // Submit query
  REQUIRE(tiledb_vcf_reader_read(reader) == TILEDB_VCF_OK);

  // Check result size
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == expected_num_records);

  // Check status
  REQUIRE(tiledb_vcf_reader_get_status(reader, &status) == TILEDB_VCF_OK);
  REQUIRE(status == TILEDB_VCF_INCOMPLETE);

  // Check a few buffer sizes
  int64_t num_offsets, num_data_elements, num_data_bytes;
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "pos_start",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_data_bytes == expected_num_records * sizeof(uint32_t));
  REQUIRE(num_offsets == 0);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader,
          "sample_name",
          &num_offsets,
          &num_data_elements,
          &num_data_bytes) == TILEDB_VCF_OK);
  REQUIRE(num_data_bytes == 31);
  REQUIRE(num_offsets == (expected_num_records + 1));

  // Check results
  REQUIRE(pos_start[0] == 10626);
  REQUIRE(pos_end[0] == 81854);
  REQUIRE(sample_name_offsets[0] == 0);
  REQUIRE(strncmp("G16", &sample_name[sample_name_offsets[0]], 3) == 0);
  REQUIRE(strncmp("1", &contig[contig_offsets[0]], 1) == 0);
  REQUIRE(fmt_GT_offsets[0] == 0);
  REQUIRE(fmt_GT[fmt_GT_offsets[0]] == 1);
  REQUIRE(fmt_GT[fmt_GT_offsets[0] + 1] == 0);

  REQUIRE(pos_start[1] == 10717);
  REQUIRE(pos_end[1] == 75204);
  REQUIRE(sample_name_offsets[1] == 3);
  REQUIRE(strncmp("G14", &sample_name[sample_name_offsets[1]], 3) == 0);
  REQUIRE(strncmp("1", &contig[contig_offsets[1]], 1) == 0);
  REQUIRE(fmt_GT_offsets[1] == 2);
  REQUIRE(fmt_GT[fmt_GT_offsets[1]] == 1);
  REQUIRE(fmt_GT[fmt_GT_offsets[1] + 1] == 0);

  REQUIRE(pos_start[2] == 10863);
  REQUIRE(pos_end[2] == 83686);
  REQUIRE(sample_name_offsets[2] == 6);
  REQUIRE(strncmp("G17", &sample_name[sample_name_offsets[2]], 3) == 0);
  REQUIRE(strncmp("1", &contig[contig_offsets[2]], 1) == 0);
  REQUIRE(fmt_GT_offsets[2] == 4);
  REQUIRE(fmt_GT[fmt_GT_offsets[2]] == 0);
  REQUIRE(fmt_GT[fmt_GT_offsets[2] + 1] == 1);

  REQUIRE(pos_start[3] == 10872);
  REQUIRE(pos_end[3] == 16376);
  REQUIRE(sample_name_offsets[3] == 9);
  REQUIRE(strncmp("G10", &sample_name[sample_name_offsets[3]], 3) == 0);
  REQUIRE(strncmp("1", &contig[contig_offsets[3]], 1) == 0);
  REQUIRE(fmt_GT_offsets[3] == 6);
  REQUIRE(fmt_GT[fmt_GT_offsets[3]] == 1);
  REQUIRE(fmt_GT[fmt_GT_offsets[3] + 1] == 0);

  REQUIRE(pos_start[4] == 11123);
  REQUIRE(pos_end[4] == 89409);
  REQUIRE(sample_name_offsets[4] == 12);
  REQUIRE(strncmp("G15", &sample_name[sample_name_offsets[4]], 3) == 0);
  REQUIRE(strncmp("1", &contig[contig_offsets[4]], 1) == 0);
  REQUIRE(fmt_GT_offsets[4] == 8);
  REQUIRE(fmt_GT[fmt_GT_offsets[4]] == 1);
  REQUIRE(fmt_GT[fmt_GT_offsets[4] + 1] == 0);

  // Check final offsets are equal to data size
  REQUIRE(sample_name_offsets[10] == 31);
  REQUIRE(fmt_GT_offsets[10] == 20);

  tiledb_vcf_reader_free(&reader);
}