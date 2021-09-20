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
#include "unit-helpers.h"

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

#define SET_BUFF_POS_START(r, nr)              \
  std::vector<uint32_t> pos_start((nr));       \
  REQUIRE(                                     \
      tiledb_vcf_reader_set_buffer_values(     \
          reader,                              \
          "pos_start",                         \
          sizeof(uint32_t) * pos_start.size(), \
          pos_start.data()) == TILEDB_VCF_OK);

#define SET_BUFF_POS_END(r, nr)              \
  std::vector<uint32_t> pos_end((nr));       \
  REQUIRE(                                   \
      tiledb_vcf_reader_set_buffer_values(   \
          reader,                            \
          "pos_end",                         \
          sizeof(uint32_t) * pos_end.size(), \
          pos_end.data()) == TILEDB_VCF_OK);

#define SET_BUFF_QUERY_BED_START(r, nr)              \
  std::vector<uint32_t> query_bed_start((nr));       \
  REQUIRE(                                           \
      tiledb_vcf_reader_set_buffer_values(           \
          reader,                                    \
          "query_bed_start",                         \
          sizeof(uint32_t) * query_bed_start.size(), \
          query_bed_start.data()) == TILEDB_VCF_OK);

#define SET_BUFF_QUERY_BED_END(r, nr)              \
  std::vector<uint32_t> query_bed_end((nr));       \
  REQUIRE(                                         \
      tiledb_vcf_reader_set_buffer_values(         \
          reader,                                  \
          "query_bed_end",                         \
          sizeof(uint32_t) * query_bed_end.size(), \
          query_bed_end.data()) == TILEDB_VCF_OK);

#define SET_BUFF_QUERY_BED_LINE(r, nr)              \
  std::vector<uint32_t> query_bed_line((nr));       \
  REQUIRE(                                          \
      tiledb_vcf_reader_set_buffer_values(          \
          reader,                                   \
          "query_bed_line",                         \
          sizeof(uint32_t) * query_bed_line.size(), \
          query_bed_line.data()) == TILEDB_VCF_OK);

#define SET_BUFF_SAMPLE_NAME(r, nr)                     \
  std::vector<int32_t> sample_name_offsets((nr) + 1);   \
  std::vector<char> sample_name((nr)*10);               \
  REQUIRE(                                              \
      tiledb_vcf_reader_set_buffer_values(              \
          (reader),                                     \
          "sample_name",                                \
          sizeof(char) * sample_name.size(),            \
          sample_name.data()) == TILEDB_VCF_OK);        \
  REQUIRE(                                              \
      tiledb_vcf_reader_set_buffer_offsets(             \
          (reader),                                     \
          "sample_name",                                \
          sizeof(int32_t) * sample_name_offsets.size(), \
          sample_name_offsets.data()) == TILEDB_VCF_OK);

#define SET_BUFF_CONTIG(r, nr)                                                \
  std::vector<int32_t> contig_offsets((nr) + 1);                              \
  std::vector<char> contig((nr)*10);                                          \
  REQUIRE(                                                                    \
      tiledb_vcf_reader_set_buffer_values(                                    \
          (reader), "contig", sizeof(char) * contig.size(), contig.data()) == \
      TILEDB_VCF_OK);                                                         \
  REQUIRE(                                                                    \
      tiledb_vcf_reader_set_buffer_offsets(                                   \
          (reader),                                                           \
          "contig",                                                           \
          sizeof(int32_t) * contig_offsets.size(),                            \
          contig_offsets.data()) == TILEDB_VCF_OK);

#define SET_BUFF_ALLELES(r, nr)                          \
  std::vector<int32_t> alleles_offsets(2 * (nr) + 1);    \
  std::vector<int32_t> alleles_list_offsets((nr) + 1);   \
  std::vector<char> alleles((nr)*20);                    \
  REQUIRE(                                               \
      tiledb_vcf_reader_set_buffer_values(               \
          (reader),                                      \
          "alleles",                                     \
          sizeof(char) * alleles.size(),                 \
          alleles.data()) == TILEDB_VCF_OK);             \
  REQUIRE(                                               \
      tiledb_vcf_reader_set_buffer_offsets(              \
          (reader),                                      \
          "alleles",                                     \
          sizeof(int32_t) * alleles_offsets.size(),      \
          alleles_offsets.data()) == TILEDB_VCF_OK);     \
  REQUIRE(                                               \
      tiledb_vcf_reader_set_buffer_list_offsets(         \
          (reader),                                      \
          "alleles",                                     \
          sizeof(int32_t) * alleles_list_offsets.size(), \
          alleles_list_offsets.data()) == TILEDB_VCF_OK);

#define SET_BUFF_FILTERS(r, nr)                           \
  std::vector<int32_t> filters_offsets(2 * (nr) + 1);     \
  std::vector<int32_t> filters_list_offsets((nr) + 1);    \
  std::vector<uint8_t> filters_bitmap((nr) / 8 + 1);      \
  std::vector<char> filters((nr)*20);                     \
  REQUIRE(                                                \
      tiledb_vcf_reader_set_buffer_values(                \
          (reader),                                       \
          "filters",                                      \
          sizeof(char) * filters.size(),                  \
          filters.data()) == TILEDB_VCF_OK);              \
  REQUIRE(                                                \
      tiledb_vcf_reader_set_buffer_offsets(               \
          (reader),                                       \
          "filters",                                      \
          sizeof(int32_t) * filters_offsets.size(),       \
          filters_offsets.data()) == TILEDB_VCF_OK);      \
  REQUIRE(                                                \
      tiledb_vcf_reader_set_buffer_list_offsets(          \
          (reader),                                       \
          "filters",                                      \
          sizeof(int32_t) * filters_list_offsets.size(),  \
          filters_list_offsets.data()) == TILEDB_VCF_OK); \
  REQUIRE(                                                \
      tiledb_vcf_reader_set_buffer_validity_bitmap(       \
          reader,                                         \
          "filters",                                      \
          sizeof(uint8_t) * filters_bitmap.size(),        \
          filters_bitmap.data()) == TILEDB_VCF_OK);

#define SET_BUFF_INFO(r, nr)                                            \
  std::vector<int32_t> info_offsets((nr) + 1);                          \
  std::vector<char> info((nr)*100);                                     \
  REQUIRE(                                                              \
      tiledb_vcf_reader_set_buffer_values(                              \
          (reader), "info", sizeof(char) * info.size(), info.data()) == \
      TILEDB_VCF_OK);                                                   \
  REQUIRE(                                                              \
      tiledb_vcf_reader_set_buffer_offsets(                             \
          (reader),                                                     \
          "info",                                                       \
          sizeof(int32_t) * info_offsets.size(),                        \
          info_offsets.data()) == TILEDB_VCF_OK);

#define SET_BUFF_FORMAT(r, nr)                                             \
  std::vector<int32_t> format_offsets((nr) + 1);                           \
  std::vector<char> format((nr)*100);                                      \
  REQUIRE(                                                                 \
      tiledb_vcf_reader_set_buffer_values(                                 \
          (reader), "fmt", sizeof(char) * format.size(), format.data()) == \
      TILEDB_VCF_OK);                                                      \
  REQUIRE(                                                                 \
      tiledb_vcf_reader_set_buffer_offsets(                                \
          (reader),                                                        \
          "fmt",                                                           \
          sizeof(int32_t) * format_offsets.size(),                         \
          format_offsets.data()) == TILEDB_VCF_OK);

#define SET_BUFF_FMT_GT(r, nr)                                               \
  std::vector<int32_t> fmt_GT_offsets((nr) + 1);                             \
  std::vector<int> fmt_GT((nr)*2);                                           \
  REQUIRE(                                                                   \
      tiledb_vcf_reader_set_buffer_values(                                   \
          (reader), "fmt_GT", sizeof(int) * fmt_GT.size(), fmt_GT.data()) == \
      TILEDB_VCF_OK);                                                        \
  REQUIRE(                                                                   \
      tiledb_vcf_reader_set_buffer_offsets(                                  \
          (reader),                                                          \
          "fmt_GT",                                                          \
          sizeof(int32_t) * fmt_GT_offsets.size(),                           \
          fmt_GT_offsets.data()) == TILEDB_VCF_OK);

#define SET_BUFF_FMT_DP(r, nr)                                               \
  std::vector<int> fmt_DP((nr));                                             \
  REQUIRE(                                                                   \
      tiledb_vcf_reader_set_buffer_values(                                   \
          (reader), "fmt_DP", sizeof(int) * fmt_DP.size(), fmt_DP.data()) == \
      TILEDB_VCF_OK);

#define SET_BUFF_FMT_PL(r, nr)                                               \
  std::vector<int32_t> fmt_PL_offsets((nr) + 1);                             \
  std::vector<int> fmt_PL((nr * 3));                                         \
  REQUIRE(                                                                   \
      tiledb_vcf_reader_set_buffer_values(                                   \
          (reader), "fmt_PL", sizeof(int) * fmt_PL.size(), fmt_PL.data()) == \
      TILEDB_VCF_OK);                                                        \
  REQUIRE(                                                                   \
      tiledb_vcf_reader_set_buffer_offsets(                                  \
          (reader),                                                          \
          "fmt_PL",                                                          \
          sizeof(int32_t) * fmt_PL_offsets.size(),                           \
          fmt_PL_offsets.data()) == TILEDB_VCF_OK);

std::vector<record> build_records(
    uint64_t num_records,
    const std::vector<char>& samples,
    const std::vector<int32_t>& sample_offsets,
    const std::vector<uint32_t>& start_pos,
    const std::vector<uint32_t>& end_pos,
    const std::vector<uint32_t>& query_bed_start,
    const std::vector<uint32_t>& query_bed_end,
    const std::vector<char>& contig,
    const std::vector<int32_t>& contig_offsets,
    const std::vector<char>& alleles,
    const std::vector<int32_t>& alleles_offsets,
    const std::vector<int32_t>& alleles_list_offsets,
    const std::vector<char>& filters,
    const std::vector<int32_t>& filters_offsets,
    const std::vector<int32_t>& filters_list_offsets,
    const std::vector<uint8_t>& filters_bitmap,
    const std::vector<char>& info,
    const std::vector<int32_t>& info_offsets,
    const std::vector<char>& fmt,
    const std::vector<int32_t>& fmt_offsets,
    const std::vector<int32_t>& fmt_GT,
    const std::vector<int32_t>& fmt_GT_offsets,
    std::vector<int> fmt_DP,
    const std::vector<int32_t>& fmt_PL = {},
    const std::vector<int32_t>& fmt_PL_offsets = {},
    const std::vector<uint32_t>& query_bed_line = {}) {
  std::vector<record> ret(num_records);

  for (size_t i = 0; i < num_records; i++) {
    std::string sample_val;
    if (!samples.empty()) {
      std::vector<char> sample_vec =
          var_value<char>(samples, sample_offsets, i);
      sample_val = std::string(sample_vec.begin(), sample_vec.end());
    }

    uint32_t start_pos_val = 0;
    if (!start_pos.empty())
      start_pos_val = start_pos[i];

    uint32_t end_pos_val = 0;
    if (!end_pos.empty())
      end_pos_val = end_pos[i];

    uint32_t query_bed_start_val = 0;
    if (!query_bed_start.empty())
      query_bed_start_val = query_bed_start[i];

    uint32_t query_bed_end_val = 0;
    if (!query_bed_end.empty())
      query_bed_end_val = query_bed_end[i];

    uint32_t query_bed_line_val = 0;
    if (!query_bed_line.empty())
      query_bed_line_val = query_bed_line[i];

    std::string contig_val;
    if (!contig.empty()) {
      std::vector<char> contig_vec = var_value<char>(contig, contig_offsets, i);
      contig_val = std::string(contig_vec.begin(), contig_vec.end());
    }

    std::vector<std::string> alleles_val;
    if (!alleles_list_offsets.empty()) {
      alleles_val =
          var_list_value(alleles, alleles_offsets, alleles_list_offsets, i);
    }

    std::vector<std::string> filters_val;
    if (!filters_list_offsets.empty()) {
      filters_val =
          var_list_value(filters, filters_offsets, filters_list_offsets, i);
    }

    bool filter_valid_val = false;
    if (!filters_bitmap.empty()) {
      size_t bitmap_index = i / 8;
      uint8_t bitmap_val = filters_bitmap[bitmap_index];
      uint8_t bit_position = i % 8;
      filter_valid_val = (bitmap_val >> bit_position) & 1;
      if (!filter_valid_val)
        filters_val.clear();
    }

    std::string info_val;
    if (!info.empty()) {
      std::vector<char> info_vec = var_value<char>(info, info_offsets, i);
      info_val = std::string(info_vec.begin(), info_vec.end());
    }

    std::string fmt_val;
    if (!fmt.empty()) {
      std::vector<char> fmt_vec = var_value<char>(fmt, fmt_offsets, i);
      fmt_val = std::string(fmt_vec.begin(), fmt_vec.end());
    }

    std::vector<int32_t> fmt_GT_val;
    if (!fmt_GT.empty())
      fmt_GT_val = var_value<int32_t>(fmt_GT, fmt_GT_offsets, i);

    int fmt_DP_val = 0;
    if (!fmt_DP.empty())
      fmt_DP_val = fmt_DP[i];

    std::vector<int32_t> fmt_PL_val;
    if (!fmt_PL.empty())
      fmt_PL_val = var_value<int32_t>(fmt_PL, fmt_PL_offsets, i);

    record vcf_record(
        sample_val,           // sample
        start_pos_val,        // start_pos
        end_pos_val,          // end_pos
        query_bed_start_val,  // query_bed_start
        query_bed_end_val,    // query_bed_end
        contig_val,           // contig
        alleles_val,          // alleles
        filters_val,          // filters
        filter_valid_val,     // filter_valid
        info_val,             // info
        fmt_val,              // fmt
        fmt_GT,               // fmt_GT
        fmt_DP_val,           // fmt_DP
        fmt_PL_val,           // fmt_PL_val
        query_bed_line_val    // query_bed_line
    );

    ret[i] = std::move(vcf_record);
  }

  return ret;
}

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

    SECTION("- V4") {
      dataset_uri = INPUT_ARRAYS_DIR_V4 + "/ingested_2samples";
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
        "sm.compute_concurrency_level=4, vfs.s3.proxy_host=abc.def.ghi";
    REQUIRE(
        tiledb_vcf_reader_set_tiledb_config(reader, config) == TILEDB_VCF_OK);
  }

  SECTION("- Invalid format") {
    const char* config =
        "sm.compute_concurrency_level=4 vfs.s3.proxy_host=abc.def.ghi";
    REQUIRE(
        tiledb_vcf_reader_set_tiledb_config(reader, config) == TILEDB_VCF_ERR);

    const char* config2 = "sm.compute_concurrency_level 4";
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

  SECTION("- V4") {
    dataset_uri = INPUT_ARRAYS_DIR_V4 + "/ingested_2samples";
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

TEST_CASE("C API: Reader get materialized attributes", "[capi][reader]") {
  tiledb_vcf_reader_t* reader = nullptr;
  REQUIRE(tiledb_vcf_reader_alloc(&reader) == TILEDB_VCF_OK);

  std::string dataset_uri;
  SECTION("- V2") {
    dataset_uri = INPUT_ARRAYS_DIR_V2 + "/ingested_2samples_GT_DP_PL";
  }

  SECTION("- V3") {
    dataset_uri = INPUT_ARRAYS_DIR_V3 + "/ingested_2samples_GT_DP_PL";
  }

  SECTION("- V4") {
    dataset_uri = INPUT_ARRAYS_DIR_V4 + "/ingested_2samples_GT_DP_PL";
  }

  REQUIRE(tiledb_vcf_reader_init(reader, dataset_uri.c_str()) == TILEDB_VCF_OK);

  int32_t count = 0;
  REQUIRE(
      tiledb_vcf_reader_get_materialized_attribute_count(reader, &count) ==
      TILEDB_VCF_OK);

  REQUIRE(count == 16);

  for (int32_t i = 0; i < count; i++) {
    char* attribute_name;
    REQUIRE(
        tiledb_vcf_reader_get_materialized_attribute_name(
            reader, i, &attribute_name) == TILEDB_VCF_OK);
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

  SECTION("- V4") {
    dataset_uri = INPUT_ARRAYS_DIR_V4 + "/ingested_2samples";
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

  SECTION("- V4") {
    dataset_uri = INPUT_ARRAYS_DIR_V4 + "/ingested_2samples";
  }

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

  SECTION("- V4") {
    dataset_uri = INPUT_ARRAYS_DIR_V4 + "/ingested_2samples";
  }

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

  SECTION("- V4") {
    version = 4;
  }

  std::string dataset_uri = INPUT_ARRAYS_DIR_V4 + "/ingested_2samples";
  if (version == 2)
    dataset_uri = INPUT_ARRAYS_DIR_V2 + "/ingested_2samples";
  else if (version == 3)
    dataset_uri = INPUT_ARRAYS_DIR_V3 + "/ingested_2samples";

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
  std::vector<record> expected_records = {
      record(
          "HG00280",
          12141,
          12277,
          12099,
          13360,
          "1",
          {"C", "<NON_REF>"},
          {},
          false,
          {'\000', '\000', '\000', '\0'},
          {'\005', '\000', '\000', '\000', 'G',    'T',    '\000', '\001',
           '\000', '\000', '\000', '\002', '\000', '\000', '\000', '\002',
           '\000', '\000', '\000', '\002', '\000', '\000', '\000', 'D',
           'P',    '\000', '\001', '\000', '\000', '\000', '\001', '\000',
           '\000', '\000', '\000', '\000', '\000', '\000', 'G',    'Q',
           '\000', '\001', '\000', '\000', '\000', '\001', '\000', '\000',
           '\000', '\000', '\000', '\000', '\000', 'M',    'I',    'N',
           '_',    'D',    'P',    '\000', '\001', '\000', '\000', '\000',
           '\001', '\000', '\000', '\000', '\000', '\000', '\000', '\000',
           'P',    'L',    '\000', '\001', '\000', '\000', '\000', '\003',
           '\000', '\000', '\000', '\000', '\000', '\000', '\000', '\000',
           '\000', '\000', '\000', '\000', '\000', '\000', '\0'},
          {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
          0),
      record(
          "HG01762",
          12141,
          12277,
          12099,
          13360,
          "1",
          {"C", "<NON_REF>"},
          {},
          false,
          {'\000', '\000', '\000', '\0'},
          {'\005', '\000', '\000', '\000', 'G',    'T',    '\000', '\001',
           '\000', '\000', '\000', '\002', '\000', '\000', '\000', '\002',
           '\000', '\000', '\000', '\002', '\000', '\000', '\000', 'D',
           'P',    '\000', '\001', '\000', '\000', '\000', '\001', '\000',
           '\000', '\000', '\000', '\000', '\000', '\000', 'G',    'Q',
           '\000', '\001', '\000', '\000', '\000', '\001', '\000', '\000',
           '\000', '\000', '\000', '\000', '\000', 'M',    'I',    'N',
           '_',    'D',    'P',    '\000', '\001', '\000', '\000', '\000',
           '\001', '\000', '\000', '\000', '\000', '\000', '\000', '\000',
           'P',    'L',    '\000', '\001', '\000', '\000', '\000', '\003',
           '\000', '\000', '\000', '\000', '\000', '\000', '\000', '\000',
           '\000', '\000', '\000', '\000', '\000', '\000', '\0'},
          {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
          0),
      record(
          "HG00280",
          12546,
          12771,
          12099,
          13360,
          "1",
          {"G", "<NON_REF>"},
          {},
          false,
          {'\000', '\000', '\000', '\0'},
          {'\005', '\000', '\000', '\000', 'G',    'T',    '\000', '\001',
           '\000', '\000', '\000', '\002', '\000', '\000', '\000', '\002',
           '\000', '\000', '\000', '\002', '\000', '\000', '\000', 'D',
           'P',    '\000', '\001', '\000', '\000', '\000', '\001', '\000',
           '\000', '\000', '\000', '\000', '\000', '\000', 'G',    'Q',
           '\000', '\001', '\000', '\000', '\000', '\001', '\000', '\000',
           '\000', '\000', '\000', '\000', '\000', 'M',    'I',    'N',
           '_',    'D',    'P',    '\000', '\001', '\000', '\000', '\000',
           '\001', '\000', '\000', '\000', '\000', '\000', '\000', '\000',
           'P',    'L',    '\000', '\001', '\000', '\000', '\000', '\003',
           '\000', '\000', '\000', '\000', '\000', '\000', '\000', '\000',
           '\000', '\000', '\000', '\000', '\000', '\000', '\0'},
          {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
          0),
      record(
          "HG01762",
          12546,
          12771,
          12099,
          13360,
          "1",
          {"G", "<NON_REF>"},
          {},
          false,
          {'\000', '\000', '\000', '\0'},
          {'\005', '\000', '\000', '\000', 'G',    'T',    '\000', '\001',
           '\000', '\000', '\000', '\002', '\000', '\000', '\000', '\002',
           '\000', '\000', '\000', '\002', '\000', '\000', '\000', 'D',
           'P',    '\000', '\001', '\000', '\000', '\000', '\001', '\000',
           '\000', '\000', '\000', '\000', '\000', '\000', 'G',    'Q',
           '\000', '\001', '\000', '\000', '\000', '\001', '\000', '\000',
           '\000', '\000', '\000', '\000', '\000', 'M',    'I',    'N',
           '_',    'D',    'P',    '\000', '\001', '\000', '\000', '\000',
           '\001', '\000', '\000', '\000', '\000', '\000', '\000', '\000',
           'P',    'L',    '\000', '\001', '\000', '\000', '\000', '\003',
           '\000', '\000', '\000', '\000', '\000', '\000', '\000', '\000',
           '\000', '\000', '\000', '\000', '\000', '\000', '\0'},
          {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
          0),
      record(
          "HG00280",
          13354,
          13374,
          12099,
          13360,
          "1",
          {"T", "<NON_REF>"},
          {std::string("LowQual")},
          true,
          {'\000', '\000', '\000', '\0'},
          {'\005', '\000', '\000', '\000', 'G',    'T',    '\000', '\001',
           '\000', '\000', '\000', '\002', '\000', '\000', '\000', '\002',
           '\000', '\000', '\000', '\002', '\000', '\000', '\000', 'D',
           'P',    '\000', '\001', '\000', '\000', '\000', '\001', '\000',
           '\000', '\000', '\017', '\000', '\000', '\000', 'G',    'Q',
           '\000', '\001', '\000', '\000', '\000', '\001', '\000', '\000',
           '\000', '*',    '\000', '\000', '\000', 'M',    'I',    'N',
           '_',    'D',    'P',    '\000', '\001', '\000', '\000', '\000',
           '\001', '\000', '\000', '\000', '\016', '\000', '\000', '\000',
           'P',    'L',    '\000', '\001', '\000', '\000', '\000', '\003',
           '\000', '\000', '\000', '\000', '\000', '\000', '\000', '\030',
           '\000', '\000', '\000', 'h',    '\001', '\000', '\0'},
          {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
          0),
      record(
          "HG01762",
          13354,
          13389,
          12099,
          13360,
          "1",
          {"T", "<NON_REF>"},
          {},
          false,
          {'\000', '\000', '\000', '\0'},
          {'\005', '\000', '\000', '\000', 'G',    'T',    '\000', '\001',
           '\000', '\000', '\000', '\002', '\000', '\000', '\000', '\002',
           '\000', '\000', '\000', '\002', '\000', '\000', '\000', 'D',
           'P',    '\000', '\001', '\000', '\000', '\000', '\001', '\000',
           '\000', '\000', '@',    '\000', '\000', '\000', 'G',    'Q',
           '\000', '\001', '\000', '\000', '\000', '\001', '\000', '\000',
           '\000', 'c',    '\000', '\000', '\000', 'M',    'I',    'N',
           '_',    'D',    'P',    '\000', '\001', '\000', '\000', '\000',
           '\001', '\000', '\000', '\000', '\036', '\000', '\000', '\000',
           'P',    'L',    '\000', '\001', '\000', '\000', '\000', '\003',
           '\000', '\000', '\000', '\000', '\000', '\000', '\000', 'B',
           '\000', '\000', '\000', '\336', '\003', '\000', '\0'},
          {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
          0),
      record(
          "HG00280",
          13452,
          13519,
          13499,
          17350,
          "1",
          {"G", "<NON_REF>"},
          {},
          false,
          {'\000', '\000', '\000', '\0'},
          {'\005', '\000', '\000', '\000', 'G',    'T',    '\000', '\001',
           '\000', '\000', '\000', '\002', '\000', '\000', '\000', '\002',
           '\000', '\000', '\000', '\002', '\000', '\000', '\000', 'D',
           'P',    '\000', '\001', '\000', '\000', '\000', '\001', '\000',
           '\000', '\000', '\n',   '\000', '\000', '\000', 'G',    'Q',
           '\000', '\001', '\000', '\000', '\000', '\001', '\000', '\000',
           '\000', '\036', '\000', '\000', '\000', 'M',    'I',    'N',
           '_',    'D',    'P',    '\000', '\001', '\000', '\000', '\000',
           '\001', '\000', '\000', '\000', '\a',   '\000', '\000', '\000',
           'P',    'L',    '\000', '\001', '\000', '\000', '\000', '\003',
           '\000', '\000', '\000', '\000', '\000', '\000', '\000', '\025',
           '\000', '\000', '\000', '\322', '\000', '\000', '\0'},
          {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
          0),
      record(
          "HG00280",
          13520,
          13544,
          13499,
          17350,
          "1",
          {"G", "<NON_REF>"},
          {},
          false,
          {'\000', '\000', '\000', '\0'},
          {'\005', '\000', '\000', '\000', 'G',    'T',    '\000', '\001',
           '\000', '\000', '\000', '\002', '\000', '\000', '\000', '\002',
           '\000', '\000', '\000', '\002', '\000', '\000', '\000', 'D',
           'P',    '\000', '\001', '\000', '\000', '\000', '\001', '\000',
           '\000', '\000', '\006', '\000', '\000', '\000', 'G',    'Q',
           '\000', '\001', '\000', '\000', '\000', '\001', '\000', '\000',
           '\000', '\f',   '\000', '\000', '\000', 'M',    'I',    'N',
           '_',    'D',    'P',    '\000', '\001', '\000', '\000', '\000',
           '\001', '\000', '\000', '\000', '\004', '\000', '\000', '\000',
           'P',    'L',    '\000', '\001', '\000', '\000', '\000', '\003',
           '\000', '\000', '\000', '\000', '\000', '\000', '\000', '\006',
           '\000', '\000', '\000', 'Z',    '\000', '\000', '\0'},
          {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
          0),
      record(
          "HG00280",
          13545,
          13689,
          13499,
          17350,
          "1",
          {"G", "<NON_REF>"},
          {},
          false,
          {'\000', '\000', '\000', '\0'},
          {'\005', '\000', '\000', '\000', 'G',    'T',    '\000', '\001',
           '\000', '\000', '\000', '\002', '\000', '\000', '\000', '\002',
           '\000', '\000', '\000', '\002', '\000', '\000', '\000', 'D',
           'P',    '\000', '\001', '\000', '\000', '\000', '\001', '\000',
           '\000', '\000', '\000', '\000', '\000', '\000', 'G',    'Q',
           '\000', '\001', '\000', '\000', '\000', '\001', '\000', '\000',
           '\000', '\000', '\000', '\000', '\000', 'M',    'I',    'N',
           '_',    'D',    'P',    '\000', '\001', '\000', '\000', '\000',
           '\001', '\000', '\000', '\000', '\000', '\000', '\000', '\000',
           'P',    'L',    '\000', '\001', '\000', '\000', '\000', '\003',
           '\000', '\000', '\000', '\000', '\000', '\000', '\000', '\000',
           '\000', '\000', '\000', '\000', '\000', '\000', '\0'},
          {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
          0),
      record(
          "HG00280",
          17319,
          17479,
          13499,
          17350,
          "1",
          {"T", "<NON_REF>"},
          {},
          false,
          {'\000', '\000', '\000', '\0'},
          {'\005', '\000', '\000', '\000', 'G',    'T',    '\000', '\001',
           '\000', '\000', '\000', '\002', '\000', '\000', '\000', '\002',
           '\000', '\000', '\000', '\002', '\000', '\000', '\000', 'D',
           'P',    '\000', '\001', '\000', '\000', '\000', '\001', '\000',
           '\000', '\000', '\000', '\000', '\000', '\000', 'G',    'Q',
           '\000', '\001', '\000', '\000', '\000', '\001', '\000', '\000',
           '\000', '\000', '\000', '\000', '\000', 'M',    'I',    'N',
           '_',    'D',    'P',    '\000', '\001', '\000', '\000', '\000',
           '\001', '\000', '\000', '\000', '\000', '\000', '\000', '\000',
           'P',    'L',    '\000', '\001', '\000', '\000', '\000', '\003',
           '\000', '\000', '\000', '\000', '\000', '\000', '\000', '\000',
           '\000', '\000', '\000', '\000', '\000', '\000', '\0'},
          {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
          0)};

  std::vector<record> records = build_records(
      num_records,
      sample_name,
      sample_name_offsets,
      pos_start,
      pos_end,
      query_bed_start,
      query_bed_end,
      contig,
      contig_offsets,
      alleles,
      alleles_offsets,
      alleles_list_offsets,
      filters,
      filters_offsets,
      filters_list_offsets,
      filters_bitmap,
      info,
      info_offsets,
      format,
      format_offsets,
      fmt_GT,
      fmt_GT_offsets,
      {});
  REQUIRE_THAT(expected_records, Catch::Matchers::UnorderedEquals(records));

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
  SECTION("- V4") {
    version = 4;
  }

  std::string dataset_uri = INPUT_ARRAYS_DIR_V4 + "/ingested_2samples_GT_DP_PL";
  if (version == 2)
    dataset_uri = INPUT_ARRAYS_DIR_V2 + "/ingested_2samples_GT_DP_PL";
  else if (version == 3)
    dataset_uri = INPUT_ARRAYS_DIR_V3 + "/ingested_2samples_GT_DP_PL";

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
  SET_BUFF_FMT_PL(reader, expected_num_records);

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
  std::vector<record> expected_records = {
      record(
          "HG00280",
          12141,
          12277,
          12099,
          13360,
          "1",
          {"C", "<NON_REF>"},
          {},
          false,
          {'\000', '\000', '\000', '\0'},
          {'\002', '\000', '\000', '\000', 'G',    'Q',    '\000', '\001',
           '\000', '\000', '\000', '\001', '\000', '\000', '\000', '\000',
           '\000', '\000', '\000', 'M',    'I',    'N',    '_',    'D',
           'P',    '\000', '\001', '\000', '\000', '\000', '\001', '\000',
           '\000', '\000', '\000', '\000', '\000', '\0'},
          {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
          0,
          {0, 0, 0}),
      record(
          "HG01762",
          12141,
          12277,
          12099,
          13360,
          "1",
          {"C", "<NON_REF>"},
          {},
          false,
          {'\000', '\000', '\000', '\0'},
          {'\002', '\000', '\000', '\000', 'G',    'Q',    '\000', '\001',
           '\000', '\000', '\000', '\001', '\000', '\000', '\000', '\000',
           '\000', '\000', '\000', 'M',    'I',    'N',    '_',    'D',
           'P',    '\000', '\001', '\000', '\000', '\000', '\001', '\000',
           '\000', '\000', '\000', '\000', '\000', '\0'},
          {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
          0,
          {0, 0, 0}),
      record(
          "HG00280",
          12546,
          12771,
          12099,
          13360,
          "1",
          {"G", "<NON_REF>"},
          {},
          false,
          {'\000', '\000', '\000', '\0'},
          {'\002', '\000', '\000', '\000', 'G',    'Q',    '\000', '\001',
           '\000', '\000', '\000', '\001', '\000', '\000', '\000', '\000',
           '\000', '\000', '\000', 'M',    'I',    'N',    '_',    'D',
           'P',    '\000', '\001', '\000', '\000', '\000', '\001', '\000',
           '\000', '\000', '\000', '\000', '\000', '\0'},
          {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
          0,
          {0, 0, 0}),
      record(
          "HG01762",
          12546,
          12771,
          12099,
          13360,
          "1",
          {"G", "<NON_REF>"},
          {},
          false,
          {'\000', '\000', '\000', '\0'},
          {'\002', '\000', '\000', '\000', 'G',    'Q',    '\000', '\001',
           '\000', '\000', '\000', '\001', '\000', '\000', '\000', '\000',
           '\000', '\000', '\000', 'M',    'I',    'N',    '_',    'D',
           'P',    '\000', '\001', '\000', '\000', '\000', '\001', '\000',
           '\000', '\000', '\000', '\000', '\000', '\0'},
          {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
          0,
          {0, 0, 0}),
      record(
          "HG00280",
          13354,
          13374,
          12099,
          13360,
          "1",
          {"T", "<NON_REF>"},
          {std::string("LowQual")},
          true,
          {'\000', '\000', '\000', '\0'},
          {'\002', '\000', '\000', '\000', 'G',    'Q',    '\000', '\001',
           '\000', '\000', '\000', '\001', '\000', '\000', '\000', '*',
           '\000', '\000', '\000', 'M',    'I',    'N',    '_',    'D',
           'P',    '\000', '\001', '\000', '\000', '\000', '\001', '\000',
           '\000', '\000', '\016', '\000', '\000', '\0'},
          {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
          15,
          {0, 24, 360}),
      record(
          "HG01762",
          13354,
          13389,
          12099,
          13360,
          "1",
          {"T", "<NON_REF>"},
          {},
          false,
          {'\000', '\000', '\000', '\0'},
          {'\002', '\000', '\000', '\000', 'G',    'Q',    '\000', '\001',
           '\000', '\000', '\000', '\001', '\000', '\000', '\000', 'c',
           '\000', '\000', '\000', 'M',    'I',    'N',    '_',    'D',
           'P',    '\000', '\001', '\000', '\000', '\000', '\001', '\000',
           '\000', '\000', '\036', '\000', '\000', '\0'},
          {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
          64,
          {0, 66, 990}),
      record(
          "HG00280",
          13452,
          13519,
          13499,
          17350,
          "1",
          {"G", "<NON_REF>"},
          {},
          false,
          {'\000', '\000', '\000', '\0'},
          {'\002', '\000', '\000', '\000', 'G',    'Q',    '\000', '\001',
           '\000', '\000', '\000', '\001', '\000', '\000', '\000', '\036',
           '\000', '\000', '\000', 'M',    'I',    'N',    '_',    'D',
           'P',    '\000', '\001', '\000', '\000', '\000', '\001', '\000',
           '\000', '\000', '\a',   '\000', '\000', '\0'},
          {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
          10,
          {0, 21, 210}),
      record(
          "HG00280",
          13520,
          13544,
          13499,
          17350,
          "1",
          {"G", "<NON_REF>"},
          {},
          false,
          {'\000', '\000', '\000', '\0'},
          {'\002', '\000', '\000', '\000', 'G',    'Q',    '\000', '\001',
           '\000', '\000', '\000', '\001', '\000', '\000', '\000', '\f',
           '\000', '\000', '\000', 'M',    'I',    'N',    '_',    'D',
           'P',    '\000', '\001', '\000', '\000', '\000', '\001', '\000',
           '\000', '\000', '\004', '\000', '\000', '\0'},
          {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
          6,
          {0, 6, 90}),
      record(
          "HG00280",
          13545,
          13689,
          13499,
          17350,
          "1",
          {"G", "<NON_REF>"},
          {},
          false,
          {'\000', '\000', '\000', '\0'},
          {'\002', '\000', '\000', '\000', 'G',    'Q',    '\000', '\001',
           '\000', '\000', '\000', '\001', '\000', '\000', '\000', '\000',
           '\000', '\000', '\000', 'M',    'I',    'N',    '_',    'D',
           'P',    '\000', '\001', '\000', '\000', '\000', '\001', '\000',
           '\000', '\000', '\000', '\000', '\000', '\0'},
          {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
          0,
          {0, 0, 0}),
      record(
          "HG00280",
          17319,
          17479,
          13499,
          17350,
          "1",
          {"T", "<NON_REF>"},
          {},
          false,
          {'\000', '\000', '\000', '\0'},
          {'\002', '\000', '\000', '\000', 'G',    'Q',    '\000', '\001',
           '\000', '\000', '\000', '\001', '\000', '\000', '\000', '\000',
           '\000', '\000', '\000', 'M',    'I',    'N',    '_',    'D',
           'P',    '\000', '\001', '\000', '\000', '\000', '\001', '\000',
           '\000', '\000', '\000', '\000', '\000', '\0'},
          {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
          0,
          {0, 0, 0})};

  std::vector<record> records = build_records(
      num_records,
      sample_name,
      sample_name_offsets,
      pos_start,
      pos_end,
      query_bed_start,
      query_bed_end,
      contig,
      contig_offsets,
      alleles,
      alleles_offsets,
      alleles_list_offsets,
      filters,
      filters_offsets,
      filters_list_offsets,
      filters_bitmap,
      info,
      info_offsets,
      format,
      format_offsets,
      fmt_GT,
      fmt_GT_offsets,
      fmt_DP,
      fmt_PL,
      fmt_PL_offsets);

  REQUIRE_THAT(expected_records, Catch::Matchers::UnorderedEquals(records));

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

  SECTION("- V4") {
    dataset_uri = INPUT_ARRAYS_DIR_V4 + "/ingested_2samples_GT_DP_PL";
  }

  REQUIRE(tiledb_vcf_reader_init(reader, dataset_uri.c_str()) == TILEDB_VCF_OK);

  // Set up samples and ranges
  const char* samples = "HG01762,HG00280";
  REQUIRE(tiledb_vcf_reader_set_samples(reader, samples) == TILEDB_VCF_OK);
  const char* regions = "1:12100-13360,1:13500-17350";
  REQUIRE(tiledb_vcf_reader_set_regions(reader, regions) == TILEDB_VCF_OK);

  // Allocate and set buffers
  const unsigned expected_num_records = 10;
  SET_BUFF_POS_START(reader, expected_num_records);
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
  std::vector<record> expected_records = {
      record(
          "HG00280",
          12141,
          12277,
          0,
          0,
          "",
          {"C", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          0,
          {}),
      record(
          "HG01762",
          12141,
          12277,
          0,
          0,
          "",
          {"C", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          0,
          {}),
      record(
          "HG00280",
          12546,
          12771,
          0,
          0,
          "",
          {"G", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          0,
          {}),
      record(
          "HG01762",
          12546,
          12771,
          0,
          0,
          "",
          {"G", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          0,
          {}),
      record(
          "HG00280",
          13354,
          13374,
          0,
          0,
          "",
          {"T", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          15,
          {}),
      record(
          "HG01762",
          13354,
          13389,
          0,
          0,
          "",
          {"T", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          64,
          {}),
      record(
          "HG00280",
          13452,
          13519,
          0,
          0,
          "",
          {"G", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          10,
          {}),
      record(
          "HG00280",
          13520,
          13544,
          0,
          0,
          "",
          {"G", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          6,
          {}),
      record(
          "HG00280",
          13545,
          13689,
          0,
          0,
          "",
          {"G", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          0,
          {}),
      record(
          "HG00280",
          17319,
          17479,
          0,
          0,
          "",
          {"T", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          0,
          {})};

  std::vector<record> records = build_records(
      num_records,
      sample_name,
      sample_name_offsets,
      pos_start,
      pos_end,
      {},
      {},
      {},
      {},
      alleles,
      alleles_offsets,
      alleles_list_offsets,
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      fmt_DP,
      {},
      {});

  REQUIRE_THAT(expected_records, Catch::Matchers::UnorderedEquals(records));

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

  SECTION("- V4") {
    dataset_uri = INPUT_ARRAYS_DIR_V4 + "/ingested_2samples_GT_DP_PL";
  }
  REQUIRE(tiledb_vcf_reader_init(reader, dataset_uri.c_str()) == TILEDB_VCF_OK);

  // Set up ranges (defaulting to all samples)
  const char* regions = "1:12100-13100";
  REQUIRE(tiledb_vcf_reader_set_regions(reader, regions) == TILEDB_VCF_OK);

  // Allocate and set buffers
  const unsigned expected_num_records = 4;
  SET_BUFF_POS_START(reader, expected_num_records);
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
  std::vector<record> expected_records = {
      record(
          "HG00280",
          12141,
          12277,
          0,
          0,
          "",
          {"C", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          0,
          {}),
      record(
          "HG01762",
          12141,
          12277,
          0,
          0,
          "",
          {"C", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          0,
          {}),
      record(
          "HG00280",
          12546,
          12771,
          0,
          0,
          "",
          {"G", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          0,
          {}),
      record(
          "HG01762",
          12546,
          12771,
          0,
          0,
          "",
          {"G", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          0,
          {})};

  std::vector<record> records = build_records(
      num_records,
      sample_name,
      sample_name_offsets,
      pos_start,
      pos_end,
      {},
      {},
      {},
      {},
      alleles,
      alleles_offsets,
      alleles_list_offsets,
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      fmt_DP,
      {},
      {});

  REQUIRE_THAT(expected_records, Catch::Matchers::UnorderedEquals(records));

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

  SECTION("- V4") {
    dataset_uri = INPUT_ARRAYS_DIR_V4 + "/ingested_2samples_GT_DP_PL";
  }
  REQUIRE(tiledb_vcf_reader_init(reader, dataset_uri.c_str()) == TILEDB_VCF_OK);

  // Set up samples and ranges
  auto bed_uri = TILEDB_VCF_TEST_INPUT_DIR + std::string("/simple.bed");
  REQUIRE(
      tiledb_vcf_reader_set_bed_file(reader, bed_uri.c_str()) == TILEDB_VCF_OK);

  // Allocate and set buffers
  const unsigned expected_num_records = 10;
  SET_BUFF_POS_START(reader, expected_num_records);
  SET_BUFF_POS_END(reader, expected_num_records);
  SET_BUFF_SAMPLE_NAME(reader, expected_num_records);
  SET_BUFF_ALLELES(reader, expected_num_records);
  SET_BUFF_FMT_DP(reader, expected_num_records);
  SET_BUFF_QUERY_BED_START(reader, expected_num_records);
  SET_BUFF_QUERY_BED_END(reader, expected_num_records);
  SET_BUFF_QUERY_BED_LINE(reader, expected_num_records);

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
  std::vector<record> expected_records = {
      record(
          "HG00280",
          12141,
          12277,
          12099,
          13360,
          "",
          {"C", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          0,
          {},
          0),
      record(
          "HG01762",
          12141,
          12277,
          12099,
          13360,
          "",
          {"C", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          0,
          {},
          0),
      record(
          "HG00280",
          12546,
          12771,
          12099,
          13360,
          "",
          {"G", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          0,
          {},
          0),
      record(
          "HG01762",
          12546,
          12771,
          12099,
          13360,
          "",
          {"G", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          0,
          {},
          0),
      record(
          "HG00280",
          13354,
          13374,
          12099,
          13360,
          "",
          {"T", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          15,
          {},
          0),
      record(
          "HG01762",
          13354,
          13389,
          12099,
          13360,
          "",
          {"T", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          64,
          {},
          0),
      record(
          "HG00280",
          13452,
          13519,
          13499,
          17350,
          "",
          {"G", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          10,
          {},
          1),
      record(
          "HG00280",
          13520,
          13544,
          13499,
          17350,
          "",
          {"G", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          6,
          {},
          1),
      record(
          "HG00280",
          13545,
          13689,
          13499,
          17350,
          "",
          {"G", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          0,
          {},
          1),
      record(
          "HG00280",
          17319,
          17479,
          13499,
          17350,
          "",
          {"T", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          0,
          {},
          1)};

  std::vector<record> records = build_records(
      num_records,
      sample_name,
      sample_name_offsets,
      pos_start,
      pos_end,
      query_bed_start,
      query_bed_end,
      {},
      {},
      alleles,
      alleles_offsets,
      alleles_list_offsets,
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      fmt_DP,
      {},
      {},
      query_bed_line);

  REQUIRE_THAT(expected_records, Catch::Matchers::UnorderedEquals(records));

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

  SECTION("- V4") {
    dataset_uri = INPUT_ARRAYS_DIR_V4 + "/ingested_2samples_GT_DP_PL";
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
  SET_BUFF_POS_START(reader, expected_num_records);
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
  std::vector<record> expected_records = {
      record(
          "HG00280",
          12141,
          12277,
          0,
          0,
          "",
          {"C", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          0,
          {}),
      record(
          "HG01762",
          12141,
          12277,
          0,
          0,
          "",
          {"C", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          0,
          {}),
      record(
          "HG00280",
          12546,
          12771,
          0,
          0,
          "",
          {"G", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          0,
          {}),
      record(
          "HG01762",
          12546,
          12771,
          0,
          0,
          "",
          {"G", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          0,
          {}),
      record(
          "HG00280",
          13354,
          13374,
          0,
          0,
          "",
          {"T", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          15,
          {}),
      record(
          "HG01762",
          13354,
          13389,
          0,
          0,
          "",
          {"T", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          64,
          {}),
      record(
          "HG00280",
          13452,
          13519,
          0,
          0,
          "",
          {"G", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          10,
          {}),
      record(
          "HG00280",
          13520,
          13544,
          0,
          0,
          "",
          {"G", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          6,
          {}),
      record(
          "HG00280",
          13545,
          13689,
          0,
          0,
          "",
          {"G", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          0,
          {}),
      record(
          "HG00280",
          17319,
          17479,
          0,
          0,
          "",
          {"T", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          0,
          {})};

  std::vector<record> records = build_records(
      num_records,
      sample_name,
      sample_name_offsets,
      pos_start,
      pos_end,
      {},
      {},
      {},
      {},
      alleles,
      alleles_offsets,
      alleles_list_offsets,
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      fmt_DP,
      {},
      {});

  REQUIRE_THAT(expected_records, Catch::Matchers::UnorderedEquals(records));

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

  SECTION("- V4") {
    dataset_uri = INPUT_ARRAYS_DIR_V4 + "/ingested_2samples_GT_DP_PL";
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

  SECTION("- V4") {
    dataset_uri = INPUT_ARRAYS_DIR_V4 + "/ingested_2samples_GT_DP_PL";
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
  SET_BUFF_ALLELES(reader, alloced_num_records);
  SET_BUFF_FMT_DP(reader, alloced_num_records);

  std::vector<record> expected_records = {
      record(
          "HG00280",
          12141,
          12277,
          12099,
          13360,
          "",
          {"C", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          0,
          {}),
      record(
          "HG01762",
          12141,
          12277,
          12099,
          13360,
          "",
          {"C", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          0,
          {}),
      record(
          "HG00280",
          12546,
          12771,
          12099,
          13360,
          "",
          {"G", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          0,
          {}),
      record(
          "HG01762",
          12546,
          12771,
          12099,
          13360,
          "",
          {"G", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          0,
          {}),
      record(
          "HG00280",
          13354,
          13374,
          12099,
          13360,
          "",
          {"T", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          15,
          {}),
      record(
          "HG01762",
          13354,
          13389,
          12099,
          13360,
          "",
          {"T", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          64,
          {}),
      record(
          "HG00280",
          13452,
          13519,
          13499,
          16000,
          "",
          {"G", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          10,
          {}),
      record(
          "HG00280",
          13520,
          13544,
          13499,
          16000,
          "",
          {"G", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          6,
          {}),
      record(
          "HG00280",
          13545,
          13689,
          13499,
          16000,
          "",
          {"G", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          0,
          {})};

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
  std::vector<record> records = build_records(
      num_records,
      sample_name,
      sample_name_offsets,
      pos_start,
      pos_end,
      query_bed_start,
      query_bed_end,
      {},
      {},
      alleles,
      alleles_offsets,
      alleles_list_offsets,
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      fmt_DP,
      {},
      {});

  REQUIRE_THAT(expected_records, Catch::Matchers::Contains(records));

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
  records = build_records(
      num_records,
      sample_name,
      sample_name_offsets,
      pos_start,
      pos_end,
      query_bed_start,
      query_bed_end,
      {},
      {},
      alleles,
      alleles_offsets,
      alleles_list_offsets,
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      fmt_DP,
      {},
      {});

  REQUIRE_THAT(expected_records, Catch::Matchers::Contains(records));

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
  records = build_records(
      num_records,
      sample_name,
      sample_name_offsets,
      pos_start,
      pos_end,
      query_bed_start,
      query_bed_end,
      {},
      {},
      alleles,
      alleles_offsets,
      alleles_list_offsets,
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      fmt_DP,
      {},
      {});

  REQUIRE_THAT(expected_records, Catch::Matchers::Contains(records));

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
  records = build_records(
      num_records,
      sample_name,
      sample_name_offsets,
      pos_start,
      pos_end,
      query_bed_start,
      query_bed_end,
      {},
      {},
      alleles,
      alleles_offsets,
      alleles_list_offsets,
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      fmt_DP,
      {},
      {});

  REQUIRE_THAT(expected_records, Catch::Matchers::Contains(records));

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
  records = build_records(
      num_records,
      sample_name,
      sample_name_offsets,
      pos_start,
      pos_end,
      query_bed_start,
      query_bed_end,
      {},
      {},
      alleles,
      alleles_offsets,
      alleles_list_offsets,
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      fmt_DP,
      {},
      {});

  REQUIRE_THAT(expected_records, Catch::Matchers::Contains(records));

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

  SECTION("- V4") {
    dataset_uri = INPUT_ARRAYS_DIR_V4 + "/ingested_2samples_GT_DP_PL";
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
  SET_BUFF_ALLELES(reader, alloced_num_records);
  SET_BUFF_FMT_DP(reader, alloced_num_records);

  std::vector<record> expected_records = {
      record(
          "HG00280",
          12141,
          12277,
          12276,
          13400,
          "",
          {"C", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          0,
          {}),
      record(
          "HG01762",
          12141,
          12277,
          12276,
          13400,
          "",
          {"C", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          0,
          {}),
      record(
          "HG00280",
          12546,
          12771,
          12276,
          13400,
          "",
          {"G", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          0,
          {}),
      record(
          "HG01762",
          12546,
          12771,
          12276,
          13400,
          "",
          {"G", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          0,
          {}),
      record(
          "HG00280",
          13354,
          13374,
          12276,
          13400,
          "",
          {"T", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          15,
          {}),
      record(
          "HG01762",
          13354,
          13389,
          12276,
          13400,
          "",
          {"T", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          64,
          {}),
      record(
          "HG00280",
          13375,
          13395,
          12276,
          13400,
          "",
          {"G", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          6,
          {}),
      record(
          "HG00280",
          13396,
          13413,
          12276,
          13400,
          "",
          {"T", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          2,
          {}),
      record(
          "HG00280",
          17319,
          17479,
          16999,
          18000,
          "",
          {"T", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          0,
          {}),
      record(
          "HG00280",
          17480,
          17486,
          16999,
          18000,
          "",
          {"A", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          3,
          {})};

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
  std::vector<record> records = build_records(
      num_records,
      sample_name,
      sample_name_offsets,
      pos_start,
      pos_end,
      query_bed_start,
      query_bed_end,
      {},
      {},
      alleles,
      alleles_offsets,
      alleles_list_offsets,
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      fmt_DP,
      {},
      {});

  REQUIRE_THAT(expected_records, Catch::Matchers::Contains(records));

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
  records = build_records(
      num_records,
      sample_name,
      sample_name_offsets,
      pos_start,
      pos_end,
      query_bed_start,
      query_bed_end,
      {},
      {},
      alleles,
      alleles_offsets,
      alleles_list_offsets,
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      fmt_DP,
      {},
      {});

  REQUIRE_THAT(expected_records, Catch::Matchers::Contains(records));

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
  records = build_records(
      num_records,
      sample_name,
      sample_name_offsets,
      pos_start,
      pos_end,
      query_bed_start,
      query_bed_end,
      {},
      {},
      alleles,
      alleles_offsets,
      alleles_list_offsets,
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      fmt_DP,
      {},
      {});

  REQUIRE_THAT(expected_records, Catch::Matchers::Contains(records));

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
  records = build_records(
      num_records,
      sample_name,
      sample_name_offsets,
      pos_start,
      pos_end,
      query_bed_start,
      query_bed_end,
      {},
      {},
      alleles,
      alleles_offsets,
      alleles_list_offsets,
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      fmt_DP,
      {},
      {});

  REQUIRE_THAT(expected_records, Catch::Matchers::Contains(records));

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
  records = build_records(
      num_records,
      sample_name,
      sample_name_offsets,
      pos_start,
      pos_end,
      query_bed_start,
      query_bed_end,
      {},
      {},
      alleles,
      alleles_offsets,
      alleles_list_offsets,
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      fmt_DP,
      {},
      {});

  REQUIRE_THAT(expected_records, Catch::Matchers::Contains(records));

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

  SECTION("- V4") {
    dataset_uri = INPUT_ARRAYS_DIR_V4 + "/ingested_2samples";
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

  SECTION("- V4") {
    dataset_uri = INPUT_ARRAYS_DIR_V4 + "/ingested_2samples_GT_DP_PL";
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
  SET_BUFF_ALLELES(reader, alloced_num_records);
  SET_BUFF_FMT_DP(reader, alloced_num_records);

  std::vector<record> expected_records = {
      record(
          "HG00280",
          12141,
          12277,
          12099,
          13360,
          "",
          {"C", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          0,
          {}),
      record(
          "HG01762",
          12141,
          12277,
          12099,
          13360,
          "",
          {"C", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          0,
          {}),
      record(
          "HG00280",
          12546,
          12771,
          12099,
          13360,
          "",
          {"G", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          0,
          {}),
      record(
          "HG01762",
          13354,
          13389,
          12099,
          13360,
          "",
          {"T", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          64,
          {}),
      record(
          "HG00280",
          13354,
          13374,
          12099,
          13360,
          "",
          {"T", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          15,
          {})};

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
  std::vector<record> records = build_records(
      num_records,
      sample_name,
      sample_name_offsets,
      pos_start,
      pos_end,
      query_bed_start,
      query_bed_end,
      {},
      {},
      alleles,
      alleles_offsets,
      alleles_list_offsets,
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      fmt_DP,
      {},
      {});

  REQUIRE_THAT(expected_records, Catch::Matchers::Contains(records));

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
  records = build_records(
      num_records,
      sample_name,
      sample_name_offsets,
      pos_start,
      pos_end,
      query_bed_start,
      query_bed_end,
      {},
      {},
      alleles,
      alleles_offsets,
      alleles_list_offsets,
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      fmt_DP,
      {},
      {});

  REQUIRE_THAT(expected_records, Catch::Matchers::Contains(records));

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

  SECTION("- V4") {
    dataset_uri = INPUT_ARRAYS_DIR_V4 + "/ingested_2samples_GT_DP_PL";
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

  std::vector<record> expected_records = {
      record(
          "HG00280", 12141, 12277, 0, 0, "", {}, {}, false, {}, {}, {}, 0, {}),
      record(
          "HG01762", 12141, 12277, 0, 0, "", {}, {}, false, {}, {}, {}, 0, {}),
      record(
          "HG00280", 12546, 12771, 0, 0, "", {}, {}, false, {}, {}, {}, 0, {}),
      record(
          "HG01762", 12546, 12771, 0, 0, "", {}, {}, false, {}, {}, {}, 0, {}),
      record(
          "HG00280", 13354, 13374, 0, 0, "", {}, {}, false, {}, {}, {}, 0, {}),
      record(
          "HG01762", 13354, 13389, 0, 0, "", {}, {}, false, {}, {}, {}, 0, {}),
      record(
          "HG00280", 17319, 17479, 0, 0, "", {}, {}, false, {}, {}, {}, 0, {}),
      record(
          "HG00280", 17480, 17486, 0, 0, "", {}, {}, false, {}, {}, {}, 0, {}),
      record(
          "HG00280", 13452, 13519, 0, 0, "", {}, {}, false, {}, {}, {}, 0, {}),
      record(
          "HG00280", 13520, 13544, 0, 0, "", {}, {}, false, {}, {}, {}, 0, {}),
      record(
          "HG00280", 13545, 13689, 0, 0, "", {}, {}, false, {}, {}, {}, 0, {}),
  };

  // Allocate and set buffers
  const unsigned allocated_num_records = 10;
  std::vector<uint32_t> pos_start0(allocated_num_records);
  std::vector<uint32_t> pos_end0(allocated_num_records);
  std::vector<int32_t> sample_name_offsets0(allocated_num_records + 1);
  std::vector<char> sample_name0(allocated_num_records * 10);
  std::vector<uint32_t> pos_start1(allocated_num_records);
  std::vector<uint32_t> pos_end1(allocated_num_records);
  std::vector<int32_t> sample_name_offsets1(allocated_num_records + 1);
  std::vector<char> sample_name1(allocated_num_records * 10);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_values(
          reader0,
          "pos_start",
          sizeof(uint32_t) * pos_start0.size(),
          pos_start0.data()) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_values(
          reader0,
          "pos_end",
          sizeof(uint32_t) * pos_end0.size(),
          pos_end0.data()) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_values(
          reader0,
          "sample_name",
          sizeof(char) * sample_name0.size(),
          sample_name0.data()) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_offsets(
          reader0,
          "sample_name",
          sizeof(int32_t) * sample_name_offsets0.size(),
          sample_name_offsets0.data()) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_values(
          reader1,
          "pos_start",
          sizeof(uint32_t) * pos_start1.size(),
          pos_start1.data()) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_values(
          reader1,
          "pos_end",
          sizeof(uint32_t) * pos_end1.size(),
          pos_end1.data()) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_values(
          reader1,
          "sample_name",
          sizeof(char) * sample_name1.size(),
          sample_name1.data()) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_offsets(
          reader1,
          "sample_name",
          sizeof(int32_t) * sample_name_offsets1.size(),
          sample_name_offsets1.data()) == TILEDB_VCF_OK);

  int64_t num_records0 = ~0;
  int64_t num_records1 = ~0;
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader0, &num_records0) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records0 == 0);

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
      tiledb_vcf_reader_get_result_num_records(reader0, &num_records0) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records0 == 10);
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader1, &num_records1) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records1 == 1);

  // Check results
  std::vector<record> records0 = build_records(
      num_records0,
      sample_name0,
      sample_name_offsets0,
      pos_start0,
      pos_end0,
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {});

  std::vector<record> records1 = build_records(
      num_records1,
      sample_name1,
      sample_name_offsets1,
      pos_start1,
      pos_end1,
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {});

  REQUIRE_THAT(expected_records, Catch::Matchers::Contains(records0));

  REQUIRE_THAT(expected_records, Catch::Matchers::Contains(records1));

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

  SECTION("- V4") {
    dataset_uri = INPUT_ARRAYS_DIR_V4 + "/ingested_2samples_GT_DP_PL";
  }
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

  std::vector<record> expected_records = {
      record(
          "HG00280", 12141, 12277, 0, 0, "", {}, {}, false, {}, {}, {}, 0, {}),
      record(
          "HG01762", 12141, 12277, 0, 0, "", {}, {}, false, {}, {}, {}, 0, {}),
      record(
          "HG00280", 12546, 12771, 0, 0, "", {}, {}, false, {}, {}, {}, 0, {}),
      record(
          "HG01762", 12546, 12771, 0, 0, "", {}, {}, false, {}, {}, {}, 0, {}),
      record(
          "HG00280", 13354, 13374, 0, 0, "", {}, {}, false, {}, {}, {}, 0, {}),
      record(
          "HG01762", 13354, 13389, 0, 0, "", {}, {}, false, {}, {}, {}, 0, {}),
      record(
          "HG00280", 13375, 13395, 0, 0, "", {}, {}, false, {}, {}, {}, 0, {}),
      record(
          "HG00280", 13396, 13413, 0, 0, "", {}, {}, false, {}, {}, {}, 0, {}),
      record(
          "HG00280", 17319, 17479, 0, 0, "", {}, {}, false, {}, {}, {}, 0, {}),
      record(
          "HG00280", 17480, 17486, 0, 0, "", {}, {}, false, {}, {}, {}, 0, {})};

  // Allocate and set buffers
  const unsigned allocated_num_records = 10;
  std::vector<uint32_t> pos_start0(allocated_num_records);
  std::vector<uint32_t> pos_end0(allocated_num_records);
  std::vector<int32_t> sample_name_offsets0(allocated_num_records + 1);
  std::vector<char> sample_name0(allocated_num_records * 10);
  std::vector<uint32_t> pos_start1(allocated_num_records);
  std::vector<uint32_t> pos_end1(allocated_num_records);
  std::vector<int32_t> sample_name_offsets1(allocated_num_records + 1);
  std::vector<char> sample_name1(allocated_num_records * 10);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_values(
          reader0,
          "pos_start",
          sizeof(uint32_t) * pos_start0.size(),
          pos_start0.data()) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_values(
          reader0,
          "pos_end",
          sizeof(uint32_t) * pos_end0.size(),
          pos_end0.data()) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_values(
          reader0,
          "sample_name",
          sizeof(char) * sample_name0.size(),
          sample_name0.data()) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_offsets(
          reader0,
          "sample_name",
          sizeof(int32_t) * sample_name_offsets0.size(),
          sample_name_offsets0.data()) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_values(
          reader1,
          "pos_start",
          sizeof(uint32_t) * pos_start1.size(),
          pos_start1.data()) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_values(
          reader1,
          "pos_end",
          sizeof(uint32_t) * pos_end1.size(),
          pos_end1.data()) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_values(
          reader1,
          "sample_name",
          sizeof(char) * sample_name1.size(),
          sample_name1.data()) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_offsets(
          reader1,
          "sample_name",
          sizeof(int32_t) * sample_name_offsets1.size(),
          sample_name_offsets1.data()) == TILEDB_VCF_OK);

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

  // Check result size
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader0, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 8);
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader1, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 3);

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
  std::vector<record> records0 = build_records(
      num_records,
      sample_name0,
      sample_name_offsets0,
      pos_start0,
      pos_end0,
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {});

  REQUIRE_THAT(expected_records, Catch::Matchers::Contains(records0));

  std::vector<record> records1 = build_records(
      num_records,
      sample_name1,
      sample_name_offsets1,
      pos_start1,
      pos_end1,
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {});

  REQUIRE_THAT(expected_records, Catch::Matchers::Contains(records1));

  tiledb_vcf_reader_free(&reader0);
  tiledb_vcf_reader_free(&reader1);
}

TEST_CASE(
    "C API: Reader submit (partitioned samples, fetch headers)",
    "[capi][reader]") {
  tiledb_vcf_reader_t *reader0 = nullptr, *reader1 = nullptr;
  REQUIRE(tiledb_vcf_reader_alloc(&reader0) == TILEDB_VCF_OK);
  REQUIRE(tiledb_vcf_reader_alloc(&reader1) == TILEDB_VCF_OK);
  std::string dataset_uri;
  SECTION("- V2") {
    dataset_uri = INPUT_ARRAYS_DIR_V2 + "/ingested_2samples";
  }

  SECTION("- V3") {
    dataset_uri = INPUT_ARRAYS_DIR_V3 + "/ingested_2samples";
  }

  SECTION("- V4") {
    dataset_uri = INPUT_ARRAYS_DIR_V4 + "/ingested_2samples";
  }
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

  std::vector<record> expected_records = {
      record(
          "HG00280", 12141, 12277, 0, 0, "", {}, {}, false, {}, {}, {}, 0, {}),
      record(
          "HG01762", 12141, 12277, 0, 0, "", {}, {}, false, {}, {}, {}, 0, {}),
      record(
          "HG00280", 12546, 12771, 0, 0, "", {}, {}, false, {}, {}, {}, 0, {}),
      record(
          "HG01762", 12546, 12771, 0, 0, "", {}, {}, false, {}, {}, {}, 0, {}),
      record(
          "HG00280", 13354, 13374, 0, 0, "", {}, {}, false, {}, {}, {}, 0, {}),
      record(
          "HG01762", 13354, 13389, 0, 0, "", {}, {}, false, {}, {}, {}, 0, {}),
      record(
          "HG00280", 13375, 13395, 0, 0, "", {}, {}, false, {}, {}, {}, 0, {}),
      record(
          "HG00280", 13396, 13413, 0, 0, "", {}, {}, false, {}, {}, {}, 0, {}),
      record(
          "HG00280", 17319, 17479, 0, 0, "", {}, {}, false, {}, {}, {}, 0, {}),
      record(
          "HG00280", 17480, 17486, 0, 0, "", {}, {}, false, {}, {}, {}, 0, {})};

  // Allocate and set buffers
  const unsigned allocated_num_records = 10;
  std::vector<uint32_t> pos_start0(allocated_num_records);
  std::vector<uint32_t> pos_end0(allocated_num_records);
  std::vector<int32_t> sample_name_offsets0(allocated_num_records + 1);
  std::vector<char> sample_name0(allocated_num_records * 10);
  std::vector<uint32_t> pos_start1(allocated_num_records);
  std::vector<uint32_t> pos_end1(allocated_num_records);
  std::vector<int32_t> sample_name_offsets1(allocated_num_records + 1);
  std::vector<char> sample_name1(allocated_num_records * 10);
  std::vector<int> fmt_DP0(allocated_num_records);
  std::vector<int> fmt_DP1(allocated_num_records);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_values(
          reader0,
          "pos_start",
          sizeof(uint32_t) * pos_start0.size(),
          pos_start0.data()) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_values(
          reader0,
          "pos_end",
          sizeof(uint32_t) * pos_end0.size(),
          pos_end0.data()) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_values(
          reader0,
          "sample_name",
          sizeof(char) * sample_name0.size(),
          sample_name0.data()) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_offsets(
          reader0,
          "sample_name",
          sizeof(int32_t) * sample_name_offsets0.size(),
          sample_name_offsets0.data()) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_values(
          reader1,
          "pos_start",
          sizeof(uint32_t) * pos_start1.size(),
          pos_start1.data()) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_values(
          reader1,
          "pos_end",
          sizeof(uint32_t) * pos_end1.size(),
          pos_end1.data()) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_values(
          reader1,
          "sample_name",
          sizeof(char) * sample_name1.size(),
          sample_name1.data()) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_offsets(
          reader1,
          "sample_name",
          sizeof(int32_t) * sample_name_offsets1.size(),
          sample_name_offsets1.data()) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_values(
          reader0,
          "fmt_DP",
          sizeof(int32_t) * fmt_DP0.size(),
          pos_start0.data()) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer_values(
          reader1,
          "fmt_DP",
          sizeof(int32_t) * fmt_DP1.size(),
          fmt_DP1.data()) == TILEDB_VCF_OK);

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

  // Check result size
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader0, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 8);
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader1, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 3);

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
  std::vector<record> records0 = build_records(
      num_records,
      sample_name0,
      sample_name_offsets0,
      pos_start0,
      pos_end0,
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {});

  REQUIRE_THAT(expected_records, Catch::Matchers::Contains(records0));

  std::vector<record> records1 = build_records(
      num_records,
      sample_name1,
      sample_name_offsets1,
      pos_start1,
      pos_end1,
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {});

  REQUIRE_THAT(expected_records, Catch::Matchers::Contains(records1));

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

  SECTION("- V4") {
    dataset_uri = INPUT_ARRAYS_DIR_V4 + "/ingested_2samples_GT_DP_PL";
  }
  REQUIRE(tiledb_vcf_reader_init(reader, dataset_uri.c_str()) == TILEDB_VCF_OK);

  // Set up samples and ranges
  const char* regions = "1:13000-13360,1:13500-16000";
  REQUIRE(tiledb_vcf_reader_set_regions(reader, regions) == TILEDB_VCF_OK);

  std::vector<record> expected_records = {
      record(
          "HG00280",
          13354,
          13374,
          0,
          0,
          "",
          {"T", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          15,
          {}),
      record(
          "HG01762",
          13354,
          13389,
          0,
          0,
          "",
          {"T", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          64,
          {}),
      record(
          "HG00280",
          13452,
          13519,
          0,
          0,
          "",
          {"G", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          10,
          {}),
      record(
          "HG00280",
          13520,
          13544,
          0,
          0,
          "",
          {"G", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          6,
          {}),
      record(
          "HG00280",
          13545,
          13689,
          0,
          0,
          "",
          {"G", "<NON_REF>"},
          {},
          false,
          {},
          {},
          {},
          0,
          {})};

  // Allocate and set buffers
  const unsigned expected_num_records = 5;
  SET_BUFF_POS_START(reader, expected_num_records);
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
  std::vector<record> records = build_records(
      num_records,
      sample_name,
      sample_name_offsets,
      pos_start,
      pos_end,
      {},
      {},
      {},
      {},
      alleles,
      alleles_offsets,
      alleles_list_offsets,
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      fmt_DP,
      {},
      {});

  REQUIRE_THAT(expected_records, Catch::Matchers::UnorderedEquals(records));

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

  SECTION("- V4") {
    dataset_uri = INPUT_ARRAYS_DIR_V4 + "/ingested_2samples";
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

  std::vector<record> expected_records = {
      record(
          "G16",
          10626,
          81854,
          10600,
          540400,
          "1",
          {},
          {},
          false,
          {},
          {},
          {1, 0, 1, 0, 0, 1, 1, 0, 1, 0, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1},
          0,
          {},
          184134),
      record(
          "G14",
          10717,
          75204,
          10600,
          540400,
          "1",
          {},
          {},
          false,
          {},
          {},
          {1, 0, 1, 0, 0, 1, 1, 0, 1, 0, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1},
          0,
          {},
          184134),
      record(
          "G17",
          10863,
          83686,
          10600,
          540400,
          "1",
          {},
          {},
          false,
          {},
          {},
          {1, 0, 1, 0, 0, 1, 1, 0, 1, 0, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1},
          0,
          {},
          184134),
      record(
          "G10",
          10872,
          16376,
          10600,
          540400,
          "1",
          {},
          {},
          false,
          {},
          {},
          {1, 0, 1, 0, 0, 1, 1, 0, 1, 0, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1},
          0,
          {},
          184134),
      record(
          "G15",
          11123,
          89409,
          10600,
          540400,
          "1",
          {},
          {},
          false,
          {},
          {},
          {1, 0, 1, 0, 0, 1, 1, 0, 1, 0, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1},
          0,
          {},
          184134),
      record(
          "G11",
          12430,
          48068,
          10600,
          540400,
          "1",
          {},
          {},
          false,
          {},
          {},
          {1, 0, 1, 0, 0, 1, 1, 0, 1, 0, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1},
          0,
          {},
          184134),
      record(
          "G13",
          13519,
          34681,
          10600,
          540400,
          "1",
          {},
          {},
          false,
          {},
          {},
          {1, 0, 1, 0, 0, 1, 1, 0, 1, 0, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1},
          0,
          {},
          184134),
      record(
          "G100",
          13792,
          102213,
          10600,
          540400,
          "1",
          {},
          {},
          false,
          {},
          {},
          {1, 0, 1, 0, 0, 1, 1, 0, 1, 0, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1},
          0,
          {},
          184134),
      record(
          "G12",
          14199,
          70699,
          10600,
          540400,
          "1",
          {},
          {},
          false,
          {},
          {},
          {1, 0, 1, 0, 0, 1, 1, 0, 1, 0, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1},
          0,
          {},
          184134),
      record(
          "G18",
          14563,
          102086,
          10600,
          540400,
          "1",
          {},
          {},
          false,
          {},
          {},
          {1, 0, 1, 0, 0, 1, 1, 0, 1, 0, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1},
          0,
          {},
          184134),
  };

  // Allocate and set buffers, only set small buffers for a few records
  const unsigned expected_num_records = 10;
  SET_BUFF_POS_START(reader, expected_num_records);
  SET_BUFF_POS_END(reader, expected_num_records);
  SET_BUFF_SAMPLE_NAME(reader, expected_num_records);
  SET_BUFF_CONTIG(reader, expected_num_records);
  SET_BUFF_FMT_GT(reader, expected_num_records);
  SET_BUFF_QUERY_BED_START(reader, expected_num_records);
  SET_BUFF_QUERY_BED_END(reader, expected_num_records);
  SET_BUFF_QUERY_BED_LINE(reader, expected_num_records);

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
  std::vector<record> records = build_records(
      num_records,
      sample_name,
      sample_name_offsets,
      pos_start,
      pos_end,
      query_bed_start,
      query_bed_end,
      contig,
      contig_offsets,
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      fmt_GT,
      fmt_GT_offsets,
      {},
      {},
      {},
      query_bed_line);

  REQUIRE_THAT(expected_records, Catch::Matchers::UnorderedEquals(records));

  // Check final offsets are equal to data size
  REQUIRE(sample_name_offsets[10] == 31);
  REQUIRE(fmt_GT_offsets[10] == 20);

  tiledb_vcf_reader_free(&reader);
}

TEST_CASE("C API: Reader BED file parsing", "[capi][reader][bed]") {
  tiledb_vcf_reader_t* reader = nullptr;
  REQUIRE(tiledb_vcf_reader_alloc(&reader) == TILEDB_VCF_OK);
  tiledb_vcf_bed_file_t* bed_file = nullptr;
  REQUIRE(tiledb_vcf_bed_file_alloc(&bed_file) == TILEDB_VCF_OK);

  std::string dataset_uri;
  SECTION("- V2") {
    dataset_uri = INPUT_ARRAYS_DIR_V2 + "/ingested_2samples_GT_DP_PL";
  }

  SECTION("- V3") {
    dataset_uri = INPUT_ARRAYS_DIR_V3 + "/ingested_2samples_GT_DP_PL";
  }

  SECTION("- V4") {
    dataset_uri = INPUT_ARRAYS_DIR_V4 + "/ingested_2samples_GT_DP_PL";
  }
  REQUIRE(tiledb_vcf_reader_init(reader, dataset_uri.c_str()) == TILEDB_VCF_OK);

  // Set up samples and ranges
  auto bed_uri = TILEDB_VCF_TEST_INPUT_DIR + std::string("/simple.bed");
  REQUIRE(
      tiledb_vcf_reader_set_bed_file(reader, bed_uri.c_str()) == TILEDB_VCF_OK);

  REQUIRE(
      tiledb_vcf_bed_file_parse(reader, bed_file, bed_uri.c_str()) ==
      TILEDB_VCF_OK);

  uint64_t total_region_count = 0;
  uint64_t contig_count = 0;
  uint64_t first_contig_count = 0;

  REQUIRE(
      tiledb_vcf_bed_file_get_total_region_count(
          bed_file, &total_region_count) == TILEDB_VCF_OK);
  REQUIRE(2 == total_region_count);

  REQUIRE(
      tiledb_vcf_bed_file_get_contig_count(bed_file, &contig_count) ==
      TILEDB_VCF_OK);
  REQUIRE(1 == contig_count);

  REQUIRE(
      tiledb_vcf_bed_file_get_contig_region_count(
          bed_file, 0, &first_contig_count) == TILEDB_VCF_OK);
  REQUIRE(2 == first_contig_count);

  uint32_t region_start, region_end;
  const char* region_contig;
  const char* region_str;
  REQUIRE(
      tiledb_vcf_bed_file_get_contig_region(
          bed_file,
          0,
          0,
          &region_str,
          &region_contig,
          &region_start,
          &region_end) == TILEDB_VCF_OK);
  REQUIRE(12099 == region_start);
  REQUIRE(13359 == region_end);
  // region_str is 0-indexed, inclusive
  REQUIRE_THAT(region_str, Catch::Matchers::Equals("1:12099-13359:0"));
  REQUIRE_THAT(region_contig, Catch::Matchers::Equals("1"));

  tiledb_vcf_bed_file_free(&bed_file);
  tiledb_vcf_reader_free(&reader);
}
