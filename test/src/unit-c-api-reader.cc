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

static std::string INPUT_ARRAYS_DIR =
    TILEDB_VCF_TEST_INPUT_DIR + std::string("/arrays");

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

  auto dataset_uri = INPUT_ARRAYS_DIR + "/ingested_2samples";

  SECTION("- Nonexistent array") {
    REQUIRE(tiledb_vcf_reader_init(reader, "abc") == TILEDB_VCF_ERR);
  }

  SECTION("- Existing sample") {
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

  tiledb_vcf_reader_free(&reader);
}

TEST_CASE("C API: Reader set regions", "[capi][reader]") {
  tiledb_vcf_reader_t* reader = nullptr;
  REQUIRE(tiledb_vcf_reader_alloc(&reader) == TILEDB_VCF_OK);
  auto dataset_uri = INPUT_ARRAYS_DIR + "/ingested_2samples";
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
  auto dataset_uri = INPUT_ARRAYS_DIR + "/ingested_2samples";
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
  auto dataset_uri = INPUT_ARRAYS_DIR + "/ingested_2samples";
  const char* all_samples = "HG01762,HG00280";
  REQUIRE(tiledb_vcf_reader_init(reader, dataset_uri.c_str()) == TILEDB_VCF_OK);

  int32_t offsets[10];
  uint32_t start_pos[10];

  // Error for offsets on fixed-len attr
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "pos_start",
          sizeof(offsets),
          offsets,
          sizeof(start_pos),
          start_pos) == TILEDB_VCF_ERR);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader, "pos_start", 0, nullptr, sizeof(start_pos), start_pos) ==
      TILEDB_VCF_OK);

  int32_t contig_offsets[10];
  char contigs[100];

  // Error for null offsets on var-len attr
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader, "contig", 0, nullptr, sizeof(contigs), contigs) ==
      TILEDB_VCF_ERR);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "contig",
          sizeof(contig_offsets),
          contig_offsets,
          sizeof(contigs),
          contigs) == TILEDB_VCF_OK);

  // Error on null data buffer
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader, "pos_start", 0, nullptr, 0, nullptr) == TILEDB_VCF_ERR);

  tiledb_vcf_reader_free(&reader);
}

TEST_CASE("C API: Reader submit (default attributes)", "[capi][reader]") {
  tiledb_vcf_reader_t* reader = nullptr;
  REQUIRE(tiledb_vcf_reader_alloc(&reader) == TILEDB_VCF_OK);
  auto dataset_uri = INPUT_ARRAYS_DIR + "/ingested_2samples";
  REQUIRE(tiledb_vcf_reader_init(reader, dataset_uri.c_str()) == TILEDB_VCF_OK);

  // Set up samples and ranges
  const char* all_samples = "HG01762,HG00280";
  REQUIRE(tiledb_vcf_reader_set_samples(reader, all_samples) == TILEDB_VCF_OK);
  const char* ranges = "1:12100-13360,1:13500-17350";
  REQUIRE(tiledb_vcf_reader_set_regions(reader, ranges) == TILEDB_VCF_OK);

  // Allocate and set buffers
  const unsigned expected_num_records = 10;
  uint32_t start_pos[expected_num_records];
  uint32_t end_pos[expected_num_records];
  int32_t sample_name_offsets[expected_num_records + 1];
  char sample_name[expected_num_records * 10];
  int32_t contig_name_offsets[expected_num_records + 1];
  char contig_name[expected_num_records * 10];
  int32_t alleles_offsets[expected_num_records + 1];
  char alleles[expected_num_records * 20];
  int32_t filter_offsets[expected_num_records + 1];
  uint8_t filter_bitmap[expected_num_records / 8 + 1];
  char filter[expected_num_records * 10];
  int32_t genotype_offsets[expected_num_records + 1];
  int genotype[expected_num_records * 2];
  int32_t info_offsets[expected_num_records + 1];
  uint8_t info[expected_num_records * 100];
  int32_t format_offsets[expected_num_records + 1];
  uint8_t format[expected_num_records * 100];
  uint32_t qrange_start[expected_num_records];
  uint32_t qrange_end[expected_num_records];
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader, "pos_start", 0, nullptr, sizeof(start_pos), start_pos) ==
      TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader, "pos_end", 0, nullptr, sizeof(end_pos), end_pos) ==
      TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "sample_name",
          sizeof(sample_name_offsets),
          sample_name_offsets,
          sizeof(sample_name),
          sample_name) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "contig",
          sizeof(contig_name_offsets),
          contig_name_offsets,
          sizeof(contig_name),
          contig_name) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "alleles",
          sizeof(alleles_offsets),
          alleles_offsets,
          sizeof(alleles),
          alleles) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "filters",
          sizeof(filter_offsets),
          filter_offsets,
          sizeof(filter),
          filter) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_validity_bitmap(
          reader, "filters", sizeof(filter_bitmap), filter_bitmap) ==
      TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "fmt_GT",
          sizeof(genotype_offsets),
          genotype_offsets,
          sizeof(genotype),
          genotype) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "fmt",
          sizeof(format_offsets),
          format_offsets,
          sizeof(format),
          format) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "info",
          sizeof(info_offsets),
          info_offsets,
          sizeof(info),
          info) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "query_bed_start",
          0,
          nullptr,
          sizeof(qrange_start),
          qrange_start) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "query_bed_end",
          0,
          nullptr,
          sizeof(qrange_end),
          qrange_end) == TILEDB_VCF_OK);

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
  int64_t buff_size, off_size;
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader, "pos_start", &off_size, &buff_size) == TILEDB_VCF_OK);
  REQUIRE(buff_size == expected_num_records * sizeof(uint32_t));
  REQUIRE(off_size == 0);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader, "sample_name", &off_size, &buff_size) == TILEDB_VCF_OK);
  REQUIRE(buff_size == 70);
  REQUIRE(off_size == (expected_num_records + 1) * sizeof(int32_t));
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader, "alleles", &off_size, &buff_size) == TILEDB_VCF_OK);
  REQUIRE(buff_size == 120);
  REQUIRE(off_size == (expected_num_records + 1) * sizeof(int32_t));

  // Check results
  REQUIRE(start_pos[0] == 12141);
  REQUIRE(end_pos[0] == 12277);
  REQUIRE(sample_name_offsets[0] == 0);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[0]], 7) == 0);
  REQUIRE(strncmp("1", &contig_name[contig_name_offsets[0]], 1) == 0);
  REQUIRE(strncmp("C,<NON_REF>", &alleles[alleles_offsets[0]], 11) == 0);
  REQUIRE((filter_bitmap[0] & ((uint8_t)1 << 0)) == 0);
  REQUIRE(filter_offsets[0] == 0);
  REQUIRE(genotype_offsets[0] == 0);
  REQUIRE(genotype[genotype_offsets[0] / sizeof(int32_t)] == 0);
  REQUIRE(genotype[genotype_offsets[0] / sizeof(int32_t) + 1] == 0);
  REQUIRE(info_offsets[0] == 0);
  REQUIRE(format_offsets[0] == 0);
  REQUIRE(qrange_start[0] == 12099);
  REQUIRE(qrange_end[0] == 13360);

  REQUIRE(start_pos[1] == 12141);
  REQUIRE(end_pos[1] == 12277);
  REQUIRE(sample_name_offsets[1] == 7);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[1]], 7) == 0);
  REQUIRE(strncmp("1", &contig_name[contig_name_offsets[1]], 1) == 0);
  REQUIRE(strncmp("C,<NON_REF>", &alleles[alleles_offsets[1]], 11) == 0);
  REQUIRE((filter_bitmap[0] & ((uint8_t)1 << 1)) == 0);
  REQUIRE(filter_offsets[1] == 0);
  REQUIRE(genotype_offsets[1] == 8);
  REQUIRE(genotype[genotype_offsets[1] / sizeof(int32_t)] == 0);
  REQUIRE(genotype[genotype_offsets[1] / sizeof(int32_t) + 1] == 0);
  REQUIRE(info_offsets[1] == 4);
  REQUIRE(format_offsets[1] == 95);
  REQUIRE(qrange_start[1] == 12099);
  REQUIRE(qrange_end[1] == 13360);

  REQUIRE(start_pos[2] == 12546);
  REQUIRE(end_pos[2] == 12771);
  REQUIRE(sample_name_offsets[2] == 14);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[2]], 7) == 0);
  REQUIRE(strncmp("1", &contig_name[contig_name_offsets[2]], 1) == 0);
  REQUIRE(strncmp("G,<NON_REF>", &alleles[alleles_offsets[2]], 11) == 0);
  REQUIRE((filter_bitmap[0] & ((uint8_t)1 << 2)) == 0);
  REQUIRE(filter_offsets[2] == 0);
  REQUIRE(genotype_offsets[2] == 16);
  REQUIRE(genotype[genotype_offsets[2] / sizeof(int32_t)] == 0);
  REQUIRE(genotype[genotype_offsets[2] / sizeof(int32_t) + 1] == 0);
  REQUIRE(info_offsets[2] == 8);
  REQUIRE(format_offsets[2] == 190);
  REQUIRE(qrange_start[2] == 12099);
  REQUIRE(qrange_end[2] == 13360);

  REQUIRE(start_pos[3] == 12546);
  REQUIRE(end_pos[3] == 12771);
  REQUIRE(sample_name_offsets[3] == 21);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[3]], 7) == 0);
  REQUIRE(strncmp("1", &contig_name[contig_name_offsets[3]], 1) == 0);
  REQUIRE(strncmp("G,<NON_REF>", &alleles[alleles_offsets[3]], 11) == 0);
  REQUIRE((filter_bitmap[0] & ((uint8_t)1 << 3)) == 0);
  REQUIRE(filter_offsets[3] == 0);
  REQUIRE(genotype_offsets[3] == 24);
  REQUIRE(genotype[genotype_offsets[3] / sizeof(int32_t)] == 0);
  REQUIRE(genotype[genotype_offsets[3] / sizeof(int32_t) + 1] == 0);
  REQUIRE(info_offsets[3] == 12);
  REQUIRE(format_offsets[3] == 285);
  REQUIRE(qrange_start[3] == 12099);
  REQUIRE(qrange_end[3] == 13360);

  REQUIRE(start_pos[4] == 13354);
  REQUIRE(end_pos[4] == 13374);
  REQUIRE(sample_name_offsets[4] == 28);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[4]], 7) == 0);
  REQUIRE(strncmp("1", &contig_name[contig_name_offsets[4]], 1) == 0);
  REQUIRE(strncmp("T,<NON_REF>", &alleles[alleles_offsets[4]], 11) == 0);
  REQUIRE((filter_bitmap[0] & ((uint8_t)1 << 4)) != 0);
  REQUIRE(filter_offsets[4] == 0);
  REQUIRE(strncmp("LowQual", &filter[filter_offsets[4]], 7) == 0);
  REQUIRE(genotype_offsets[4] == 32);
  REQUIRE(genotype[genotype_offsets[4] / sizeof(int32_t)] == 0);
  REQUIRE(genotype[genotype_offsets[4] / sizeof(int32_t) + 1] == 0);
  REQUIRE(info_offsets[4] == 16);
  REQUIRE(format_offsets[4] == 380);
  REQUIRE(qrange_start[4] == 12099);
  REQUIRE(qrange_end[4] == 13360);

  REQUIRE(start_pos[5] == 13354);
  REQUIRE(end_pos[5] == 13389);
  REQUIRE(sample_name_offsets[5] == 35);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[5]], 7) == 0);
  REQUIRE(strncmp("1", &contig_name[contig_name_offsets[5]], 1) == 0);
  REQUIRE(strncmp("T,<NON_REF>", &alleles[alleles_offsets[5]], 11) == 0);
  REQUIRE((filter_bitmap[0] & ((uint8_t)1 << 5)) == 0);
  REQUIRE(filter_offsets[5] == 7);
  REQUIRE(genotype_offsets[5] == 40);
  REQUIRE(genotype[genotype_offsets[5] / sizeof(int32_t)] == 0);
  REQUIRE(genotype[genotype_offsets[5] / sizeof(int32_t) + 1] == 0);
  REQUIRE(info_offsets[5] == 20);
  REQUIRE(format_offsets[5] == 475);
  REQUIRE(qrange_start[5] == 12099);
  REQUIRE(qrange_end[5] == 13360);

  REQUIRE(start_pos[6] == 13452);
  REQUIRE(end_pos[6] == 13519);
  REQUIRE(sample_name_offsets[6] == 42);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[6]], 7) == 0);
  REQUIRE(strncmp("1", &contig_name[contig_name_offsets[6]], 1) == 0);
  REQUIRE(strncmp("G,<NON_REF>", &alleles[alleles_offsets[6]], 11) == 0);
  REQUIRE((filter_bitmap[0] & ((uint8_t)1 << 6)) == 0);
  REQUIRE(filter_offsets[6] == 7);
  REQUIRE(genotype_offsets[6] == 48);
  REQUIRE(genotype[genotype_offsets[6] / sizeof(int32_t)] == 0);
  REQUIRE(genotype[genotype_offsets[6] / sizeof(int32_t) + 1] == 0);
  REQUIRE(info_offsets[6] == 24);
  REQUIRE(format_offsets[6] == 570);
  REQUIRE(qrange_start[6] == 13499);
  REQUIRE(qrange_end[6] == 17350);

  REQUIRE(start_pos[7] == 13520);
  REQUIRE(end_pos[7] == 13544);
  REQUIRE(sample_name_offsets[7] == 49);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[7]], 7) == 0);
  REQUIRE(strncmp("1", &contig_name[contig_name_offsets[7]], 1) == 0);
  REQUIRE(strncmp("G,<NON_REF>", &alleles[alleles_offsets[7]], 11) == 0);
  REQUIRE((filter_bitmap[0] & ((uint8_t)1 << 7)) == 0);
  REQUIRE(filter_offsets[7] == 7);
  REQUIRE(genotype_offsets[7] == 56);
  REQUIRE(genotype[genotype_offsets[7] / sizeof(int32_t)] == 0);
  REQUIRE(genotype[genotype_offsets[7] / sizeof(int32_t) + 1] == 0);
  REQUIRE(info_offsets[7] == 28);
  REQUIRE(format_offsets[7] == 665);
  REQUIRE(qrange_start[7] == 13499);
  REQUIRE(qrange_end[7] == 17350);

  REQUIRE(start_pos[8] == 13545);
  REQUIRE(end_pos[8] == 13689);
  REQUIRE(sample_name_offsets[8] == 56);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[8]], 7) == 0);
  REQUIRE(strncmp("1", &contig_name[contig_name_offsets[8]], 1) == 0);
  REQUIRE(strncmp("G,<NON_REF>", &alleles[alleles_offsets[8]], 11) == 0);
  REQUIRE((filter_bitmap[1] & ((uint8_t)1 << 0)) == 0);
  REQUIRE(filter_offsets[8] == 7);
  REQUIRE(genotype_offsets[8] == 64);
  REQUIRE(genotype[genotype_offsets[8] / sizeof(int32_t)] == 0);
  REQUIRE(genotype[genotype_offsets[8] / sizeof(int32_t) + 1] == 0);
  REQUIRE(info_offsets[8] == 32);
  REQUIRE(format_offsets[8] == 760);
  REQUIRE(qrange_start[8] == 13499);
  REQUIRE(qrange_end[8] == 17350);

  REQUIRE(start_pos[9] == 17319);
  REQUIRE(end_pos[9] == 17479);
  REQUIRE(sample_name_offsets[9] == 63);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[9]], 7) == 0);
  REQUIRE(strncmp("1", &contig_name[contig_name_offsets[9]], 1) == 0);
  REQUIRE(strncmp("T,<NON_REF>", &alleles[alleles_offsets[9]], 11) == 0);
  REQUIRE((filter_bitmap[1] & ((uint8_t)1 << 1)) == 0);
  REQUIRE(filter_offsets[9] == 7);
  REQUIRE(genotype_offsets[9] == 72);
  REQUIRE(genotype[genotype_offsets[9] / sizeof(int32_t)] == 0);
  REQUIRE(genotype[genotype_offsets[9] / sizeof(int32_t) + 1] == 0);
  REQUIRE(info_offsets[9] == 36);
  REQUIRE(format_offsets[9] == 855);
  REQUIRE(qrange_start[9] == 13499);
  REQUIRE(qrange_end[9] == 17350);

  // Check final offsets are equal to data size
  REQUIRE(sample_name_offsets[10] == 70);
  REQUIRE(filter_offsets[10] == 7);
  REQUIRE(genotype_offsets[10] == 80);
  REQUIRE(info_offsets[10] == 40);
  REQUIRE(format_offsets[10] == 950);

  tiledb_vcf_reader_free(&reader);
}

TEST_CASE("C API: Reader submit (optional attributes)", "[capi][reader]") {
  tiledb_vcf_reader_t* reader = nullptr;
  REQUIRE(tiledb_vcf_reader_alloc(&reader) == TILEDB_VCF_OK);
  auto dataset_uri = INPUT_ARRAYS_DIR + "/ingested_2samples_GT_DP_PL";
  REQUIRE(tiledb_vcf_reader_init(reader, dataset_uri.c_str()) == TILEDB_VCF_OK);

  // Set up samples and ranges
  const char* all_samples = "HG01762,HG00280";
  REQUIRE(tiledb_vcf_reader_set_samples(reader, all_samples) == TILEDB_VCF_OK);
  const char* regions = "1:12100-13360,1:13500-17350";
  REQUIRE(tiledb_vcf_reader_set_regions(reader, regions) == TILEDB_VCF_OK);

  // Allocate and set buffers
  const unsigned expected_num_records = 10;
  uint32_t start_pos[expected_num_records];
  uint32_t end_pos[expected_num_records];
  int32_t sample_name_offsets[expected_num_records + 1];
  char sample_name[expected_num_records * 10];
  int32_t contig_name_offsets[expected_num_records + 1];
  char contig_name[expected_num_records * 10];
  int32_t alleles_offsets[expected_num_records + 1];
  char alleles[expected_num_records * 20];
  int32_t filter_offsets[expected_num_records + 1];
  uint8_t filter_bitmap[expected_num_records / 8 + 1];
  char filter[expected_num_records * 10];
  int32_t genotype_offsets[expected_num_records + 1];
  int genotype[expected_num_records * 2];
  int32_t info_offsets[expected_num_records + 1];
  uint8_t info[expected_num_records * 100];
  int32_t format_offsets[expected_num_records + 1];
  uint8_t format[expected_num_records * 100];
  uint32_t qrange_start[expected_num_records];
  uint32_t qrange_end[expected_num_records];
  int32_t dp_offsets[expected_num_records + 1];
  int dp[expected_num_records];
  int32_t pl_offsets[expected_num_records + 1];
  int pl[expected_num_records * 3];
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader, "pos_start", 0, nullptr, sizeof(start_pos), start_pos) ==
      TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader, "pos_end", 0, nullptr, sizeof(end_pos), end_pos) ==
      TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "sample_name",
          sizeof(sample_name_offsets),
          sample_name_offsets,
          sizeof(sample_name),
          sample_name) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "contig",
          sizeof(contig_name_offsets),
          contig_name_offsets,
          sizeof(contig_name),
          contig_name) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "alleles",
          sizeof(alleles_offsets),
          alleles_offsets,
          sizeof(alleles),
          alleles) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "filters",
          sizeof(filter_offsets),
          filter_offsets,
          sizeof(filter),
          filter) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_validity_bitmap(
          reader, "filters", sizeof(filter_bitmap), filter_bitmap) ==
      TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "fmt_GT",
          sizeof(genotype_offsets),
          genotype_offsets,
          sizeof(genotype),
          genotype) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "fmt",
          sizeof(format_offsets),
          format_offsets,
          sizeof(format),
          format) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "info",
          sizeof(info_offsets),
          info_offsets,
          sizeof(info),
          info) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "query_bed_start",
          0,
          nullptr,
          sizeof(qrange_start),
          qrange_start) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "query_bed_end",
          0,
          nullptr,
          sizeof(qrange_end),
          qrange_end) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader, "fmt_DP", sizeof(dp_offsets), dp_offsets, sizeof(dp), dp) ==
      TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader, "fmt_PL", sizeof(pl_offsets), pl_offsets, sizeof(pl), pl) ==
      TILEDB_VCF_OK);

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
  REQUIRE(start_pos[0] == 12141);
  REQUIRE(end_pos[0] == 12277);
  REQUIRE(sample_name_offsets[0] == 0);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[0]], 7) == 0);
  REQUIRE(strncmp("1", &contig_name[contig_name_offsets[0]], 1) == 0);
  REQUIRE(strncmp("C,<NON_REF>", &alleles[alleles_offsets[0]], 11) == 0);
  REQUIRE((filter_bitmap[0] & ((uint8_t)1 << 0)) == 0);
  REQUIRE(filter_offsets[0] == 0);
  REQUIRE(genotype_offsets[0] == 0);
  REQUIRE(genotype[genotype_offsets[0] / sizeof(int32_t)] == 0);
  REQUIRE(genotype[genotype_offsets[0] / sizeof(int32_t) + 1] == 0);
  REQUIRE(info_offsets[0] == 0);
  REQUIRE(format_offsets[0] == 0);
  REQUIRE(qrange_start[0] == 12099);
  REQUIRE(qrange_end[0] == 13360);
  REQUIRE(dp[dp_offsets[0] / sizeof(int32_t)] == 0);
  REQUIRE(pl[pl_offsets[0] / sizeof(int32_t)] == 0);
  REQUIRE(pl[pl_offsets[0] / sizeof(int32_t) + 1] == 0);
  REQUIRE(pl[pl_offsets[0] / sizeof(int32_t) + 2] == 0);

  REQUIRE(start_pos[1] == 12141);
  REQUIRE(end_pos[1] == 12277);
  REQUIRE(sample_name_offsets[1] == 7);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[1]], 7) == 0);
  REQUIRE(strncmp("1", &contig_name[contig_name_offsets[1]], 1) == 0);
  REQUIRE(strncmp("C,<NON_REF>", &alleles[alleles_offsets[1]], 11) == 0);
  REQUIRE((filter_bitmap[0] & ((uint8_t)1 << 1)) == 0);
  REQUIRE(filter_offsets[1] == 0);
  REQUIRE(genotype_offsets[1] == 8);
  REQUIRE(genotype[genotype_offsets[1] / sizeof(int32_t)] == 0);
  REQUIRE(genotype[genotype_offsets[1] / sizeof(int32_t) + 1] == 0);
  REQUIRE(info_offsets[1] == 4);
  REQUIRE(format_offsets[1] == 38);
  REQUIRE(qrange_start[1] == 12099);
  REQUIRE(qrange_end[1] == 13360);
  REQUIRE(dp[dp_offsets[1] / sizeof(int32_t)] == 0);
  REQUIRE(pl[pl_offsets[1] / sizeof(int32_t)] == 0);
  REQUIRE(pl[pl_offsets[1] / sizeof(int32_t) + 1] == 0);
  REQUIRE(pl[pl_offsets[1] / sizeof(int32_t) + 2] == 0);

  REQUIRE(start_pos[2] == 12546);
  REQUIRE(end_pos[2] == 12771);
  REQUIRE(sample_name_offsets[2] == 14);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[2]], 7) == 0);
  REQUIRE(strncmp("1", &contig_name[contig_name_offsets[2]], 1) == 0);
  REQUIRE(strncmp("G,<NON_REF>", &alleles[alleles_offsets[2]], 11) == 0);
  REQUIRE((filter_bitmap[0] & ((uint8_t)1 << 2)) == 0);
  REQUIRE(filter_offsets[2] == 0);
  REQUIRE(genotype_offsets[2] == 16);
  REQUIRE(genotype[genotype_offsets[2] / sizeof(int32_t)] == 0);
  REQUIRE(genotype[genotype_offsets[2] / sizeof(int32_t) + 1] == 0);
  REQUIRE(info_offsets[2] == 8);
  REQUIRE(format_offsets[2] == 76);
  REQUIRE(qrange_start[2] == 12099);
  REQUIRE(qrange_end[2] == 13360);
  REQUIRE(dp[dp_offsets[2] / sizeof(int32_t)] == 0);
  REQUIRE(pl[pl_offsets[2] / sizeof(int32_t)] == 0);
  REQUIRE(pl[pl_offsets[2] / sizeof(int32_t) + 1] == 0);
  REQUIRE(pl[pl_offsets[2] / sizeof(int32_t) + 2] == 0);

  REQUIRE(start_pos[3] == 12546);
  REQUIRE(end_pos[3] == 12771);
  REQUIRE(sample_name_offsets[3] == 21);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[3]], 7) == 0);
  REQUIRE(strncmp("1", &contig_name[contig_name_offsets[3]], 1) == 0);
  REQUIRE(strncmp("G,<NON_REF>", &alleles[alleles_offsets[3]], 11) == 0);
  REQUIRE((filter_bitmap[0] & ((uint8_t)1 << 3)) == 0);
  REQUIRE(filter_offsets[3] == 0);
  REQUIRE(genotype_offsets[3] == 24);
  REQUIRE(genotype[genotype_offsets[3] / sizeof(int32_t)] == 0);
  REQUIRE(genotype[genotype_offsets[3] / sizeof(int32_t) + 1] == 0);
  REQUIRE(info_offsets[3] == 12);
  REQUIRE(format_offsets[3] == 114);
  REQUIRE(qrange_start[3] == 12099);
  REQUIRE(qrange_end[3] == 13360);
  REQUIRE(dp[dp_offsets[3] / sizeof(int32_t)] == 0);
  REQUIRE(pl[pl_offsets[3] / sizeof(int32_t)] == 0);
  REQUIRE(pl[pl_offsets[3] / sizeof(int32_t) + 1] == 0);
  REQUIRE(pl[pl_offsets[3] / sizeof(int32_t) + 2] == 0);

  REQUIRE(start_pos[4] == 13354);
  REQUIRE(end_pos[4] == 13374);
  REQUIRE(sample_name_offsets[4] == 28);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[4]], 7) == 0);
  REQUIRE(strncmp("1", &contig_name[contig_name_offsets[4]], 1) == 0);
  REQUIRE(strncmp("T,<NON_REF>", &alleles[alleles_offsets[4]], 11) == 0);
  REQUIRE((filter_bitmap[0] & ((uint8_t)1 << 4)) != 0);
  REQUIRE(filter_offsets[4] == 0);
  REQUIRE(strncmp("LowQual", &filter[filter_offsets[4]], 7) == 0);
  REQUIRE(genotype_offsets[4] == 32);
  REQUIRE(genotype[genotype_offsets[4] / sizeof(int32_t)] == 0);
  REQUIRE(genotype[genotype_offsets[4] / sizeof(int32_t) + 1] == 0);
  REQUIRE(info_offsets[4] == 16);
  REQUIRE(format_offsets[4] == 152);
  REQUIRE(qrange_start[4] == 12099);
  REQUIRE(qrange_end[4] == 13360);
  REQUIRE(dp[dp_offsets[4] / sizeof(int32_t)] == 15);
  REQUIRE(pl[pl_offsets[4] / sizeof(int32_t)] == 0);
  REQUIRE(pl[pl_offsets[4] / sizeof(int32_t) + 1] == 24);
  REQUIRE(pl[pl_offsets[4] / sizeof(int32_t) + 2] == 360);

  REQUIRE(start_pos[5] == 13354);
  REQUIRE(end_pos[5] == 13389);
  REQUIRE(sample_name_offsets[5] == 35);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[5]], 7) == 0);
  REQUIRE(strncmp("1", &contig_name[contig_name_offsets[5]], 1) == 0);
  REQUIRE(strncmp("T,<NON_REF>", &alleles[alleles_offsets[5]], 11) == 0);
  REQUIRE((filter_bitmap[0] & ((uint8_t)1 << 5)) == 0);
  REQUIRE(filter_offsets[5] == 7);
  REQUIRE(genotype_offsets[5] == 40);
  REQUIRE(genotype[genotype_offsets[5] / sizeof(int32_t)] == 0);
  REQUIRE(genotype[genotype_offsets[5] / sizeof(int32_t) + 1] == 0);
  REQUIRE(info_offsets[5] == 20);
  REQUIRE(format_offsets[5] == 190);
  REQUIRE(qrange_start[5] == 12099);
  REQUIRE(qrange_end[5] == 13360);
  REQUIRE(dp[dp_offsets[5] / sizeof(int32_t)] == 64);
  REQUIRE(pl[pl_offsets[5] / sizeof(int32_t)] == 0);
  REQUIRE(pl[pl_offsets[5] / sizeof(int32_t) + 1] == 66);
  REQUIRE(pl[pl_offsets[5] / sizeof(int32_t) + 2] == 990);

  REQUIRE(start_pos[6] == 13452);
  REQUIRE(end_pos[6] == 13519);
  REQUIRE(sample_name_offsets[6] == 42);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[6]], 7) == 0);
  REQUIRE(strncmp("1", &contig_name[contig_name_offsets[6]], 1) == 0);
  REQUIRE(strncmp("G,<NON_REF>", &alleles[alleles_offsets[6]], 11) == 0);
  REQUIRE((filter_bitmap[0] & ((uint8_t)1 << 6)) == 0);
  REQUIRE(filter_offsets[6] == 7);
  REQUIRE(genotype_offsets[6] == 48);
  REQUIRE(genotype[genotype_offsets[6] / sizeof(int32_t)] == 0);
  REQUIRE(genotype[genotype_offsets[6] / sizeof(int32_t) + 1] == 0);
  REQUIRE(info_offsets[6] == 24);
  REQUIRE(format_offsets[6] == 228);
  REQUIRE(qrange_start[6] == 13499);
  REQUIRE(qrange_end[6] == 17350);
  REQUIRE(dp[dp_offsets[6] / sizeof(int32_t)] == 10);
  REQUIRE(pl[pl_offsets[6] / sizeof(int32_t)] == 0);
  REQUIRE(pl[pl_offsets[6] / sizeof(int32_t) + 1] == 21);
  REQUIRE(pl[pl_offsets[6] / sizeof(int32_t) + 2] == 210);

  REQUIRE(start_pos[7] == 13520);
  REQUIRE(end_pos[7] == 13544);
  REQUIRE(sample_name_offsets[7] == 49);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[7]], 7) == 0);
  REQUIRE(strncmp("1", &contig_name[contig_name_offsets[7]], 1) == 0);
  REQUIRE(strncmp("G,<NON_REF>", &alleles[alleles_offsets[7]], 11) == 0);
  REQUIRE((filter_bitmap[0] & ((uint8_t)1 << 7)) == 0);
  REQUIRE(filter_offsets[7] == 7);
  REQUIRE(genotype_offsets[7] == 56);
  REQUIRE(genotype[genotype_offsets[7] / sizeof(int32_t)] == 0);
  REQUIRE(genotype[genotype_offsets[7] / sizeof(int32_t) + 1] == 0);
  REQUIRE(info_offsets[7] == 28);
  REQUIRE(format_offsets[7] == 266);
  REQUIRE(qrange_start[7] == 13499);
  REQUIRE(qrange_end[7] == 17350);
  REQUIRE(dp[dp_offsets[7] / sizeof(int32_t)] == 6);
  REQUIRE(pl[pl_offsets[7] / sizeof(int32_t)] == 0);
  REQUIRE(pl[pl_offsets[7] / sizeof(int32_t) + 1] == 6);
  REQUIRE(pl[pl_offsets[7] / sizeof(int32_t) + 2] == 90);

  REQUIRE(start_pos[8] == 13545);
  REQUIRE(end_pos[8] == 13689);
  REQUIRE(sample_name_offsets[8] == 56);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[8]], 7) == 0);
  REQUIRE(strncmp("1", &contig_name[contig_name_offsets[8]], 1) == 0);
  REQUIRE(strncmp("G,<NON_REF>", &alleles[alleles_offsets[8]], 11) == 0);
  REQUIRE((filter_bitmap[1] & ((uint8_t)1 << 0)) == 0);
  REQUIRE(filter_offsets[8] == 7);
  REQUIRE(genotype_offsets[8] == 64);
  REQUIRE(genotype[genotype_offsets[8] / sizeof(int32_t)] == 0);
  REQUIRE(genotype[genotype_offsets[8] / sizeof(int32_t) + 1] == 0);
  REQUIRE(info_offsets[8] == 32);
  REQUIRE(format_offsets[8] == 304);
  REQUIRE(qrange_start[8] == 13499);
  REQUIRE(qrange_end[8] == 17350);
  REQUIRE(dp[dp_offsets[8] / sizeof(int32_t)] == 0);
  REQUIRE(pl[pl_offsets[8] / sizeof(int32_t)] == 0);
  REQUIRE(pl[pl_offsets[8] / sizeof(int32_t) + 1] == 0);
  REQUIRE(pl[pl_offsets[8] / sizeof(int32_t) + 2] == 0);

  REQUIRE(start_pos[9] == 17319);
  REQUIRE(end_pos[9] == 17479);
  REQUIRE(sample_name_offsets[9] == 63);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[9]], 7) == 0);
  REQUIRE(strncmp("1", &contig_name[contig_name_offsets[9]], 1) == 0);
  REQUIRE(strncmp("T,<NON_REF>", &alleles[alleles_offsets[9]], 11) == 0);
  REQUIRE((filter_bitmap[1] & ((uint8_t)1 << 1)) == 0);
  REQUIRE(filter_offsets[9] == 7);
  REQUIRE(genotype_offsets[9] == 72);
  REQUIRE(genotype[genotype_offsets[9] / sizeof(int32_t)] == 0);
  REQUIRE(genotype[genotype_offsets[9] / sizeof(int32_t) + 1] == 0);
  REQUIRE(info_offsets[9] == 36);
  REQUIRE(format_offsets[9] == 342);
  REQUIRE(qrange_start[9] == 13499);
  REQUIRE(qrange_end[9] == 17350);
  REQUIRE(dp[dp_offsets[9] / sizeof(int32_t)] == 0);
  REQUIRE(pl[pl_offsets[9] / sizeof(int32_t)] == 0);
  REQUIRE(pl[pl_offsets[9] / sizeof(int32_t) + 1] == 0);
  REQUIRE(pl[pl_offsets[9] / sizeof(int32_t) + 2] == 0);

  tiledb_vcf_reader_free(&reader);
}

TEST_CASE("C API: Reader submit (subselect attributes)", "[capi][reader]") {
  tiledb_vcf_reader_t* reader = nullptr;
  REQUIRE(tiledb_vcf_reader_alloc(&reader) == TILEDB_VCF_OK);
  auto dataset_uri = INPUT_ARRAYS_DIR + "/ingested_2samples_GT_DP_PL";
  REQUIRE(tiledb_vcf_reader_init(reader, dataset_uri.c_str()) == TILEDB_VCF_OK);

  // Set up samples and ranges
  const char* samples = "HG01762,HG00280";
  REQUIRE(tiledb_vcf_reader_set_samples(reader, samples) == TILEDB_VCF_OK);
  const char* regions = "1:12100-13360,1:13500-17350";
  REQUIRE(tiledb_vcf_reader_set_regions(reader, regions) == TILEDB_VCF_OK);

  // Allocate and set buffers
  const unsigned expected_num_records = 10;
  uint32_t end_pos[expected_num_records];
  int32_t sample_name_offsets[expected_num_records + 1];
  char sample_name[expected_num_records * 10];
  int32_t alleles_offsets[expected_num_records + 1];
  char alleles[expected_num_records * 20];
  int32_t dp_offsets[expected_num_records + 1];
  int dp[expected_num_records];
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader, "pos_end", 0, nullptr, sizeof(end_pos), end_pos) ==
      TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "sample_name",
          sizeof(sample_name_offsets),
          sample_name_offsets,
          sizeof(sample_name),
          sample_name) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "alleles",
          sizeof(alleles_offsets),
          alleles_offsets,
          sizeof(alleles),
          alleles) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader, "fmt_DP", sizeof(dp_offsets), dp_offsets, sizeof(dp), dp) ==
      TILEDB_VCF_OK);

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
  REQUIRE(end_pos[0] == 12277);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[0]], 7) == 0);
  REQUIRE(strncmp("C,<NON_REF>", &alleles[alleles_offsets[0]], 11) == 0);
  REQUIRE(dp[dp_offsets[0] / sizeof(int32_t)] == 0);

  REQUIRE(end_pos[1] == 12277);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[1]], 7) == 0);
  REQUIRE(strncmp("C,<NON_REF>", &alleles[alleles_offsets[1]], 11) == 0);
  REQUIRE(dp[dp_offsets[1] / sizeof(int32_t)] == 0);

  REQUIRE(end_pos[2] == 12771);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[2]], 7) == 0);
  REQUIRE(strncmp("G,<NON_REF>", &alleles[alleles_offsets[2]], 11) == 0);
  REQUIRE(dp[dp_offsets[2] / sizeof(int32_t)] == 0);

  REQUIRE(end_pos[3] == 12771);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[3]], 7) == 0);
  REQUIRE(strncmp("G,<NON_REF>", &alleles[alleles_offsets[3]], 11) == 0);
  REQUIRE(dp[dp_offsets[3] / sizeof(int32_t)] == 0);

  REQUIRE(end_pos[4] == 13374);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[4]], 7) == 0);
  REQUIRE(strncmp("T,<NON_REF>", &alleles[alleles_offsets[4]], 11) == 0);
  REQUIRE(dp[dp_offsets[4] / sizeof(int32_t)] == 15);

  REQUIRE(end_pos[5] == 13389);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[5]], 7) == 0);
  REQUIRE(strncmp("T,<NON_REF>", &alleles[alleles_offsets[5]], 11) == 0);
  REQUIRE(dp[dp_offsets[5] / sizeof(int32_t)] == 64);

  REQUIRE(end_pos[6] == 13519);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[6]], 7) == 0);
  REQUIRE(strncmp("G,<NON_REF>", &alleles[alleles_offsets[6]], 11) == 0);
  REQUIRE(dp[dp_offsets[6] / sizeof(int32_t)] == 10);

  REQUIRE(end_pos[7] == 13544);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[7]], 7) == 0);
  REQUIRE(strncmp("G,<NON_REF>", &alleles[alleles_offsets[7]], 11) == 0);
  REQUIRE(dp[dp_offsets[7] / sizeof(int32_t)] == 6);

  REQUIRE(end_pos[8] == 13689);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[8]], 7) == 0);
  REQUIRE(strncmp("G,<NON_REF>", &alleles[alleles_offsets[8]], 11) == 0);
  REQUIRE(dp[dp_offsets[8] / sizeof(int32_t)] == 0);

  REQUIRE(end_pos[9] == 17479);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[9]], 7) == 0);
  REQUIRE(strncmp("T,<NON_REF>", &alleles[alleles_offsets[9]], 11) == 0);
  REQUIRE(dp[dp_offsets[9] / sizeof(int32_t)] == 0);

  tiledb_vcf_reader_free(&reader);
}

TEST_CASE("C API: Reader submit (all samples)", "[capi][reader]") {
  tiledb_vcf_reader_t* reader = nullptr;
  REQUIRE(tiledb_vcf_reader_alloc(&reader) == TILEDB_VCF_OK);
  auto dataset_uri = INPUT_ARRAYS_DIR + "/ingested_2samples_GT_DP_PL";
  REQUIRE(tiledb_vcf_reader_init(reader, dataset_uri.c_str()) == TILEDB_VCF_OK);

  // Set up ranges (defaulting to all samples)
  const char* regions = "1:12100-13100";
  REQUIRE(tiledb_vcf_reader_set_regions(reader, regions) == TILEDB_VCF_OK);

  // Allocate and set buffers
  const unsigned expected_num_records = 4;
  uint32_t end_pos[expected_num_records];
  int32_t sample_name_offsets[expected_num_records + 1];
  char sample_name[expected_num_records * 10];
  int32_t alleles_offsets[expected_num_records + 1];
  char alleles[expected_num_records * 20];
  int32_t dp_offsets[expected_num_records + 1];
  int dp[expected_num_records];
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader, "pos_end", 0, nullptr, sizeof(end_pos), end_pos) ==
      TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "sample_name",
          sizeof(sample_name_offsets),
          sample_name_offsets,
          sizeof(sample_name),
          sample_name) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "alleles",
          sizeof(alleles_offsets),
          alleles_offsets,
          sizeof(alleles),
          alleles) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader, "fmt_DP", sizeof(dp_offsets), dp_offsets, sizeof(dp), dp) ==
      TILEDB_VCF_OK);

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
  REQUIRE(end_pos[0] == 12277);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[0]], 7) == 0);
  REQUIRE(strncmp("C,<NON_REF>", &alleles[alleles_offsets[0]], 11) == 0);
  REQUIRE(dp[dp_offsets[0] / sizeof(int32_t)] == 0);

  REQUIRE(end_pos[1] == 12277);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[1]], 7) == 0);
  REQUIRE(strncmp("C,<NON_REF>", &alleles[alleles_offsets[1]], 11) == 0);
  REQUIRE(dp[dp_offsets[1] / sizeof(int32_t)] == 0);

  REQUIRE(end_pos[2] == 12771);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[2]], 7) == 0);
  REQUIRE(strncmp("G,<NON_REF>", &alleles[alleles_offsets[2]], 11) == 0);
  REQUIRE(dp[dp_offsets[2] / sizeof(int32_t)] == 0);

  REQUIRE(end_pos[3] == 12771);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[3]], 7) == 0);
  REQUIRE(strncmp("G,<NON_REF>", &alleles[alleles_offsets[3]], 11) == 0);
  REQUIRE(dp[dp_offsets[3] / sizeof(int32_t)] == 0);

  tiledb_vcf_reader_free(&reader);
}

TEST_CASE("C API: Reader submit (BED file)", "[capi][reader]") {
  tiledb_vcf_reader_t* reader = nullptr;
  REQUIRE(tiledb_vcf_reader_alloc(&reader) == TILEDB_VCF_OK);
  auto dataset_uri = INPUT_ARRAYS_DIR + "/ingested_2samples_GT_DP_PL";
  REQUIRE(tiledb_vcf_reader_init(reader, dataset_uri.c_str()) == TILEDB_VCF_OK);

  // Set up samples and ranges
  auto bed_uri = TILEDB_VCF_TEST_INPUT_DIR + std::string("/simple.bed");
  REQUIRE(
      tiledb_vcf_reader_set_bed_file(reader, bed_uri.c_str()) == TILEDB_VCF_OK);

  // Allocate and set buffers
  const unsigned expected_num_records = 10;
  uint32_t end_pos[expected_num_records];
  int32_t sample_name_offsets[expected_num_records + 1];
  char sample_name[expected_num_records * 10];
  int32_t alleles_offsets[expected_num_records + 1];
  char alleles[expected_num_records * 20];
  int32_t dp_offsets[expected_num_records + 1];
  int dp[expected_num_records];
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader, "pos_end", 0, nullptr, sizeof(end_pos), end_pos) ==
      TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "sample_name",
          sizeof(sample_name_offsets),
          sample_name_offsets,
          sizeof(sample_name),
          sample_name) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "alleles",
          sizeof(alleles_offsets),
          alleles_offsets,
          sizeof(alleles),
          alleles) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader, "fmt_DP", sizeof(dp_offsets), dp_offsets, sizeof(dp), dp) ==
      TILEDB_VCF_OK);

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
  REQUIRE(end_pos[0] == 12277);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[0]], 7) == 0);
  REQUIRE(strncmp("C,<NON_REF>", &alleles[alleles_offsets[0]], 11) == 0);
  REQUIRE(dp[dp_offsets[0] / sizeof(int32_t)] == 0);

  REQUIRE(end_pos[1] == 12277);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[1]], 7) == 0);
  REQUIRE(strncmp("C,<NON_REF>", &alleles[alleles_offsets[1]], 11) == 0);
  REQUIRE(dp[dp_offsets[1] / sizeof(int32_t)] == 0);

  REQUIRE(end_pos[2] == 12771);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[2]], 7) == 0);
  REQUIRE(strncmp("G,<NON_REF>", &alleles[alleles_offsets[2]], 11) == 0);
  REQUIRE(dp[dp_offsets[2] / sizeof(int32_t)] == 0);

  REQUIRE(end_pos[3] == 12771);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[3]], 7) == 0);
  REQUIRE(strncmp("G,<NON_REF>", &alleles[alleles_offsets[3]], 11) == 0);
  REQUIRE(dp[dp_offsets[3] / sizeof(int32_t)] == 0);

  REQUIRE(end_pos[4] == 13374);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[4]], 7) == 0);
  REQUIRE(strncmp("T,<NON_REF>", &alleles[alleles_offsets[4]], 11) == 0);
  REQUIRE(dp[dp_offsets[4] / sizeof(int32_t)] == 15);

  REQUIRE(end_pos[5] == 13389);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[5]], 7) == 0);
  REQUIRE(strncmp("T,<NON_REF>", &alleles[alleles_offsets[5]], 11) == 0);
  REQUIRE(dp[dp_offsets[5] / sizeof(int32_t)] == 64);

  REQUIRE(end_pos[6] == 13519);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[6]], 7) == 0);
  REQUIRE(strncmp("G,<NON_REF>", &alleles[alleles_offsets[6]], 11) == 0);
  REQUIRE(dp[dp_offsets[6] / sizeof(int32_t)] == 10);

  REQUIRE(end_pos[7] == 13544);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[7]], 7) == 0);
  REQUIRE(strncmp("G,<NON_REF>", &alleles[alleles_offsets[7]], 11) == 0);
  REQUIRE(dp[dp_offsets[7] / sizeof(int32_t)] == 6);

  REQUIRE(end_pos[8] == 13689);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[8]], 7) == 0);
  REQUIRE(strncmp("G,<NON_REF>", &alleles[alleles_offsets[8]], 11) == 0);
  REQUIRE(dp[dp_offsets[8] / sizeof(int32_t)] == 0);

  REQUIRE(end_pos[9] == 17479);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[9]], 7) == 0);
  REQUIRE(strncmp("T,<NON_REF>", &alleles[alleles_offsets[9]], 11) == 0);
  REQUIRE(dp[dp_offsets[9] / sizeof(int32_t)] == 0);

  tiledb_vcf_reader_free(&reader);
}

TEST_CASE("C API: Reader submit (samples file)", "[capi][reader]") {
  tiledb_vcf_reader_t* reader = nullptr;
  REQUIRE(tiledb_vcf_reader_alloc(&reader) == TILEDB_VCF_OK);
  auto dataset_uri = INPUT_ARRAYS_DIR + "/ingested_2samples_GT_DP_PL";
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
  uint32_t end_pos[expected_num_records];
  int32_t sample_name_offsets[expected_num_records + 1];
  char sample_name[expected_num_records * 10];
  int32_t alleles_offsets[expected_num_records + 1];
  char alleles[expected_num_records * 20];
  int32_t dp_offsets[expected_num_records + 1];
  int dp[expected_num_records];
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader, "pos_end", 0, nullptr, sizeof(end_pos), end_pos) ==
      TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "sample_name",
          sizeof(sample_name_offsets),
          sample_name_offsets,
          sizeof(sample_name),
          sample_name) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "alleles",
          sizeof(alleles_offsets),
          alleles_offsets,
          sizeof(alleles),
          alleles) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader, "fmt_DP", sizeof(dp_offsets), dp_offsets, sizeof(dp), dp) ==
      TILEDB_VCF_OK);

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
  REQUIRE(end_pos[0] == 12277);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[0]], 7) == 0);
  REQUIRE(strncmp("C,<NON_REF>", &alleles[alleles_offsets[0]], 11) == 0);
  REQUIRE(dp[dp_offsets[0] / sizeof(int32_t)] == 0);

  REQUIRE(end_pos[1] == 12277);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[1]], 7) == 0);
  REQUIRE(strncmp("C,<NON_REF>", &alleles[alleles_offsets[1]], 11) == 0);
  REQUIRE(dp[dp_offsets[1] / sizeof(int32_t)] == 0);

  REQUIRE(end_pos[2] == 12771);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[2]], 7) == 0);
  REQUIRE(strncmp("G,<NON_REF>", &alleles[alleles_offsets[2]], 11) == 0);
  REQUIRE(dp[dp_offsets[2] / sizeof(int32_t)] == 0);

  REQUIRE(end_pos[3] == 12771);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[3]], 7) == 0);
  REQUIRE(strncmp("G,<NON_REF>", &alleles[alleles_offsets[3]], 11) == 0);
  REQUIRE(dp[dp_offsets[3] / sizeof(int32_t)] == 0);

  REQUIRE(end_pos[4] == 13374);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[4]], 7) == 0);
  REQUIRE(strncmp("T,<NON_REF>", &alleles[alleles_offsets[4]], 11) == 0);
  REQUIRE(dp[dp_offsets[4] / sizeof(int32_t)] == 15);

  REQUIRE(end_pos[5] == 13389);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[5]], 7) == 0);
  REQUIRE(strncmp("T,<NON_REF>", &alleles[alleles_offsets[5]], 11) == 0);
  REQUIRE(dp[dp_offsets[5] / sizeof(int32_t)] == 64);

  REQUIRE(end_pos[6] == 13519);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[6]], 7) == 0);
  REQUIRE(strncmp("G,<NON_REF>", &alleles[alleles_offsets[6]], 11) == 0);
  REQUIRE(dp[dp_offsets[6] / sizeof(int32_t)] == 10);

  REQUIRE(end_pos[7] == 13544);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[7]], 7) == 0);
  REQUIRE(strncmp("G,<NON_REF>", &alleles[alleles_offsets[7]], 11) == 0);
  REQUIRE(dp[dp_offsets[7] / sizeof(int32_t)] == 6);

  REQUIRE(end_pos[8] == 13689);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[8]], 7) == 0);
  REQUIRE(strncmp("G,<NON_REF>", &alleles[alleles_offsets[8]], 11) == 0);
  REQUIRE(dp[dp_offsets[8] / sizeof(int32_t)] == 0);

  REQUIRE(end_pos[9] == 17479);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[9]], 7) == 0);
  REQUIRE(strncmp("T,<NON_REF>", &alleles[alleles_offsets[9]], 11) == 0);
  REQUIRE(dp[dp_offsets[9] / sizeof(int32_t)] == 0);

  tiledb_vcf_reader_free(&reader);
}

TEST_CASE("C API: Reader submit (empty result set)", "[capi][reader]") {
  tiledb_vcf_reader_t* reader = nullptr;
  REQUIRE(tiledb_vcf_reader_alloc(&reader) == TILEDB_VCF_OK);
  auto dataset_uri = INPUT_ARRAYS_DIR + "/ingested_2samples_GT_DP_PL";
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
  auto dataset_uri = INPUT_ARRAYS_DIR + "/ingested_2samples_GT_DP_PL";
  REQUIRE(tiledb_vcf_reader_init(reader, dataset_uri.c_str()) == TILEDB_VCF_OK);

  // Set up samples and ranges
  const char* regions = "1:12100-13360,1:13500-16000";
  REQUIRE(tiledb_vcf_reader_set_regions(reader, regions) == TILEDB_VCF_OK);

  // Allocate and set buffers
  const unsigned expected_num_records = 9;
  const unsigned alloced_num_records = 2;
  uint32_t start_pos[alloced_num_records];
  uint32_t end_pos[alloced_num_records];
  int32_t sample_name_offsets[alloced_num_records + 1];
  char sample_name[alloced_num_records * 10];
  uint32_t qrange_start[alloced_num_records];
  uint32_t qrange_end[alloced_num_records];
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader, "pos_start", 0, nullptr, sizeof(start_pos), start_pos) ==
      TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader, "pos_end", 0, nullptr, sizeof(end_pos), end_pos) ==
      TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "sample_name",
          sizeof(sample_name_offsets),
          sample_name_offsets,
          sizeof(sample_name),
          sample_name) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "query_bed_start",
          0,
          nullptr,
          sizeof(qrange_start),
          qrange_start) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "query_bed_end",
          0,
          nullptr,
          sizeof(qrange_end),
          qrange_end) == TILEDB_VCF_OK);

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
  int64_t buff_size, off_size;
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader, "pos_start", &off_size, &buff_size) == TILEDB_VCF_OK);
  REQUIRE(buff_size == alloced_num_records * sizeof(uint32_t));
  REQUIRE(off_size == 0);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader, "sample_name", &off_size, &buff_size) == TILEDB_VCF_OK);
  REQUIRE(buff_size == 14);
  REQUIRE(off_size == (alloced_num_records + 1) * sizeof(int32_t));

  // Check first results
  REQUIRE(start_pos[0] == 12141);
  REQUIRE(end_pos[0] == 12277);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[0]], 7) == 0);
  REQUIRE(qrange_start[0] == 12099);
  REQUIRE(qrange_end[0] == 13360);

  REQUIRE(start_pos[1] == 12141);
  REQUIRE(end_pos[1] == 12277);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[1]], 7) == 0);
  REQUIRE(qrange_start[1] == 12099);
  REQUIRE(qrange_end[1] == 13360);

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
  REQUIRE(start_pos[0] == 12546);
  REQUIRE(end_pos[0] == 12771);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[0]], 7) == 0);
  REQUIRE(qrange_start[0] == 12099);
  REQUIRE(qrange_end[0] == 13360);

  REQUIRE(start_pos[1] == 12546);
  REQUIRE(end_pos[1] == 12771);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[1]], 7) == 0);
  REQUIRE(qrange_start[1] == 12099);
  REQUIRE(qrange_end[1] == 13360);

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
          reader, "pos_start", &off_size, &buff_size) == TILEDB_VCF_OK);
  REQUIRE(buff_size == alloced_num_records * sizeof(uint32_t));
  REQUIRE(off_size == 0);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader, "sample_name", &off_size, &buff_size) == TILEDB_VCF_OK);
  REQUIRE(buff_size == 14);
  REQUIRE(off_size == (alloced_num_records + 1) * sizeof(int32_t));

  // Check next results
  REQUIRE(start_pos[0] == 13354);
  REQUIRE(end_pos[0] == 13374);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[0]], 7) == 0);
  REQUIRE(qrange_start[0] == 12099);
  REQUIRE(qrange_end[0] == 13360);

  REQUIRE(start_pos[1] == 13354);
  REQUIRE(end_pos[1] == 13389);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[1]], 7) == 0);
  REQUIRE(qrange_start[1] == 12099);
  REQUIRE(qrange_end[1] == 13360);

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
  REQUIRE(start_pos[0] == 13452);
  REQUIRE(end_pos[0] == 13519);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[0]], 7) == 0);
  REQUIRE(qrange_start[0] == 13499);
  REQUIRE(qrange_end[0] == 16000);

  REQUIRE(start_pos[1] == 13520);
  REQUIRE(end_pos[1] == 13544);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[1]], 7) == 0);
  REQUIRE(qrange_start[1] == 13499);
  REQUIRE(qrange_end[1] == 16000);

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
  REQUIRE(start_pos[0] == 13545);
  REQUIRE(end_pos[0] == 13689);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[0]], 7) == 0);
  REQUIRE(qrange_start[0] == 13499);
  REQUIRE(qrange_end[0] == 16000);

  // Check total num records
  REQUIRE(tot_num_records == expected_num_records);

  // Check last buffer sizes
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader, "pos_start", &off_size, &buff_size) == TILEDB_VCF_OK);
  REQUIRE(buff_size == 1 * sizeof(uint32_t));
  REQUIRE(off_size == 0);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader, "sample_name", &off_size, &buff_size) == TILEDB_VCF_OK);
  REQUIRE(buff_size == 7);
  REQUIRE(off_size == 2 * sizeof(int32_t));

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
  auto dataset_uri = INPUT_ARRAYS_DIR + "/ingested_2samples_GT_DP_PL";
  REQUIRE(tiledb_vcf_reader_init(reader, dataset_uri.c_str()) == TILEDB_VCF_OK);

  // Set up samples and ranges
  const char* regions = "1:12277-13400,1:17000-18000";
  REQUIRE(tiledb_vcf_reader_set_regions(reader, regions) == TILEDB_VCF_OK);

  // Allocate and set buffers
  const unsigned expected_num_records = 10;
  const unsigned alloced_num_records = 2;
  uint32_t start_pos[alloced_num_records];
  uint32_t end_pos[alloced_num_records];
  int32_t sample_name_offsets[alloced_num_records + 1];
  char sample_name[alloced_num_records * 10];
  uint32_t qrange_start[alloced_num_records];
  uint32_t qrange_end[alloced_num_records];
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader, "pos_start", 0, nullptr, sizeof(start_pos), start_pos) ==
      TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader, "pos_end", 0, nullptr, sizeof(end_pos), end_pos) ==
      TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "sample_name",
          sizeof(sample_name_offsets),
          sample_name_offsets,
          sizeof(sample_name),
          sample_name) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "query_bed_start",
          0,
          nullptr,
          sizeof(qrange_start),
          qrange_start) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "query_bed_end",
          0,
          nullptr,
          sizeof(qrange_end),
          qrange_end) == TILEDB_VCF_OK);

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
  int64_t buff_size, off_size;
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader, "pos_start", &off_size, &buff_size) == TILEDB_VCF_OK);
  REQUIRE(buff_size == alloced_num_records * sizeof(uint32_t));
  REQUIRE(off_size == 0);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader, "sample_name", &off_size, &buff_size) == TILEDB_VCF_OK);
  REQUIRE(buff_size == 14);
  REQUIRE(off_size == (alloced_num_records + 1) * sizeof(int32_t));

  // Check first results
  REQUIRE(start_pos[0] == 12141);
  REQUIRE(end_pos[0] == 12277);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[0]], 7) == 0);
  REQUIRE(qrange_start[0] == 12276);
  REQUIRE(qrange_end[0] == 13400);

  REQUIRE(start_pos[1] == 12141);
  REQUIRE(end_pos[1] == 12277);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[1]], 7) == 0);
  REQUIRE(qrange_start[1] == 12276);
  REQUIRE(qrange_end[1] == 13400);

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
  REQUIRE(start_pos[0] == 12546);
  REQUIRE(end_pos[0] == 12771);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[0]], 7) == 0);
  REQUIRE(qrange_start[0] == 12276);
  REQUIRE(qrange_end[0] == 13400);

  REQUIRE(start_pos[1] == 12546);
  REQUIRE(end_pos[1] == 12771);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[1]], 7) == 0);
  REQUIRE(qrange_start[1] == 12276);
  REQUIRE(qrange_end[1] == 13400);

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
          reader, "pos_start", &off_size, &buff_size) == TILEDB_VCF_OK);
  REQUIRE(buff_size == alloced_num_records * sizeof(uint32_t));
  REQUIRE(off_size == 0);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader, "sample_name", &off_size, &buff_size) == TILEDB_VCF_OK);
  REQUIRE(buff_size == 14);
  REQUIRE(off_size == (alloced_num_records + 1) * sizeof(int32_t));

  // Check next results
  REQUIRE(start_pos[0] == 13354);
  REQUIRE(end_pos[0] == 13374);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[0]], 7) == 0);
  REQUIRE(qrange_start[0] == 12276);
  REQUIRE(qrange_end[0] == 13400);

  REQUIRE(start_pos[1] == 13354);
  REQUIRE(end_pos[1] == 13389);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[1]], 7) == 0);
  REQUIRE(qrange_start[1] == 12276);
  REQUIRE(qrange_end[1] == 13400);

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
  REQUIRE(start_pos[0] == 13375);
  REQUIRE(end_pos[0] == 13395);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[0]], 7) == 0);
  REQUIRE(qrange_start[0] == 12276);
  REQUIRE(qrange_end[0] == 13400);

  REQUIRE(start_pos[1] == 13396);
  REQUIRE(end_pos[1] == 13413);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[1]], 7) == 0);
  REQUIRE(qrange_start[1] == 12276);
  REQUIRE(qrange_end[1] == 13400);

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
  REQUIRE(start_pos[0] == 17319);
  REQUIRE(end_pos[0] == 17479);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[0]], 7) == 0);
  REQUIRE(qrange_start[0] == 16999);
  REQUIRE(qrange_end[0] == 18000);

  REQUIRE(start_pos[1] == 17480);
  REQUIRE(end_pos[1] == 17486);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[1]], 7) == 0);
  REQUIRE(qrange_start[1] == 16999);
  REQUIRE(qrange_end[1] == 18000);

  // Check total num records
  REQUIRE(tot_num_records == expected_num_records);

  // Check last buffer sizes
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader, "pos_start", &off_size, &buff_size) == TILEDB_VCF_OK);
  REQUIRE(buff_size == 2 * sizeof(uint32_t));
  REQUIRE(off_size == 0);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader, "sample_name", &off_size, &buff_size) == TILEDB_VCF_OK);
  REQUIRE(buff_size == 14);
  REQUIRE(off_size == 3 * sizeof(int32_t));

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
      "does not exist.";
  REQUIRE(tiledb_vcf_error_get_message(error, &msg) == TILEDB_VCF_OK);
  REQUIRE(std::string(msg) == expected_msg);
  tiledb_vcf_error_free(&error);

  // Check OK operation clears error message
  auto dataset_uri = INPUT_ARRAYS_DIR + "/ingested_2samples";
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
  auto dataset_uri = INPUT_ARRAYS_DIR + "/ingested_2samples_GT_DP_PL";
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
  uint32_t start_pos[alloced_num_records];
  uint32_t end_pos[alloced_num_records];
  int32_t sample_name_offsets[alloced_num_records + 1];
  char sample_name[alloced_num_records * 10];
  uint32_t qrange_start[alloced_num_records];
  uint32_t qrange_end[alloced_num_records];
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader, "pos_start", 0, nullptr, sizeof(start_pos), start_pos) ==
      TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader, "pos_end", 0, nullptr, sizeof(end_pos), end_pos) ==
      TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "sample_name",
          sizeof(sample_name_offsets),
          sample_name_offsets,
          sizeof(sample_name),
          sample_name) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "query_bed_start",
          0,
          nullptr,
          sizeof(qrange_start),
          qrange_start) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "query_bed_end",
          0,
          nullptr,
          sizeof(qrange_end),
          qrange_end) == TILEDB_VCF_OK);

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
  int64_t buff_size, off_size;
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader, "pos_start", &off_size, &buff_size) == TILEDB_VCF_OK);
  REQUIRE(buff_size == alloced_num_records * sizeof(uint32_t));
  REQUIRE(off_size == 0);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader, "sample_name", &off_size, &buff_size) == TILEDB_VCF_OK);
  REQUIRE(buff_size == 14);
  REQUIRE(off_size == (alloced_num_records + 1) * sizeof(int32_t));

  // Check first results
  REQUIRE(start_pos[0] == 12141);
  REQUIRE(end_pos[0] == 12277);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[0]], 7) == 0);
  REQUIRE(qrange_start[0] == 12099);
  REQUIRE(qrange_end[0] == 13360);

  REQUIRE(start_pos[1] == 12141);
  REQUIRE(end_pos[1] == 12277);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[1]], 7) == 0);
  REQUIRE(qrange_start[1] == 12099);
  REQUIRE(qrange_end[1] == 13360);

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
  REQUIRE(start_pos[0] == 12546);
  REQUIRE(end_pos[0] == 12771);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[0]], 7) == 0);
  REQUIRE(qrange_start[0] == 12099);
  REQUIRE(qrange_end[0] == 13360);

  // Check total num records
  REQUIRE(tot_num_records == expected_num_records);

  // Check last buffer sizes
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader, "pos_start", &off_size, &buff_size) == TILEDB_VCF_OK);
  REQUIRE(buff_size == 1 * sizeof(uint32_t));
  REQUIRE(off_size == 0);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader, "sample_name", &off_size, &buff_size) == TILEDB_VCF_OK);
  REQUIRE(buff_size == 7);
  REQUIRE(off_size == 2 * sizeof(int32_t));

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
  auto dataset_uri = INPUT_ARRAYS_DIR + "/ingested_2samples_GT_DP_PL";
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
  uint32_t end_pos0[allocated_num_records];
  int32_t sample_name_offsets0[allocated_num_records + 1];
  char sample_name0[allocated_num_records * 10];
  uint32_t end_pos1[allocated_num_records];
  int32_t sample_name_offsets1[allocated_num_records + 1];
  char sample_name1[allocated_num_records * 10];
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader0, "pos_end", 0, nullptr, sizeof(end_pos0), end_pos0) ==
      TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader0,
          "sample_name",
          sizeof(sample_name_offsets0),
          sample_name_offsets0,
          sizeof(sample_name0),
          sample_name0) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader1, "pos_end", 0, nullptr, sizeof(end_pos1), end_pos1) ==
      TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader1,
          "sample_name",
          sizeof(sample_name_offsets1),
          sample_name_offsets1,
          sizeof(sample_name1),
          sample_name1) == TILEDB_VCF_OK);

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
  REQUIRE(end_pos0[0] == 12277);
  REQUIRE(strncmp("HG00280", &sample_name0[sample_name_offsets0[0]], 7) == 0);

  REQUIRE(end_pos0[1] == 12277);
  REQUIRE(strncmp("HG01762", &sample_name0[sample_name_offsets0[1]], 7) == 0);

  REQUIRE(end_pos0[2] == 12771);
  REQUIRE(strncmp("HG00280", &sample_name0[sample_name_offsets0[2]], 7) == 0);

  REQUIRE(end_pos0[3] == 12771);
  REQUIRE(strncmp("HG01762", &sample_name0[sample_name_offsets0[3]], 7) == 0);

  REQUIRE(end_pos0[4] == 13374);
  REQUIRE(strncmp("HG00280", &sample_name0[sample_name_offsets0[4]], 7) == 0);

  REQUIRE(end_pos0[5] == 13389);
  REQUIRE(strncmp("HG01762", &sample_name0[sample_name_offsets0[5]], 7) == 0);

  REQUIRE(end_pos0[6] == 13519);
  REQUIRE(strncmp("HG00280", &sample_name0[sample_name_offsets0[6]], 7) == 0);

  REQUIRE(end_pos0[7] == 13544);
  REQUIRE(strncmp("HG00280", &sample_name0[sample_name_offsets0[7]], 7) == 0);

  REQUIRE(end_pos0[8] == 13689);
  REQUIRE(strncmp("HG00280", &sample_name0[sample_name_offsets0[8]], 7) == 0);

  REQUIRE(end_pos0[9] == 17479);
  REQUIRE(strncmp("HG00280", &sample_name0[sample_name_offsets0[9]], 7) == 0);

  REQUIRE(end_pos1[0] == 17486);
  REQUIRE(strncmp("HG00280", &sample_name1[sample_name_offsets1[0]], 7) == 0);

  tiledb_vcf_reader_free(&reader0);
  tiledb_vcf_reader_free(&reader1);
}

TEST_CASE("C API: Reader submit (partitioned samples)", "[capi][reader]") {
  tiledb_vcf_reader_t *reader0 = nullptr, *reader1 = nullptr;
  REQUIRE(tiledb_vcf_reader_alloc(&reader0) == TILEDB_VCF_OK);
  REQUIRE(tiledb_vcf_reader_alloc(&reader1) == TILEDB_VCF_OK);
  auto dataset_uri = INPUT_ARRAYS_DIR + "/ingested_2samples_GT_DP_PL";
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
  uint32_t end_pos0[allocated_num_records];
  int32_t sample_name_offsets0[allocated_num_records];
  char sample_name0[allocated_num_records * 10];
  uint32_t end_pos1[allocated_num_records];
  int32_t sample_name_offsets1[allocated_num_records];
  char sample_name1[allocated_num_records * 10];
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader0, "pos_end", 0, nullptr, sizeof(end_pos0), end_pos0) ==
      TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader0,
          "sample_name",
          sizeof(sample_name_offsets0),
          sample_name_offsets0,
          sizeof(sample_name0),
          sample_name0) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader1, "pos_end", 0, nullptr, sizeof(end_pos1), end_pos1) ==
      TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader1,
          "sample_name",
          sizeof(sample_name_offsets1),
          sample_name_offsets1,
          sizeof(sample_name1),
          sample_name1) == TILEDB_VCF_OK);

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
  REQUIRE(end_pos0[0] == 12277);
  REQUIRE(strncmp("HG00280", &sample_name0[sample_name_offsets0[0]], 7) == 0);

  REQUIRE(end_pos0[1] == 12771);
  REQUIRE(strncmp("HG00280", &sample_name0[sample_name_offsets0[1]], 7) == 0);

  REQUIRE(end_pos0[2] == 13374);
  REQUIRE(strncmp("HG00280", &sample_name0[sample_name_offsets0[2]], 7) == 0);

  REQUIRE(end_pos0[3] == 13519);
  REQUIRE(strncmp("HG00280", &sample_name0[sample_name_offsets0[3]], 7) == 0);

  REQUIRE(end_pos0[4] == 13544);
  REQUIRE(strncmp("HG00280", &sample_name0[sample_name_offsets0[4]], 7) == 0);

  REQUIRE(end_pos0[5] == 13689);
  REQUIRE(strncmp("HG00280", &sample_name0[sample_name_offsets0[5]], 7) == 0);

  REQUIRE(end_pos0[6] == 17479);
  REQUIRE(strncmp("HG00280", &sample_name0[sample_name_offsets0[6]], 7) == 0);

  REQUIRE(end_pos0[7] == 17486);
  REQUIRE(strncmp("HG00280", &sample_name0[sample_name_offsets0[7]], 7) == 0);

  REQUIRE(end_pos1[0] == 12277);
  REQUIRE(strncmp("HG01762", &sample_name1[sample_name_offsets1[0]], 7) == 0);

  REQUIRE(end_pos1[1] == 12771);
  REQUIRE(strncmp("HG01762", &sample_name1[sample_name_offsets1[1]], 7) == 0);

  REQUIRE(end_pos1[2] == 13389);
  REQUIRE(strncmp("HG01762", &sample_name1[sample_name_offsets1[2]], 7) == 0);

  tiledb_vcf_reader_free(&reader0);
  tiledb_vcf_reader_free(&reader1);
}

TEST_CASE("C API: Reader submit (ranges will overlap)", "[capi][reader]") {
  tiledb_vcf_reader_t* reader = nullptr;
  REQUIRE(tiledb_vcf_reader_alloc(&reader) == TILEDB_VCF_OK);
  auto dataset_uri = INPUT_ARRAYS_DIR + "/ingested_2samples_GT_DP_PL";
  REQUIRE(tiledb_vcf_reader_init(reader, dataset_uri.c_str()) == TILEDB_VCF_OK);

  // Set up samples and ranges
  const char* regions = "1:13000-13360,1:13500-16000";
  REQUIRE(tiledb_vcf_reader_set_regions(reader, regions) == TILEDB_VCF_OK);

  // Allocate and set buffers
  const unsigned expected_num_records = 5;
  uint32_t end_pos[expected_num_records];
  int32_t sample_name_offsets[expected_num_records + 1];
  char sample_name[expected_num_records * 10];
  int32_t alleles_offsets[expected_num_records + 1];
  char alleles[expected_num_records * 20];
  int32_t dp_offsets[expected_num_records + 1];
  int dp[expected_num_records];
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader, "pos_end", 0, nullptr, sizeof(end_pos), end_pos) ==
      TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "sample_name",
          sizeof(sample_name_offsets),
          sample_name_offsets,
          sizeof(sample_name),
          sample_name) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "alleles",
          sizeof(alleles_offsets),
          alleles_offsets,
          sizeof(alleles),
          alleles) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader, "fmt_DP", sizeof(dp_offsets), dp_offsets, sizeof(dp), dp) ==
      TILEDB_VCF_OK);

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
  REQUIRE(end_pos[0] == 13374);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[0]], 7) == 0);
  REQUIRE(strncmp("T,<NON_REF>", &alleles[alleles_offsets[0]], 11) == 0);
  REQUIRE(dp[dp_offsets[0] / sizeof(int32_t)] == 15);

  REQUIRE(end_pos[1] == 13389);
  REQUIRE(strncmp("HG01762", &sample_name[sample_name_offsets[1]], 7) == 0);
  REQUIRE(strncmp("T,<NON_REF>", &alleles[alleles_offsets[1]], 11) == 0);
  REQUIRE(dp[dp_offsets[1] / sizeof(int32_t)] == 64);

  REQUIRE(end_pos[2] == 13519);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[2]], 7) == 0);
  REQUIRE(strncmp("G,<NON_REF>", &alleles[alleles_offsets[2]], 11) == 0);
  REQUIRE(dp[dp_offsets[2] / sizeof(int32_t)] == 10);

  REQUIRE(end_pos[3] == 13544);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[3]], 7) == 0);
  REQUIRE(strncmp("G,<NON_REF>", &alleles[alleles_offsets[3]], 11) == 0);
  REQUIRE(dp[dp_offsets[3] / sizeof(int32_t)] == 6);

  REQUIRE(end_pos[4] == 13689);
  REQUIRE(strncmp("HG00280", &sample_name[sample_name_offsets[4]], 7) == 0);
  REQUIRE(strncmp("G,<NON_REF>", &alleles[alleles_offsets[4]], 11) == 0);
  REQUIRE(dp[dp_offsets[4] / sizeof(int32_t)] == 0);

  tiledb_vcf_reader_free(&reader);
}

TEST_CASE(
    "C API: Reader submit (incomplete, uneven attributes)",
    "[capi][reader][incomplete]") {
  tiledb_vcf_reader_t* reader = nullptr;
  REQUIRE(tiledb_vcf_reader_alloc(&reader) == TILEDB_VCF_OK);
  auto dataset_uri = INPUT_ARRAYS_DIR + "/ingested_2samples";
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
  int32_t alleles_offsets[3];
  char alleles[17];
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "sample_name",
          sizeof(sample_name_offsets),
          sample_name_offsets,
          sizeof(sample_name),
          sample_name) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "contig",
          sizeof(contig_offsets),
          contig_offsets,
          sizeof(contig),
          contig) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_reader_set_buffer(
          reader,
          "alleles",
          sizeof(alleles_offsets),
          alleles_offsets,
          sizeof(alleles),
          alleles) == TILEDB_VCF_OK);

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
  int64_t off_size = 0, buff_size = 0;
  REQUIRE(
      tiledb_vcf_reader_get_result_num_records(reader, &num_records) ==
      TILEDB_VCF_OK);
  REQUIRE(num_records == 1);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader, "sample_name", &off_size, &buff_size) == TILEDB_VCF_OK);
  REQUIRE(off_size == 2 * sizeof(int32_t));
  REQUIRE(buff_size == 7);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader, "contig", &off_size, &buff_size) == TILEDB_VCF_OK);
  REQUIRE(off_size == 2 * sizeof(int32_t));
  REQUIRE(buff_size == 1);

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
          reader, "sample_name", &off_size, &buff_size) == TILEDB_VCF_OK);
  REQUIRE(off_size == 2 * sizeof(int32_t));
  REQUIRE(buff_size == 7);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader, "contig", &off_size, &buff_size) == TILEDB_VCF_OK);
  REQUIRE(off_size == 2 * sizeof(int32_t));
  REQUIRE(buff_size == 1);

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
          reader, "sample_name", &off_size, &buff_size) == TILEDB_VCF_OK);
  REQUIRE(off_size == 2 * sizeof(int32_t));
  REQUIRE(buff_size == 7);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader, "contig", &off_size, &buff_size) == TILEDB_VCF_OK);
  REQUIRE(off_size == 2 * sizeof(int32_t));
  REQUIRE(buff_size == 1);

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
          reader, "sample_name", &off_size, &buff_size) == TILEDB_VCF_OK);
  REQUIRE(off_size == 2 * sizeof(int32_t));
  REQUIRE(buff_size == 7);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader, "contig", &off_size, &buff_size) == TILEDB_VCF_OK);
  REQUIRE(off_size == 2 * sizeof(int32_t));
  REQUIRE(buff_size == 1);

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
          reader, "sample_name", &off_size, &buff_size) == TILEDB_VCF_OK);
  REQUIRE(off_size == 2 * sizeof(int32_t));
  REQUIRE(buff_size == 7);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader, "contig", &off_size, &buff_size) == TILEDB_VCF_OK);
  REQUIRE(off_size == 2 * sizeof(int32_t));
  REQUIRE(buff_size == 1);

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
          reader, "sample_name", &off_size, &buff_size) == TILEDB_VCF_OK);
  REQUIRE(off_size == 2 * sizeof(int32_t));
  REQUIRE(buff_size == 7);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader, "contig", &off_size, &buff_size) == TILEDB_VCF_OK);
  REQUIRE(off_size == 2 * sizeof(int32_t));
  REQUIRE(buff_size == 1);

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
          reader, "sample_name", &off_size, &buff_size) == TILEDB_VCF_OK);
  REQUIRE(off_size == 2 * sizeof(int32_t));
  REQUIRE(buff_size == 7);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader, "contig", &off_size, &buff_size) == TILEDB_VCF_OK);
  REQUIRE(off_size == 2 * sizeof(int32_t));
  REQUIRE(buff_size == 1);

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
          reader, "sample_name", &off_size, &buff_size) == TILEDB_VCF_OK);
  REQUIRE(off_size == 2 * sizeof(int32_t));
  REQUIRE(buff_size == 7);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader, "contig", &off_size, &buff_size) == TILEDB_VCF_OK);
  REQUIRE(off_size == 2 * sizeof(int32_t));
  REQUIRE(buff_size == 1);

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
          reader, "sample_name", &off_size, &buff_size) == TILEDB_VCF_OK);
  REQUIRE(off_size == 2 * sizeof(int32_t));
  REQUIRE(buff_size == 7);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader, "contig", &off_size, &buff_size) == TILEDB_VCF_OK);
  REQUIRE(off_size == 2 * sizeof(int32_t));
  REQUIRE(buff_size == 1);

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
          reader, "sample_name", &off_size, &buff_size) == TILEDB_VCF_OK);
  REQUIRE(off_size == 2 * sizeof(int32_t));
  REQUIRE(buff_size == 7);
  REQUIRE(
      tiledb_vcf_reader_get_result_size(
          reader, "contig", &off_size, &buff_size) == TILEDB_VCF_OK);
  REQUIRE(off_size == 2 * sizeof(int32_t));
  REQUIRE(buff_size == 1);

  tiledb_vcf_reader_free(&reader);
}