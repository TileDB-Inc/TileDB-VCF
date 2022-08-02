/**
 * @file   writer.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2019 TileDB, Inc.
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
 */

#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <tiledbvcf/tiledbvcf.h>

#include <map>
#include <set>

#include "writer.h"

namespace py = pybind11;

namespace {
void check_error(tiledb_vcf_writer_t* writer, int32_t rc) {
  if (rc != TILEDB_VCF_OK) {
    std::string msg =
        "TileDB-VCF-Py: Error getting tiledb_vcf_error_t error message.";
    tiledb_vcf_error_t* err = nullptr;
    const char* c_msg = nullptr;
    if (tiledb_vcf_writer_get_last_error(writer, &err) == TILEDB_VCF_OK &&
        tiledb_vcf_error_get_message(err, &c_msg) == TILEDB_VCF_OK) {
      msg = std::string(c_msg);
    }
    throw std::runtime_error(msg);
  }
}
}  // namespace

namespace tiledbvcfpy {

Writer::Writer()
    : ptr(nullptr, deleter) {
  tiledb_vcf_writer_t* r;
  if (tiledb_vcf_writer_alloc(&r) != TILEDB_VCF_OK)
    throw std::runtime_error(
        "TileDB-VCF-Py: Failed to allocate tiledb_vcf_writer_t instance.");
  ptr.reset(r);
}

void Writer::init(const std::string& dataset_uri) {
  auto writer = ptr.get();
  check_error(writer, tiledb_vcf_writer_init(writer, dataset_uri.c_str()));
}

void Writer::set_tiledb_stats_enabled(const bool stats_enabled) {
  auto writer = ptr.get();
  check_error(
      writer,
      tiledb_vcf_writer_set_tiledb_stats_enabled(writer, stats_enabled));
}

void Writer::set_samples(const std::string& samples) {
  auto writer = ptr.get();
  check_error(writer, tiledb_vcf_writer_set_samples(writer, samples.c_str()));
}

void Writer::set_extra_attributes(const std::string& attributes) {
  auto writer = ptr.get();
  check_error(
      writer,
      tiledb_vcf_writer_set_extra_attributes(writer, attributes.c_str()));
}

void Writer::set_vcf_attributes(const std::string& vcf_uri) {
  auto writer = ptr.get();
  check_error(
      writer, tiledb_vcf_writer_set_vcf_attributes(writer, vcf_uri.c_str()));
}

void Writer::set_checksum(const std::string& checksum) {
  auto writer = ptr.get();
  tiledb_vcf_checksum_type_t checksum_type = TILEDB_VCF_CHECKSUM_SHA256;

  if (checksum == "md5")
    checksum_type = TILEDB_VCF_CHECKSUM_MD5;
  else if (checksum == "sha256")
    checksum_type = TILEDB_VCF_CHECKSUM_SHA256;
  else if (checksum == "none")
    checksum_type = TILEDB_VCF_CHECKSUM_NONE;

  check_error(
      writer, tiledb_vcf_writer_set_checksum_type(writer, checksum_type));
}

void Writer::set_allow_duplicates(const bool& allow_duplicates) {
  auto writer = ptr.get();
  check_error(
      writer, tiledb_vcf_writer_set_allow_duplicates(writer, allow_duplicates));
}

void Writer::set_tile_capacity(const uint64_t tile_capacity) {
  auto writer = ptr.get();
  check_error(
      writer, tiledb_vcf_writer_set_tile_capacity(writer, tile_capacity));
}

void Writer::set_anchor_gap(const uint32_t anchor_gap) {
  auto writer = ptr.get();
  check_error(writer, tiledb_vcf_writer_set_anchor_gap(writer, anchor_gap));
}

void Writer::set_num_threads(const uint32_t threads) {
  auto writer = ptr.get();
  check_error(writer, tiledb_vcf_writer_set_num_threads(writer, threads));
}

void Writer::set_total_memory_budget_mb(const uint32_t total_memory_budget_mb) {
  auto writer = ptr.get();
  check_error(
      writer,
      tiledb_vcf_writer_set_total_memory_budget_mb(
          writer, total_memory_budget_mb));
}

void Writer::set_total_memory_percentage(const float total_memory_percentage) {
  auto writer = ptr.get();
  check_error(
      writer,
      tiledb_vcf_writer_set_total_memory_percentage(
          writer, total_memory_percentage));
}

void Writer::set_ratio_tiledb_memory(const float ratio_tiledb_memory) {
  auto writer = ptr.get();
  check_error(
      writer,
      tiledb_vcf_writer_set_ratio_tiledb_memory(writer, ratio_tiledb_memory));
}

void Writer::set_max_tiledb_memory_mb(const uint32_t max_tiledb_memory_mb) {
  auto writer = ptr.get();
  check_error(
      writer,
      tiledb_vcf_writer_set_max_tiledb_memory_mb(writer, max_tiledb_memory_mb));
}

void Writer::set_input_record_buffer_mb(const uint32_t input_record_buffer_mb) {
  auto writer = ptr.get();
  check_error(
      writer,
      tiledb_vcf_writer_set_input_record_buffer_mb(
          writer, input_record_buffer_mb));
}

void Writer::set_avg_vcf_record_size(const uint32_t avg_vcf_record_size) {
  auto writer = ptr.get();
  check_error(
      writer,
      tiledb_vcf_writer_set_avg_vcf_record_size(writer, avg_vcf_record_size));
}

void Writer::set_ratio_task_size(const float ratio_task_size) {
  auto writer = ptr.get();
  check_error(
      writer, tiledb_vcf_writer_set_ratio_task_size(writer, ratio_task_size));
}

void Writer::set_ratio_output_flush(const float ratio_output_flush) {
  auto writer = ptr.get();
  check_error(
      writer,
      tiledb_vcf_writer_set_ratio_output_flush(writer, ratio_output_flush));
}

void Writer::set_thread_task_size(const uint32_t size) {
  auto writer = ptr.get();
  check_error(writer, tiledb_vcf_writer_set_thread_task_size(writer, size));
}

void Writer::set_memory_budget(const uint32_t memory_mb) {
  auto writer = ptr.get();
  check_error(writer, tiledb_vcf_writer_set_memory_budget(writer, memory_mb));
}

void Writer::set_scratch_space(const std::string& path, uint64_t size) {
  auto writer = ptr.get();
  check_error(
      writer, tiledb_vcf_writer_set_scratch_space(writer, path.c_str(), size));
}

void Writer::set_max_num_records(const uint64_t max_num_records) {
  auto writer = ptr.get();
  check_error(
      writer, tiledb_vcf_writer_set_max_num_records(writer, max_num_records));
}

void Writer::set_verbose(bool verbose) {
  auto writer = ptr.get();
  check_error(writer, tiledb_vcf_writer_set_verbose(writer, verbose));
}

void Writer::create_dataset() {
  auto writer = ptr.get();
  check_error(writer, tiledb_vcf_writer_create_dataset(writer));
}

void Writer::register_samples() {
  auto writer = ptr.get();
  check_error(writer, tiledb_vcf_writer_register(writer));
}

void Writer::ingest_samples() {
  auto writer = ptr.get();
  check_error(writer, tiledb_vcf_writer_store(writer));
}

void Writer::deleter(tiledb_vcf_writer_t* w) {
  tiledb_vcf_writer_free(&w);
}

int32_t Writer::get_schema_version() {
  auto writer = ptr.get();
  int32_t version;
  check_error(writer, tiledb_vcf_writer_get_dataset_version(writer, &version));
  return version;
}

void Writer::set_tiledb_config(const std::string& config_str) {
  auto writer = ptr.get();
  check_error(
      writer, tiledb_vcf_writer_set_tiledb_config(writer, config_str.c_str()));
}

void Writer::set_sample_batch_size(const uint64_t size) {
  auto writer = ptr.get();
  check_error(writer, tiledb_vcf_writer_set_sample_batch_size(writer, size));
}

bool Writer::get_tiledb_stats_enabled() {
  auto writer = ptr.get();
  bool stats_enabled;
  check_error(
      writer,
      tiledb_vcf_writer_get_tiledb_stats_enabled(writer, &stats_enabled));
  return stats_enabled;
}

std::string Writer::get_tiledb_stats() {
  auto writer = ptr.get();
  char* stats;
  check_error(writer, tiledb_vcf_writer_get_tiledb_stats(writer, &stats));
  return std::string(stats);
}

std::string Writer::version() {
  const char* version_str;
  tiledb_vcf_version(&version_str);
  return version_str;
}

void Writer::set_resume(const bool resume) {
  auto writer = ptr.get();
  check_error(
      writer,
      tiledb_vcf_writer_set_resume_sample_partial_ingestion(writer, resume));
}

void Writer::set_contig_fragment_merging(const bool contig_fragment_merging) {
  auto writer = ptr.get();
  check_error(
      writer,
      tiledb_vcf_writer_set_contig_fragment_merging(
          writer, contig_fragment_merging));
}

void Writer::set_contigs_to_keep_separate(
    const std::vector<std::string>& contigs_to_keep_separate) {
  auto writer = ptr.get();

  // Convert vector to char**
  std::vector<const char*> contigs;

  for (const auto& contig : contigs_to_keep_separate)
    contigs.push_back(contig.c_str());

  check_error(
      writer,
      tiledb_vcf_writer_set_contigs_to_keep_separate(
          writer, contigs.data(), contigs.size()));
}

void Writer::set_contigs_to_allow_merging(
    const std::vector<std::string>& contigs_to_allow_merging) {
  auto writer = ptr.get();

  // Convert vector to char**
  std::vector<const char*> contigs;

  for (const auto& contig : contigs_to_allow_merging)
    contigs.push_back(contig.c_str());

  check_error(
      writer,
      tiledb_vcf_writer_set_contigs_to_allow_merging(
          writer, contigs.data(), contigs.size()));
}

void Writer::set_contig_mode(int contig_mode) {
  auto writer = ptr.get();

  check_error(writer, tiledb_vcf_writer_set_contig_mode(writer, contig_mode));
}

void Writer::set_enable_allele_count(bool enable) {
  auto writer = ptr.get();
  check_error(
      writer, tiledb_vcf_writer_set_enable_allele_count(writer, enable));
}

void Writer::set_enable_variant_stats(bool enable) {
  auto writer = ptr.get();
  check_error(
      writer, tiledb_vcf_writer_set_enable_variant_stats(writer, enable));
}

}  // namespace tiledbvcfpy
