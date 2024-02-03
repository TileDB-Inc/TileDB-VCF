/**
 * @file   reader.cc
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

#include <stdint.h>
#include <sys/types.h>
#include <cmath>
#include <stdexcept>

#include "reader.h"

namespace py = pybind11;

namespace {
void check_error(tiledb_vcf_reader_t* reader, int32_t rc) {
  if (rc != TILEDB_VCF_OK) {
    std::string msg =
        "TileDB-VCF-Py: Error getting tiledb_vcf_error_t error message.";
    tiledb_vcf_error_t* err = nullptr;
    const char* c_msg = nullptr;
    if (tiledb_vcf_reader_get_last_error(reader, &err) == TILEDB_VCF_OK &&
        tiledb_vcf_error_get_message(err, &c_msg) == TILEDB_VCF_OK) {
      msg = std::string(c_msg);
    }
    throw std::runtime_error(msg);
  }
}
}  // namespace

namespace tiledbvcfpy {

void config_logging(const std::string& level, const std::string& logfile) {
  tiledb_vcf_config_logging(level.c_str(), logfile.c_str());
}

Reader::Reader()
    : ptr(nullptr, deleter)
    , mem_budget_mb_(2 * 1024) {
  tiledb_vcf_reader_t* r;
  if (tiledb_vcf_reader_alloc(&r) != TILEDB_VCF_OK)
    throw std::runtime_error(
        "TileDB-VCF-Py: Failed to allocate tiledb_vcf_reader_t instance.");
  ptr.reset(r);
}

void Reader::init(const std::string& dataset_uri) {
  auto reader = ptr.get();
  check_error(reader, tiledb_vcf_reader_init(reader, dataset_uri.c_str()));
}

void Reader::reset() {
  auto reader = ptr.get();
  check_error(reader, tiledb_vcf_reader_reset(reader));
}

void Reader::set_attributes(const std::vector<std::string>& attributes) {
  attributes_ = attributes;
}

void Reader::set_tiledb_stats_enabled(const bool stats_enabled) {
  auto reader = ptr.get();
  check_error(
      reader,
      tiledb_vcf_reader_set_tiledb_stats_enabled(reader, stats_enabled));
}

void Reader::set_samples(const std::string& samples) {
  auto reader = ptr.get();
  check_error(reader, tiledb_vcf_reader_set_samples(reader, samples.c_str()));
}

void Reader::set_samples_file(const std::string& uri) {
  auto reader = ptr.get();
  check_error(reader, tiledb_vcf_reader_set_samples_file(reader, uri.c_str()));
}

void Reader::set_regions(const std::string& regions) {
  auto reader = ptr.get();
  check_error(reader, tiledb_vcf_reader_set_regions(reader, regions.c_str()));
}

void Reader::set_bed_file(const std::string& uri) {
  auto reader = ptr.get();
  check_error(reader, tiledb_vcf_reader_set_bed_file(reader, uri.c_str()));
}

void Reader::set_bed_array(const std::string& uri) {
  auto reader = ptr.get();
  check_error(reader, tiledb_vcf_reader_set_bed_array(reader, uri.c_str()));
}

void Reader::set_region_partition(int32_t partition, int32_t num_partitions) {
  auto reader = ptr.get();
  check_error(
      reader,
      tiledb_vcf_reader_set_region_partition(
          reader, partition, num_partitions));
}

void Reader::set_sample_partition(int32_t partition, int32_t num_partitions) {
  auto reader = ptr.get();
  check_error(
      reader,
      tiledb_vcf_reader_set_sample_partition(
          reader, partition, num_partitions));
}

void Reader::set_memory_budget(int32_t memory_mb) {
  mem_budget_mb_ = memory_mb;

  // TileDB-VCF gets two thirds the budget, we use the other third for buffer
  // allocation.
  auto reader = ptr.get();
  check_error(
      reader,
      tiledb_vcf_reader_set_memory_budget(
          reader, uint64_t(mem_budget_mb_ / 3.0 * 2.0)));
}

void Reader::set_sort_regions(bool sort_regions) {
  auto reader = ptr.get();
  check_error(
      reader, tiledb_vcf_reader_set_sort_regions(reader, sort_regions ? 1 : 0));
}

void Reader::set_max_num_records(int64_t max_num_records) {
  auto reader = ptr.get();
  check_error(
      reader, tiledb_vcf_reader_set_max_num_records(reader, max_num_records));
}

void Reader::set_tiledb_config(const std::string& config_str) {
  auto reader = ptr.get();
  check_error(
      reader, tiledb_vcf_reader_set_tiledb_config(reader, config_str.c_str()));
}

void Reader::set_verbose(bool verbose) {
  auto reader = ptr.get();
  check_error(reader, tiledb_vcf_reader_set_verbose(reader, verbose));
}

void Reader::set_export_to_disk(bool export_to_disk) {
  auto reader = ptr.get();
  check_error(
      reader, tiledb_vcf_reader_set_export_to_disk(reader, export_to_disk));
}

void Reader::set_merge(bool merge) {
  auto reader = ptr.get();
  check_error(reader, tiledb_vcf_reader_set_merge(reader, merge));
}

void Reader::set_output_format(const std::string& output_format) {
  auto reader = ptr.get();
  check_error(
      reader,
      tiledb_vcf_reader_set_output_format(reader, output_format.c_str()));
}

void Reader::set_output_path(const std::string& output_path) {
  auto reader = ptr.get();
  check_error(
      reader, tiledb_vcf_reader_set_output_path(reader, output_path.c_str()));
}

void Reader::set_output_dir(const std::string& output_dir) {
  auto reader = ptr.get();
  check_error(
      reader, tiledb_vcf_reader_set_output_dir(reader, output_dir.c_str()));
}

void Reader::set_af_filter(const std::string& af_filter) {
  auto reader = ptr.get();
  check_error(
      reader, tiledb_vcf_reader_set_af_filter(reader, af_filter.c_str()));
}

void Reader::set_scan_all_samples(const bool scan_all_samples) {
  auto reader = ptr.get();
  check_error(
      reader, tiledb_vcf_reader_set_scan_all_samples(reader, scan_all_samples));
}

void Reader::read(const bool release_buffs) {
  auto reader = ptr.get();
  bool af_filter_enabled = false;
  if (tiledb_vcf_reader_get_af_filter_exists(reader, &af_filter_enabled) ==
      TILEDB_VCF_ERR)
    throw std::runtime_error("TileDB-VCF-Py: Error finding AF filter.");
  if (!af_filter_enabled) {
    for (std::string attribute : attributes_) {
      if (attribute == "info_TILEDB_IAF") {
        tiledb_vcf_reader_set_af_filter(reader, ">=0");
        af_filter_enabled = true;
      }
    }
  }
  if (af_filter_enabled) {
    // add alleles buffer only if not already present
    bool add_alleles = true;
    bool add_GT = true;
    for (std::string attribute : attributes_) {
      add_alleles = add_alleles && attribute != "alleles";
      add_GT = add_GT && attribute != "fmt_GT";
    }
    if (add_alleles) {
      attributes_.emplace_back("alleles");
    }
    if (add_GT) {
      attributes_.emplace_back("fmt_GT");
    }
  }
  alloc_buffers(release_buffs);
  set_buffers();

  {
    // Release python GIL while reading data from TileDB
    py::gil_scoped_release release;
    check_error(reader, tiledb_vcf_reader_read(reader));
  }

  tiledb_vcf_read_status_t status;
  check_error(reader, tiledb_vcf_reader_get_status(reader, &status));
  if (status != TILEDB_VCF_COMPLETED && status != TILEDB_VCF_INCOMPLETE)
    throw std::runtime_error(
        "TileDB-VCF-Py: Error submitting read; unhandled read status.");
}

void Reader::alloc_buffers(const bool release_buffs) {
  auto reader = ptr.get();

  // Release old buffers. TODO: reuse when possible
  if (release_buffs)
    release_buffers();

  // Get a count of the number of buffers required.
  int num_buffers = 0;
  for (const auto& attr : attributes_) {
    tiledb_vcf_attr_datatype_t datatype = TILEDB_VCF_UINT8;
    int32_t var_len = 0, nullable = 0, list = 0;
    check_error(
        reader,
        tiledb_vcf_reader_get_attribute_type(
            reader, attr.c_str(), &datatype, &var_len, &nullable, &list));
    num_buffers += 1;
    num_buffers += var_len ? 1 : 0;
    num_buffers += nullable ? 1 : 0;
    num_buffers += list ? 1 : 0;
  }

  if (num_buffers == 0)
    return;

  // Only use one third the budget because TileDB-VCF gets the other two thirds.
  const int64_t budget_mb = mem_budget_mb_ / 3.0;

  // The undocumented "0 MB" budget is used only for testing incomplete queries.
  int64_t alloc_size_bytes;
  if (mem_budget_mb_ == 0) {
    alloc_size_bytes = 10;  // Some small value
  } else {
    alloc_size_bytes = (budget_mb * 1024 * 1024) / num_buffers;
  }

  for (const auto& attr : attributes_) {
    tiledb_vcf_attr_datatype_t datatype = TILEDB_VCF_UINT8;
    int32_t var_len = 0, nullable = 0, list = 0;
    check_error(
        reader,
        tiledb_vcf_reader_get_attribute_type(
            reader, attr.c_str(), &datatype, &var_len, &nullable, &list));

    // Allocate the buffers
    size_t count = alloc_size_bytes / type_size(datatype);
    buffers_.emplace_back(BufferInfo::create(
        attr, datatype, count, var_len ? count : 0, nullable, list));
  }
}

void Reader::set_buffers() {
  auto reader = ptr.get();
  for (auto& buffer : buffers_) {
    auto& attr = buffer->name();
    auto& data = buffer->data();
    auto& offsets = buffer->offsets();
    auto& list_offsets = buffer->list_offsets();
    auto& bitmap = buffer->bitmap();

    check_error(
        reader,
        tiledb_vcf_reader_set_buffer_values(
            reader, attr.c_str(), data.capacity(), data.data()));

    if (buffer->is_var_len()) {
      check_error(
          reader,
          tiledb_vcf_reader_set_buffer_offsets(
              reader, attr.c_str(), offsets.capacity(), offsets.data()));
    }

    if (buffer->is_list()) {
      check_error(
          reader,
          tiledb_vcf_reader_set_buffer_list_offsets(
              reader,
              attr.c_str(),
              list_offsets.capacity(),
              list_offsets.data()));
    }

    if (buffer->is_nullable()) {
      check_error(
          reader,
          tiledb_vcf_reader_set_buffer_validity_bitmap(
              reader, attr.c_str(), bitmap.capacity(), bitmap.data()));
    }
  }
}

void Reader::release_buffers() {
  auto reader = ptr.get();
  check_error(reader, tiledb_vcf_reader_reset_buffers(reader));

  buffers_.clear();
}

py::object Reader::get_results_arrow() {
  auto reader = ptr.get();

  int64_t num_records = result_num_records();
  for (auto& buffer : buffers_) {
    // build new arrow::array based on the result size
    int64_t num_offsets = 0, num_data_elements = 0, num_data_bytes = 0;
    check_error(
        reader,
        tiledb_vcf_reader_get_result_size(
            reader,
            buffer->name().c_str(),
            &num_offsets,
            &num_data_elements,
            &num_data_bytes));

    build_arrow_array_from_buffer(
        buffer, num_records, num_offsets, num_data_elements);
  }

  return buffers_to_table(buffers_);
}

int64_t Reader::result_num_records() {
  auto reader = ptr.get();
  int64_t result = 0;
  check_error(
      reader, tiledb_vcf_reader_get_result_num_records(reader, &result));
  return result;
}

bool Reader::completed() {
  auto reader = ptr.get();
  tiledb_vcf_read_status_t status;
  check_error(reader, tiledb_vcf_reader_get_status(reader, &status));
  return status == TILEDB_VCF_COMPLETED;
}

bool Reader::get_tiledb_stats_enabled() {
  auto reader = ptr.get();
  bool stats_enabled;
  check_error(
      reader,
      tiledb_vcf_reader_get_tiledb_stats_enabled(reader, &stats_enabled));
  return stats_enabled;
}

std::string Reader::get_tiledb_stats() {
  auto reader = ptr.get();
  char* stats;
  check_error(reader, tiledb_vcf_reader_get_tiledb_stats(reader, &stats));
  return std::string(stats);
}

int32_t Reader::get_schema_version() {
  auto reader = ptr.get();
  int32_t version;
  check_error(reader, tiledb_vcf_reader_get_dataset_version(reader, &version));
  return version;
}

std::vector<std::string> Reader::get_fmt_attributes() {
  auto reader = ptr.get();
  int32_t count;
  std::vector<std::string> attrs;
  check_error(
      reader, tiledb_vcf_reader_get_fmt_attribute_count(reader, &count));

  for (int32_t i = 0; i < count; i++) {
    char* name;
    check_error(
        reader, tiledb_vcf_reader_get_fmt_attribute_name(reader, i, &name));
    attrs.emplace_back(name);
  }

  return attrs;
}

std::vector<std::string> Reader::get_info_attributes() {
  auto reader = ptr.get();
  int32_t count;
  std::vector<std::string> attrs;
  check_error(
      reader, tiledb_vcf_reader_get_info_attribute_count(reader, &count));

  for (int32_t i = 0; i < count; i++) {
    char* name;
    check_error(
        reader, tiledb_vcf_reader_get_info_attribute_name(reader, i, &name));
    attrs.emplace_back(name);
  }

  return attrs;
}

std::vector<std::string> Reader::get_queryable_attributes() {
  auto reader = ptr.get();
  int32_t count;
  std::vector<std::string> attrs;
  check_error(
      reader, tiledb_vcf_reader_get_queryable_attribute_count(reader, &count));

  for (int32_t i = 0; i < count; i++) {
    char* name;
    check_error(
        reader,
        tiledb_vcf_reader_get_queryable_attribute_name(reader, i, &name));
    attrs.emplace_back(name);
  }

  return attrs;
}

std::vector<std::string> Reader::get_materialized_attributes() {
  auto reader = ptr.get();
  int32_t count;
  std::vector<std::string> attrs;
  check_error(
      reader,
      tiledb_vcf_reader_get_materialized_attribute_count(reader, &count));

  for (int32_t i = 0; i < count; i++) {
    char* name;
    check_error(
        reader,
        tiledb_vcf_reader_get_materialized_attribute_name(reader, i, &name));
    attrs.emplace_back(name);
  }

  return attrs;
}

int32_t Reader::get_sample_count() {
  auto reader = ptr.get();
  int32_t count;
  check_error(reader, tiledb_vcf_reader_get_sample_count(reader, &count));
  return count;
}

std::vector<std::string> Reader::get_sample_names() {
  auto reader = ptr.get();
  int32_t count;
  std::vector<std::string> names;
  check_error(reader, tiledb_vcf_reader_get_sample_count(reader, &count));

  names.reserve(count);
  for (int32_t i = 0; i < count; i++) {
    const char* name;
    check_error(reader, tiledb_vcf_reader_get_sample_name(reader, i, &name));
    names.emplace_back(name);
  }

  return names;
}

py::object Reader::get_variant_stats_results() {
  auto reader = ptr.get();

  size_t num_rows, alleles_size;
  check_error(reader, tiledb_vcf_reader_prepare_variant_stats(reader));
  check_error(
      reader,
      tiledb_vcf_reader_get_variant_stats_buffer_sizes(
          reader, &num_rows, &alleles_size));

  auto pos = BufferInfo::create("pos", TILEDB_VCF_INT32, num_rows);
  auto allele =
      BufferInfo::create("alleles", TILEDB_VCF_CHAR, num_rows, alleles_size);
  auto ac = BufferInfo::create("ac", TILEDB_VCF_INT32, num_rows);
  auto an = BufferInfo::create("an", TILEDB_VCF_INT32, num_rows);
  auto af = BufferInfo::create("af", TILEDB_VCF_FLOAT32, num_rows);

  check_error(
      reader,
      tiledb_vcf_reader_read_from_variant_stats(
          reader,
          reinterpret_cast<uint32_t*>(pos->data().data()),
          reinterpret_cast<char*>(allele->data().data()),
          reinterpret_cast<int32_t*>(allele->offsets().data()),
          reinterpret_cast<int*>(ac->data().data()),
          reinterpret_cast<int*>(an->data().data()),
          reinterpret_cast<float*>(af->data().data())));

  build_arrow_array_from_buffer(pos, num_rows, 0, num_rows);
  build_arrow_array_from_buffer(allele, num_rows, 0, alleles_size);
  build_arrow_array_from_buffer(ac, num_rows, 0, num_rows);
  build_arrow_array_from_buffer(an, num_rows, 0, num_rows);
  build_arrow_array_from_buffer(af, num_rows, 0, num_rows);

  std::vector<std::shared_ptr<BufferInfo>> buffers = {pos, allele, ac, an, af};
  return buffers_to_table(buffers);
}

py::object Reader::get_allele_count_results() {
  auto reader = ptr.get();

  size_t num_rows, refs_size, alts_size, filters_size, gts_size;
  check_error(reader, tiledb_vcf_reader_prepare_allele_count(reader));
  check_error(
      reader,
      tiledb_vcf_reader_get_allele_count_buffer_sizes(
          reader, &num_rows, &refs_size, &alts_size, &filters_size, &gts_size));
  if (num_rows > 0) {
    auto pos = BufferInfo::create("pos", TILEDB_VCF_INT32, num_rows);
    auto ref = BufferInfo::create("ref", TILEDB_VCF_CHAR, num_rows, refs_size);
    auto alt = BufferInfo::create("alt", TILEDB_VCF_CHAR, num_rows, alts_size);
    auto filter =
        BufferInfo::create("filter", TILEDB_VCF_CHAR, num_rows, filters_size);
    auto gt = BufferInfo::create("gt", TILEDB_VCF_CHAR, num_rows, gts_size);
    auto count = BufferInfo::create("count", TILEDB_VCF_INT32, num_rows);

    check_error(
        reader,
        tiledb_vcf_reader_read_from_allele_count(
            reader,
            reinterpret_cast<uint32_t*>(pos->data().data()),
            reinterpret_cast<char*>(ref->data().data()),
            reinterpret_cast<uint32_t*>(ref->offsets().data()),
            reinterpret_cast<char*>(alt->data().data()),
            reinterpret_cast<uint32_t*>(alt->offsets().data()),
            reinterpret_cast<char*>(filter->data().data()),
            reinterpret_cast<uint32_t*>(filter->offsets().data()),
            reinterpret_cast<char*>(gt->data().data()),
            reinterpret_cast<uint32_t*>(gt->offsets().data()),
            reinterpret_cast<int32_t*>(count->data().data())));

    build_arrow_array_from_buffer(pos, num_rows, 0, num_rows);
    build_arrow_array_from_buffer(ref, num_rows, 0, num_rows);
    build_arrow_array_from_buffer(alt, num_rows, 0, num_rows);
    build_arrow_array_from_buffer(filter, num_rows, 0, num_rows);
    build_arrow_array_from_buffer(gt, num_rows, 0, num_rows);
    build_arrow_array_from_buffer(count, num_rows, 0, num_rows);

    std::vector<std::shared_ptr<BufferInfo>> buffers = {
        pos, ref, alt, filter, gt, count};
    return buffers_to_table(buffers);
  }
  return py::cast<py::none> Py_None;
}

void Reader::deleter(tiledb_vcf_reader_t* r) {
  tiledb_vcf_reader_free(&r);
}

void Reader::set_buffer_percentage(float buffer_percentage) {
  auto reader = ptr.get();
  check_error(
      reader,
      tiledb_vcf_reader_set_buffer_percentage(reader, buffer_percentage));
}

void Reader::set_tiledb_tile_cache_percentage(float tile_percentage) {
  auto reader = ptr.get();
  check_error(
      reader,
      tiledb_vcf_reader_set_tiledb_tile_cache_percentage(
          reader, tile_percentage));
}

void Reader::set_check_samples_exist(bool samples_exists) {
  auto reader = ptr.get();
  check_error(
      reader,
      tiledb_vcf_reader_set_check_samples_exist(reader, samples_exists));
}

std::string Reader::version() {
  const char* version_str;
  tiledb_vcf_version(&version_str);
  return version_str;
}

void Reader::set_enable_progress_estimation(
    const bool& enable_progress_estimation) {
  auto reader = ptr.get();
  check_error(
      reader,
      tiledb_vcf_reader_set_enable_progress_estimation(
          reader, enable_progress_estimation));
}

void Reader::set_debug_print_vcf_regions(const bool& print_vcf_regions) {
  auto reader = ptr.get();
  check_error(
      reader,
      tiledb_vcf_reader_set_debug_print_vcf_regions(reader, print_vcf_regions));
}

void Reader::set_debug_print_sample_list(const bool& print_sample_list) {
  auto reader = ptr.get();
  check_error(
      reader,
      tiledb_vcf_reader_set_debug_print_sample_list(reader, print_sample_list));
}

void Reader::set_debug_print_tiledb_query_ranges(
    const bool& tiledb_query_ranges) {
  auto reader = ptr.get();
  check_error(
      reader,
      tiledb_vcf_reader_set_debug_print_tiledb_query_ranges(
          reader, tiledb_query_ranges));
}
}  // namespace tiledbvcfpy
