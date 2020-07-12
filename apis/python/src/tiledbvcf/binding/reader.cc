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

#include <arrow/python/pyarrow.h>
#include <tiledbvcf/arrow.h>
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

  // TileDB-VCF gets half the budget, we use the other half for buffer
  // allocation.
  auto reader = ptr.get();
  check_error(
      reader, tiledb_vcf_reader_set_memory_budget(reader, mem_budget_mb_ / 2));
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

void Reader::read() {
  auto reader = ptr.get();
  alloc_buffers();
  set_buffers();

  check_error(reader, tiledb_vcf_reader_read(reader));
  tiledb_vcf_read_status_t status;
  check_error(reader, tiledb_vcf_reader_get_status(reader, &status));
  if (status != TILEDB_VCF_COMPLETED && status != TILEDB_VCF_INCOMPLETE)
    throw std::runtime_error(
        "TileDB-VCF-Py: Error submitting read; unhandled read status.");
}

void Reader::alloc_buffers() {
  auto reader = ptr.get();

  // Release old buffers. TODO: reuse when possible
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

  // Only use half the budget because TileDB-VCF gets the other half.
  const int64_t budget_mb = mem_budget_mb_ / 2;

  // The undocumented "0 MB" budget is used only for testing incomplete queries.
  int64_t alloc_size_bytes;
  if (mem_budget_mb_ == 0) {
    alloc_size_bytes = 10;  // Some small value
  } else {
    alloc_size_bytes = (budget_mb * 1024 * 1024) / num_buffers;
    if (alloc_size_bytes < (10 * 1024 * 1024))
      throw std::runtime_error(
          "TileDB-VCF-Py: buffer allocation size is below the minimum of 10MB. "
          "Try increasing the memory budget.");
  }

  for (const auto& attr : attributes_) {
    tiledb_vcf_attr_datatype_t datatype = TILEDB_VCF_UINT8;
    int32_t var_len = 0, nullable = 0, list = 0;
    check_error(
        reader,
        tiledb_vcf_reader_get_attribute_type(
            reader, attr.c_str(), &datatype, &var_len, &nullable, &list));

    buffers_.emplace_back();
    BufferInfo& buffer = buffers_.back();
    buffer.attr_name = attr;

    auto dtype = to_numpy_dtype(datatype);
    size_t count = alloc_size_bytes / dtype.itemsize();
    // Forcing a stride of 1 here (I think) guarantees the backing memory
    // will be contiguous.
    buffer.data = py::array(dtype, {count}, {1});

    if (var_len == 1) {
      size_t count = alloc_size_bytes / sizeof(int32_t);
      buffer.offsets = py::array_t<int32_t, py::array::c_style>(count);
    }

    if (list == 1) {
      size_t count = alloc_size_bytes / sizeof(int32_t);
      buffer.list_offsets = py::array_t<int32_t, py::array::c_style>(count);
    }

    if (nullable == 1) {
      buffer.bitmap = py::array_t<uint8_t, py::array::c_style>(count);
    }
  }
}

void Reader::set_buffers() {
  auto reader = ptr.get();
  for (auto& buff : buffers_) {
    const auto& attr = buff.attr_name;
    py::buffer_info offsets_info = buff.offsets.request(true);
    py::buffer_info list_offsets_info = buff.list_offsets.request(true);
    py::buffer_info data_info = buff.data.request(true);
    py::buffer_info bitmap_info = buff.bitmap.request(true);

    size_t offsets_bytes = offsets_info.itemsize * offsets_info.shape[0];
    size_t list_offsets_bytes =
        list_offsets_info.itemsize * list_offsets_info.shape[0];
    size_t data_bytes = data_info.itemsize * data_info.shape[0];
    size_t bitmap_bytes = bitmap_info.itemsize * bitmap_info.shape[0];

    check_error(
        reader,
        tiledb_vcf_reader_set_buffer_values(
            reader, attr.c_str(), data_bytes, data_info.ptr));

    if (offsets_bytes > 0)
      check_error(
          reader,
          tiledb_vcf_reader_set_buffer_offsets(
              reader,
              attr.c_str(),
              offsets_bytes,
              reinterpret_cast<int32_t*>(offsets_info.ptr)));

    if (list_offsets_bytes > 0)
      check_error(
          reader,
          tiledb_vcf_reader_set_buffer_list_offsets(
              reader,
              attr.c_str(),
              list_offsets_bytes,
              reinterpret_cast<int32_t*>(list_offsets_info.ptr)));

    if (bitmap_bytes > 0)
      check_error(
          reader,
          tiledb_vcf_reader_set_buffer_validity_bitmap(
              reader,
              attr.c_str(),
              bitmap_bytes,
              static_cast<uint8_t*>(bitmap_info.ptr)));
  }
}

void Reader::release_buffers() {
  for (auto& b : buffers_) {
    b.data.release();
    b.offsets.release();
    b.list_offsets.release();
    b.bitmap.release();
  }

  buffers_.clear();
}

std::map<std::string, std::pair<py::array, py::array>> Reader::get_buffers() {
  std::map<std::string, std::pair<py::array, py::array>> result;
  for (auto& buff : buffers_) {
    const auto& attr = buff.attr_name;
    result[attr] = {buff.offsets, buff.data};
  }
  return result;
}

py::object Reader::get_results_arrow() {
  auto reader = ptr.get();
  std::shared_ptr<arrow::Table> table = tiledb::vcf::Arrow::to_arrow(reader);
  if (table == nullptr)
    throw std::runtime_error(
        "TileDB-VCF-Py: Error converting to Arrow; null Array.");
  PyObject* obj = arrow::py::wrap_table(table);
  if (obj == nullptr) {
    PyErr_PrintEx(1);
    throw std::runtime_error(
        "TileDB-VCF-Py: Error converting to Arrow; null Python object.");
  }
  return py::reinterpret_steal<py::object>(obj);
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

int32_t Reader::get_fmt_attribute_count() {
  auto reader = ptr.get();
  int32_t count;
  check_error(reader, tiledb_vcf_reader_get_fmt_attribute_count(reader, &count));
  return (count);
}

std::string Reader::get_fmt_attribute_name(int32_t index) {
  auto reader = ptr.get();
  char* name;
  check_error(reader, tiledb_vcf_reader_get_fmt_attribute_name(reader, index, &name));
  return std::string(name);
}

int32_t Reader::get_info_attribute_count() {
  auto reader = ptr.get();
  int32_t count;
  check_error(reader, tiledb_vcf_reader_get_info_attribute_count(reader, &count));
  return (count);
}

std::string Reader::get_info_attribute_name(int32_t index) {
  auto reader = ptr.get();
  char* name;
  check_error(reader, tiledb_vcf_reader_get_info_attribute_name(reader, index, &name));
  return std::string(name);
}

int32_t Reader::get_queryable_attribute_count() {
  auto reader = ptr.get();
  int32_t count;
  check_error(reader, tiledb_vcf_reader_get_queryable_attribute_count(reader, &count));
  return (count);
}

std::string Reader::get_queryable_attribute_name(int32_t index) {
  auto reader = ptr.get();
  char* name;
  check_error(reader, tiledb_vcf_reader_get_queryable_attribute_name(reader, index, &name));
  return std::string(name);
}

py::dtype Reader::to_numpy_dtype(tiledb_vcf_attr_datatype_t datatype) {
  switch (datatype) {
    case TILEDB_VCF_CHAR:
      return py::dtype("S1");
    case TILEDB_VCF_UINT8:
      return py::dtype::of<uint8_t>();
    case TILEDB_VCF_INT32:
      return py::dtype::of<int32_t>();
    case TILEDB_VCF_FLOAT32:
      return py::dtype::of<float>();
    default:
      throw std::runtime_error(
          "TileDB-VCF-Py: Error converting to numpy dtype; unhandled "
          "datatype " +
          std::to_string(datatype));
  }
}

void Reader::deleter(tiledb_vcf_reader_t* r) {
  tiledb_vcf_reader_free(&r);
}

}  // namespace tiledbvcfpy
