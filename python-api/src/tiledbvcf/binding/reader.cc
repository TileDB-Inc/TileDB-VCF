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
    , alloc_size_bytes_(20 * 1024 * 1024) {
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
  attributes_.insert(attributes.begin(), attributes.end());
}

void Reader::set_buffer_alloc_size(int64_t nbytes) {
  alloc_size_bytes_ = nbytes;
}

void Reader::set_samples(const std::string& samples) {
  auto reader = ptr.get();
  check_error(reader, tiledb_vcf_reader_set_samples(reader, samples.c_str()));
}

void Reader::set_regions(const std::string& regions) {
  auto reader = ptr.get();
  check_error(reader, tiledb_vcf_reader_set_regions(reader, regions.c_str()));
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

  prepare_result_buffers();
}

void Reader::alloc_buffers() {
  auto reader = ptr.get();
  for (const auto& attr : attributes_) {
    tiledb_vcf_attr_datatype_t datatype = TILEDB_VCF_UINT8;
    int32_t var_len = 0, nullable = 0;
    check_error(
        reader,
        tiledb_vcf_reader_get_attribute_type(
            reader, attr.c_str(), &datatype, &var_len, &nullable));

    BufferPair& buffer = buffers_[attr];

    auto dtype = to_numpy_dtype(datatype);
    size_t count = alloc_size_bytes_ / dtype.itemsize();
    buffer.data = py::array(dtype, count);

    if (var_len == 1) {
      size_t count = alloc_size_bytes_ / sizeof(int32_t);
      buffer.offsets = py::array(py::dtype::of<int32_t>(), count);
    }

    if (nullable == 1) {
      buffer.bitmap = py::array(py::dtype::of<uint8_t>(), count);
    }
  }
}

void Reader::set_buffers() {
  auto reader = ptr.get();
  for (auto& it : buffers_) {
    const auto& attr = it.first;
    BufferPair& buff = it.second;
    py::buffer_info offsets_info = buff.offsets.request(true);
    py::buffer_info data_info = buff.data.request(true);
    py::buffer_info bitmap_info = buff.bitmap.request(true);

    size_t offsets_bytes = offsets_info.itemsize * offsets_info.shape[0];
    size_t data_bytes = data_info.itemsize * data_info.shape[0];
    size_t bitmap_bytes = bitmap_info.itemsize * bitmap_info.shape[0];

    int32_t* offsets_ptr = offsets_bytes == 0 ?
                               nullptr :
                               reinterpret_cast<int32_t*>(offsets_info.ptr);

    check_error(
        reader,
        tiledb_vcf_reader_set_buffer(
            reader,
            attr.c_str(),
            offsets_bytes,
            offsets_ptr,
            data_bytes,
            data_info.ptr));

    if (bitmap_bytes > 0)
      check_error(
          reader,
          tiledb_vcf_reader_set_validity_bitmap(
              reader,
              attr.c_str(),
              bitmap_bytes,
              static_cast<uint8_t*>(bitmap_info.ptr)));
  }
}

void Reader::prepare_result_buffers() {
  auto reader = ptr.get();
  for (auto& it : buffers_) {
    const auto& attr = it.first;
    BufferPair& buff = it.second;
    py::buffer_info offsets_info = buff.offsets.request(true);
    py::buffer_info data_info = buff.data.request(true);
    py::buffer_info bitmap_info = buff.bitmap.request(true);

    int64_t offset_size = 0, data_size = 0;
    check_error(
        reader,
        tiledb_vcf_reader_get_result_size(
            reader, attr.c_str(), &offset_size, &data_size));

    int64_t num_offsets = offset_size / sizeof(int32_t);
    if (buff.offsets.size() > 0) {
      buff.offsets.resize({num_offsets});
    }

    int64_t num_data_elts = data_size / buff.data.itemsize();
    buff.data.resize({num_data_elts});

    if (bitmap_info.shape[0] > 0) {
      buff.bitmap.resize({num_offsets - 1});
    }
  }
}

std::map<std::string, std::pair<py::array, py::array>> Reader::get_buffers() {
  std::map<std::string, std::pair<py::array, py::array>> result;
  for (auto& it : buffers_) {
    const auto& attr = it.first;
    BufferPair& buff = it.second;
    result[attr] = {buff.offsets, buff.data};
  }
  return result;
}

py::object Reader::get_results_arrow() {
  auto reader = ptr.get();
  std::shared_ptr<arrow::Table> table = tiledb::vcf::Arrow::to_arrow(reader);
  PyObject* obj = arrow::py::wrap_table(table);
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