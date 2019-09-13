/**
 * @file   arrow.h
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
 *
 * @section DESCRIPTION
 *
 * This file declares the Arrow API (C++) for TileDB-VCF.
 */

#ifndef TILEDB_VCF_ARROW_H
#define TILEDB_VCF_ARROW_H

#include <arrow/api.h>

#include "tiledbvcf.h"

namespace tiledb {
namespace vcf {

class Arrow {
 public:
  static std::shared_ptr<arrow::Table> to_arrow(tiledb_vcf_reader_t* reader) {
    int num_buffers = 0;
    check_error(
        reader,
        tiledb_vcf_reader_get_num_buffers(reader, &num_buffers),
        "Error getting number of buffers from reader object");

    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (int i = 0; i < num_buffers; i++) {
      // Get name and buffer pointers
      const char* name = nullptr;
      int32_t* offsets = nullptr;
      int64_t offset_size = 0;
      void* data = nullptr;
      int64_t data_size = 0;
      uint8_t* bitmap = nullptr;
      int64_t bitmap_size = 0;
      check_error(
          reader,
          tiledb_vcf_reader_get_buffer(
              reader, i, &name, &offsets, &offset_size, &data, &data_size),
          "Error getting buffer by index");
      check_error(
          reader,
          tiledb_vcf_reader_get_validity_bitmap(
              reader, i, &bitmap, &bitmap_size),
          "Error getting bitmap buffer by index");

      // Get datatype
      tiledb_vcf_attr_datatype_t datatype = TILEDB_VCF_UINT8;
      int32_t is_var_len = 0, is_nullable = 0;
      check_error(
          reader,
          tiledb_vcf_reader_get_attribute_type(
              reader, name, &datatype, &is_var_len, &is_nullable),
          "Error getting buffer datatype");

      // Get actual buffer result size
      int64_t num_offsets = 0, num_data_elements = 0, num_data_bytes = 0;
      check_error(
          reader,
          tiledb_vcf_reader_get_result_size(
              reader, name, &num_offsets, &num_data_elements, &num_data_bytes),
          "Error getting buffer result size");

      // Create Arrow wrapper on buffer
      std::shared_ptr<arrow::Field> field =
          arrow::field(name, arrow_dtype(datatype, is_var_len == 1));
      fields.push_back(field);

      std::shared_ptr<arrow::Array> array = make_arrow_array(
          datatype, num_offsets, num_data_elements, offsets, data, bitmap);
      arrays.push_back(array);
    }

    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    return arrow::Table::Make(schema, arrays);
  }

 private:
  /** Returns the value of x/y (integer division) rounded up. */
  static int64_t ceil(int64_t x, int64_t y) {
    if (y == 0)
      return 0;
    return x / y + (x % y != 0);
  }

  static void check_error(
      tiledb_vcf_reader_t* reader, int32_t rc, const char* msg) {
    if (rc != TILEDB_VCF_OK) {
      std::string msg_str = "TileDB-VCF Arrow error: " + std::string(msg);
      tiledb_vcf_error_t* err = nullptr;
      const char* reader_err_msg = nullptr;
      if (tiledb_vcf_reader_get_last_error(reader, &err) == TILEDB_VCF_OK &&
          tiledb_vcf_error_get_message(err, &reader_err_msg) == TILEDB_VCF_OK) {
        msg_str += "; ";
        msg_str += std::string(reader_err_msg);
        tiledb_vcf_error_free(&err);
      }
      throw std::runtime_error(msg_str);
    }
  }

  static void check_error(const arrow::Status& st) {
    if (!st.ok()) {
      std::string msg_str = "TileDB-VCF Arrow error: " + st.message();
      throw std::runtime_error(msg_str);
    }
  }

  static std::shared_ptr<arrow::Array> make_arrow_array(
      tiledb_vcf_attr_datatype_t datatype,
      int64_t num_offsets,
      int64_t num_data_elements,
      int32_t* offset_buff,
      void* buff,
      uint8_t* bitmap) {
    if (offset_buff == nullptr) {
      // Fixed-length attribute. None of the fixed-length attributes in
      // TileDB-VCF are (currently) nullable.
      if (bitmap != nullptr)
        throw std::runtime_error(
            "Error converting to Arrow Array; unhandled nullable fixed-len.");

      switch (datatype) {
        case TILEDB_VCF_UINT8:
          return make_arrow_array<uint8_t, arrow::UInt8Array>(
              num_data_elements, buff);
        case TILEDB_VCF_INT32:
          return make_arrow_array<int32_t, arrow::Int32Array>(
              num_data_elements, buff);
        case TILEDB_VCF_FLOAT32:
          return make_arrow_array<float, arrow::FloatArray>(
              num_data_elements, buff);
        default:
          throw std::runtime_error(
              "Error converting to Arrow Array; unhandled fixed-len datatype.");
      }
    } else {
      // Variable-length attribute. All of the variable-length attributes in
      // TileDB-VCF are (currently) nullable.
      auto dtype = arrow_dtype(datatype, true);
      switch (datatype) {
        case TILEDB_VCF_CHAR:
          return make_arrow_string_array(
              num_offsets, num_data_elements, offset_buff, buff, bitmap);
        case TILEDB_VCF_UINT8:
          return make_arrow_array<uint8_t, arrow::UInt8Array>(
              dtype, num_offsets, num_data_elements, offset_buff, buff, bitmap);
        case TILEDB_VCF_INT32:
          return make_arrow_array<int32_t, arrow::Int32Array>(
              dtype, num_offsets, num_data_elements, offset_buff, buff, bitmap);
        case TILEDB_VCF_FLOAT32:
          return make_arrow_array<float, arrow::FloatArray>(
              dtype, num_offsets, num_data_elements, offset_buff, buff, bitmap);
        default:
          throw std::runtime_error(
              "Error converting to Arrow Array; unhandled var-len datatype.");
      }
    }
  }

  template <typename T, typename ArrayT>
  static std::shared_ptr<arrow::Array> make_arrow_array(
      int64_t num_data_elements, void* buff) {
    auto arrow_buff =
        arrow::Buffer::Wrap(reinterpret_cast<T*>(buff), num_data_elements);
    return std::shared_ptr<arrow::Array>(
        new ArrayT(num_data_elements, arrow_buff));
  }

  static std::shared_ptr<arrow::Array> make_arrow_string_array(
      int64_t num_offsets,
      int64_t num_data_elements,
      int32_t* offset_buff,
      void* buff,
      uint8_t* bitmap) {
    auto arrow_buff =
        arrow::Buffer::Wrap(reinterpret_cast<char*>(buff), num_data_elements);

    auto arrow_offsets = arrow::Buffer::Wrap(offset_buff, num_offsets);

    std::shared_ptr<arrow::Buffer> arrow_nulls;
    if (bitmap != nullptr)
      arrow_nulls = arrow::Buffer::Wrap(bitmap, ceil(num_data_elements, 8));

    return std::shared_ptr<arrow::Array>(new arrow::StringArray(
        num_offsets - 1, arrow_offsets, arrow_buff, arrow_nulls));
  }

  template <typename T, typename ArrayT>
  static std::shared_ptr<arrow::Array> make_arrow_array(
      const std::shared_ptr<arrow::DataType>& dtype,
      int64_t num_offsets,
      int64_t num_data_elements,
      int32_t* offset_buff,
      void* buff,
      uint8_t* bitmap) {
    auto arrow_buff =
        arrow::Buffer::Wrap(reinterpret_cast<T*>(buff), num_data_elements);
    std::shared_ptr<arrow::Array> arrow_values(
        new ArrayT(num_data_elements, arrow_buff));

    auto arrow_offsets = arrow::Buffer::Wrap(offset_buff, num_offsets);

    std::shared_ptr<arrow::Buffer> arrow_nulls;
    if (bitmap != nullptr)
      arrow_nulls = arrow::Buffer::Wrap(bitmap, ceil(num_data_elements, 8));

    return std::shared_ptr<arrow::Array>(new arrow::ListArray(
        dtype, num_offsets - 1, arrow_offsets, arrow_values, arrow_nulls));
  }

  static std::shared_ptr<arrow::DataType> arrow_dtype(
      tiledb_vcf_attr_datatype_t datatype, bool var_len) {
    switch (datatype) {
      case TILEDB_VCF_CHAR:
        return arrow::utf8();
      case TILEDB_VCF_UINT8:
        return var_len ? arrow::list(arrow::uint8()) : arrow::uint8();
      case TILEDB_VCF_INT32:
        return var_len ? arrow::list(arrow::int32()) : arrow::int32();
      case TILEDB_VCF_FLOAT32:
        return var_len ? arrow::list(arrow::float32()) : arrow::float32();
      default:
        throw std::runtime_error(
            "Error converting TileDB-VCF datatype to Arrow; unknown datatype.");
    }
  }
};

}  // namespace vcf
}  // namespace tiledb

#endif