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

/**
 * Class providing integration between TileDB-VCF results and Apache Arrow.
 */
class Arrow {
 public:
  /**
   * Creates a zero-copy Arrow Table wrapper around the data in the given
   * reader's result buffers.
   *
   * @param reader Reader whose results to wrap
   * @return Arrow Table wrapping the reader's results
   */
  static std::shared_ptr<arrow::Table> to_arrow(tiledb_vcf_reader_t* reader) {
    int num_buffers = 0;
    check_error(
        reader,
        tiledb_vcf_reader_get_num_buffers(reader, &num_buffers),
        "Error getting number of buffers from reader object");

    int64_t num_records = 0;
    check_error(
        reader,
        tiledb_vcf_reader_get_result_num_records(reader, &num_records),
        "Error getting number of records from reader object");

    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (int i = 0; i < num_buffers; i++) {
      // Get buffer info
      BufferInfo buffer_info = get_buffer_info(reader, i);

      // Get actual buffer result size
      int64_t num_offsets = 0, num_data_elements = 0, num_data_bytes = 0;
      check_error(
          reader,
          tiledb_vcf_reader_get_result_size(
              reader,
              buffer_info.name.c_str(),
              &num_offsets,
              &num_data_elements,
              &num_data_bytes),
          "Error getting buffer result size");

      // Create Arrow wrapper on buffer
      std::shared_ptr<arrow::Field> field =
          arrow::field(buffer_info.name, arrow_field_dtype(buffer_info));
      fields.push_back(field);

      std::shared_ptr<arrow::Array> array = make_arrow_array(
          buffer_info, num_records, num_offsets, num_data_elements);
      arrays.push_back(array);
    }

    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    auto table = arrow::Table::Make(schema, arrays, num_records);
    check_error(table->Validate());
    return table;
  }

 private:
  /** Helper type encapsulating information about buffered data. */
  struct BufferInfo {
    std::string name = "";
    tiledb_vcf_attr_datatype_t datatype = TILEDB_VCF_UINT8;
    bool var_len = false;
    bool nullable = false;
    bool list = false;
    void* values_buff = nullptr;
    int32_t* offset_buff = nullptr;
    int32_t* list_offset_buff = nullptr;
    uint8_t* bitmap_buff = nullptr;
  };

  /** Returns the value of x/y (integer division) rounded up. */
  static int64_t ceil(int64_t x, int64_t y) {
    if (y == 0)
      return 0;
    return x / y + (x % y != 0);
  }

  /**
   * Checks the given TileDB-VCF return code for an error, throwing an exception
   * if it is an error code.
   */
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

  /**
   * Checks the given Arrow Status for an error, throwing an exception if it is
   * an error status.
   */
  static void check_error(const arrow::Status& st) {
    if (!st.ok()) {
      std::string msg_str = "TileDB-VCF Arrow error: " + st.message();
      throw std::runtime_error(msg_str);
    }
  }

  /** Gets information about a buffer by index. */
  static BufferInfo get_buffer_info(tiledb_vcf_reader_t* reader, int32_t i) {
    BufferInfo result;

    const char* c_name = nullptr;
    check_error(
        reader,
        tiledb_vcf_reader_get_buffer_values(
            reader, i, &c_name, &result.values_buff),
        "Error getting value buffer by index");
    result.name = c_name;

    check_error(
        reader,
        tiledb_vcf_reader_get_buffer_offsets(
            reader, i, &c_name, &result.offset_buff),
        "Error getting offsets buffer by index");

    check_error(
        reader,
        tiledb_vcf_reader_get_buffer_list_offsets(
            reader, i, &c_name, &result.list_offset_buff),
        "Error getting list offsets buffer by index");

    check_error(
        reader,
        tiledb_vcf_reader_get_buffer_validity_bitmap(
            reader, i, &c_name, &result.bitmap_buff),
        "Error getting bitmap buffer by index");

    int32_t is_var_len = 0, is_nullable = 0, is_list = 0;
    check_error(
        reader,
        tiledb_vcf_reader_get_attribute_type(
            reader,
            c_name,
            &result.datatype,
            &is_var_len,
            &is_nullable,
            &is_list),
        "Error getting buffer datatype");
    result.var_len = is_var_len == 1;
    result.nullable = is_nullable == 1;
    result.list = is_list == 1;

    // Sanity checks
    if (result.values_buff == nullptr)
      throw std::runtime_error(
          "TileDB-VCF Arrow error: attribute with null values buffer.");
    if (result.var_len && result.offset_buff == nullptr)
      throw std::runtime_error(
          "TileDB-VCF Arrow error: var-len attribute with null offsets "
          "buffer.");
    if (result.nullable && result.bitmap_buff == nullptr)
      throw std::runtime_error(
          "TileDB-VCF Arrow error: nullable attribute with null bitmap "
          "buffer.");
    if (result.list && result.list_offset_buff == nullptr)
      throw std::runtime_error(
          "TileDB-VCF Arrow error: list attribute with null list offsets "
          "buffer.");

    return result;
  }

  /**
   * Creates a zero-copy Arrow Array wrapper around the given buffer.
   */
  static std::shared_ptr<arrow::Array> make_arrow_array(
      const BufferInfo& buffer_info,
      int64_t num_records,
      int64_t num_offsets,
      int64_t num_data_elements) {
    if (buffer_info.var_len) {
      // Variable-length attribute.
      switch (buffer_info.datatype) {
        case TILEDB_VCF_CHAR:
          return make_arrow_string_array(
              buffer_info, num_records, num_offsets, num_data_elements);
        case TILEDB_VCF_UINT8:
          return make_arrow_array<uint8_t, arrow::UInt8Array>(
              buffer_info, num_records, num_offsets, num_data_elements);
        case TILEDB_VCF_INT32:
          return make_arrow_array<int32_t, arrow::Int32Array>(
              buffer_info, num_records, num_offsets, num_data_elements);
        case TILEDB_VCF_FLOAT32:
          return make_arrow_array<float, arrow::FloatArray>(
              buffer_info, num_records, num_offsets, num_data_elements);
        default:
          throw std::runtime_error(
              "Error converting to Arrow Array; unhandled var-len datatype.");
      }
    } else {
      switch (buffer_info.datatype) {
        case TILEDB_VCF_UINT8:
          return make_arrow_array<uint8_t, arrow::UInt8Array>(
              buffer_info, num_records, num_data_elements);
        case TILEDB_VCF_INT32:
          return make_arrow_array<int32_t, arrow::Int32Array>(
              buffer_info, num_records, num_data_elements);
        case TILEDB_VCF_FLOAT32:
          return make_arrow_array<float, arrow::FloatArray>(
              buffer_info, num_records, num_data_elements);
        default:
          throw std::runtime_error(
              "Error converting to Arrow Array; unhandled fixed-len datatype "
              "for " +
              buffer_info.name);
      }
    }
  }

  /** Creates a zero-copy Arrow Array for fixed-length data. */
  template <typename T, typename ArrayT>
  static std::shared_ptr<arrow::Array> make_arrow_array(
      const BufferInfo& buffer_info,
      int64_t num_records,
      int64_t num_data_elements) {
    auto arrow_buff = arrow::Buffer::Wrap(
        reinterpret_cast<T*>(buffer_info.values_buff), num_data_elements);

    // Handle nulls
    if (buffer_info.nullable) {
      std::shared_ptr<arrow::Buffer> arrow_nulls =
          arrow::Buffer::Wrap(buffer_info.bitmap_buff, ceil(num_records, 8));
      return std::shared_ptr<arrow::Array>(
          new ArrayT(num_data_elements, arrow_buff, arrow_nulls));
    }

    return std::shared_ptr<arrow::Array>(
        new ArrayT(num_data_elements, arrow_buff));
  }

  /** Creates a zero-copy Arrow Array for string-typed data. */
  static std::shared_ptr<arrow::Array> make_arrow_string_array(
      const BufferInfo& buffer_info,
      int64_t num_records,
      int64_t num_offsets,
      int64_t num_data_elements) {
    auto arrow_values = arrow::Buffer::Wrap(
        reinterpret_cast<char*>(buffer_info.values_buff), num_data_elements);
    auto arrow_offsets =
        arrow::Buffer::Wrap(buffer_info.offset_buff, num_offsets);

    std::shared_ptr<arrow::Buffer> arrow_nulls;
    if (buffer_info.nullable)
      arrow_nulls =
          arrow::Buffer::Wrap(buffer_info.bitmap_buff, ceil(num_records, 8));

    if (buffer_info.list) {
      // List of var-len char attribute.
      const int64_t num_list_offsets = num_records == 0 ? 0 : num_records + 1;
      const int64_t num_strings = num_offsets == 0 ? 0 : num_offsets - 1;
      auto arrow_list_offsets =
          arrow::Buffer::Wrap(buffer_info.list_offset_buff, num_list_offsets);
      auto string_array = std::shared_ptr<arrow::Array>(
          new arrow::StringArray(num_strings, arrow_offsets, arrow_values));
      return std::shared_ptr<arrow::Array>(new arrow::ListArray(
          arrow::list(arrow::utf8()),
          num_records,
          arrow_list_offsets,
          string_array,
          arrow_nulls));
    } else {
      // Normal var-len char attribute.
      const int64_t num_cells = num_offsets == 0 ? 0 : num_offsets - 1;
      return std::shared_ptr<arrow::Array>(new arrow::StringArray(
          num_cells, arrow_offsets, arrow_values, arrow_nulls));
    }
  }

  /** Creates a zero-copy Arrow Array for variable-length data. */
  template <typename T, typename ArrayT>
  static std::shared_ptr<arrow::Array> make_arrow_array(
      const BufferInfo& buffer_info,
      int64_t num_records,
      int64_t num_offsets,
      int64_t num_data_elements) {
    auto dtype = arrow_dtype(buffer_info.datatype);
    auto arrow_values = arrow::Buffer::Wrap(
        reinterpret_cast<T*>(buffer_info.values_buff), num_data_elements);
    auto arrow_offsets =
        arrow::Buffer::Wrap(buffer_info.offset_buff, num_offsets);

    std::shared_ptr<arrow::Buffer> arrow_nulls;
    if (buffer_info.nullable)
      arrow_nulls =
          arrow::Buffer::Wrap(buffer_info.bitmap_buff, ceil(num_records, 8));

    std::shared_ptr<arrow::Array> values_array(
        new ArrayT(num_data_elements, arrow_values));

    if (buffer_info.list) {
      // List of var-len attribute.
      const int64_t num_list_offsets = num_records == 0 ? 0 : num_records + 1;
      auto arrow_list_offsets =
          arrow::Buffer::Wrap(buffer_info.list_offset_buff, num_list_offsets);

      const int64_t num_list_elts = num_offsets == 0 ? 0 : num_offsets - 1;
      std::shared_ptr<arrow::Array> elts_array(new arrow::ListArray(
          arrow::list(dtype), num_list_elts, arrow_offsets, values_array));

      return std::shared_ptr<arrow::Array>(new arrow::ListArray(
          arrow::list(arrow::list(dtype)),
          num_records,
          arrow_list_offsets,
          elts_array,
          arrow_nulls));
    } else {
      // Normal var-len attribute.
      const int64_t num_cells = num_offsets == 0 ? 0 : num_offsets - 1;
      auto dtype = arrow_dtype(buffer_info.datatype);
      return std::shared_ptr<arrow::Array>(new arrow::ListArray(
          arrow::list(dtype),
          num_cells,
          arrow_offsets,
          values_array,
          arrow_nulls));
    }
  }

  static std::shared_ptr<arrow::DataType> arrow_dtype(
      tiledb_vcf_attr_datatype_t datatype) {
    switch (datatype) {
      case TILEDB_VCF_CHAR:
        return arrow::utf8();
      case TILEDB_VCF_UINT8:
        return arrow::uint8();
      case TILEDB_VCF_INT32:
        return arrow::int32();
      case TILEDB_VCF_FLOAT32:
        return arrow::float32();
      default:
        throw std::runtime_error(
            "Error converting TileDB-VCF datatype to Arrow; unknown datatype.");
    }
  }

  static std::shared_ptr<arrow::DataType> arrow_field_dtype(
      const BufferInfo& buffer_info) {
    auto dtype = arrow_dtype(buffer_info.datatype);
    if (buffer_info.var_len && buffer_info.datatype != TILEDB_VCF_CHAR)
      dtype = arrow::list(dtype);
    if (buffer_info.list)
      dtype = arrow::list(dtype);
    return dtype;
  }
};  // namespace vcf

}  // namespace vcf
}  // namespace tiledb

#endif
