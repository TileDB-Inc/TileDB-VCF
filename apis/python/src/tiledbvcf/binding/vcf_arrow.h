/**
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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

#ifndef TILEDB_VCF_ARROW_H
#define TILEDB_VCF_ARROW_H

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <tiledbvcf.h>

#include "stats/carrow.h"

namespace py = pybind11;

namespace tiledbvcfpy {

/**
 * @brief Return the size of the given datatype.
 *
 * @param datatype VCF datatype
 * @return int
 */
inline int type_size(tiledb_vcf_attr_datatype_t datatype) {
  switch (datatype) {
    case TILEDB_VCF_UINT8:
      return sizeof(uint8_t);
    case TILEDB_VCF_INT32:
      return sizeof(int32_t);
    case TILEDB_VCF_FLOAT32:
      return sizeof(float);
    case TILEDB_VCF_CHAR:
      return sizeof(char);
    case TILEDB_VCF_FLAG:
      return sizeof(uint8_t);
    default:
      throw std::runtime_error(
          "Error converting TileDB datatype to numpy datatype; "
          "unsupported datatype");
  }
}

/**
 * @brief Return the arrow datatype string for the given datatype.
 *
 * @param datatype VCF datatype
 * @return std::string_view Arrow datatype string
 */
inline std::string_view to_arrow_datatype(tiledb_vcf_attr_datatype_t datatype) {
  switch (datatype) {
    case TILEDB_VCF_UINT8:
      return "C";
    case TILEDB_VCF_INT32:
      return "i";
    case TILEDB_VCF_FLOAT32:
      return "f";
    case TILEDB_VCF_CHAR:
      return "z";
    case TILEDB_VCF_FLAG:
      return "C";
    default:
      throw std::runtime_error(
          "Error converting TileDB datatype to Arrow datatype; "
          "unsupported datatype");
  }
}

class BufferInfo {
 public:
  /**
   * @brief Create a BufferInfo object
   *
   * @param name Attribute name
   * @param datatype Attribute datatype
   * @param num_rows Number of rows to allocate
   * @param is_var_len Attribute is variable length
   * @param is_nullable Attribute is nullable
   * @param is_list Attribute is a list
   * @return std::shared_ptr<BufferInfo>
   */
  static std::shared_ptr<BufferInfo> create(
      std::string name,
      tiledb_vcf_attr_datatype_t datatype,
      int num_rows,
      int num_elements = 0,
      bool is_nullable = false,
      bool is_list = false) {
    return std::make_shared<BufferInfo>(
        name, datatype, num_rows, num_elements, is_nullable, is_list);
  }

  /**
   * @brief Construct a new Buffer Info object
   *
   * @param name Attribute name
   * @param datatype Attribute datatype
   * @param num_rows Number of rows to allocate
   * @param num_elements aggregate number of elements in variable-length array;
   * set to 0 for fixed length
   * @param is_nullable Attribute is nullable
   * @param is_list Attribute is a list
   */
  BufferInfo(
      std::string_view name,
      tiledb_vcf_attr_datatype_t datatype,
      uint32_t num_rows,
      uint32_t num_elements = 0,
      bool is_nullable = false,
      bool is_list = false)
      : name_(name)
      , datatype_(datatype)
      , arrow_datatype_(to_arrow_datatype(datatype))
      , type_size_(type_size(datatype)) {
    // Allocate buffers
    if (num_elements) {  // variable length
      data_.reserve(num_elements * type_size_);
      offsets_.reserve(num_rows + 1);
    } else {
      data_.reserve(num_rows * type_size_);
    }
    if (is_nullable) {
      bitmap_.reserve(num_rows);
    }
    if (is_list) {
      list_offsets_.reserve(num_rows + 1);
    }
  }

  /**
   * @brief Return the name of the attribute.
   *
   * @return std::string&
   */
  const std::string& name() {
    return name_;
  }

  /**
   * @brief Return the datatype of the attribute.
   *
   * @return tiledb_vcf_attr_datatype_t
   */
  tiledb_vcf_attr_datatype_t datatype() {
    return datatype_;
  }

  /**
   * @brief Return the arrow datatype string.
   *
   * @return std::string&
   */
  const std::string& arrow_datatype() {
    return arrow_datatype_;
  }

  /**
   * @brief Return the data buffer.
   *
   * @return std::vector<std::byte>&
   */
  std::vector<std::byte>& data() {
    return data_;
  }

  /**
   * @brief Return the offsets buffer.
   *
   * @return std::vector<int32_t>&
   */
  std::vector<int32_t>& offsets() {
    return offsets_;
  }

  /**
   * @brief Return the list offsets buffer.
   *
   * @return std::vector<int32_t>&
   */
  std::vector<int32_t>& list_offsets() {
    return list_offsets_;
  }

  /**
   * @brief Return the validity bitmap buffer.
   *
   * @return std::vector<uint8_t>&
   */
  std::vector<uint8_t>& bitmap() {
    return bitmap_;
  }

  /**
   * @brief Return true if the attribute is variable length.
   */
  bool is_var_len() const {
    return offsets_.capacity() > 0;
  }

  /**
   * @brief Return true if the attribute is nullable.
   */
  bool is_nullable() const {
    return bitmap_.capacity() > 0;
  }

  /**
   * @brief Return true if the attribute is a list.
   */
  bool is_list() const {
    return list_offsets_.capacity() > 0;
  }

  /**
   * @brief Set the arrow array structs.
   *
   * @param schema Arrow schema
   * @param array Arrow array
   */
  void set_arrow_array(ArrowSchema* schema, ArrowArray* array) {
    schema_ = schema;
    array_ = array;
  }

  /**
   * @brief Return the arrow schema.
   *
   * @return ArrowSchema*
   */
  ArrowSchema* schema() {
    return schema_;
  }

  /**
   * @brief Return the arrow array.
   *
   * @return ArrowArray*
   */
  ArrowArray* array() {
    return array_;
  }

 private:
  // Name of attribute.
  std::string name_;

  // TileDB-VCF datatype.
  tiledb_vcf_attr_datatype_t datatype_;

  // Arrow datatype.
  std::string arrow_datatype_;

  // Size of datatype.
  int type_size_;

  // Offsets buffer, for var-len attributes.
  std::vector<int32_t> offsets_;

  // List offsets buffer, for list var-len attributes.
  std::vector<int32_t> list_offsets_;

  // Data buffer.
  std::vector<std::byte> data_;

  // Null-value bitmap, for nullable attributes.
  std::vector<uint8_t> bitmap_;

  // Arrow array
  ArrowArray* array_;

  // Arrow schema
  ArrowSchema* schema_;
};

/**
 * @brief Build the Arrow C data structures from the given buffer based on the
 * number of rows, offsets, and data elements from the query.
 *
 * @param buffer Buffer to build from
 * @param num_rows Number of rows in the result
 * @param num_offsets Number of offsets in the result
 * @param num_data_elements Number of data elements in the result
 */
void build_arrow_array_from_buffer(
    std::shared_ptr<BufferInfo> buffer,
    uint64_t num_rows,
    uint64_t num_offsets,
    uint64_t num_data_elements);

/**
 * @brief Convert the given buffers to an Arrow table.
 *
 * @param buffers Buffers to convert
 * @return py::object Arrow table
 */
py::object buffers_to_table(std::vector<std::shared_ptr<BufferInfo>>& buffers);

}  // namespace tiledbvcfpy

#endif  // TILEDB_VCF_ARROW_H
