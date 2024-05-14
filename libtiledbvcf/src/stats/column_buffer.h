/**
 * @file   column_buffer.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2024 TileDB, Inc.
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
 *   This declares the column buffer API
 */

#ifndef COLUMN_BUFFER_H
#define COLUMN_BUFFER_H

#include <span>
#include <tiledb/tiledb>

namespace tiledb::vcf {

using namespace tiledb;

/**
 * @brief Class to store data for a TileDB dimension or attribute.
 *
 */
class ColumnBuffer {
  inline static const size_t DEFAULT_ALLOC_BYTES = 128 << 20;  // 128 MiB
  inline static const std::string CONFIG_KEY_INIT_BYTES =
      "vcf.init_buffer_bytes";

 public:
  //===================================================================
  //= public static
  //===================================================================

  /**
   * @brief Create a ColumnBuffer from an array and column name.
   *
   * @param array TileDB array
   * @param name TileDB dimension or attribute name
   * @return ColumnBuffer
   */
  static std::shared_ptr<ColumnBuffer> create(
      std::shared_ptr<Array> array, std::string_view name);

  /**
   * @brief Create a ColumnBuffer with the provided parameters. The primary
   * use case is to create a ColumnBuffer that will be populated with
   * push_back/push_null and passed directly to Arrow.
   *
   * @param name Column name
   * @param type TileDB datatype
   * @param is_var Column type is variable length
   * @param is_nullable Column can contain null values
   * @return ColumnBuffer
   */
  static std::shared_ptr<ColumnBuffer> create(
      std::string_view name,
      tiledb_datatype_t type,
      bool is_var = false,
      bool is_nullable = false);

  /**
   * @brief Convert a bytemap to a bitmap in place.
   *
   */
  static void to_bitmap(std::span<uint8_t> bytemap);

  //===================================================================
  //= public non-static
  //===================================================================

  /**
   * @brief Construct a new ColumnBuffer object
   *
   * @param name Column name
   * @param type TileDB datatype
   * @param num_cells Number of cells
   * @param num_bytes Number of bytes
   * @param is_var Column type is variable length
   * @param is_nullable Column can contain null values
   * @param is_arrow The buffer is being built for Arrow
   */
  ColumnBuffer(
      std::string_view name,
      tiledb_datatype_t type,
      size_t num_cells,
      size_t num_bytes,
      bool is_var = false,
      bool is_nullable = false,
      bool is_arrow = false);

  ~ColumnBuffer();

  ColumnBuffer() = delete;
  ColumnBuffer(const ColumnBuffer&) = delete;
  ColumnBuffer(ColumnBuffer&&) = default;

  /**
   * @brief Attach this ColumnBuffer to a TileDB query.
   *
   * @param query TileDB query
   */
  void attach(Query& query);

  /**
   * @brief Size num_cells_ to match the read query results.
   *
   * @param query TileDB query
   */
  size_t update_size(const Query& query);

  /**
   * @brief Return the number of cells in the buffer.
   *
   * @return size_t
   */
  size_t size() const {
    return num_cells_;
  }

  /**
   * @brief Return a view of the ColumnBuffer data.
   *
   * @tparam T Data type
   * @return std::span<T> data view
   */
  template <typename T>
  std::span<T> data() {
    if (is_var_) {
      return std::span<T>((T*)data_.data(), num_elements_);
    }
    return std::span<T>((T*)data_.data(), num_cells_);
  }

  /**
   * @brief Return data in a vector of strings.
   *
   * @return std::vector<std::string>
   */
  std::vector<std::string> strings();

  /**
   * @brief Return a string_view of the string at the provided cell index.
   *
   * @param index Cell index
   * @return std::string_view string view
   */
  std::string_view string_view(uint64_t index);

  /**
   * @brief Return a view of the ColumnBuffer offsets.
   *
   * @return std::span<uint64_t> offsets view
   */
  std::span<uint64_t> offsets() {
    if (!is_var_) {
      throw std::runtime_error(
          "[ColumnBuffer] Offsets buffer not defined for " + name_);
    }

    return std::span<uint64_t>(offsets_.data(), num_cells_);
  }

  /**
   * @brief Return a view of the validity buffer.
   *
   * @return std::span<uint8_t> validity view
   */
  std::span<uint8_t> validity() {
    if (!is_nullable_) {
      throw std::runtime_error(
          "[ColumnBuffer] Validity buffer not defined for " + name_);
    }
    return std::span<uint8_t>(validity_.data(), num_cells_);
  }

  /**
   * @brief Return the name of the buffer.
   *
   * @return std::string_view
   */
  std::string_view name() {
    return name_;
  }

  /**
   * @brief Return the type of the buffer.
   *
   * @return tiledb_datatype_t type
   */
  tiledb_datatype_t type() const {
    return type_;
  }

  /**
   * @brief Return true if the buffer contains variable length data.
   */
  bool is_var() const {
    return is_var_;
  }

  /**
   * @brief Return true if the buffer contains nullable data.
   */
  bool is_nullable() const {
    return is_nullable_;
  }

  /**
   * @brief Convert the data bytemap to a bitmap in place.
   *
   */
  void data_to_bitmap() {
    ColumnBuffer::to_bitmap(data<uint8_t>());
  }

  /**
   * @brief Convert the validity bytemap to a bitmap in place.
   *
   */
  void validity_to_bitmap() {
    ColumnBuffer::to_bitmap(validity());
  }

  /**
   * @brief Push data into the buffer.
   *
   * This template handles non-variable length data types.
   *
   * @tparam T data type
   * @param value data to add to the buffer
   * @return size_t size of the buffer in bytes
   */
  template <typename T>
  size_t push_back(T value) {
    if (sizeof(T) != type_size_) {
      throw std::runtime_error(
          "[ColumnBuffer] Data type size mismatch for " + name_);
    }

    // Allocate more memory if needed
    while (data_.capacity() < (num_cells_ + 1) * type_size_) {
      // Update the data buffer size
      data_.reserve(data_.capacity() + DEFAULT_ALLOC_BYTES);
    }

    // Resize the buffer and copy data into it.
    // Resize is required because memcpy-ing data into the buffer does not
    // update the size, which is required when reserving more memory.
    data_.resize(data_.size() + type_size_);
    memcpy(data_.data() + num_cells_ * type_size_, &value, type_size_);
    num_cells_++;

    size_t bytes = num_cells_ * type_size_;

    if (is_nullable_) {
      validity_.push_back(1);
      bytes += validity_.size() * sizeof(uint8_t);
    }

    return bytes;
  }

  /**
   * @brief Push string data into the buffer.
   *
   * @param value data to add to the buffer
   * @return size_t size of the buffer in bytes
   */
  size_t push_back(const std::string& value) {
    if (!is_var_) {
      throw std::runtime_error(
          "[ColumnBuffer] Variable length data not supported for " + name_);
    }

    // Allocate more memory if needed
    while (data_.capacity() < num_elements_ + value.size()) {
      data_.reserve(data_.capacity() + DEFAULT_ALLOC_BYTES);
    }

    // Resize the buffer and copy data into it.
    // Resize is required because memcpy-ing data into the buffer does not
    // update the size, which is required when reserving more memory.
    data_.resize(data_.size() + value.size());
    memcpy(data_.data() + num_elements_, value.data(), value.size());
    num_cells_++;

    // The offset is the number of bytes in the data buffer.
    if (!is_arrow_) {
      offsets_.push_back(num_elements_);
    }
    num_elements_ += value.size();

    // Add extra offset for arrow.
    if (is_arrow_) {
      offsets_.push_back(num_elements_);
    }

    size_t bytes = num_elements_ + offsets_.size() * sizeof(uint64_t);

    if (is_nullable_) {
      validity_.push_back(1);
      bytes += validity_.size() * sizeof(uint8_t);
    }

    return bytes;
  }

  /**
   * @brief Push string data into the buffer.
   *
   * @param value data to add to the buffer
   * @return size_t size of the buffer in bytes
   */
  size_t push_back(std::string_view value) {
    if (!is_var_) {
      throw std::runtime_error(
          "[ColumnBuffer] Variable length data not supported for " + name_);
    }

    // Allocate more memory if needed
    while (data_.capacity() < num_elements_ + value.size()) {
      data_.reserve(data_.capacity() + DEFAULT_ALLOC_BYTES);
    }

    // Resize the buffer and copy data into it.
    // Resize is required because memcpy-ing data into the buffer does not
    // update the size, which is required when reserving more memory.
    data_.resize(data_.size() + value.size());
    memcpy(data_.data() + num_elements_, value.data(), value.size());
    num_cells_++;

    // The offset is the number of bytes in the data buffer.
    if (!is_arrow_) {
      offsets_.push_back(num_elements_);
    }
    num_elements_ += value.size();

    // Add extra offset for arrow.
    if (is_arrow_) {
      offsets_.push_back(num_elements_);
    }

    size_t bytes = num_elements_ + offsets_.size() * sizeof(uint64_t);

    if (is_nullable_) {
      validity_.push_back(1);
      bytes += validity_.size() * sizeof(uint8_t);
    }

    return bytes;
  }

  /**
   * @brief Push variable length data into the buffer.
   *
   * @tparam T data type
   * @param value data to add to the buffer
   * @return size_t size of the buffer in bytes
   */
  template <typename T>
  size_t push_back(const std::span<T> value) {
    if (!is_var_) {
      throw std::runtime_error(
          "[ColumnBuffer] Variable length data not supported for " + name_);
    }

    // Allocate more memory if needed
    while (data_.capacity() < (num_elements_ + value.size()) * type_size_) {
      data_.reserve(data_.capacity() + DEFAULT_ALLOC_BYTES);
    }

    // Resize the buffer and copy data into it.
    // Resize is required because memcpy-ing data into the buffer does not
    // update the size, which is required when reserving more memory.
    data_.resize(data_.size() + value.size() * type_size_);
    memcpy(
        data_.data() + num_elements_ * type_size_,
        value.data(),
        value.size() * type_size_);
    num_cells_++;

    // The offset is the number of elements in the data buffer.
    if (!is_arrow_) {
      offsets_.push_back(num_elements_ * type_size_);
    }
    num_elements_ += value.size();

    // Add extra offset for arrow.
    if (is_arrow_) {
      offsets_.push_back(num_elements_ * type_size_);
    }

    size_t bytes =
        num_elements_ * type_size_ + offsets_.size() * sizeof(uint64_t);
    if (is_nullable_) {
      validity_.push_back(1);
      bytes += validity_.size() * sizeof(uint8_t);
    }

    return bytes;
  }

  /**
   * @brief Push variable length data into the buffer.
   *
   * @tparam T data type
   * @param value data to add to the buffer
   * @return size_t size of the buffer in bytes
   */
  template <typename T>
  size_t push_back(const std::vector<T>& value) {
    if (!is_var_) {
      throw std::runtime_error(
          "[ColumnBuffer] Variable length data not supported for " + name_);
    }

    // Allocate more memory if needed
    while (data_.capacity() < (num_elements_ + value.size()) * type_size_) {
      data_.reserve(data_.capacity() + DEFAULT_ALLOC_BYTES);
    }

    // Resize the buffer and copy data into it.
    // Resize is required because memcpy-ing data into the buffer does not
    // update the size, which is required when reserving more memory.
    data_.resize(data_.size() + value.size() * type_size_);
    memcpy(
        data_.data() + num_elements_ * type_size_,
        value.data(),
        value.size() * type_size_);
    num_cells_++;

    // The offset is the number of elements in the data buffer.
    if (!is_arrow_) {
      offsets_.push_back(num_elements_ * type_size_);
    }
    num_elements_ += value.size();

    // Add extra offset for arrow.
    if (is_arrow_) {
      offsets_.push_back(num_elements_ * type_size_);
    }

    size_t bytes =
        num_elements_ * type_size_ + offsets_.size() * sizeof(uint64_t);
    if (is_nullable_) {
      validity_.push_back(1);
      bytes += validity_.size() * sizeof(uint8_t);
    }

    return bytes;
  }

  /**
   * @brief Push a null cell into the buffer.
   *
   * @return size_t size of the buffer in bytes
   */
  size_t push_null() {
    if (!is_nullable_) {
      throw std::runtime_error(
          "[ColumnBuffer] Nullable data not supported for " + name_);
    }

    if (is_var_) {
      if (offsets_.size() == 0) {
        offsets_.push_back(0);
      } else {
        offsets_.push_back(num_elements_ * type_size_);
      }
    }

    num_cells_++;
    validity_.push_back(0);

    size_t bytes = num_cells_ * type_size_ +
                   offsets_.size() * sizeof(uint64_t) +
                   validity_.size() * sizeof(uint8_t);

    return bytes;
  }

  /**
   * @brief Clear the buffer.
   *
   */
  void clear() {
    num_cells_ = 0;
    num_elements_ = 0;
    data_.clear();
    offsets_.clear();
    validity_.clear();
  }

 private:
  //===================================================================
  //= private static
  //===================================================================

  /**
   * @brief Allocate and return a ColumnBuffer.
   *
   * @param array TileDB array
   * @param name Column name
   * @param type TileDB datatype
   * @param is_var True if variable length data
   * @param is_nullable True if nullable data
   * @return ColumnBuffer
   */
  static std::shared_ptr<ColumnBuffer> alloc(
      std::shared_ptr<Array> array,
      std::string_view name,
      tiledb_datatype_t type,
      bool is_var,
      bool is_nullable);

  //===================================================================
  //= private non-static
  //===================================================================

  // Name of the column from the schema.
  std::string name_;

  // Data type of the column from the schema.
  tiledb_datatype_t type_;

  // Bytes per element.
  uint64_t type_size_;

  // Number of cells.
  uint64_t num_cells_ = 0;

  // Number of elements for variable length data.
  uint64_t num_elements_ = 0;

  // If true, the data type is variable length
  bool is_var_;

  // If true, the data is nullable
  bool is_nullable_;

  // Data buffer.
  std::vector<std::byte> data_;

  // Offsets buffer (optional).
  std::vector<uint64_t> offsets_;

  // Validity buffer (optional).
  std::vector<uint8_t> validity_;

  // The buffer is being built for Arrow, add an extra offset
  bool is_arrow_ = false;
};

}  // namespace tiledb::vcf
#endif
