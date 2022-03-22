/**
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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

#ifndef TILEDB_VCF_BUFFER_H
#define TILEDB_VCF_BUFFER_H

#include <string>
#include <vector>

#include <tiledb/query.h>

namespace tiledb {
namespace vcf {

/**
 * A simple buffer for efficient resize/clear operations.
 */
class Buffer {
 public:
  Buffer();

  ~Buffer();

  Buffer(Buffer&& other);

  Buffer& operator=(const Buffer& other);

  Buffer(const Buffer& other);

  Buffer& operator=(Buffer&& other);

  void start_expecting();

  // If we finish a cell but never get data, add the null value
  void stop_expecting();

  bool expecting() const;

  void append(const void* data, size_t bytes);

  void clear();

  void reserve(size_t s, bool clear_new = false);

  void resize(size_t s, bool clear_new = false);

  void reserve_offsets(size_t s);

  void resize_offsets(size_t s);

  std::vector<uint64_t>& offsets();

  const std::vector<uint64_t>& offsets() const;

  size_t size() const;

  size_t alloced_size() const;

  void set_query_buffer(const std::string& attr, tiledb::Query& q);

  template <typename T>
  T* data() const {
    return (T*)data_;
  }

  /**
   * Fetch a string value as a string_view to avoid copy
   *
   * @param element_index
   * @return
   */
  std::string_view value(uint64_t element_index) const;

  /**
   * Fetch data as a string_view in vector
   * @return
   */
  std::vector<std::string_view> data() const;

  template <typename T>
  T value(uint64_t element_index) const {
    const auto offset = element_index * sizeof(T);
    assert(data_);
    assert(offset + sizeof(T) <= data_size_);
    return *(T*)(data_ + offset);
  }

  template <typename T>
  T* value(uint64_t element_index, uint64_t* size) const {
    assert(!offsets_.empty());
    assert(data_);
    uint64_t end = element_index == offset_nelts_ - 1 ?
                       data_effective_size_ :
                       offsets_[element_index + 1];
    uint64_t start = offsets_[element_index];
    *size = end - start;
    return (T*)(data_ + start);
  }

  template <typename T>
  uint64_t nelts() const {
    return data_size_ / sizeof(T);
  }

  template <typename T>
  uint64_t allocated_nelts() const {
    return data_alloced_size_ / sizeof(T);
  }

  void effective_size(uint64_t size);

  void swap(Buffer& other);

  void offset_nelts(uint64_t offset_nelts);

 private:
  bool expecting_;

  char* data_;

  uint64_t data_alloced_size_;

  uint64_t data_size_;

  uint64_t data_effective_size_;

  uint64_t offset_nelts_;

  std::vector<uint64_t> offsets_;

  void realloc(uint64_t new_alloced_size, bool clear_new);
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_BUFFER_H
