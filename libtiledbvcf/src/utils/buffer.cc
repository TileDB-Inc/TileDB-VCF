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

#include <cstdlib>

#include "utils/buffer.h"

namespace tiledb {
namespace vcf {

Buffer::Buffer()
    : expecting_(false)
    , data_(nullptr)
    , data_alloced_size_(0)
    , data_size_(0)
    , data_effective_size_(0)
    , offset_nelts_(0) {
}

Buffer::~Buffer() {
  std::free(data_);
}

Buffer::Buffer(Buffer&& other)
    : Buffer() {
  swap(other);
}

Buffer::Buffer(const Buffer& other) {
  expecting_ = other.expecting_;
  data_ = nullptr;
  data_alloced_size_ = 0;
  data_size_ = other.data_size_;
  if (data_size_ > 0) {
    realloc(other.data_size_, false);
    std::memcpy(data_, other.data_, other.data_size_);
  }
  offsets_.insert(offsets_.end(), other.offsets_.begin(), other.offsets_.end());
}

Buffer& Buffer::operator=(const Buffer& other) {
  Buffer clone(other);
  swap(clone);
  return *this;
}

Buffer& Buffer::operator=(Buffer&& other) {
  swap(other);
  return *this;
}

void Buffer::start_expecting() {
  expecting_ = true;
}

void Buffer::stop_expecting() {
  if (expecting_) {
    offsets_.push_back(data_size_);
    realloc(data_size_ + 1, false);
    data_[data_size_++] = 0;
  }
}

bool Buffer::expecting() const {
  return expecting_;
}

void Buffer::append(const void* data, size_t bytes) {
  expecting_ = false;
  realloc(data_size_ + bytes, false);
  std::memcpy(data_ + data_size_, data, bytes);
  data_size_ += bytes;
}

void Buffer::clear() {
  offsets_.clear();
  data_size_ = 0;
  offset_nelts_ = 0;
  data_effective_size_ = 0;
}

void Buffer::reserve(size_t s, bool clear_new) {
  if (s > data_alloced_size_)
    realloc(s, clear_new);
}

void Buffer::resize(size_t s, bool clear_new) {
  if (s > data_alloced_size_)
    realloc(s, clear_new);
  data_size_ = s;
  data_effective_size_ = s;
}

void Buffer::reserve_offsets(size_t s) {
  offsets_.reserve(s);
}

void Buffer::resize_offsets(size_t s) {
  offsets_.resize(s);
}

std::vector<uint64_t>& Buffer::offsets() {
  return offsets_;
}

const std::vector<uint64_t>& Buffer::offsets() const {
  return offsets_;
}

size_t Buffer::size() const {
  return data_size_;
}

size_t Buffer::alloced_size() const {
  return data_alloced_size_;
}

void Buffer::set_query_buffer(const std::string& attr, tiledb::Query& q) {
  q.set_buffer(
      attr, offsets_.data(), offsets_.size(), (uint8_t*)data_, data_size_);
}

void Buffer::realloc(uint64_t new_alloced_size, bool clear_new) {
  if (new_alloced_size == 0)
    return;

  auto old_alloc = data_alloced_size_;

  if (data_alloced_size_ == 0) {
    data_alloced_size_ = new_alloced_size;
    data_ = (char*)std::realloc(data_, data_alloced_size_);
  } else if (new_alloced_size > data_alloced_size_) {
    while (new_alloced_size > data_alloced_size_)
      data_alloced_size_ *= 2;
    data_ = (char*)std::realloc(data_, data_alloced_size_);
  }

  auto new_alloc = data_alloced_size_;
  if (new_alloc > old_alloc && clear_new) {
    auto count = new_alloc - old_alloc;
    memset(data_ + data_size_, 0, count);
  }

  assert(data_ != nullptr);
}

void Buffer::effective_size(uint64_t size) {
  data_effective_size_ = size;
}

void Buffer::offset_nelts(uint64_t offset_nelts) {
  offset_nelts_ = offset_nelts;
}

void Buffer::swap(Buffer& other) {
  std::swap(expecting_, other.expecting_);
  std::swap(data_, other.data_);
  std::swap(data_alloced_size_, other.data_alloced_size_);
  std::swap(data_size_, other.data_size_);
  offsets_.swap(other.offsets_);
}

std::string_view Buffer::value(uint64_t element_index) const {
  assert(!offsets_.empty());
  assert(data_);

  uint64_t start = offsets_[element_index];
  uint64_t len = strlen(data_ + start);
  return std::string_view(data_ + start, len);
}

std::vector<std::string_view> Buffer::data() const {
  assert(!offsets_.empty());
  assert(data_);

  std::vector<std::string_view> vec(offset_nelts_);
  for (uint64_t i = 0; i < offset_nelts_; i++) {
    vec[i] = value(i);
  }

  return vec;
}

}  // namespace vcf
}  // namespace tiledb
