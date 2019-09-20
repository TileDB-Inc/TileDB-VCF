/**
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

#include "utils/bitmap.h"
#include "utils/utils.h"

namespace tiledb {
namespace vcf {

Bitmap::Bitmap(void* buffer, size_t buffer_size) {
  buffer_ = static_cast<uint8_t*>(buffer);
  buffer_size_ = buffer_size;
  nbits_ = buffer_size * 8;
}

size_t Bitmap::nbits() const {
  return nbits_;
}

void Bitmap::set(size_t i) {
  size_t idx = i / 8;
  uint8_t off = i % 8;
  buffer_[idx] |= (1 << off);
}

void Bitmap::set_all() {
  std::memset(buffer_, 0xff, buffer_size_);
}

void Bitmap::clear(size_t i) {
  size_t idx = i / 8;
  uint8_t off = i % 8;
  buffer_[idx] &= ~uint8_t(1 << off);
}

void Bitmap::clear_all() {
  std::memset(buffer_, 0, buffer_size_);
}

bool Bitmap::get(size_t i) const {
  size_t idx = i / 8;
  uint8_t off = i % 8;
  return (buffer_[idx] & uint8_t(1 << off)) != 0;
}

}  // namespace vcf
}  // namespace tiledb
