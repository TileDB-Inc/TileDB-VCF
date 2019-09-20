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

#ifndef TILEDB_VCF_BITMAP_H
#define TILEDB_VCF_BITMAP_H

#include <bitset>
#include <cstdint>
#include <vector>

namespace tiledb {
namespace vcf {

/** Simple class for bitmap manipulation. */
class Bitmap {
 public:
  Bitmap(void* buffer, size_t buffer_size);

  size_t nbits() const;

  void set(size_t i);

  void set_all();

  void clear(size_t i);

  void clear_all();

  bool get(size_t i) const;

 private:
  size_t nbits_;

  uint8_t* buffer_;

  size_t buffer_size_;
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_BITMAP_H
