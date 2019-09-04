/**
 * @file   constants.cc
 *
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
 *
 * @section DESCRIPTION
 *
 * This file defines the TileDB constants.
 */

#include <cstring>
#include <limits>

#include "utils/constants.h"

namespace tiledb {
namespace vcf {

const Constants& Constants::values() {
  static Constants singleton;
  return singleton;
}

Constants::Constants() {
  // The reason this class is a singleton is that the only safe way to do
  // type punning like this in C++ is by using memcpy. We cast away the field's
  // const qualifier only here, in the constructor, for initialization.

  const uint32_t null_int32_bits = 0x80000000;  // Same as the BCF2 spec
  std::memcpy(
      const_cast<int32_t*>(&null_int32_),
      &null_int32_bits,
      sizeof(null_int32_bits));

  const uint32_t null_float32_bits = 0x7f800001;  // Same as the BCF2 spec
  std::memcpy(
      const_cast<float*>(&null_float32_),
      &null_float32_bits,
      sizeof(null_float32_bits));
}

const int32_t& Constants::null_int32() const {
  return null_int32_;
}

const float& Constants::null_float32() const {
  return null_float32_;
}

}  // namespace vcf
}  // namespace tiledb