/**
 * @file   constants.h
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
 * This file declares the TileDB-VCF constants.
 */

#ifndef TILEDB_VCF_CONSTANTS_H
#define TILEDB_VCF_CONSTANTS_H

#include <cinttypes>
#include <string>

namespace tiledb {
namespace vcf {

/** Singleton class containing globally defined constant values. */
class Constants {
 public:
  Constants(const Constants&) = delete;
  Constants(Constants&&) = delete;
  Constants& operator=(Constants&&) = delete;
  Constants& operator=(const Constants&) = delete;

  static const Constants& values();

  const int32_t& null_int32() const;
  const float& null_float32() const;

 private:
  const int32_t null_int32_ = 0;
  const float null_float32_ = 0;

  Constants();
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_CONSTANTS_H
