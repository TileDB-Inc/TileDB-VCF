/**
 * @file   unit-bitmap.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2019 TileDB Inc.
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
 * Tests for Bitmap.
 */

#include "catch.hpp"

#include "utils/bitmap.h"
#include "utils/buffer.h"

#include <cstring>
#include <fstream>
#include <iostream>

using namespace tiledb::vcf;

TEST_CASE("TileDB-VCF: Test bitmap class", "[tiledbvcf][bitmap]") {
  Buffer buff;
  buff.resize(113);
  Bitmap b(buff.data<void>(), buff.size());
  REQUIRE(b.nbits() == 113 * 8);

  b.set_all();
  for (size_t i = 0; i < b.nbits(); i++)
    REQUIRE(b.get(i));

  b.clear_all();
  for (size_t i = 0; i < b.nbits(); i++)
    REQUIRE(!b.get(i));

  for (size_t i = 0; i < b.nbits(); i++) {
    b.set(i);
    REQUIRE(b.get(i));
  }

  Bitmap b2(buff.data<void>(), buff.size());
  for (size_t i = 0; i < b2.nbits(); i++)
    REQUIRE(b2.get(i));
}
