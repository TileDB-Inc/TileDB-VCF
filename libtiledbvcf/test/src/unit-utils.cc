/**
 * @file   unit-utils.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2026 TileDB Inc.
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
 * Tests for utils.
 */

#include "catch.hpp"

#include "utils/utils.h"

#include <vector>

using namespace tiledb::vcf;

TEST_CASE("TileDB-VCF: Test join util", "[tiledbvcf][utils]") {
  std::vector<std::string> v;
  std::string expected_result = "";
  REQUIRE(utils::join(v, ',').compare(expected_result) == 0);
  REQUIRE(utils::join(v, ',', true).compare(expected_result) == 0);
  REQUIRE(utils::join(v, ',', false).compare(expected_result) == 0);

  v.emplace_back("a");
  v.emplace_back("b");
  v.emplace_back("c");
  expected_result = "a,b,c";
  REQUIRE(utils::join(v, ',').compare(expected_result) == 0);
  REQUIRE(utils::join(v, ',', true).compare(expected_result) == 0);
  REQUIRE(utils::join(v, ',', false).compare(expected_result) == 0);

  v.emplace_back("");
  v.emplace_back("");
  v.emplace_back("d");
  v.emplace_back("");
  expected_result = "a,b,c,d";
  REQUIRE(utils::join(v, ',').compare(expected_result) == 0);
  REQUIRE(utils::join(v, ',', true).compare(expected_result) == 0);
  expected_result = "a,b,c,,,d,";
  REQUIRE(utils::join(v, ',', false).compare(expected_result) == 0);
}
