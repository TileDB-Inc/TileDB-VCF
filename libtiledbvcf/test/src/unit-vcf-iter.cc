/**
 * @file   unit-vcf-iter.cc
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
 * Tests for VCF record iteration.
 */

#include "catch.hpp"

#include "vcf/vcf_utils.h"
#include "vcf/vcf_v2.h"
#include "vcf/vcf_v3.h"
#include "vcf/vcf_v4.h"

#include <cstring>
#include <iostream>

using namespace tiledb::vcf;

static const std::string input_dir = TILEDB_VCF_TEST_INPUT_DIR;

namespace {

void check_iter_v2(
    VCFV2* vcf,
    const std::vector<std::tuple<std::string, int, int>>& expected) {
  HtslibValueMem val;
  for (unsigned i = 0; i < expected.size(); i++) {
    const auto& tup = expected[i];
    bcf1_t* r = vcf->curr_rec();
    REQUIRE(r != nullptr);
    REQUIRE(bcf_seqname(vcf->hdr(), r) == std::get<0>(tup));
    REQUIRE(r->pos == std::get<1>(tup));
    REQUIRE(
        VCFUtils::get_end_pos(vcf->hdr(), r, &val) ==
        static_cast<uint32_t>(std::get<2>(tup)));

    bool b = vcf->next();
    if (i < expected.size() - 1)
      REQUIRE(b);  // should be more records in vcf
    else
      REQUIRE(!b);  // should not be more records in vcf
  }
}

void check_iter_v3(
    VCFV3* vcf,
    const std::vector<std::tuple<std::string, int, int>>& expected) {
  HtslibValueMem val;
  for (unsigned i = 0; i < expected.size(); i++) {
    const auto& tup = expected[i];
    SafeSharedBCFRec r = vcf->front_record();
    vcf->pop_record();
    REQUIRE(r != nullptr);
    REQUIRE(bcf_seqname(vcf->hdr(), r.get()) == std::get<0>(tup));
    REQUIRE(r->pos == std::get<1>(tup));
    REQUIRE(
        VCFUtils::get_end_pos(vcf->hdr(), r.get(), &val) ==
        static_cast<uint32_t>(std::get<2>(tup)));
  }

  REQUIRE(vcf->front_record() == nullptr);
}

void check_iter_v4(
    VCFV4* vcf,
    const std::vector<std::tuple<std::string, int, int>>& expected) {
  HtslibValueMem val;
  for (unsigned i = 0; i < expected.size(); i++) {
    const auto& tup = expected[i];
    SafeSharedBCFRec r = vcf->front_record();
    vcf->pop_record();
    REQUIRE(r != nullptr);
    REQUIRE(bcf_seqname(vcf->hdr(), r.get()) == std::get<0>(tup));
    REQUIRE(r->pos == std::get<1>(tup));
    REQUIRE(
        VCFUtils::get_end_pos(vcf->hdr(), r.get(), &val) ==
        static_cast<uint32_t>(std::get<2>(tup)));
  }

  REQUIRE(vcf->front_record() == nullptr);
}

}  // namespace

TEST_CASE("VCF: Test basic V2 iterator", "[tiledbvcf][iter][v2]") {
  VCFV2 vcf;
  REQUIRE(!vcf.is_open());

  vcf.open(input_dir + "/small.bcf");
  REQUIRE(vcf.is_open());
  REQUIRE(vcf.curr_rec() == nullptr);
  REQUIRE(vcf.seek("1", 0));
  REQUIRE(vcf.curr_rec() != nullptr);

  // Work around some compilers complaining about brace-init
  using Tup = std::tuple<std::string, int, int>;
  check_iter_v2(
      &vcf,
      {{
          Tup{"1", 12140, 12276},
          Tup{"1", 12545, 12770},
          Tup{"1", 13353, 13388},
      }});
  REQUIRE(!vcf.next());

  REQUIRE(vcf.seek("1", 12140));
  check_iter_v2(
      &vcf,
      {{
          Tup{"1", 12140, 12276},
          Tup{"1", 12545, 12770},
          Tup{"1", 13353, 13388},
      }});
  REQUIRE(!vcf.next());

  REQUIRE(vcf.seek("1", 12500));
  check_iter_v2(
      &vcf,
      {{
          Tup{"1", 12545, 12770},
          Tup{"1", 13353, 13388},
      }});
  REQUIRE(!vcf.next());

  REQUIRE(vcf.seek("1", 12600));
  check_iter_v2(
      &vcf,
      {{
          Tup{"1", 12545, 12770},
          Tup{"1", 13353, 13388},
      }});
  REQUIRE(!vcf.next());

  REQUIRE(vcf.seek("1", 13388));
  check_iter_v2(
      &vcf,
      {{
          Tup{"1", 13353, 13388},
      }});
  REQUIRE(!vcf.next());

  REQUIRE(!vcf.seek("1", 14000));
  REQUIRE(!vcf.next());

  vcf.close();
  REQUIRE(!vcf.is_open());
  REQUIRE(vcf.curr_rec() == nullptr);
  REQUIRE(!vcf.next());
  REQUIRE(!vcf.seek("1", 0));
}

TEST_CASE("VCF: Test V2 iterator", "[tiledbvcf][iter][v2]") {
  VCFV2 vcf;
  vcf.open(input_dir + "/random_synthetic/G1.bcf");
  REQUIRE(vcf.is_open());
  REQUIRE(vcf.curr_rec() == nullptr);
  REQUIRE(vcf.seek("7", 0));

  // Work around some compilers complaining about brace-init
  using Tup = std::tuple<std::string, int, int>;
  check_iter_v2(
      &vcf,
      {{
          Tup{"7", 11989, 70061},
          Tup{"7", 77583, 107339},
          Tup{"7", 111162, 165905},
      }});
  // Check iterator doesn't span to the next contig
  REQUIRE(!vcf.next());
}

TEST_CASE("VCF: Test basic V3 iterator", "[tiledbvcf][iter][v3]") {
  VCFV3 vcf;
  REQUIRE(!vcf.is_open());

  vcf.open(input_dir + "/small.bcf");
  REQUIRE(vcf.is_open());
  REQUIRE(vcf.seek("1", 0));

  // Work around some compilers complaining about brace-init
  using Tup = std::tuple<std::string, int, int>;
  check_iter_v3(
      &vcf,
      {{
          Tup{"1", 12140, 12276},
          Tup{"1", 12545, 12770},
          Tup{"1", 13353, 13388},
      }});
  REQUIRE(!vcf.front_record());

  REQUIRE(vcf.seek("1", 12140));
  check_iter_v3(
      &vcf,
      {{
          Tup{"1", 12140, 12276},
          Tup{"1", 12545, 12770},
          Tup{"1", 13353, 13388},
      }});
  REQUIRE(!vcf.front_record());

  REQUIRE(vcf.seek("1", 12500));
  check_iter_v3(
      &vcf,
      {{
          Tup{"1", 12545, 12770},
          Tup{"1", 13353, 13388},
      }});
  REQUIRE(!vcf.front_record());

  REQUIRE(vcf.seek("1", 12600));
  check_iter_v3(
      &vcf,
      {{
          Tup{"1", 12545, 12770},
          Tup{"1", 13353, 13388},
      }});
  REQUIRE(!vcf.front_record());

  REQUIRE(vcf.seek("1", 13388));
  check_iter_v3(
      &vcf,
      {{
          Tup{"1", 13353, 13388},
      }});
  REQUIRE(!vcf.front_record());

  REQUIRE(!vcf.seek("1", 14000));
  REQUIRE(!vcf.front_record());

  vcf.close();
  REQUIRE(!vcf.is_open());
  REQUIRE(!vcf.front_record());
  REQUIRE(!vcf.seek("1", 0));
}

TEST_CASE("VCF: Test V3 iterator", "[tiledbvcf][iter][v3]") {
  VCFV3 vcf;
  vcf.open(input_dir + "/random_synthetic/G1.bcf");
  REQUIRE(vcf.is_open());
  REQUIRE(vcf.seek("7", 0));

  // Work around some compilers complaining about brace-init
  using Tup = std::tuple<std::string, int, int>;
  check_iter_v3(
      &vcf,
      {{
          Tup{"7", 11989, 70061},
          Tup{"7", 77583, 107339},
          Tup{"7", 111162, 165905},
      }});
  // Check iterator doesn't span to the next contig
  REQUIRE(!vcf.front_record());
}

TEST_CASE("VCF: Test basic V4 iterator", "[tiledbvcf][iter][v4]") {
  VCFV4 vcf;
  REQUIRE(!vcf.is_open());

  vcf.open(input_dir + "/small.bcf");
  REQUIRE(vcf.is_open());
  REQUIRE(vcf.seek("1", 0));

  // Work around some compilers complaining about brace-init
  using Tup = std::tuple<std::string, int, int>;
  check_iter_v4(
      &vcf,
      {{
          Tup{"1", 12140, 12276},
          Tup{"1", 12545, 12770},
          Tup{"1", 13353, 13388},
      }});
  REQUIRE(!vcf.front_record());

  REQUIRE(vcf.seek("1", 12140));
  check_iter_v4(
      &vcf,
      {{
          Tup{"1", 12140, 12276},
          Tup{"1", 12545, 12770},
          Tup{"1", 13353, 13388},
      }});
  REQUIRE(!vcf.front_record());

  REQUIRE(vcf.seek("1", 12500));
  check_iter_v4(
      &vcf,
      {{
          Tup{"1", 12545, 12770},
          Tup{"1", 13353, 13388},
      }});
  REQUIRE(!vcf.front_record());

  REQUIRE(vcf.seek("1", 12600));
  check_iter_v4(
      &vcf,
      {{
          Tup{"1", 13353, 13388},
      }});
  REQUIRE(!vcf.front_record());

  REQUIRE(!vcf.seek("1", 14000));
  REQUIRE(!vcf.front_record());

  vcf.close();
  REQUIRE(!vcf.is_open());
  REQUIRE(!vcf.front_record());
  REQUIRE(!vcf.seek("1", 0));
}

TEST_CASE("VCF: Test V4 iterator", "[tiledbvcf][iter][v4]") {
  VCFV4 vcf;
  vcf.open(input_dir + "/random_synthetic/G1.bcf");
  REQUIRE(vcf.is_open());
  REQUIRE(vcf.seek("7", 0));

  // Work around some compilers complaining about brace-init
  using Tup = std::tuple<std::string, int, int>;
  check_iter_v4(
      &vcf,
      {{
          Tup{"7", 11989, 70061},
          Tup{"7", 77583, 107339},
          Tup{"7", 111162, 165905},
      }});
  // Check iterator doesn't span to the next contig
  REQUIRE(!vcf.front_record());
}
