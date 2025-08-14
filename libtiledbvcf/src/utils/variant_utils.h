/**
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB, Inc.
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
 * This file declares utility functions for working with individual variants
 * not associated with a VCF file.
 *
 */

#ifndef TILEDB_VCF_VARIANT_UTILS_H
#define TILEDB_VCF_VARIANT_UTILS_H

#include <string>

#include <nlohmann/json.hpp>

#include "utils/logger_public.h"
#include "vcf/vcf_utils.h"

namespace tiledb {
namespace vcf {

class Variant {
 public:
  std::string sample_name;
  std::string chrom;
  uint32_t pos;
  std::string id;
  std::string ref;
  std::string alt;
  float qual;
  std::string filter;
  std::string info;
  std::string format;
  std::string sample;

  Variant(const std::string& json);

  ~Variant();

  /**
   * @brief
   * Converts the variant to a line from a VCF file.
   * @return The VCF as a string
   */
  std::string to_vcf_line();

  /**
   * @brief
   * Converts the variant to a HTSlib BCF record.
   * @param hdr The HTSlib header the record will be used with
   * @param rec The pointer to save the BCF recored to
   */
  void to_record(const bcf_hdr_t* hdr, bcf1_t* rec);

 private:
  void set_filter(const nlohmann::json& j);

  void set_info(const nlohmann::json& j);

  void set_format_and_sample(const nlohmann::json& j);
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_VARIANT_UTILS_H
