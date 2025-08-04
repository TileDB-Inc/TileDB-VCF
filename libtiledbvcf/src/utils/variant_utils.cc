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
 */

#include "utils/utils.h"
#include "utils/variant_utils.h"

namespace tiledb {
namespace vcf {

/** Dumps JSON representing a string in a manner that excludes quotes. */
inline std::string dump_string(const nlohmann::json& j) {
  std::string data;
  if (j.is_string()) {
    j.get_to(data);
  } else {
    data = j.dump();
  }
  return data;
}

Variant::Variant(const std::string& json) {
  nlohmann::json j = nlohmann::json::parse(json);
  j.at("sample").get_to(sample_name);
  j.at("chrom").get_to(chrom);
  j.at("pos").get_to(pos);
  j.at("id").get_to(id);
  j.at("ref").get_to(ref);
  j.at("alt").get_to(alt);
  j.at("qual").get_to(qual);
  set_filter(j.at("filter"));
  set_info(j.at("info"));
  set_format_and_sample(j.at("format"));
}

Variant::~Variant() {
}

std::string Variant::to_vcf_line() {
  std::string pos_str = std::to_string(pos);
  std::string qual_str = std::to_string(qual);
  std::vector<std::string> columns = {
    chrom, pos_str, id, ref, alt, qual_str, filter, info, format, sample
  };
  std::string delim = "\t";
  std::string line = utils::join(columns, delim);

  return line;
}

void Variant::to_record(const bcf_hdr_t* hdr, bcf1_t* rec) {
  // Convert the variant to a BCF kstring
  std::string line_str = to_vcf_line();
  size_t length = line_str.length() + 1;
  size_t size = sizeof(char) * length;
  char* line_cstr = new char[length];
  std::strcpy(line_cstr, line_str.c_str());
  kstring_t line_kstr = {length, size, line_cstr};

  // Convert the string to a BCF record
  if (vcf_parse(&line_kstr, hdr, rec) != 0) {
    std::string message = fmt::format(
        "Failed to convert VCF line to BCF record: \"{}\"",
        line_str);
    LOG_ERROR(message);
  }

  // NOTE: vcf_parse cleans up line_cstr
}

void Variant::set_filter(const nlohmann::json& j) {
  std::vector<std::string> filters;
  j.get_to(filters);
  std::string delim = ";";
  filter = utils::join(filters, delim);
}

void Variant::set_info(const nlohmann::json& j) {
  std::vector<std::string> fields;
  std::string data;
  for (auto& [key, value] : j.items()) {
    data = dump_string(value);
    fields.emplace_back(key + "=" + data);
  }
  std::string delim = ";";
  info = utils::join(fields, delim);
}

void Variant::set_format_and_sample(const nlohmann::json& j) {
  std::vector<std::string> format_fields;
  std::vector<std::string> sample_fields;
  std::string data;
  for (auto& [key, value] : j.items()) {
    format_fields.emplace_back(key);
    data = dump_string(value);
    sample_fields.emplace_back(data);
  }
  std::string delim = ":";
  format = utils::join(format_fields, delim);
  sample = utils::join(sample_fields, delim);
}

}  // namespace vcf
}  // namespace tiledb
