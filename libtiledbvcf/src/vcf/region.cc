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

#include <algorithm>

#include "utils/utils.h"
#include "vcf/region.h"

namespace tiledb {
namespace vcf {

Region::Region()
    : min(0)
    , max(0)
    , seq_offset(std::numeric_limits<uint32_t>::max()) {
}

Region::Region(const std::string& seq, unsigned min, unsigned max)
    : seq_name(seq)
    , min(min)
    , max(max)
    , seq_offset(std::numeric_limits<uint32_t>::max()) {
}

Region::Region(const std::string& str, Type parse_from) {
  auto r = parse_region(str, parse_from);
  seq_name = r.seq_name;
  min = r.min;
  max = r.max;
  seq_offset = std::numeric_limits<uint32_t>::max();
}

std::string Region::to_str(Type type) const {
  switch (type) {
    case Type::ZeroIndexedInclusive:
      return seq_name + ':' + std::to_string(min) + '-' + std::to_string(max);
    case Type::ZeroIndexedHalfOpen:
      return seq_name + ':' + std::to_string(min) + '-' +
             std::to_string(max + 1);
    case Type::OneIndexedInclusive:
      return seq_name + ':' + std::to_string(min + 1) + '-' +
             std::to_string(max + 1);
    default:
      throw std::invalid_argument("Unknown region type for string conversion.");
  }
}

Region Region::parse_region(
    const std::string& region_str, Region::Type parse_from) {
  if (region_str.empty())
    return {"", 0, 0};

  Region result;
  std::vector<std::string> region_split = utils::split(region_str, ':');

  if (region_split.size() == 1) {
    region_split.push_back("0-0");
  } else if (region_split.size() != 2)
    throw std::invalid_argument(
        "Error parsing region string '" + region_str + "'; invalid format.");

  result.seq_name = region_split[0];

  // Strip commas
  region_split[1].erase(
      std::remove(region_split[1].begin(), region_split[1].end(), ','),
      region_split[1].end());

  // Range
  region_split = utils::split(region_split[1], '-');
  if (region_split.size() != 2)
    throw std::invalid_argument(
        "Error parsing region string; invalid region format, should be "
        "CHR:XX,XXX-YY,YYY\n\t" +
        region_str);

  try {
    result.min = std::stoul(region_split[0]);
    result.max = std::stoul(region_split[1]);
  } catch (std::exception& e) {
    throw std::invalid_argument(
        "Error parsing region string '" + region_str + "'");
  }

  if (result.min > result.max)
    throw std::invalid_argument("Invalid region, min > max.");

  switch (parse_from) {
    case Region::Type::ZeroIndexedInclusive:
      // Do nothing.
      break;
    case Region::Type::ZeroIndexedHalfOpen:
      assert(result.max > 0);
      result.max -= 1;
      break;
    case Region::Type::OneIndexedInclusive:
      assert(result.min > 0 && result.max > 0);
      result.min -= 1;
      result.max -= 1;
      break;
  }

  return result;
}

void Region::parse_bed_file(
    const VFS& vfs,
    const std::string& bed_file_uri,
    std::vector<Region>* result) {
  auto per_line = [&result](std::string* line) {
    auto fields = utils::split(*line, '\t');
    if (fields.size() < 3)
      throw std::runtime_error(
          "Error parsing BED file: line '" + *line + "' has invalid format.");

    // Contig name
    std::string contig_name = fields[0];
    if (contig_name.empty())
      throw std::runtime_error(
          "Error parsing BED file: line '" + *line +
          "' has empty contig name.");

    // Range min/max, with some error checks.
    uint32_t min = 0, max = 0;
    try {
      min = static_cast<uint32_t>(std::stoul(fields[1]));
      max = static_cast<uint32_t>(std::stoul(fields[2]));
    } catch (const std::exception& e) {
      throw std::runtime_error(
          "Error parsing BED file: could not parse min/max from line '" +
          *line + "'.");
    }
    if (min > max)
      throw std::runtime_error(
          "Error parsing BED file: range from line '" + *line +
          "' has min > max.");
    if (min == max)
      throw std::runtime_error(
          "Error parsing BED file: range from line '" + *line +
          "' is length 0.");
    if (max == 0)
      throw std::runtime_error(
          "Error parsing BED file: max from line '" + *line + "' is 0.");
    max--;

    result->emplace_back(contig_name, min, max);
  };

  utils::read_file_lines(vfs, bed_file_uri, per_line);
}

void Region::sort(
    const std::map<std::string, uint32_t>& contig_offsets,
    std::vector<Region>* regions) {
  std::sort(
      regions->begin(),
      regions->end(),
      [&contig_offsets](const Region& a, const Region& b) {
        auto it_a = contig_offsets.find(a.seq_name);
        if (it_a == contig_offsets.end())
          throw std::runtime_error(
              "Error sorting regions list; no contig offset found for '" +
              a.seq_name + "'.");
        auto it_b = contig_offsets.find(b.seq_name);
        if (it_b == contig_offsets.end())
          throw std::runtime_error(
              "Error sorting regions list; no contig offset found for '" +
              b.seq_name + "'.");
        const uint32_t global_min_a = it_a->second + a.min;
        const uint32_t global_min_b = it_b->second + b.min;
        return global_min_a < global_min_b;
      });
}

}  // namespace vcf
}  // namespace tiledb