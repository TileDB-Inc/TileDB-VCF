/**
 * @file $FILE
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 */

#include "bed_file.h"
namespace tiledb {
namespace vcf {

void BedFile::parse(const std::string& path) {
  path_ = path;

  Region::parse_bed_file_htslib(path_, &regions_);
  regions_.sort();

  // TODO: make function to return vector of lists of contigs so we don't need
  // to break it out here right not parse_bed_file puts it all back together..
  std::string last_contig;
  regions_per_contig_.clear();
  for (const auto& region : regions_) {
    if (last_contig != region.seq_name) {
      regions_per_contig_.emplace_back();
      last_contig = region.seq_name;
    }
    regions_per_contig_.back().emplace_back(region);
  }
}

uint64_t BedFile::contig_count() {
  return regions_per_contig_.size();
}

void BedFile::contig_count(uint64_t* count) {
  *count = regions_per_contig_.size();
}

uint64_t BedFile::total_region_count() {
  return regions_.size();
}

void BedFile::total_region_count(uint64_t* count) {
  *count = regions_.size();
}

uint64_t BedFile::contig_region_count(const uint64_t& contig_index) {
  if (contig_index > regions_per_contig_.size() - 1)
    throw std::runtime_error(
        "contig index " + std::to_string(contig_index) +
        " greater than contig count " +
        std::to_string(regions_per_contig_.size()));

  return regions_per_contig_[contig_index].size();
}

void BedFile::contig_region_count(
    const uint64_t& contig_index, uint64_t* count) {
  if (contig_index > regions_per_contig_.size() - 1)
    throw std::runtime_error(
        "contig index " + std::to_string(contig_index) +
        " greater than contig count " +
        std::to_string(regions_per_contig_.size()));

  *count = regions_per_contig_[contig_index].size();
}

Region BedFile::contig_region(
    const uint64_t& contig_index, const uint64_t& region_index) {
  if (contig_index > regions_per_contig_.size() - 1)
    throw std::runtime_error(
        "contig index " + std::to_string(contig_index) +
        " greater than contig count " +
        std::to_string(regions_per_contig_.size()));
  if (region_index > regions_per_contig_[contig_index].size() - 1)
    throw std::runtime_error(
        "region_index index " + std::to_string(region_index) +
        " greater than region count " +
        std::to_string(regions_per_contig_[contig_index].size()) +
        " for contig index " + std::to_string(contig_index));

  return regions_per_contig_[contig_index][region_index];
}

void BedFile::contig_region(
    const uint64_t& contig_index,
    const uint64_t& region_index,
    Region** region) {
  if (contig_index > regions_per_contig_.size() - 1)
    throw std::runtime_error(
        "contig index " + std::to_string(contig_index) +
        " greater than contig count " +
        std::to_string(regions_per_contig_.size()));
  if (region_index > regions_per_contig_[contig_index].size() - 1)
    throw std::runtime_error(
        "region_index index " + std::to_string(region_index) +
        " greater than region count " +
        std::to_string(regions_per_contig_[contig_index].size()) +
        " for contig index " + std::to_string(contig_index));

  *region = &regions_per_contig_[contig_index][region_index];
}

}  // namespace vcf
}  // namespace tiledb
