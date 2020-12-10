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

#ifndef TILEDB_VCF_REGION_H
#define TILEDB_VCF_REGION_H

#include <tiledb/vfs.h>
#include <list>
#include <map>
#include <string>
#include "vcf_utils.h"

namespace tiledb {
namespace vcf {

// Forward declare SafeRegionFh
typedef std::unique_ptr<bcf_sr_regions_t, decltype(&bcf_sr_regions_destroy)>
    SafeRegionFh;

/**
 * Struct representing a parsed region string.
 *
 * Regions values this of this type are always treated as 0-indexed, inclusive
 * intervals.
 */
struct Region {
  enum class Type {
    ZeroIndexedInclusive,
    ZeroIndexedHalfOpen,
    OneIndexedInclusive,
  };

  Region();

  Region(const std::string& seq, unsigned min, unsigned max);

  Region(const std::string& str, Type parse_from);

  std::string to_str(Type type = Type::ZeroIndexedInclusive) const;

  /**
   * Comparator used in sorting by contig name
   * @param other
   * @return
   */
  bool operator<(const Region& other) const {
    if (seq_name == other.seq_name)
      return min < other.min;
    return seq_name < other.seq_name;
  }

  /**
   * Parse a region in the format: SEQ_NAME:MIN_POS-MAX_POS
   *
   * Commas are stripped.
   */
  static Region parse_region(
      const std::string& region_str, Region::Type parse_from);

  /**
   * Parses a BED file using htslib.
   *
   * @param vfs TileDB VFS instance to use
   * @param bed_file_uri URI of BED file to parse
   * @param result Vector to hold parsed Regions
   */
  static void parse_bed_file_htslib(
      const std::string& bed_file_uri, std::list<Region>* result);

  /**
   * Parses a chromosome section of a BED file using htslib
   *
   * @param vfs TileDB VFS instance to use
   * @param bed_file_uri URI of BED file to parse
   * @param result Vector to hold parsed Regions
   */
  static std::list<Region> parse_bed_file_htslib_section(
      SafeRegionFh regions_file, const char* chr);

  /**
   * Sorts the given list of Regions by their global offsets, using the provided
   * offsets map.
   *
   * @param contig_offsets Map of contig name -> global offset
   * @param regions Vector of regions to be sorted (in-place)
   */
  static void sort(
      const std::map<std::string, uint32_t>& contig_offsets,
      std::vector<Region>* regions);

  /** Contig (sequence/chromosome) name */
  std::string seq_name;

  /** Min position, 0-based inclusive. */
  uint32_t min;

  /** Max position, 0-based inclusive. */
  uint32_t max;

  /** Optional field storing the global offset of the contig. */
  uint32_t seq_offset;
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_REGION_H
