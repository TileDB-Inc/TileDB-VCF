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
 * Regions values of this type are always treated as 0-indexed, inclusive
 * intervals.
 *
 * The region_str member always contains a 0-indexed, inclusive string
 * representation of the region with an extra line number field.
 *
 *     contig:start-end:line
 *
 * When creating Region objects from the following sources, they must be
 * converted to 0-indexed, inclusive intervals:
 *   - BED files contain 0-indexed, half-open regions. However, htslib converts
 *     the regions to 0-indexed, inclusive, so no modification is required.
 *   - Regions specified on the command line (-r) are 1-indexed, inclusive and
 *     are converted to 0-index, inclusive in Region::Region(str)
 */
struct Region {
  Region();

  /*
   * Create a Region object.
   *
   * The min and max values must be 0-indexed, inclusive when creating the
   * object.
   *
   * If line == -1, the region was not created from a BED file
   */
  Region(const std::string& seq, uint32_t min, uint32_t max, int32_t line = -1);

  /*
   * Create Region object from a region string of two types:
   *
   *  1. A manually specified 1-indexed, inclusive string with the format:
   *
   *       contig:start-end
   *
   *     A line number must not be included for the string to be treated as
   *     1-indexed, inclusive.
   *
   *  2. An internal 0-indexed, inclusive string created by Region::to_str:
   *
   *       contig:start-end:line
   */
  Region(const std::string& str);

  /*
   * Convert the Region object to a region string with the format:
   *
   *   contig:start-end:line
   *
   * The Region object and string are always 0-indexed, inclusive
   */
  std::string to_str() const;

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
   * Parse a region in the format: SEQ_NAME:MIN_POS-MAX_POS[:LINE]
   *
   * Commas are stripped.
   */
  static Region parse_region(const std::string& region_str);

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

  /** string representation */
  std::string region_str;

  /** Contig (sequence/chromosome) name */
  std::string seq_name;

  /** Min position, 0-based inclusive. */
  uint32_t min;

  /** Max position, 0-based inclusive. */
  uint32_t max;

  /** Optional field storing the global offset of the contig. */
  uint32_t seq_offset;

  /** Optional line number from bed file. */
  int32_t line;
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_REGION_H
