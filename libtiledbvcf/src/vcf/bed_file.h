/**
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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

#ifndef TILEDB_VCF_BED_FILE_H
#define TILEDB_VCF_BED_FILE_H

#include <vector>
#include "region.h"

namespace tiledb {
namespace vcf {
class BedFile {
 public:
  //  explicit BedFile(){};

  /**
   * Parse the bedfile
   */
  void parse(const std::string& path);

  /**
   * Return number of contigs in file
   * @return
   */
  uint64_t contig_count();
  void contig_count(uint64_t* count);

  /**
   * Return total number of regions
   * @return
   */
  uint64_t total_region_count();
  void total_region_count(uint64_t* count);

  /**
   * Number of regions for given contig
   * @param config
   * @return
   */
  //             uint64_t contig_region_count(const std::string& config);

  //             Region contig_region(const std::string& contig, const uint64_t
  //             region_index)

  /**
   * Get count of regions for contig index
   * @param contig_index
   * @return
   */
  uint64_t contig_region_count(const uint64_t& contig_index);
  void contig_region_count(const uint64_t& contig_index, uint64_t* count);

  /**
   * Return region for given contig and region index
   * @param contig_index
   * @param region_index
   * @return Region
   */
  Region contig_region(
      const uint64_t& contig_index, const uint64_t& region_index);

  void contig_region(
      const uint64_t& contig_index,
      const uint64_t& region_index,
      Region** reigon);

 private:
  std::string path_;

  std::list<Region> regions_;

  std::vector<std::vector<Region>> regions_per_contig_;
};
}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_BED_FILE_H
