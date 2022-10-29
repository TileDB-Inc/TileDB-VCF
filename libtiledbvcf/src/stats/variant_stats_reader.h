/**
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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

#ifndef TILEDB_VCF_VARIANT_STATS_READER_H
#define TILEDB_VCF_VARIANT_STATS_READER_H

#include <future>
#include <tiledb/tiledb>

#include "managed_query.h"
#include "variant_stats.h"
#include "vcf/region.h"

namespace tiledb::vcf {

/**
 * @brief The VariantStatsReader class reads and calculates stats in the
 * variant_stats array.
 *
 */

class VariantStatsReader {
 public:
  VariantStatsReader(std::shared_ptr<Context> ctx, std::string_view root_uri);
  VariantStatsReader() = delete;

  VariantStatsReader(const VariantStatsReader&) = delete;

  VariantStatsReader(VariantStatsReader&&) = default;

  void add_region(Region region) {
    regions_.push_back(region);
  }

  void compute_af(std::string condition);

  /**
   * @brief Enable AF filtering.
   *
   * @return true if AF filtering is enabled
   */
  bool enable_af();

  bool pass(uint32_t pos, const std::string& allele);

 private:
  std::shared_ptr<Array> array_;

  std::vector<Region> regions_;

  std::string condition_;

  // TODO: change this to a map of pos -> allele for alleles that pass the
  // filter condition
  std::unordered_map<uint32_t, std::unordered_map<std::string, float>> af_map_;

  // Future for compute thread
  std::future<void> compute_future_;

  void compute_af_worker_();
};
}  // namespace tiledb::vcf

#endif
