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

class AFMap {
 public:
  void insert(uint32_t pos, const std::string& allele, int ac) {
    // add ac to the an for this position
    ac_map_[pos].first += ac;

    // add ac to the ac for this position, allele
    ac_map_[pos].second[allele] += ac;

    if (false) {
      LOG_TRACE(
          "[AFMap] insert {} {} {} -> ac={} an={}",
          pos,
          allele,
          ac,
          ac_map_[pos].second[allele],
          ac_map_[pos].first);
    }
  }

  float af(uint32_t pos, const std::string& allele) {
    // calculate af = ac / an
    std::unordered_map<
        uint32_t,
        std::pair<int, std::unordered_map<std::string, int>>>::const_iterator
        pos_map_iterator = ac_map_.find(pos);

    // Return -1.0 if the allele was not called
    if (pos_map_iterator == ac_map_.end()) {
      return -1.0;
    }

    const std::pair<int, std::unordered_map<std::string, int>>& pos_map =
        pos_map_iterator->second;

    // We don't know that allele was called in this sample. Ask nicely for the
    // allele count.
    decltype(pos_map.second)::const_iterator next_allele =
        pos_map.second.find(allele);

    // First multiply by 1.0 to force a float type, then look up AC from the
    // above iterator. Substitute 0 for the AC value if it is absent in pos_map.
    // Divide by AN (pos_map.first) to get AF.
    return 1.0 *
           (next_allele == pos_map.second.end() ? 0 : next_allele->second) /
           pos_map.first;
  }

  void clear() {
    ac_map_.clear();
  }

 private:
  // map: pos -> (an, map: allele -> ac)
  std::unordered_map<
      uint32_t,
      std::pair<int, std::unordered_map<std::string, int>>>
      ac_map_;
};

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

  std::pair<bool, float> pass(uint32_t pos, const std::string& allele);

 private:
  std::shared_ptr<Array> array_;

  std::vector<Region> regions_;

  std::string condition_;

  tiledb_query_condition_op_t condition_op_;

  float threshold_;

  AFMap af_map_;

  bool async_query_ = false;

  // Future for compute thread
  std::future<void> compute_future_;

  void compute_af_worker_();

  void parse_condition_();
};
}  // namespace tiledb::vcf

#endif
