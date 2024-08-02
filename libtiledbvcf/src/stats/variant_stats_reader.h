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

#include <cstdint>
#include <future>
#include <set>
#include <tiledb/tiledb>

#include "dataset/tiledbvcfdataset.h"
#include "managed_query.h"
#include "variant_stats.h"
#include "vcf/region.h"

namespace tiledb::vcf {

/**
 * @brief A class to store allele counts and compute allele frequency.
 *
 */
class AFMap {
 public:
  /**
   * @brief Insert an Allele Count into the map.
   *
   * @param pos Position of the allele
   * @param allele Allele value
   * @param ac Allele Count
   */
  void insert(
      uint32_t pos,
      const std::string& allele,
      int ac,
      int an = 0,
      uint32_t end = 0) {
    // add encountered ref block to the cache
    if (array_version >= 3 && allele == "nr") {
      ref_block_cache_.push_back({pos, end, ac, an});
    } else {
      if (pos >= min_pos) {
        // Should this insertion be tallied for contributing to transport
        // (Arrow)  buffer size?
        bool should_tally =
            !ac_map_.contains(pos) || !ac_map_[pos].second.contains(allele);
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
        if (should_tally) {
          allele_aggregate_size_ += allele.length();
          allele_cardinality_++;
        }
      }
    }
  }

  /**
   * @brief Change ref block selection to match or exceed specified position
   *
   * @param pos position of ref block to find or exceed
   *
   */
  void advance_to_ref_block(uint32_t pos);

  /**
   * @brief Transition from loading ref blocks from array to validating and
   * computing AC/AN for frequencies
   *
   */
  void finalize_ref_block_cache();

  /**
   * @brief Accessor for buffer size metrics
   *
   * @return std::tuple<size_t, size_t> Allele Frequency
   */
  std::tuple<size_t, size_t> buffer_sizes() {
    return std::make_tuple(allele_cardinality_, allele_aggregate_size_);
  }

  /**
   * @brief Compute the Allele Frequency for an allele at the given position.
   *
   * @param pos Position of the allele
   * @param allele Allele value
   * @return float Allele Frequency
   */
  inline std::tuple<float, uint32_t, uint32_t> af(
      uint32_t pos, const std::string& allele);

  /**
   * @brief Compute the Allele Frequency for an allele at the given position,
   * accounting for the full population of the dataset
   *
   * @param pos Position of the allele
   * @param allele Allele value
   * @param num_samples Number of samples in the entire dataset
   * @return float Allele Frequency
   */
  inline std::tuple<float, uint32_t, uint32_t> af(
      uint32_t pos, const std::string& allele, size_t num_samples);

  /**
   * @brief Compute the Allele Frequency for an allele at the given position.
   *
   * @param pos Position of the allele
   * @param allele Allele value
   * @return float Allele Frequency
   */
  inline std::tuple<float, uint32_t, uint32_t> af_v3(
      uint32_t pos, const std::string& allele);

  /**
   * @brief Compute the Allele Frequency for an allele at the given position,
   * accounting for the full population of the dataset
   *
   * @param pos Position of the allele
   * @param allele Allele value
   * @param num_samples Number of samples in the entire dataset
   * @return float Allele Frequency
   */
  inline std::tuple<float, uint32_t, uint32_t> af_v3(
      uint32_t pos, const std::string& allele, size_t num_samples);

  /**
   * @brief Clear the map.
   *
   */
  void clear() {
    ac_map_.clear();
    ref_block_cache_.clear();
    ac_sum_ = 0;
    an_sum_ = 0;
    active_pos_ = 0;
  }

  /**
   * @brief Populate buffers
   *
   * @param pos buffer of int representing position
   * @param allele variable buffer of char representing allele
   * @param allele_offsets allele offsets
   * @param ac buffer of int representing allele count
   * @param an buffer of int representing allele number
   * @param af buffer of float representing internal allele frequency
   */
  void retrieve_variant_stats(
      uint32_t* pos,
      char* allele,
      int32_t* allele_offsets,
      int* ac,
      int* an,
      float_t* af);

  uint32_t array_version = 2;

  /** minimum position for single-range queries */
  uint32_t min_pos = 0;

 private:
  struct RefBlock {
    uint32_t start;
    uint32_t end;
    int32_t ac;
    int32_t an;
  };

  /**
   * @brief The RefBlockComp class serves as a comparator for RefBlocks to be
   * sorted.
   *
   */
  class RefBlockComp {
   public:
    /**
     * @brief sort ref blocks directly in the cache, ascending by start
     *
     */
    bool operator()(const RefBlock& a, const RefBlock& b) const;

    /**
     * @brief sort ref block pointers to the cache, ascending by end
     *
     */
    bool operator()(const RefBlock* a, const RefBlock* b) const;
  };

  /** Allele Count map: pos -> (an, map: allele -> ac) */
  std::unordered_map<
      uint32_t,
      std::pair<int, std::unordered_map<std::string, int>>>
      ac_map_;

  /** track running sum of AC when computing IAF for GVCF */
  uint64_t ac_sum_ = 0;

  /** track running sum of AN when computing IAF for GVCF */
  uint64_t an_sum_ = 0;

  /** track position when computing IAF for GVCF */
  uint64_t active_pos_ = 0;

  /** ref block, selected from ref_block_cache_, that demarcates the beginning
   * of the overlap  with the current position */
  std::vector<RefBlock>::iterator selected_ref_block_;
  /** ref block pointer, selected from ref_block_by_end_, that demarcates the
   * end of the overlap with the current position */
  std::vector<const RefBlock*>::iterator selected_ref_block_end_;

  /** keep track of all ref blocks in range */
  std::vector<RefBlock> ref_block_cache_;

  /** keep track of ref blocks remaining */
  std::vector<const RefBlock*> ref_block_by_end_;

  /** buffer size for transporting allele contents */
  size_t allele_aggregate_size_ = 0;

  /** number of rows for transporting allele contents */
  size_t allele_cardinality_ = 0;
};

/**
 * @brief The VariantStatsReader class reads and calculates stats in the
 * variant_stats array.
 *
 */
class VariantStatsReader {
 public:
  /**
   * @brief Construct a new Variant Stats Reader object
   *
   * @param ctx TileDB context
   * @param group TileDB-VCF dataset group
   * @param async_query If true, query variant stats in parallel with the data
   * array.
   */
  VariantStatsReader(
      std::shared_ptr<Context> ctx,
      const Group& group,
      bool async_query = true);

  VariantStatsReader() = delete;
  VariantStatsReader(const VariantStatsReader&) = delete;
  VariantStatsReader(VariantStatsReader&&) = default;

  /**
   * @brief Get variant stats array version
   *
   * @return variant stats array schema version
   *
   */
  uint32_t array_version();

  /**
   * @brief Add a region to the next allele frequency computation
   *
   * @param region
   * @param set_min this single region represents the bounds of a standalone
   * variant stats query
   */
  void add_region(Region region, bool set_min = false) {
    // Wait for any previous async queries to complete
    wait();
    regions_.push_back(region);
    af_map_.min_pos = set_min ? region.min : 0;
  }

  /**
   * @brief Populate buffers
   *
   * @param pos buffer of int representing position
   * @param allele variable buffer of char representing allele
   * @param allele_offsets allele offsets
   * @param ac buffer of int representing allele count
   * @param an buffer of int representing allele number
   * @param af buffer of float representing internal allele frequency
   */
  void retrieve_variant_stats(
      uint32_t* pos,
      char* allele,
      int32_t* allele_offsets,
      int* ac,
      int* an,
      float_t* af);

  /**
   * @brief Accessor for buffer size metrics when retrieving variant stats
   *
   * @return std::tuple<size_t, size_t> Allele Frequency
   */
  std::tuple<size_t, size_t> variant_stats_buffer_sizes() {
    return af_map_.buffer_sizes();
  }

  /**
   * @brief Compute AF for the provided regions
   *
   */
  void compute_af();

  /**
   * @brief Set an AF filtering constraint
   *
   * @param condition filtering constraint to set
   */
  void set_condition(std::string condition);

  /**
   * @brief Wait until the AF computation is complete.
   *
   */
  void wait();

  /**
   * @brief Check if AF filtering is enabled.
   *
   * @return true if AF filtering is enabled
   */
  bool is_enabled() {
    return !condition_.empty();
  }

  /**
   * @brief Check if the allele at the given position passes the allele filter.
   *
   * @param pos Position of the allele
   * @param allele Allele value
   * @return std::pair<bool, float> (Allele passed the filter, AF value)
   */
  std::tuple<bool, float, uint32_t, uint32_t> pass(
      uint32_t pos,
      const std::string& allele,
      bool scan_all_samples,
      size_t num_samples);

 private:
  /** maximum length to extend variant stats query */
  int32_t max_length_ = 0;

  // Variant stats array
  std::shared_ptr<Array> array_;

  // List of regions to compute AF
  std::vector<Region> regions_;

  // Allele frequency filter condition provided by user
  std::string condition_;

  // Allele frequency filter condition op, parsed from condition_
  tiledb_query_condition_op_t condition_op_;

  // Allele frequency filter threshold, pared from condition_
  float threshold_;

  // Allele frequency map
  AFMap af_map_;

  // If true, query variant stats in parallel with the data array.
  bool async_query_ = true;

  // Future for compute thread
  std::future<void> compute_future_;

  // Worker function to compute allele frequency
  void compute_af_worker_();

  // Parse the user providied allele filter condition
  void parse_condition_();
};
}  // namespace tiledb::vcf

#endif
