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
 */
class AFMap {
 public:
  // allele frequency type: <AF, AC, AN>
  typedef std::tuple<float, uint32_t, uint32_t> af_t;

  /**
   * An abstract class that defines the interface for computing allele frequency
   * stats.
   */
  class AFComputer {
   public:
    /**
     * @param map The AFMap allele frequencies are being computed for
     */
    AFComputer(const AFMap* map)
        : af_map_(map) {
      // use a function pointer to avoid a branch point when computing AF
      if (map->array_version <= 2)
        af_ptr = &AFComputer::af_v2;
      else
        af_ptr = &AFComputer::af_v3;
    }

    virtual ~AFComputer() = default;

    /**
     * Computes the allele frequency for an allele at the given position.
     *
     * @param pos Position of the allele
     * @param allele Allele value
     * @return Allele frequency
     */
    af_t af(const uint32_t pos, const std::string& allele) const {
      return (this->*af_ptr)(pos, allele);
    }

   protected:
    const AFMap* af_map_;

   private:
    /** A pointer to the member function actually used to compute allele
     * frequency. */
    af_t (AFComputer::*af_ptr)(
        const uint32_t pos, const std::string& allele) const;

    /** Computes the allele frequency for version 1 and 2 stats arrays. */
    virtual af_t af_v2(const uint32_t pos, const std::string& allele) const = 0;

    /** Computes the allele frequency for version 3+ stats arrays. */
    virtual af_t af_v3(const uint32_t pos, const std::string& allele) const = 0;
  };

  /**
   * A class for computing allele frequency stats for single alleles.
   */
  class AFComputerSingle : public AFComputer {
   public:
    /**
     * @param map The AFMap allele frequencies are being computed for
     */
    AFComputerSingle(const AFMap* map)
        : AFComputer(map) {
    }

   private:
    inline float compute_af_v3(const int ac, const int an) const {
      return 1.0 * ac / an;
    }

    /** Computes the allele frequency for version 1 and 2 stats arrays. */
    af_t af_v2(const uint32_t pos, const std::string& allele) const {
      // Return -1.0 if the allele was not called
      pos_stats_map_t::const_iterator pos_map_itr =
          af_map_->stats_map_.find(pos);
      if (pos_map_itr == af_map_->stats_map_.end()) {
        return {-1.0, 0, 0};
      }
      // Get the allele count
      const auto& [allele_an, ac_map] = pos_map_itr->second;
      const ac_map_t::const_iterator& ac_itr = ac_map.find(allele);
      // Compute AF, substituting 0 for the AF and AC values if allele not in
      // pos's an_ac
      if (ac_itr == ac_map.end()) {
        return {0, 0, allele_an};
      }
      const int allele_ac = ac_itr->second;
      const float af = 1.0 * allele_ac / allele_an;
      return {af, allele_ac, allele_an};
    }

    /** Computes the allele frequency for version 3+ stats arrays. */
    af_t af_v3(const uint32_t pos, const std::string& allele) const {
      // Return -1.0 if the allele was not called
      pos_stats_map_t::const_iterator pos_map_itr =
          af_map_->stats_map_.find(pos);
      if (pos_map_itr == af_map_->stats_map_.end()) {
        return {-1.0, 0, 0};
      }
      // Get the allele count
      const auto& [allele_an, ac_map] = pos_map_itr->second;
      const ac_map_t::const_iterator& ac_itr = ac_map.find(allele);
      bool is_ref = allele == "ref";
      // Compute AF
      uint32_t ac = is_ref ? af_map_->ac_sum_ : 0;
      uint32_t an = allele_an + af_map_->an_sum_;
      if (ac_itr == ac_map.end()) {
        float af = is_ref ? compute_af_v3(af_map_->ac_sum_, an) : 0;
        uint32_t unmapped_an = allele_an + af_map_->an_sum_;
        return {af, ac, unmapped_an};
      }
      const int allele_ac = ac + ac_itr->second;
      float af = compute_af_v3(allele_ac, an);
      return {af, allele_ac, an};
    }
  };

  /**
   * A class for computing allelel frequencies stats relativekto all samples in
   * the dataset.
   */
  class AFComputerAll : public AFComputer {
   public:
    /**
     * @param map The AFMap allele frequencies are being computed for
     * @param n The total number of samples in the dataset
     */
    AFComputerAll(const AFMap* map, const size_t n)
        : AFComputer(map)
        , num_samples(n) {
    }

   private:
    const size_t num_samples;

    inline float compute_af_v3(const int ac, const int an) const {
      return 1.0 * ac / an;
    }

    /** Computes the allele frequency for version 1 and 2 stats arrays. */
    af_t af_v2(const uint32_t pos, const std::string& allele) const {
      // Return -1.0 if the allele was not called
      pos_stats_map_t::const_iterator pos_map_itr =
          af_map_->stats_map_.find(pos);
      if (pos_map_itr == af_map_->stats_map_.end()) {
        return {-1.0, 0, 0};
      }
      // Get the allele count
      const auto& [allele_an, ac_map] = pos_map_itr->second;
      const ac_map_t::const_iterator& ac_itr = ac_map.find(allele);
      // Compute AF, substituting 0 for the AF and AC values if allele not in
      // pos's an_ac
      const uint32_t an = num_samples * 2;
      if (ac_itr == ac_map.end()) {
        return {0, 0, an};
      }
      const int allele_ac = ac_itr->second;
      const float af = 1.0 * allele_ac / num_samples / 2;
      return {af, allele_ac, an};
    }

    /** Computes the allele frequency for version 3+ stats arrays. */
    af_t af_v3(const uint32_t pos, const std::string& allele) const {
      // Return -1.0 if the allele was not called
      pos_stats_map_t::const_iterator pos_map_itr =
          af_map_->stats_map_.find(pos);
      if (pos_map_itr == af_map_->stats_map_.end()) {
        return {-1.0, 0, 0};
      }
      // Get the allele count
      const auto& [allele_an, ac_map] = pos_map_itr->second;
      const ac_map_t::const_iterator& ac_itr = ac_map.find(allele);
      bool is_ref = allele == "ref";
      // Compute AF
      uint32_t ac = is_ref ? af_map_->ac_sum_ : 0;
      uint32_t an = num_samples * 2 + af_map_->an_sum_;
      if (ac_itr == ac_map.end()) {
        float af = is_ref ? compute_af_v3(af_map_->ac_sum_, an) : 0;
        uint32_t unmapped_an = num_samples / 2 + af_map_->an_sum_;
        return {af, ac, unmapped_an};
      }
      const int allele_ac = ac + ac_itr->second;
      float af = compute_af_v3(allele_ac, an);
      return {af, allele_ac, an};
    }
  };

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
        bool should_tally = !stats_map_.contains(pos) ||
                            !stats_map_[pos].second.contains(allele);
        // add ac to the an for this position
        stats_map_[pos].first += ac;

        // add ac to the ac for this position, allele
        stats_map_[pos].second[allele] += ac;

        if (false) {
          LOG_TRACE(
              "[AFMap] insert {} {} {} -> ac={} an={}",
              pos,
              allele,
              ac,
              stats_map_[pos].second[allele],
              stats_map_[pos].first);
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
   * @brief Clear the map.
   *
   */
  void clear() {
    stats_map_.clear();
    ref_block_cache_.clear();
    ac_sum_ = 0;
    an_sum_ = 0;
    active_pos_ = 0;
  }

  /**
   * @brief Populates variant stats buffers using the given AFComputer to
   * compute allele frequencies
   *
   * @param af_computer The AFComputer to use when computing allele frequencies
   * @param pos_buffer buffer of int representing position
   * @param allele_buffer variable buffer of char representing allele
   * @param allele_offsets buffer of offsets for allele_buffer
   * @param ac_buffer buffer of int representing allele count
   * @param an_buffer buffer of int representing allele number
   * @param af_buffer buffer of float representing internal allele frequency
   */
  void retrieve_variant_stats(
      const AFComputer& af_computer,
      uint32_t* pos_buffer,
      char* allele_buffer,
      int32_t* allele_offsets,
      int* ac_buffer,
      int* an_buffer,
      float_t* af_buffer);

  uint32_t array_version = 2;

  /** minimum position for single-range queries */
  uint32_t min_pos = 0;

 private:
  /** position stats map: pos -> (an, map: allele -> ac) */
  typedef std::unordered_map<std::string, int> ac_map_t;
  typedef std::pair<int, ac_map_t> an_ac_t;
  typedef std::unordered_map<uint32_t, an_ac_t> pos_stats_map_t;

  struct RefBlock {
    uint32_t start;
    uint32_t end;
    int32_t ac;
    int32_t an;
  };

  /**
   * The RefBlockComp class serves as a comparator for RefBlocks to be sorted.
   */
  class RefBlockComp {
   public:
    /**
     * Sort ref blocks directly in the cache, ascending by start
     */
    bool operator()(const RefBlock& a, const RefBlock& b) const;

    /**
     * Sort ref block pointers to the cache, ascending by end
     */
    bool operator()(const RefBlock* a, const RefBlock* b) const;
  };

  /** allele Count map */
  pos_stats_map_t stats_map_;

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
   * @brief Populates variant stats buffers
   *
   * @param num_samples Number of samples in the entire dataset
   * @param pos_buffer buffer of int representing position
   * @param allele_buffer variable buffer of char representing allele
   * @param allele_offsets buffer of offsets for allele_buffer
   * @param ac_buffer buffer of int representing allele count
   * @param an_buffer buffer of int representing allele number
   * @param af_buffer buffer of float representing internal allele frequency
   */
  void retrieve_variant_stats(
      uint32_t* pos_buffer,
      char* allele_buffer,
      int32_t* allele_offsets,
      int* ac_buffer,
      int* an_buffer,
      float_t* af_buffer);

  /**
   * @brief Scans all samples when computing internal allele frequency and
   * populates variant stats buffers
   *
   * @param num_samples Number of samples in the entire dataset
   * @param pos_buffer buffer of int representing position
   * @param allele_buffer variable buffer of char representing allele
   * @param allele_offsets buffer of offsets for allele_buffer
   * @param ac_buffer buffer of int representing allele count
   * @param an_buffer buffer of int representing allele number
   * @param af_buffer buffer of float representing internal allele frequency
   */
  void retrieve_variant_stats(
      const size_t num_samples,
      uint32_t* pos_buffer,
      char* allele_buffer,
      int32_t* allele_offsets,
      int* ac_buffer,
      int* an_buffer,
      float_t* af_buffer);

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
   * @param scan_all_samples Scan all samples when computing internal allele
   * frequency
   * @param num_samples Number of samples in the entire dataset
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
