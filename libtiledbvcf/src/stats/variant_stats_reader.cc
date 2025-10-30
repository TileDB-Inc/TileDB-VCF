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

#include <algorithm>
#include <regex>
#include <stdexcept>

#include "variant_stats_reader.h"

namespace tiledb::vcf {

inline int pos_comparator(const void* a, const void* b) {
  uint32_t first = *reinterpret_cast<const uint32_t*>(a);
  uint32_t second = *reinterpret_cast<const uint32_t*>(b);
  if (first == second) {
    return 0;
  } else {
    if (first > second) {
      return 1;
    } else {
      return -1;
    }
  }
}

bool AFMap::RefBlockComp::operator()(
    const RefBlock& a, const RefBlock& b) const {
  if (a.start < b.start) {
    return true;
  } else {
    if (a.start == b.start && a.end < b.end) {
      return true;
    }
  }
  return false;
}

bool AFMap::RefBlockComp::operator()(
    const RefBlock* a, const RefBlock* b) const {
  if (a->end < b->end) {
    return true;
  } else {
    if (a->end == b->end && a->start < b->start) {
      return true;
    }
  }
  return false;
}

inline void AFMap::advance_to_ref_block(uint32_t pos) {
  if (pos + 1 == active_pos_) {
    return;
  }
  if (pos + 1 < active_pos_) {
    throw std::runtime_error(
        "[VariantStatsReader] ref block computation performed on incompletely "
        "sorted positions");
  }
  active_pos_++;
  // ref blocks entering scope
  while (selected_ref_block_ != ref_block_cache_.end() &&
         selected_ref_block_->start <= pos) {
    ac_sum_ += selected_ref_block_->ac;
    an_sum_ += selected_ref_block_->an;
    selected_ref_block_++;
  }
  // ref blocks exiting scope
  while (selected_ref_block_end_ != ref_block_by_end_.end() &&
         (**selected_ref_block_end_).end < pos) {
    ac_sum_ -= (**selected_ref_block_end_).ac;
    an_sum_ -= (**selected_ref_block_end_).an;
    selected_ref_block_end_++;
  }
}

void AFMap::finalize_ref_block_cache() {
  std::sort(ref_block_cache_.begin(), ref_block_cache_.end(), RefBlockComp());
  ref_block_by_end_.clear();
  for (RefBlock& selected_block : ref_block_cache_) {
    ref_block_by_end_.push_back(&selected_block);
  }
  std::sort(ref_block_by_end_.begin(), ref_block_by_end_.end(), RefBlockComp());
  selected_ref_block_ = ref_block_cache_.begin();
  selected_ref_block_end_ = ref_block_by_end_.begin();
}

inline void AFMap::retrieve_variant_stats(
    const AFComputer& af_computer,
    uint32_t* pos_buffer,
    char* allele_buffer,
    int32_t* allele_offsets,
    int* ac_buffer,
    int* an_buffer,
    float_t* af_buffer) {
  size_t row = 0;
  for (const auto& [pos, an_ac] : stats_map_) {
    for (size_t remaining_to_insert = an_ac.second.size();
         remaining_to_insert > 0;
         remaining_to_insert--) {
      pos_buffer[row] = pos;
      row++;
      if (row > allele_cardinality_) {
        throw std::runtime_error(
            "[VariantStatsReader] export buffer size computed incorrectly: "
            "too low");
      }
    }
  }
  if (row < allele_cardinality_) {
    throw std::runtime_error(
        "[VariantStatsReader] export buffer size computed incorrectly: too "
        "high");
  }

  qsort(pos_buffer, allele_cardinality_, sizeof(uint32_t), pos_comparator);
  allele_offsets[0] = 0;
  for (row = 0; row < allele_cardinality_;) {
    ac_map_t& ac_map = stats_map_[pos_buffer[row]].second;
    for (auto& [allele, _] : ac_map) {
      auto [af, ac, an] = af_computer.af(pos_buffer[row], allele);
      ac_buffer[row] = ac;
      an_buffer[row] = an;
      af_buffer[row] = af;
      std::memcpy(
          allele_buffer + allele_offsets[row], allele.c_str(), allele.size());
      allele_offsets[row + 1] = allele_offsets[row] + allele.size();
      row++;
    }
  }
}

VariantStatsReader::VariantStatsReader(
    std::shared_ptr<Context> ctx, const Group& group, bool async_query)
    : async_query_(async_query) {
  auto uri = VariantStats::get_uri(group);
  LOG_DEBUG("[VariantStatsReader] Opening array {}", uri);
  array_ = std::make_shared<Array>(*ctx, uri, TILEDB_READ);
  const void* alt_max = 0;
  tiledb_datatype_t alt_max_datatype = TILEDB_ANY;
  uint32_t alt_max_num = 0;
  array_->get_metadata("max_length", &alt_max_datatype, &alt_max_num, &alt_max);
  if (alt_max) {
    if (alt_max_datatype == TILEDB_INT32 && alt_max_num == 1) {
      max_length_ = *((int32_t*)alt_max);
    }
  }
  const void* variant_stats_version_read;
  uint32_t& variant_stats_version = af_map_.array_version;
  tiledb_datatype_t version_datatype;
  uint32_t version_count;
  array_->get_metadata(
      "version",
      &version_datatype,
      &version_count,
      &variant_stats_version_read);
  if (version_datatype == TILEDB_UINT32 && version_count == 1) {
    std::memcpy(
        &variant_stats_version,
        variant_stats_version_read,
        tiledb_datatype_size(version_datatype) * version_count);
  } else {
    throw std::runtime_error("Invalid metadata in stats table");
  }
  if (variant_stats_version < 2) {
    throw std::runtime_error(
        "Variant stats table from dataset ingested with older version of "
        "TileDB-VCF; to use internal IAF support, reingest this dataset with "
        "stats enabled."
        "version");
  }
}

uint32_t VariantStatsReader::array_version() {
  return af_map_.array_version;
}

void VariantStatsReader::retrieve_variant_stats(
    uint32_t* pos_buffer,
    char* allele_buffer,
    int32_t* allele_offsets,
    int* ac_buffer,
    int* an_buffer,
    float_t* af_buffer) {
  // there is no thread-safe implementation of this yet
  if (async_query_) {
    throw std::runtime_error(
        "[VariantStatsReader] can not retrieve variant stats when async quries "
        "are enabled");
  }
  AFMap::AFComputerSingle af_computer(&af_map_);
  af_map_.retrieve_variant_stats(
      af_computer,
      pos_buffer,
      allele_buffer,
      allele_offsets,
      ac_buffer,
      an_buffer,
      af_buffer);
}

void VariantStatsReader::retrieve_variant_stats(
    const size_t num_samples,
    uint32_t* pos_buffer,
    char* allele_buffer,
    int32_t* allele_offsets,
    int* ac_buffer,
    int* an_buffer,
    float_t* af_buffer) {
  // there is no thread-safe implementation of this yet
  if (async_query_) {
    throw std::runtime_error(
        "[VariantStatsReader] can not retrieve variant stats when async quries "
        "are enabled");
  }
  AFMap::AFComputerAll af_computer(&af_map_, num_samples);
  af_map_.retrieve_variant_stats(
      af_computer,
      pos_buffer,
      allele_buffer,
      allele_offsets,
      ac_buffer,
      an_buffer,
      af_buffer);
}

void VariantStatsReader::compute_af() {
  // If regions is empty, af_map_ is up to date so no more work to do
  if (regions_.empty()) {
    return;
  }

  if (async_query_) {
    TRY_CATCH_THROW(
        compute_future_ = std::async(
            std::launch::async, &VariantStatsReader::compute_af_worker_, this));
  } else {
    compute_af_worker_();
  }
}

void VariantStatsReader::set_condition(std::string condition) {
  condition_ = condition;
}

void VariantStatsReader::wait() {
  if (async_query_) {
    if (compute_future_.valid()) {
      TRY_CATCH_THROW(compute_future_.wait());
    }
  }
}

std::tuple<bool, float, uint32_t, uint32_t> VariantStatsReader::pass(
    uint32_t pos,
    const std::string& allele,
    bool scan_all_samples,
    size_t num_samples) {
  if (array_version() > 2) {
    af_map_.advance_to_ref_block(pos);
  }
  if (!allele.compare("<NON_REF>")) {
    // TODO: replace placeholder return values if necessary
    return {false, 0.0, 0, 0};
  }
  std::unique_ptr<AFMap::AFComputer> af_computer;
  if (scan_all_samples) {
    af_computer = std::make_unique<AFMap::AFComputerAll>(&af_map_, num_samples);
  } else {
    af_computer = std::make_unique<AFMap::AFComputerSingle>(&af_map_);
  }
  auto [af, ac, an] = af_computer->af(pos, allele);

  // Fail the filter if allele was not called
  if (af < 0.0) {
    LOG_DEBUG("[VariantStatsReader] {}:{} not called", pos, allele);
    // TODO: replace placeholder return values if necessary
    return {false, 0.0, 0, 0};
  }

  LOG_TRACE(
      "[AF Filter] checking {} {} = {} <= {}", pos, allele, af, threshold_);

  bool pass = false;

  switch (condition_op_) {
    case TILEDB_LT:
      pass = af < threshold_;
      break;
    case TILEDB_LE:
      pass = af <= threshold_;
      break;
    case TILEDB_GT:
      pass = af > threshold_;
      break;
    case TILEDB_GE:
      pass = af >= threshold_;
      break;
    case TILEDB_EQ:
      pass = af == threshold_;
      break;
    case TILEDB_NE:
      pass = af != threshold_;
    default:
      throw std::runtime_error("[VariantStatsReader] Invalid IAF operation");
  }
  return {pass, af, ac, an};
}

void VariantStatsReader::parse_condition_() {
  if (condition_.empty()) {
    condition_op_ = TILEDB_LE;
    threshold_ = 1.0;
    return;
  }
  std::regex re("([<=>!]+)\\s*(\\S+)");
  std::smatch sm;

  if (std::regex_search(condition_, sm, re)) {
    auto op_str = sm.str(1);
    if (op_str == "<") {
      condition_op_ = TILEDB_LT;
    } else if (op_str == "<=") {
      condition_op_ = TILEDB_LE;
    } else if (op_str == ">") {
      condition_op_ = TILEDB_GT;
    } else if (op_str == ">=") {
      condition_op_ = TILEDB_GE;
    } else if (op_str == "==") {
      condition_op_ = TILEDB_EQ;
    } else if (op_str == "!=") {
      condition_op_ = TILEDB_NE;
    } else {
      throw std::runtime_error(
          fmt::format("Invalid IAF operation: '{}'", op_str));
    }

    auto value_str = sm.str(2);
    try {
      threshold_ = std::stof(value_str);
    } catch (const std::exception& e) {
      throw std::runtime_error(
          fmt::format("Invalid IAF threshold: '{}'", value_str));
    }

    LOG_DEBUG(
        "[VariantStatsReader] condition '{}': {}, {}",
        condition_,
        op_str,
        threshold_);

  } else {
    throw std::runtime_error(
        fmt::format(
            "Cannot parse the provided IAF condition: '{}'", condition_));
  }
}

void VariantStatsReader::compute_af_worker_() {
  uint32_t& variant_stats_version = af_map_.array_version;

  // parse condition provided by user
  parse_condition_();

  // Clear old filter
  af_map_.clear();

  auto query_start_timer = std::chrono::steady_clock::now();
  LOG_INFO("[VariantStatsReader] compute_af start");

  // Setup the query
  ManagedQuery mq(array_, "variant_stats", TILEDB_UNORDERED);
  mq.select_columns({"pos", "sample", "allele", "ac", "an", "end"});
  mq.select_point<std::string>("contig", regions_.front().seq_name);
  for (auto& region : regions_) {
    LOG_DEBUG("[VariantStatsReader] compute_af for region={}", region.to_str());
    mq.select_ranges<uint32_t>(
        "pos",
        {{region.min - ((variant_stats_version > 2) ?
                            std::min<uint32_t>(region.min, max_length_) :
                            0),
          region.max}});
  }
  regions_.clear();

  // Process the results
  while (!mq.is_complete()) {
    mq.submit();
    auto num_rows = mq.results()->num_rows();

    for (unsigned int i = 0; i < num_rows; i++) {
      auto pos = mq.data<uint32_t>("pos")[i];
      auto allele = std::string(mq.string_view("allele", i));
      auto ac = mq.data<int32_t>("ac")[i];
      if (variant_stats_version >= 3) {
        auto an = mq.data<int32_t>("an")[i];
        auto end = mq.data<uint32_t>("end")[i];
        af_map_.insert(pos, allele, ac, an, end);
      } else {
        af_map_.insert(pos, allele, ac);
      }
    }
  }

  LOG_INFO(
      "[VariantStatsReader] query completed in {:.3f} sec. (VmRSS = {})",
      utils::chrono_duration(query_start_timer),
      utils::memory_usage_str());
  af_map_.finalize_ref_block_cache();
}

}  // namespace tiledb::vcf
