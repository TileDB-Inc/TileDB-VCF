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

#include "variant_stats_reader.h"

namespace tiledb::vcf {

VariantStatsReader::VariantStatsReader(
    std::shared_ptr<Context> ctx, std::string_view root_uri) {
  auto uri = VariantStats::get_uri(root_uri);
  LOG_DEBUG("[VariantStatsReader] Opening array {}", uri);
  array_ = std::make_shared<Array>(*ctx, uri, TILEDB_READ);
}

void VariantStatsReader::compute_af(std::string condition) {
  condition_ = condition;

  // If condition is empty, nothing to do
  // If regions is empty, af_map_ is up to date so no more work to do
  if (condition.empty() || regions_.empty()) {
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

bool VariantStatsReader::enable_af() {
  // If condition is empty, af filter is disabled
  if (condition_.empty()) {
    return false;
  }

  // Wait until compute thread is finished

  if (async_query_) {
    TRY_CATCH_THROW(compute_future_.wait());
  }
  return true;
}

bool VariantStatsReader::pass(uint32_t pos, const std::string& allele) {
  float af = af_map_.af(pos, allele);

  // Fail the filter if allele was not called
  if (af < 0.0) {
    LOG_DEBUG("[VariantStatsReader] {}:{} not called", pos, allele);
    return false;
  }

  LOG_TRACE(
      "[AF Filter] checking {} {} = {} <= {}", pos, allele, af, threshold_);

  return af <= threshold_;
}

void VariantStatsReader::compute_af_worker_() {
  auto tokens = utils::split(condition_);
  threshold_ = std::stof(tokens[1]);
  LOG_DEBUG(
      "[VariantStatsReader] condition {} {} {}",
      condition_,
      tokens[0],
      threshold_);

  // Clear old filter
  af_map_.clear();

  auto query_start_timer = std::chrono::steady_clock::now();
  LOG_INFO("[VariantStatsReader] compute_af start");

  // Setup the query
  ManagedQuery mq(array_, "variant_stats", TILEDB_UNORDERED);
  mq.select_columns({"pos", "allele", "ac"});
  mq.select_point<std::string>("contig", regions_.front().seq_name);
  for (auto& region : regions_) {
    LOG_DEBUG("[VariantStatsReader] compute_af for region={}", region.to_str());
    mq.select_ranges<uint32_t>("pos", {{region.min, region.max}});
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

      af_map_.insert(pos, allele, ac);
    }
  }

  LOG_INFO(
      "[VariantStatsReader] query completed in {:.3f} sec. (VmRSS = {})",
      utils::chrono_duration(query_start_timer),
      utils::memory_usage_str());
}

}  // namespace tiledb::vcf
