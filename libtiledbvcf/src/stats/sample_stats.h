/**
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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

#ifndef TILEDB_VCF_SAMPLE_STATS_H
#define TILEDB_VCF_SAMPLE_STATS_H

#include <map>
#include <string>
#include <unordered_map>
#include <vector>

#include <tiledb/tiledb>
#include <tiledb/tiledb_experimental>

#include "stats/array_buffers.h"
#include "tiledbvcf_export.h"

// Forward declarations for htslib
struct bcf1_t;
struct bcf_hdr_t;

namespace tiledb::vcf {

/**
 * @brief The SampleStats class stores per-sample statistics in a TileDB array.
 *
 * The sample stats are similar to the stats calculated by `bcftools stats` and
 * Hail's sample_qc.
 *
 * For TileDB-VCF v4, each writer worker calculates sample stats for the records
 * it processes and stores the results in a fragment of the TileDB array. The
 * stats can be queried and aggregated across all fragments to get the sample
 * stats.
 */
class SampleStats {
 public:
  /**
   * @brief Create a new sample stats array.
   *
   * @param ctx TileDB context
   * @param root_uri URI of TileDB-VCF dataset
   * @param compression_level zstd compression level
   */
  static void create(
      Context& ctx, const std::string& root_uri, int compression_level = 9);

  /**
   * @brief Check if the array exists.
   *
   * @param group TileDB-VCF dataset group
   * @return true If the array exists
   */
  static bool exists(const Group& group);

  /**
   * @brief Initialize the sample stats array. Disable the sample stats
   * processing if the array does not exist.
   *
   * @param ctx TileDB context
   * @param group TileDB-VCF group
   * @param delete_mode Open the array in delete mode
   */
  static void init(
      std::shared_ptr<Context> ctx,
      const Group& group,
      bool delete_mode = false);

  /**
   * @brief Delete sample from the array.
   *
   * @param sample Sample name
   */
  static void delete_sample(const std::string& sample);

  /**
   * @brief Close the sample stats array.
   */
  static void close();

  /**
   * @brief Consolidate commits
   *
   * @param ctx TileDB context
   * @param group TileDB-VCF dataset group
   */
  static void consolidate_commits(
      std::shared_ptr<Context> ctx, const Group& group);

  /**
   * @brief Consolidate fragment metadata
   *
   * @param ctx TileDB context
   * @param group TileDB-VCF dataset group
   */
  static void consolidate_fragment_metadata(
      std::shared_ptr<Context> ctx, const Group& group);

  /**
   * @brief Vacuum commits
   *
   * @param ctx TileDB context
   * @param group TileDB-VCF dataset group
   */
  static void vacuum_commits(std::shared_ptr<Context> ctx, const Group& group);

  /**
   * @brief Vacuum fragment metadata
   *
   * @param ctx TileDB context
   * @param group TileDB-VCF dataset group
   */
  static void vacuum_fragment_metadata(
      std::shared_ptr<Context> ctx, const Group& group);

  /**
   * @brief Read the sample stats for the provided or all samples.
   */

  TILEDBVCF_EXPORT
  static std::shared_ptr<ArrayBuffers> sample_qc(
      std::string dataset_uri,
      std::vector<std::string> samples = {},
      std::map<std::string, std::string> config = {});

  // Constructor
  SampleStats() = default;

  // Destructor
  ~SampleStats();

  // Copy and move constructors/assignments
  SampleStats(const SampleStats&) = delete;
  SampleStats& operator=(const SampleStats&) = delete;
  SampleStats(SampleStats&&) = delete;
  SampleStats& operator=(SampleStats&&) = delete;

  /**
   * @brief Add a record to the sample stats.
   *
   * @param hdr VCF header for the record
   * @param sample VCF sample name
   * @param contig Contig part of the record's locus
   * @param pos Position part of the record's locus
   * @param record VCF record
   */
  void process(
      const bcf_hdr_t* hdr,
      const std::string& sample,
      const std::string& contig,
      uint32_t pos,
      bcf1_t* rec);

  /**
   * @brief Write buffered stats to the TileDB array and reset the buffers.
   *
   * For the sample stats, there is no need to flush the buffers until the
   * ingestion is complete. The required flush happens in the destructor.
   *
   * Note: Each ingestion thread will add one fragment to the sample stats
   * array.
   *
   * @param finalize If true, finalize the query.
   */
  void flush(bool finalize = false);

 private:
  // Array URI basename
  inline static const std::string SAMPLE_STATS_ARRAY = "sample_stats";

  // Array version
  inline static const int SAMPLE_STATS_VERSION = 1;

  // TileDB array pointer
  inline static std::shared_ptr<Array> array_ = nullptr;

  // Enable flag, disabled when the array does not exist
  inline static bool enabled_ = false;

  // Reusable htslib buffer for bcf_get_* functions
  int* dst_ = nullptr;

  // Reusable htslib buffer size for bcf_get_* functions
  int ndst_ = 0;

  // Current contig
  std::string contig_;

  // Aggregate stats for each sample. map: sample -> (map: field -> count)
  std::map<std::string, std::unordered_map<std::string, uint64_t>> stats_;

  // Get the URI for the array from the root group
  static std::string get_uri_(const Group& group);

  // Get the URI for the array from the root URI
  static std::string get_uri_(
      const std::string& root_uri, bool relative = false);
};

}  // namespace tiledb::vcf

#endif  // TILEDB_VCF_SAMPLE_STATS_H
