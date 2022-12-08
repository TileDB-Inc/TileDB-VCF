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

#ifndef TILEDB_VCF_VARIANT_STATS_H
#define TILEDB_VCF_VARIANT_STATS_H

#include <atomic>
#include <map>
#include <mutex>
#include <string>
#include <vector>

#include <htslib/vcf.h>
#include <tiledb/tiledb>
#include <tiledb/tiledb_experimental>  // for the new group api

namespace tiledb::vcf {

/**
 * @brief The VariantStats class adds useful variant stats to arrays at
 * ingestion time.
 *
 * The variant stats arrays contain data that is used to efficiently compute
 * population wide statistics for each variant (allele count, allele frequency,
 * hom_ref, het, hom_var, etc.). Creating these stats during ingestion saves the
 * cost of reading all the variants after ingestion to create the same stats.
 * Updating the variant stats after ingesting new samples is very efficient
 * because the stats are computed from existing data in the variant stats array.
 * In other words, we do not need to read all of the variants again after adding
 * new samples.
 *
 * The implementation of the TileDB query shared by multiple threads matches the
 * the same approach used in Writer::ingest_samples_v4. A mutex is added to
 * protect the query, but in theory is not needed because the threads need/have
 * a cooperation mechanism to ensure data is written in GLOBAL_ORDER.
 *
 * The array metadata contains a CSV list of sample names (metadata value)
 * included in each fragment (metadata key). This mapping of fragment uri to
 * sample name can be used to clean up after an ingestion failure or to remove
 * sample data from the array.
 */

class VariantStats {
 public:
  //===================================================================
  //= public static
  //===================================================================
  /**
   * @brief Create the array.
   *
   * @param ctx TileDB context
   * @param root_uri TileDB-VCF dataset uri
   * @param checksum TileDB checksum filter
   */
  static void create(
      Context& ctx, const std::string& root_uri, tiledb_filter_type_t checksum);

  /**
   * @brief Open array and create query object.
   *
   * Disables variant stat ingestion if the TileDB-VCF does not contain the
   * expected variant array.
   *
   * @param ctx TileDB context
   * @param root_uri TileDB-VCF dataset uri
   */
  static void init(std::shared_ptr<Context> ctx, const std::string& root_uri);

  /**
   * @brief Finalize the currently open write query.
   *
   */
  static void finalize();

  /**
   * @brief Finalize the query and close the array.
   *
   */
  static void close();

  /**
   * @brief Get the uri for the array
   *
   * @param root_uri
   * @return std::string
   */
  static std::string get_uri(std::string_view root_uri, bool relative = false);

  /**
   * @brief Consolidate commits
   *
   * @param ctx TileDB context
   * @param tiledb_config TileDB config
   * @param root_uri URI for the VCF dataset
   */
  static void consolidate_commits(
      std::shared_ptr<Context> ctx,
      const std::vector<std::string>& tiledb_config,
      const std::string& root_uri);

  /**
   * @brief Consolidate fragment metadata
   *
   * @param ctx TileDB context
   * @param tiledb_config TileDB config
   * @param root_uri URI for the VCF dataset
   */
  static void consolidate_fragment_metadata(
      std::shared_ptr<Context> ctx,
      const std::vector<std::string>& tiledb_config,
      const std::string& root_uri);

  /**
   * @brief Vacuum commits
   *
   * @param ctx TileDB context
   * @param tiledb_config TileDB config
   * @param root_uri URI for the VCF dataset
   */
  static void vacuum_commits(
      std::shared_ptr<Context> ctx,
      const std::vector<std::string>& tiledb_config,
      const std::string& root_uri);

  /**
   * @brief Vacuum fragment metadata
   *
   * @param ctx TileDB context
   * @param tiledb_config TileDB config
   * @param root_uri URI for the VCF dataset
   */
  static void vacuum_fragment_metadata(
      std::shared_ptr<Context> ctx,
      const std::vector<std::string>& tiledb_config,
      const std::string& root_uri);

  //===================================================================
  //= public non-static
  //===================================================================
  VariantStats();

  ~VariantStats();

  /**
   * @brief Add a record to the stats computation buffer.
   *
   * Records must be added in order of genomic locus (contig, pos) and each
   * record must be added exactly once.
   *
   * @param hdr VCF header for the record
   * @param sample_name VCF sample name
   * @param contig Contig part of the record's locus
   * @param pos Position part of the record's locus
   * @param record VCF record
   */
  void process(
      bcf_hdr_t* hdr,
      const std::string& sample_name,
      const std::string& contig,
      uint32_t pos,
      bcf1_t* record);

  /**
   * @brief Write buffered stats to the TileDB array and reset the buffers.
   *
   */
  void flush();

 private:
  //===================================================================
  //= private static
  //===================================================================

  // Array config
  inline static const std::string VARIANT_STATS_ARRAY = "variant_stats";

  // Array version
  inline static const int VARIANT_STATS_VERSION = 1;

  // Array columns
  enum ColumnNames { CONTIG, POS, ALLELE };
  inline static const std::vector<std::string> COLUMN_STR = {
      "contig", "pos", "allele"};

  // Array attributes
  enum Attr { AC = 0, N_HOM, N_CALLED, LAST_ };
  inline static const std::vector<std::string> ATTR_STR = {
      "ac", "n_hom", "n_called"};

  // Number of records in the fragment
  inline static std::atomic_int contig_records_ = 0;

  // TileDB context pointer
  inline static std::shared_ptr<Context> ctx_ = nullptr;

  // TileDB array pointer
  inline static std::unique_ptr<Array> array_ = nullptr;

  // TileDB query pointer
  inline static std::unique_ptr<Query> query_ = nullptr;

  // Mutex to protect query_ and fragment_sample_names_
  inline static std::mutex query_lock_;

  // Enable flag, disabled when the array does not exist
  inline static bool enabled_ = false;

  // Sample names included in the fragment
  inline static std::set<std::string> fragment_sample_names_;

  //===================================================================
  //= private non-static
  //===================================================================

  // Set of sample names in this query (per thread)
  std::set<std::string> sample_names_;

  // Stats per allele at the current locus: map allele -> (map attr -> value)
  std::map<std::string, std::unordered_map<int, int32_t>> values_;

  // Contig of the current locus
  std::string contig_;

  // Position of the current locus
  uint32_t pos_;

  // Buffer for contigs
  std::string contig_buffer_;

  // Buffer for contig offsets
  std::vector<uint64_t> contig_offsets_;

  // Buffer for positions
  std::vector<int32_t> pos_buffer_;

  // Buffer for alleles
  std::string allele_buffer_;

  // Buffer for allele offsets
  std::vector<uint64_t> allele_offsets_;

  // Buffer for attribute values: map Attr -> value
  std::unordered_map<int, std::vector<int32_t>> attr_buffers_;

  // Reusable htslib buffer for bcf_get_* functions
  int* dst_ = nullptr;

  // Reusable htslib buffer size for bcf_get_* functions
  int ndst_ = 0;

  /**
   * @brief Move stats for the current locus to the TileDB buffers and start
   * collecting stats at the next locus.
   *
   */
  void update_results();
};

}  // namespace tiledb::vcf

#endif
