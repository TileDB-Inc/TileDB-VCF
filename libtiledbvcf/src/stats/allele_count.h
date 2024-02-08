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

#ifndef TILEDB_VCF_ALLELE_COUNT_H
#define TILEDB_VCF_ALLELE_COUNT_H

#include <atomic>
#include <map>
#include <mutex>
#include <string>
#include <vector>

#include <htslib/vcf.h>
#include <tiledb/tiledb>
#include <tiledb/tiledb_experimental>  // for the new group api

#include "vcf/region.h"

namespace tiledb::vcf {

/**
 * @brief The AlleleCount class adds useful variant stats to arrays at
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

class AlleleCount {
 public:
  //===================================================================
  //= public static
  //===================================================================
  /**
   * @brief Get the URI from TileDB-VCF dataset group
   *
   * @param group TileDB-VCF dataset group
   * @return std::string Array URI
   */
  static std::string get_uri(const Group& group);

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
   * @brief Check if the array exists.
   *
   * @param group TileDB-VCF dataset group
   * @return true If the array exists
   */
  static bool exists(const Group& group);

  /**
   * @brief Open array and create query object.
   *
   * Disables allele count ingestion if the TileDB-VCF does not contain the
   * expected array.
   *
   * @param ctx TileDB context
   * @param group TileDB-VCF dataset group
   */
  static void init(std::shared_ptr<Context> ctx, const Group& group);

  /**
   * @brief Close the array.
   *
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

  //===================================================================
  //= public non-static
  //===================================================================
  AlleleCount(bool delete_mode = false);

  ~AlleleCount();

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
      const bcf_hdr_t* hdr,
      const std::string& sample_name,
      const std::string& contig,
      uint32_t pos,
      bcf1_t* record);

  /**
   * @brief Write buffered stats to the TileDB array and reset the buffers.
   *
   * If finalize is true, the query buffers are cleared and the query is
   * finalized. Finalize should be called after all records have been
   * processed for a contig or merged contig.
   *
   * @param finalize If true, finalize the query.
   */
  void flush(bool finalize = false);

 private:
  //===================================================================
  //= private static
  //===================================================================

  /**
   * @brief Finalize the currently open write query.
   *
   */
  static void finalize_query();

  /**
   * @brief Get the URI for the array from the root URI
   *
   * @param root_uri TileDB-VCF dataset URI
   * @return std::string Array URI
   */
  static std::string get_uri(
      const std::string& root_uri, bool relative = false);

  // Array URI basename
  inline static const std::string ALLELE_COUNT_ARRAY = "allele_count";

  // Array version
  inline static const int ALLELE_COUNT_VERSION = 1;

  // Array columns
  enum Columns { CONTIG, POS, REF, ALT, FILTER, GT, COUNT };
  inline static const std::vector<std::string> COLUMN_NAME = {
      "contig", "pos", "ref", "alt", "filter", "gt", "count"};

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

  // Count delta is +1 in ingest mode, -1 in delete mode
  int count_delta_ = 1;

  // Set of sample names in this query (per thread)
  std::set<std::string> sample_names_;

  // Counts grouped by "key" at the current locus.
  // Use map to keep dimension keys sorted and maintain global order.
  std::map<std::string, int32_t> count_;

  // Contig of the current locus
  std::string contig_;

  // Position of the current locus
  uint32_t pos_;

  // Buffer for contigs
  std::string contig_buffer_;

  // Buffer for contig offsets
  std::vector<uint64_t> contig_offsets_;

  // Buffer for positions
  std::vector<uint32_t> pos_buffer_;

  // Buffer for ref
  std::string ref_buffer_;

  // Buffer for ref offsets
  std::vector<uint64_t> ref_offsets_;

  // Buffer for alt
  std::string alt_buffer_;

  // Buffer for alt offsets
  std::vector<uint64_t> alt_offsets_;

  // Buffer for filter
  std::string filter_buffer_;

  // Buffer for filter offsets
  std::vector<uint64_t> filter_offsets_;

  // Buffer for gt
  std::string gt_buffer_;

  // Buffer for gt offsets
  std::vector<uint64_t> gt_offsets_;

  // Buffer for count values
  std::vector<int32_t> count_buffer_;

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

/**
 * The AlleleCountKey class stores records from the allele_count array for use
 * as keys in a key-value pair.
 *
 */
class AlleleCountKey {
 public:
  /** position */
  uint32_t pos;
  /** reference **/
  std::string ref;
  /** alternate **/
  std::string alt;
  /** filter **/
  std::string filter;
  /** genotype **/
  std::string gt;

  AlleleCountKey(
      uint32_t pos,
      std::string ref,
      std::string alt,
      std::string filter,
      std::string gt);

  AlleleCountKey(AlleleCountKey&& toMove);

  AlleleCountKey(const AlleleCountKey& toCopy);

  bool operator<(const AlleleCountKey& b) const;

  bool operator>(const AlleleCountKey& b) const;

  bool operator==(const AlleleCountKey& b) const;
};

/*
  The AlleleCountReader class contains all the core functionality necessary to
  read and aggregate the contents of the allele_count array.

*/
class AlleleCountReader {
 public:
  /**
   * Reads the contents of the allele count array for the region
   */
  void prepare_allele_count(Region region);

  /**
   * Reads the grouped contents of the allele_count array into a set of buffers
   * @param contig_offsets contig offset buffer (n+1 cardinality)
   * @param pos position buffer
   * @param ref ref buffer
   * @param ref_offsets ref offset buffer (n+1 cardinality)
   * @param alt alt buffer
   * @param alt_offsets alt offset buffer (n+1 cardinality)
   * @param filter filter buffer
   * @param filter_offsets filter offset buffer (n+1 cardinality)
   * @param af buffer of float representing internal allele frequency
   */
  void read_from_allele_count(
      uint32_t* pos,
      char* ref,
      uint32_t* ref_offsets,
      char* alt,
      uint32_t* alt_offsets,
      char* filter,
      uint32_t* filter_offsets,
      char* gt,
      uint32_t* gt_offsets,
      int32_t* count);

  /**
   * Returns the cardinality and aggregate allele length of expanded stats rows
   */
  std::tuple<size_t, size_t, size_t, size_t, size_t>
  allele_count_buffer_sizes();

  // TODO: move this utils and unite with implementation in variant_stats
  /**
   * @brief Get the URI from TileDB-VCF dataset group
   *
   * @param group TileDB-VCF dataset group
   * @param array_name name of array to be opened
   * @return std::string Array URI
   */
  static std::string get_uri(const Group& group, std::string array_name);

  AlleleCountReader(std::shared_ptr<Context> ctx, const Group& group);

 private:
  std::map<AlleleCountKey, int32_t> AlleleCountGroupBy;
  std::shared_ptr<Array> array_;
};

}  // namespace tiledb::vcf

#endif
