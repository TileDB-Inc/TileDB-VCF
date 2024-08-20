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
   * @brief Assign the array version to write
   *
   * @param version the version to write
   */
  static void set_array_version(uint32_t version);
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
   * @param group TileDB-VCF dataset group
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
   * Disables variant stat ingestion if the TileDB-VCF does not contain the
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
  VariantStats(bool delete_mode = false);

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
  void flush(bool clear = false);

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
  inline static const std::string VARIANT_STATS_ARRAY = "variant_stats";

  // Array version
  inline static const uint32_t VARIANT_STATS_VERSION = 3;
  inline static const uint32_t VARIANT_STATS_MIN_VERSION = 2;

  // Array columns
  enum ColumnNames { CONTIG, POS, SAMPLE, ALLELE };
  inline static const std::vector<std::string> COLUMN_STR = {
      "contig", "pos", "sample", "allele"};

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

  // maximum allele length encountered
  static int32_t max_length_;

  // Array version
  static uint32_t array_version_;

  // Flag to indicate if AN is present in the schema, needed to handle
  // the case where AN could be missing in a version 2 schema
  static bool an_present_;

  //===================================================================
  //= private non-static
  //===================================================================

  // Count delta is +1 in ingest mode, -1 in delete mode
  int count_delta_ = 1;

  // Set of sample names in this query (per thread)
  std::set<std::string> sample_names_;

  struct FieldValues {
    int32_t ac = 0;
    int32_t an = 0;
    int32_t n_hom = 0;
    uint32_t max_length = 0;
    uint32_t end = 0;
  };

  // Stats per allele at the current locus: map allele -> (map attr -> value)
  std::map<std::string, FieldValues> values_;

  // Contig of the current locus
  std::string contig_;

  // current sample
  std::string sample_;

  // Position of the current locus
  uint32_t pos_;

  // End Position of the current locus
  uint32_t end_;

  // Buffer for contigs
  std::string contig_buffer_;

  // Buffer for contig offsets
  std::vector<uint64_t> contig_offsets_;

  // Buffer for positions
  std::vector<uint32_t> pos_buffer_;

  // Buffer for samples
  std::string sample_buffer_;

  // Buffer for contig offsets
  std::vector<uint64_t> sample_offsets_;

  // Buffer for alleles
  std::string allele_buffer_;

  // Buffer for allele offsets
  std::vector<uint64_t> allele_offsets_;

  std::vector<int32_t> ac_buffer_;
  std::vector<int32_t> an_buffer_;
  std::vector<int32_t> n_hom_buffer_;
  std::vector<uint32_t> max_length_buffer_;
  std::vector<uint32_t> end_buffer_;

  // Reusable htslib buffer for bcf_get_* functions
  int* dst_ = nullptr;

  // Reusable htslib buffer size for bcf_get_* functions
  int ndst_ = 0;

  /**
   * @brief Add a record to the stats computation buffer, using version 2
   * schema.
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
  void process_v2(
      const bcf_hdr_t* hdr,
      const std::string& sample_name,
      const std::string& contig,
      uint32_t pos,
      bcf1_t* record);

  /**
   * @brief Add a record to the stats computation buffer, using version 3
   * schema.
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
  void process_v3(
      const bcf_hdr_t* hdr,
      const std::string& sample_name,
      const std::string& contig,
      uint32_t pos,
      bcf1_t* record);

  /**
   * @brief Move stats for the current locus to the TileDB buffers and start
   * collecting stats at the next locus.
   *
   */
  void update_results();

  /**
   * @brief Create an ALT string from the reference and alternate alleles.
   *
   * @param ref Reference allele
   * @param alt Alternate allele
   * @return std::string ALT string
   */
  std::string alt_string(char* ref, char* alt);

  /**
   * @brief Create an ALT string from the reference and alternate alleles.
   *
   * @param ref Reference allele
   * @param alt Alternate allele
   * @return std::string ALT string
   */
  std::string alt_string_v3(char* ref, char* alt);
};

}  // namespace tiledb::vcf

#endif
