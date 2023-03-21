/**
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2019-2021 TileDB, Inc.
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

#ifndef TILEDB_VCF_WRITER_H
#define TILEDB_VCF_WRITER_H

#include <future>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <htslib/vcf.h>
#include <tiledb/tiledb>

#include "dataset/attribute_buffer_set.h"
#include "dataset/tiledbvcfdataset.h"
#include "utils/utils.h"
#include "vcf/htslib_value.h"
#include "write/writer_worker_v4.h"

namespace tiledb {
namespace vcf {

/* ********************************* */
/*       AUXILIARY DATATYPES         */
/* ********************************* */

// Forward declaration
class WriterWorkerV4;

/** Arguments/params for dataset ingestion. */
struct IngestionParams {
  std::string uri;
  std::string log_level;
  std::string log_file;
  std::string samples_file_uri;
  std::vector<std::string> sample_uris;
  unsigned num_threads = std::thread::hardware_concurrency();
  unsigned part_size_mb = 50;
  bool verbose = false;
  ScratchSpaceInfo scratch_space;
  bool remove_samples_file = false;
  // Max number of VCF records to read into memory
  uint32_t max_record_buffer_size = 50000;         // legacy option
  bool use_legacy_max_record_buffer_size = false;  // if true, use legacy option
  std::vector<std::string> tiledb_config;
  bool tiledb_stats_enabled = false;
  bool tiledb_stats_enabled_vcf_header_array = false;

  /**
   * Max length (# columns) of an ingestion "task". This value is derived
   * empirically to optimize ingestion performance. It affects available
   * parallelism as well as load balancing of ingestion work across threads.
   */
  unsigned thread_task_size = 5000000;           // legacy option
  unsigned use_legacy_thread_task_size = false;  // if true, use legacy option

  // Total memory budget parameters
  uint32_t total_memory_budget_mb = utils::system_memory_mb() * 0.75;
  float total_memory_percentage = 0.0;

  // Parameters used to distribute the memory budget
  uint32_t input_record_buffer_mb = 1;  // per sample, per thread
  uint32_t max_tiledb_memory_mb = 4096;
  float ratio_tiledb_memory = 0.5;

  // Components of total memory budget
  uint32_t tiledb_memory_budget_mb;  // sm.mem.total_budget
  uint32_t output_memory_budget_mb;  // record heap, attribute buffers

  // Average vcf record size used to estimate the number of records that
  // fit in memory.
  int avg_vcf_record_size = 512;

  // Scaling factor for the worker task range. Scaling the task range avoids
  // flushing the attribute buffers before the record heap is empty.
  // (performance optimization)
  float ratio_task_size = 0.75;

  // Max size of TileDB buffers before flushing. Defaults to 1GB
  uint32_t max_tiledb_buffer_size_mb = 1024;  // legacy option
  bool use_legacy_max_tiledb_buffer_size_mb =
      false;  // if true, use legacy option
  float ratio_output_flush =
      0.75;  // ratio of output buffer capacity that triggers a flush to TileDB

  // Number of samples per batch for ingestion (default: 10).
  uint32_t sample_batch_size = 10;

  // Should the fragment info of data be loaded
  // This is used for resuming partial ingestions
  bool load_data_array_fragment_info = false;

  // Should we check if the samples have been partial ingested?
  // This might have a significant performance penalty on large arrays
  bool resume_sample_partial_ingestion = false;

  // Enable merging of contigs into fragments which contain multiple. This is an
  // optimization to reduce fragment count when the list of contigs is very
  // large. This can improve performance due to slow s3/azure/gcs listings when
  // there is hundreds of thousands of prefixes.
  bool contig_fragment_merging = true;

  // These are the contig that will not be merged so they are guaranteed to be
  // there own fragments The user can override this default list, we default to
  // human contigs in UCSC and ensembl formats
  std::set<std::string> contigs_to_keep_separate = {
      "chr1",  "chr2",  "chr3",  "chr4",  "chr5",  "chr6",  "chr7",  "chr8",
      "chr9",  "chr10", "chr11", "chr12", "chr13", "chr14", "chr15", "chr16",
      "chr17", "chr18", "chr19", "chr20", "chr21", "chr22", "chrY",  "chrX",
      "chrM",  "1",     "2",     "3",     "4",     "5",     "6",     "7",
      "8",     "9",     "10",    "11",    "12",    "13",    "14",    "15",
      "16",    "17",    "18",    "19",    "20",    "21",    "22",    "X",
      "Y",     "MT"};

  // These are the contigs which will be forced to be merged into combined
  // fragments By default we use the blacklist since usually there are less
  // non-mergeable contigs
  std::set<std::string> contigs_to_allow_merging = {};

  enum class ContigMode { ALL, SEPARATE, MERGED };

  // Controls which contigs are ingested:
  //  ALL = all contigs
  //  SEPARATE = contigs in the contigs_to_keep_separate
  //  MERGED = contigs not in the contigs_to_keep_separate
  ContigMode contig_mode = ContigMode::ALL;
};

/* ********************************* */
/*              WRITER               */
/* ********************************* */

/**
 * The Writer class provides an API to create an empty dataset, register samples
 * to a dataset, and ingest samples.
 *
 * The ingestion process is parallelized using multiple "writer workers" which
 * independently parse sections of the genomic space across samples into
 * columnar buffers suitable for writing to TileDB. Then, the Writer instance
 * performs the actual TileDB query submit of the data in each worker's set of
 * buffers.
 *
 * The TileDB queries are submitted in global order, which means the
 * writer/ingestion process is responsible for ensuring the cells are sorted
 * according to the global order. This constraint is what drives the design
 * around record heaps and task-based parallelism. This also necessitates
 * batching the samples into ingestion groups of length equal to the row tile
 * extent.
 */
class Writer {
 public:
  /* ********************************* */
  /*            PUBLIC API             */
  /* ********************************* */

  /** Constructor. */
  Writer();

  ~Writer();

  /**
   * Initializes the writer for storing to the given dataset. Opens the array,
   * creates the TileDB query, etc.
   *
   * @param dataset Dataset where samples will be stored
   * @param params uri URI of array
   */
  void init(const std::string& uri, const std::string& config_str = "");

  /**
   * Update parameters computed from other parameters.
   *
   * @param params IngesttionParams& Ingestion params (will be modified)
   */
  void update_params(IngestionParams& params);

  /**
   * Set writer tiledb config parameters, these can also be passed directly on
   * the ingestion params
   * @param config_str csv string of tiledb options in key2=value1,key2=value2
   * form
   */
  void set_tiledb_config(const std::string& config_str);

  /** Convenience function to set all parameters from the given struct. */
  void set_all_params(const IngestionParams& params);

  /** Sets the URI of the dataset being ingested to. */
  void set_dataset_uri(const std::string& uri);

  /**
   * Sets the list of URIs of samples to register/ingest. This can be used
   * freely in combination with a separate samples file that contains one sample
   * URI per line.
   *
   * @param sample_uris CSV string of sample URIs.
   */
  void set_sample_uris(const std::string& sample_uris);

  /**
   * Sets the list of extra info/fmt fields that will be extracted as separate
   * TileDB attributes in the underlying array.
   *
   * @param attributes CSV string of extra attributes
   */
  void set_extra_attributes(const std::string& attributes);

  /**
   * Sets the info and fmt fields that should be extracted as
   * separate TileDB attributes using all fields in the provided VCF file.
   *
   * @param vcf_uri VCF file used to extract the info and fmt fields.
   */
  void set_vcf_attributes(const std::string& vcf_uri);

  /**
   * Sets the checksum type for filter on new dataset arrays
   *
   * @param checksum
   */
  void set_checksum_type(const int& checksum);
  void set_checksum_type(const tiledb_filter_type_t& checksum);

  /**
   * Sets whether duplicates are allowed in the sample array or not
   * @param set_allow_duplicates
   */
  void set_allow_duplicates(const bool& allow_duplicates);

  /**
   * Sets the data array's tile capacity
   * @param tile_capacity
   */
  void set_tile_capacity(const uint64_t tile_capacity);

  /**
   * Defines the length of gaps between inserted anchor records.
   * @param tile_capacity
   */
  void set_anchor_gap(const uint32_t anchor_gap);

  /** Creates an empty dataset based on parameters that have been set. */
  void create_dataset();

  /** Registers samples based on parameters that have been set. */
  void register_samples();

  /** Ingests samples based on parameters that have been set. */
  void ingest_samples();

  /** Set number of ingestion threads. */
  void set_num_threads(const unsigned threads);

  /** Set the total memory budget for ingestion (MiB) */
  void set_total_memory_budget_mb(const uint32_t total_memory_budget_mb);

  /** Set the percentage of total system memory used for ingestion
   * (overrides 'total_memory_budget_mb') */
  void set_total_memory_percentage(const float total_memory_percentage);

  /** Set the ratio of memory budget allocated to TileDB::sm.mem.total_budget */
  void set_ratio_tiledb_memory(const float ratio_tiledb_memory);

  /** Set the maximum memory allocated to TileDB::sm.mem.total_budget (MiB) */
  void set_max_tiledb_memory_mb(const uint32_t max_tiledb_memory_mb);

  /** Set the size of input record buffer for each sample file (MiB) */
  void set_input_record_buffer_mb(const uint32_t input_record_buffer_mb);

  /** Set the average VCF record size (bytes) */
  void set_avg_vcf_record_size(const uint32_t avg_vcf_record_size);

  /** Set the ratio of worker task size to computed task size */
  void set_ratio_task_size(const float ratio_task_size);

  /** Set the ratio of output buffer capacity that triggers a flush to TileDB */
  void set_ratio_output_flush(const float ratio_output_flush);

  /** Set the max length of an ingestion task. */
  void set_thread_task_size(const unsigned size);

  /** Set the max size of TileDB buffers before flushing. Defaults to 1GB. */
  void set_memory_budget(const unsigned mb);

  /** Set ingestion scatch space for ingestion or registration */
  void set_scratch_space(const std::string& path, uint64_t size);

  /** Set max number of VCF records to buffer per file */
  void set_record_limit(const uint64_t max_num_records);

  /**
   * Sets verbose mode on or off
   * @param verbose setting
   */
  void set_verbose(const bool& verbose);

  /** Enable tiledb stats */
  void set_tiledb_stats_enabled(bool stats_enabled);

  /** Returns if tiledb stats are enabled */
  void tiledb_stats_enabled(bool* enabled) const;

  /** Enable tiledb stats for the vcf header array */
  void set_tiledb_stats_enabled_vcf_header_array(bool stats_enabled);

  /** Returns if tiledb stats are enabled for the vcf header array */
  void tiledb_stats_enabled_vcf_header_array(bool* enabled) const;

  /** Fetches tiledb stats as a string */
  void tiledb_stats(char** stats);

  /** Gets the version number of the open dataset. */
  void dataset_version(int32_t* version) const;

  /** Set the sample batch size for storing. */
  void set_sample_batch_size(const uint64_t size);

  /** Set resume support for partial ingestion. */
  void set_resume_sample_partial_ingestion(const bool);

  /** Set contig fragment merging. */
  void set_contig_fragment_merging(const bool contig_fragment_merging);

  /** Set list of contigs to keep separate. */
  void set_contigs_to_keep_separate(
      const std::set<std::string>& contigs_to_keep_separate);

  /** Set list of contigs to allow to be merged. */
  void set_contigs_to_allow_merging(
      const std::set<std::string>& contigs_to_allow_merging);

  /** Set contig ingestion mode. */
  void set_contig_mode(int contig_mode);

  /**
    Enable the allele count ingestion task
  */
  void set_enable_allele_count(bool enable);

  /**
    Enable the variant stats ingestion task
  */
  void set_enable_variant_stats(bool enable);

 private:
  /* ********************************* */
  /*          PRIVATE ATTRIBUTES       */
  /* ********************************* */

  std::unique_ptr<Config> tiledb_config_;
  std::unique_ptr<Config> vfs_config_;
  std::shared_ptr<Context> ctx_;
  std::unique_ptr<VFS> vfs_;
  std::unique_ptr<Array> array_;
  std::unique_ptr<Query> query_;
  /** Handle on the dataset being written to. */
  std::unique_ptr<TileDBVCFDataset> dataset_;
  /** Vector of futures from async query finalizes. */
  std::vector<std::future<void>> finalize_tasks_;

  CreationParams creation_params_;
  RegistrationParams registration_params_;
  IngestionParams ingestion_params_;
  size_t total_records_expected_ = 0;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /**
   * Initializes the writer for storing to the given dataset. Opens the array,
   * creates the TileDB query, etc.
   *
   * @param dataset Dataset where samples will be stored
   * @param params Ingestion parameter
   */
  void init(const IngestionParams& params);

  /**
   * Prepares the samples list to be ingested. This combines the sample URI list
   * and file, sorts the samples by ID (row coord) and returns the resulting
   * list.
   */
  std::vector<SampleAndIndex> prepare_sample_list(
      const IngestionParams& params) const;

  /**
   * Prepares the samples list to be ingested. This combines the sample URI list
   * and file, sorts the samples by ID (row coord) and returns the resulting
   * list.
   */
  std::vector<SampleAndIndex> prepare_sample_list_v4(
      const IngestionParams& params) const;

  /**
   * Prepares a list of disjoint genomic regions that cover the whole genome.
   * This is used to split up work across the ingestion threads, and so the
   * returned list depends on the thread task size parameter.
   */
  std::vector<Region> prepare_region_list(const IngestionParams& params) const;

  /**
   * Prepares a list of disjoint genomic regions that cover the whole genome.
   * This is used to split up work across the ingestion threads, and so the
   * returned list depends on the contig task size.
   */
  std::vector<Region> prepare_region_list(
      const std::vector<Region>& all_contigs,
      const std::map<std::string, uint32_t>& contig_task_size) const;

  /**
   * Ingests a batch of samples.
   *
   * @param params Ingestion parameters
   * @param samples List of samples to ingest with this call
   * @param regions List of regions covering the whole genome
   * @return A pair (num_records_ingested, num_anchors_ingested)
   */
  std::pair<uint64_t, uint64_t> ingest_samples(
      const IngestionParams& params,
      const std::vector<SampleAndIndex>& samples,
      std::vector<Region>& regions);

  /**
   * Ingests a batch of samples.
   *
   * @param params Ingestion parameters
   * @param samples List of samples to ingest with this call
   * @param regions List of regions covering the whole genome
   * @return A pair (num_records_ingested, num_anchors_ingested)
   */
  std::pair<uint64_t, uint64_t> ingest_samples_v4(
      const IngestionParams& params,
      const std::vector<SampleAndIndex>& samples,
      std::vector<Region>& regions,
      std::unordered_map<
          std::pair<std::string, std::string>,
          std::vector<std::pair<std::string, std::string>>,
          pair_hash> map);

  static void finalize_query(std::unique_ptr<tiledb::Query> query);

  /**
   *
   * @param contig to check mergability on
   * @return true if contig is allowed to be merged based on whitelist/blacklist
   */
  bool check_contig_mergeable(const std::string& contig);

  /**
   * @brief Get the merged fragment index for the contig
   *
   * @param contig Contig name
   * @return int Merged fragment index
   */
  int get_merged_fragment_index(const std::string& contig);

  /**
   * @brief Write anchors to a TileDB fragment.
   *
   * @param worker Writer worker containing the anchors.
   */
  size_t write_anchors(WriterWorkerV4& worker);
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_WRITER_H
