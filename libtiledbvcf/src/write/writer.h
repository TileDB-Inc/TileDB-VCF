/**
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2019 TileDB, Inc.
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
#include "vcf/htslib_value.h"

namespace tiledb {
namespace vcf {

/* ********************************* */
/*       AUXILIARY DATATYPES         */
/* ********************************* */

/** Arguments/params for dataset ingestion. */
struct IngestionParams {
  std::string uri;
  std::string samples_file_uri;
  std::vector<std::string> sample_uris;
  unsigned num_threads = std::thread::hardware_concurrency();
  unsigned part_size_mb = 50;
  bool verbose = false;
  ScratchSpaceInfo scratch_space;
  bool remove_samples_file = false;
  // Max number of VCF records to read into memory
  uint64_t max_record_buffer_size = 50000;
  std::vector<std::string> tiledb_config;
  bool tiledb_stats_enabled = false;
  bool tiledb_stats_enabled_vcf_header_array = false;

  /**
   * Max length (# columns) of an ingestion "task". This value is derived
   * empirically to optimize ingestion performance. It affects available
   * parallelism as well as load balancing of ingestion work across threads.
   */
  unsigned thread_task_size = 5000000;

  // Max size of TileDB buffers before flushing. Defaults to 1GB
  uint64_t max_tiledb_buffer_size_mb = 1024;

  // Number of samples per batch for ingestion
  uint64_t sample_batch_size = 10;
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

  /** Set ingestion scatch space for ingestion or registration */
  void set_scratch_space(const std::string& path, uint64_t size);

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

 private:
  /* ********************************* */
  /*          PRIVATE ATTRIBUTES       */
  /* ********************************* */

  std::unique_ptr<Config> tiledb_config_;
  std::unique_ptr<Context> ctx_;
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
   * returned list depends on the thread task size parameter.
   */
  std::vector<Region> prepare_region_list(
      const std::vector<Region>& all_contigs,
      const IngestionParams& params) const;

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
      std::vector<Region>& regions);

  static void finalize_query(std::unique_ptr<tiledb::Query> query);
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_WRITER_H
