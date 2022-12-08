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

#ifndef TILEDB_VCF_READER_H
#define TILEDB_VCF_READER_H

#include <future>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <htslib/vcf.h>
#include <tiledb/tiledb>

#include "dataset/attribute_buffer_set.h"
#include "dataset/tiledbvcfdataset.h"
#include "enums/attr_datatype.h"
#include "enums/read_status.h"
#include "read/exporter.h"
#include "read/in_memory_exporter.h"
#include "read/read_query_results.h"
#include "stats/variant_stats_reader.h"

namespace tiledb {
namespace vcf {

/* ********************************* */
/*       AUXILIARY DATATYPES         */
/* ********************************* */

/** Pair of values used for partitioning. */
struct PartitionInfo {
  unsigned partition_index = 0;
  unsigned num_partitions = 1;
};

struct MemoryBudgetBreakdown {
  uint64_t buffers = 1024;
  uint64_t tiledb_tile_cache = 1024;
  uint64_t tiledb_memory_budget = 1024;
  float buffers_percentage = 25;
  float tile_cache_percentage = 10;
};

struct DebugParams {
  // Print out tiledb query range
  bool print_tiledb_query_ranges = false;

  // Print regions from bedfile or user passed
  bool print_vcf_regions = false;

  // Print user set sample list
  bool print_sample_list = false;
};

/** Arguments/params for export. */
struct ExportParams {
  // Basic export params:
  std::string uri;
  std::string log_level;
  std::string log_file;
  std::string samples_file_uri;
  std::string regions_file_uri;
  std::vector<std::string> sample_names;
  std::vector<std::string> regions;
  std::string output_dir;
  std::string upload_dir;
  std::string output_path;
  std::vector<std::string> tsv_fields;
  PartitionInfo sample_partitioning;
  PartitionInfo region_partitioning;
  ExportFormat format = ExportFormat::CompressedBCF;
  bool verbose = false;
  bool export_to_disk = false;
  bool export_combined_vcf = false;
  bool cli_count_only = false;
  bool sort_regions = true;
  uint64_t max_num_records = std::numeric_limits<uint64_t>::max();
  std::vector<std::string> tiledb_config;
  std::unordered_map<std::string, std::string> tiledb_config_map;

  bool tiledb_stats_enabled = false;
  bool tiledb_stats_enabled_vcf_header_array = false;

  // Memory/performance params:
  uint64_t memory_budget_mb = 2 * 1024;
  MemoryBudgetBreakdown memory_budget_breakdown;

  // Should we check that the sample names passed for export exist in the array
  // and error out if not This can add latency which might not be cared about
  // because we have to fetch the list of samples from the VCF header array
  bool check_samples_exist = true;

  // Should we skip trying to estimate the number of records and percent
  // complete? This is useful when you want verbose but not the performance
  // impact.
  bool enable_progress_estimation = false;

  // Debug parameters for optional debug information
  struct DebugParams debug_params;

  // Minimum super region size, used to handle overlapping regions.
  // Recommed setting to 1 to create smaller super regions and reduce the
  // time to find the first intersecting region. Increase the setting if
  // the memory overhead of super regions needs to be reduced.
  int min_super_region_size = 1;

  // Should results be sorted on real_start_pos
  bool sort_real_start_pos = false;

  // AF filter with the format "OP VALUE"
  //   where OP = < | <= | > | >= | == | !=
  //   and VALUE = float
  // If empty, AF filtering is not applied
  std::string af_filter = "";
};

/* ********************************* */
/*              READER               */
/* ********************************* */

/**
 * The Reader class exposes an interface for performing exports from a
 * TileDB-VCF dataset.
 *
 * The same algorithm is used regardless of whether we are exporting to disk
 * or to a set of in-memory buffers (via the C API). The interface also allows
 * for partitioning the export either across samples, or regions, or both.
 */
class Reader {
 public:
  /* ********************************* */
  /*            PUBLIC API             */
  /* ********************************* */

  /** Constructor. */
  Reader();

  /** Destructor. */
  ~Reader();

  /** Unimplemented rule-of-5. */
  Reader(Reader&&) = delete;
  Reader(const Reader&) = delete;
  Reader& operator=(Reader&&) = delete;
  Reader& operator=(const Reader&) = delete;

  /** Initializes the reader for reading from the given dataset. */
  void open_dataset(const std::string& dataset_uri);

  /** Performs a blocking read operation. */
  void read();

  /**
   * Resets the read state (but not the parameters), allowing another read
   * operation to occur without reopening the dataset.
   */
  void reset();

  /** Reset user buffers. Used to reuse a reader but for different attributes.
   */
  void reset_buffers();

  /** Convenience function to set all parameters from the given struct. */
  void set_all_params(const ExportParams& params);

  /** Sets the sample names list parameter. */
  void set_samples(const std::string& samples);

  /** Sets the regions list parameter. */
  void set_regions(const std::string& regions);

  /** Sets the sort regionsparameter. */
  void set_sort_regions(bool sort_regions);

  /** Sets the samples file URI parameter. */
  void set_samples_file(const std::string& uri);

  /** Sets the BED file URI parameter. */
  void set_bed_file(const std::string& uri);

  /** Sets the region partitioning. */
  void set_region_partition(uint64_t partition_idx, uint64_t num_partitions);

  /** Sets the sample partitioning. */
  void set_sample_partition(uint64_t partition_idx, uint64_t num_partitions);

  /** Sets the values buffer pointer and size (in bytes) for an attribute. */
  void set_buffer_values(
      const std::string& attribute, void* buff, int64_t buff_size);

  /** Sets the offsets buffer pointer and size (in bytes) for an attribute. */
  void set_buffer_offsets(
      const std::string& attribute, int32_t* buff, int64_t buff_size);

  /**
   * Sets the list offsets buffer pointer and size (in bytes) for an attribute.
   */
  void set_buffer_list_offsets(
      const std::string& attribute, int32_t* buff, int64_t buff_size);

  /** Sets the bitmap buffer pointer and size (in bytes) for an attribute. */
  void set_buffer_validity_bitmap(
      const std::string& attribute, uint8_t* buff, int64_t buff_size);

  /**
   * Sets the memory budget parameter.
   *
   * The memory budget is split 50/50 between TileDB's internal memory budget,
   * and our Reader query buffers. For the Reader query buffers, we allocate
   * two sets (due to double-buffering). That means the allocation size of the
   * query buffers *per attribute* is:
   *
   *   ((mem_budget / 2) / num_query_buffers) / 2.
   *
   * Example: Suppose you want to target a query buffer size of 100MB per
   * attribute. Suppose the query needs 3 fixed-len attributes and 2 var-len.
   * That is a total of 3 + 4 = 7 query buffers that need to be allocated. So:
   *
   *   required_mem_budget = 100 * 2 * 7 * 2 = 2800 MB
   */
  void set_memory_budget(unsigned mb);

  /** Sets the attribute buffer size parameter. */
  void set_record_limit(uint64_t max_num_records);

  /** Sets TileDB config parameters. */
  void set_tiledb_config(const std::string& config_str);

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

  /** Returns the read status of the last read operation. */
  ReadStatus read_status() const;

  /** Returns the number of records last exported. */
  uint64_t num_records_exported() const;

  /** Gets the version number of the open dataset. */
  void dataset_version(int32_t* version) const;

  /** Retrieve list of sample names. */
  void get_samples(std::string* samples) const;

  /** Returns the size of the result last exported for the given attribute. */
  void result_size(
      const std::string& attribute,
      int64_t* num_offsets,
      int64_t* num_data_elements,
      int64_t* num_data_bytes) const;

  /**
   * Returns the datatype, var-length, and nullable setting of the given
   * attribute.
   */
  void attribute_datatype(
      const std::string& attribute,
      AttrDatatype* datatype,
      bool* var_len,
      bool* nullable,
      bool* list) const;

  /** Returns the number of in-memory user buffers that have been set. */
  void num_buffers(int32_t* num_buffers) const;

  /**
   * Gets the name and pointer of the attribute data buffer previously set. This
   * is for in-memory export only.
   */
  void get_buffer_values(
      int32_t buffer_idx, const char** name, void** data_buff) const;

  /**
   * Gets the name and pointer of the attribute offsets buffer previously set.
   * This is for in-memory export only.
   */
  void get_buffer_offsets(
      int32_t buffer_idx, const char** name, int32_t** buff) const;

  /**
   * Gets the name and pointer of the attribute list offsets buffer previously
   * set. This is for in-memory export only.
   */
  void get_buffer_list_offsets(
      int32_t buffer_idx, const char** name, int32_t** buff) const;

  /**
   * Gets the name and pointer of the attribute validity buffer previously set.
   * This is for in-memory export only.
   */
  void get_buffer_validity_bitmap(
      int32_t buffer_idx, const char** name, uint8_t** buff) const;

  /**
   * Get the count of queryable attributes
   * @param count of attributes
   */
  void queryable_attribute_count(int32_t* count);

  /**
   * Get a queryable attribute name by index
   * @param index of attribute to fetch
   * @param name of attribute
   */
  void queryable_attribute_name(int32_t index, char** name);

  /**
   * Get the count of materialized attributes
   * @param count of attributes
   */
  void materialized_attribute_count(int32_t* count);

  /**
   * Get a materialized attribute name by index
   * @param index of attribute to fetch
   * @param name of attribute
   */
  void materialized_attribute_name(int32_t index, char** name);

  /**
   * Get the count of fmt attributes
   * @param count of attributes
   */
  void fmt_attribute_count(int32_t* count);

  /**
   * Get a fmt attribute name by index
   * @param index of attribute to fetch
   * @param name of attribute
   */
  void fmt_attribute_name(int32_t index, char** name);

  /**
   * Get the count of info attributes
   * @param count of attributes
   */
  void info_attribute_count(int32_t* count);

  /**
   * Get an info attribute name by index
   * @param index of attribute to fetch
   * @param name of attribute
   */
  void info_attribute_name(int32_t index, char** name);

  /**
   * Get the number of registered samples
   * @param sample count
   */
  void sample_count(int32_t* count);

  /**
   * Retrieve sample name by index
   * @param index of sample
   * @param name of sample
   */
  void sample_name(int32_t index, const char** name);

  /**
   * Sets verbose mode on or off
   * @param verbose setting
   */
  void set_verbose(const bool& verbose);

  /**
   * Sets export to disk mode on or off
   * @param export_to_disk setting
   */
  void set_export_to_disk(const bool export_to_disk);

  /**
   * Sets combine VCF mode on or off
   * @param merge setting
   */
  void set_merge(const bool merge);

  /**
   * Sets export output format
   * @param output_format setting
   */
  void set_output_format(const std::string& output_format);

  /**
   * Sets export output path
   * @param output_path setting
   */
  void set_output_path(const std::string& output_path);

  /**
   * Sets export output directory
   * @param output_dir setting
   */
  void set_output_dir(const std::string& output_dir);

  /**
   * returns whether the AF filter is set or enabled
   */
  bool af_filter_enabled();

  /**
   * sets AF filter expression
   * @param af_filter setting
   */
  void set_af_filter(const std::string& af_filter);

  /**
   * Sets disabling of progress estimation in verbose mode
   * @param enable_progress_estimation setting
   */
  void set_enable_progress_estimation(const bool& enable_progress_estimation);

  /**
   * Percentage of buffer size to tiledb memory budget
   * @param buffer_percentage
   */
  void set_buffer_percentage(const float& buffer_percentage);

  /**
   * Percentage of tiledb tile cache size to overall memory budget
   * @param tile_cache_percentage
   */
  void set_tiledb_tile_cache_percentage(const float& tile_cache_percentage);

  /**
   * Set if the list of user passed samples should be validated to exist before
   * running the query
   * @param check_samples_exist
   */
  void set_check_samples_exist(const bool check_samples_exist);

  /**
   * Set if vcf regions should be printed in verbose mode
   * @param print_vcf_regions
   */
  void set_debug_print_vcf_regions(bool print_vcf_regions);

  /**
   * Set if sample list should be printed in verbose mode
   * @param print_sample_list
   */
  void set_debug_print_sample_list(bool print_sample_list);

  /**
   * Set if TileDB query ranges should be printed in verbose mode
   * @param print_tiledb_query_ranges
   */
  void set_debug_print_tiledb_query_ranges(bool print_tiledb_query_ranges);

 private:
  /* ********************************* */
  /*           PRIVATE DATATYPES       */
  /* ********************************* */

  /** Helper struct containing a column range being queried. */
  struct QueryRegion {
    uint32_t col_min;
    uint32_t col_max;
    std::string contig;
  };

  /** Helper struct containing stats for a group of regions */
  struct SuperRegion {
    size_t region_min;  // minimum region index
    uint32_t end_max;   // maximum end position
  };

  /**
   * Structure holding all of the state for the current read operation. The read
   * state tracks all of the information that is required to implement
   * incomplete queries (e.g. if an in-memory export runs out of space when
   * copying to user buffers).
   */
  struct ReadState {
    /** Status of the current read operation. */
    ReadStatus status = ReadStatus::UNINITIALIZED;

    /** Lower sample ID of the sample (row) range currently being queried. */
    uint32_t sample_min = 0;

    /** Upper sample ID of the sample (row) range currently being queried. */
    uint32_t sample_max = 0;

    /** Map of current relative sample ID -> sample name. Only used for v2/v3 */
    std::unordered_map<uint32_t, SampleAndId> current_samples;

    /** Map of current relative sample ID -> VCF header instance. */
    std::unordered_map<uint32_t, SafeBCFHdr> current_hdrs;

    std::unordered_map<std::string, size_t> current_hdrs_lookup;

    /**
     * Stores the index to a region that was unsuccessfully reported
     * in the last read.
     */
    size_t last_intersecting_region_idx_ = 0;

    /**
     * Current index into the `regions` vector, used for finding intersecting
     * regions efficiently.
     */
    size_t region_idx = 0;

    /** The original genomic regions specified by the user to export. */
    std::vector<Region> regions;

    /**
     * Contains merged regions per contig to support overlapping regions.
     * If the super_region vector does not exist for a contig, there are no
     * overlapping regions in that contig.
     */
    std::unordered_map<std::string, std::vector<SuperRegion>> super_regions;

    /** Store index positions to only compare again regions for a contig */
    std::unordered_map<std::string, std::vector<size_t>>
        regions_index_per_contig;

    /**
     * The corresponding widened and merged regions that will be used as the
     * column ranges in the TileDB query.
     */
    std::vector<QueryRegion> query_regions;
    std::vector<std::pair<std::string, std::vector<QueryRegion>>>
        query_regions_v4;

    /** The index of the current batch of samples being exported. */
    size_t batch_idx = 0;

    /** The index of the current batch of samples being exported. */
    size_t query_contig_batch_idx = 0;

    /** The samples being exported, batched by space tile. */
    std::vector<std::vector<SampleAndId>> sample_batches;

    /** current sample batch list */
    std::vector<SampleAndId> current_sample_batches;

    /** Total number of records exported across all incomplete reads. */
    uint64_t total_num_records_exported = 0;

    /** Estimated number of records for query. */
    uint64_t query_estimated_num_records = 0;
    uint64_t total_query_records_processed = 0;

    /** The number of records exported during the last read, complete or not. */
    uint64_t last_num_records_exported = 0;

    /** Underlying TileDB array. */
    std::shared_ptr<Array> array;

    /** TileDB query object. */
    std::unique_ptr<Query> query;

    /** Struct containing query results from last TileDB query. */
    ReadQueryResults query_results;

    /**
     * Current index of cell being processed in query results. Used to support
     * resuming incomplete reads.
     */
    uint64_t cell_idx = 0;

    /** indicates if the user is querying all samples in the array, this cause
     * some special case optimizations. */
    bool all_samples = false;

    /** Does the export need headers to be fetched. */
    bool need_headers = false;
  };

  /* ********************************* */
  /*          PRIVATE ATTRIBUTES       */
  /* ********************************* */

  /** Parameters controlling the export. */
  ExportParams params_;

  /** TileDB context. */
  std::shared_ptr<tiledb::Context> ctx_;

  /** TileDB VFS instance. */
  std::unique_ptr<tiledb::VFS> vfs_;

  /** Handle on the dataset being exported from. */
  std::unique_ptr<TileDBVCFDataset> dataset_;

  /** Exporter instance (BCF, TSV, in-mem, etc). May be null. */
  std::unique_ptr<Exporter> exporter_;

  /** The read state. */
  ReadState read_state_;

  /** Set of attribute buffers holding TileDB query results. */
  std::unique_ptr<AttributeBufferSet> buffers_a;

  /** Variant stats filter */
  std::unique_ptr<VariantStatsReader> af_filter_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /** Ensures that a VariantStatsReader be available if applicable. */
  void init_af_filter();

  /** Swaps any existing exporter with an InMemoryExporter, and returns it. */
  InMemoryExporter* set_in_memory_exporter();

  /**
   * Starts the next read batch (i.e. next space tile), initializing the read
   * state. Returns true if there was a next batch, or false if there were no
   * more batches.
   */
  bool next_read_batch();
  bool next_read_batch_v2_v3();
  bool next_read_batch_v4();

  /**
   * Runs the TileDB-VCF read algorithm for the current batch. Returns false if,
   * during in-memory export, a user buffer filled up (which means it was an
   * incomplete read operation). Else, returns true.
   */
  bool read_current_batch();

  /** Initializes the batches and exporter before the first read. */
  void init_for_reads();
  void init_for_reads_v2();
  void init_for_reads_v3();
  void init_for_reads_v4();

  /** Initializes the exporter before the first read. */
  void init_exporter();

  /**
   * Prepares the batches (per space tile) of samples to be exported. This
   * merges the list of sample names with the contents of the samples file,
   * sorts by sample ID, and batches by space tile.
   */
  std::vector<std::vector<SampleAndId>> prepare_sample_batches() const;

  /**
   * Prepares sample batches for export
   * @param all_samples Boolean which is set as ouput to indicated if all
   * samples are being queried by the user or not
   * @return vector of samples
   */
  std::vector<std::vector<SampleAndId>> prepare_sample_batches_v4(
      bool* all_samples) const;

  /** Merges the list of sample names with the contents of the samples file. */
  std::vector<SampleAndId> prepare_sample_names() const;

  /** Merges the list of sample names with the contents of the samples file for
   * v4 arrays.
   *
   * @param all_samples Boolean which is set as ouput to indicated if all
   * samples are being queried by the user or not
   * @return vector of samples
   */
  std::vector<SampleAndId> prepare_sample_names_v4(bool* all_samples) const;

  /**
   * Prepares the regions to be queried and exported. This merges the list of
   * regions with the contents of the regions file, sorts, and performs the
   * anchor gap widening and merging process.
   */
  void prepare_regions_v4(
      std::vector<Region>* regions,
      std::unordered_map<std::string, std::vector<size_t>>*
          regions_index_per_contig,
      std::vector<std::pair<std::string, std::vector<QueryRegion>>>*
          query_regions);

  /**
   * Prepares the regions to be queried and exported. This merges the list of
   * regions with the contents of the regions file, sorts, and performs the
   * anchor gap widening and merging process.
   */
  void prepare_regions_v3(
      std::vector<Region>* regions,
      std::vector<QueryRegion>* query_regions) const;

  /**
   * Prepares the regions to be queried and exported. This merges the list of
   * regions with the contents of the regions file, sorts, and performs the
   * anchor gap widening and merging process.
   */
  void prepare_regions_v2(
      std::vector<Region>* regions,
      std::vector<QueryRegion>* query_regions) const;

  /** Allocates required attribute buffers to receive TileDB query data. */
  void prepare_attribute_buffers();

  /**
   * Search the regions vector for the first region that can intersect with
   * the real_start position. If a region is found, modify first_region and
   * return true. Otherwise, return false.
   */
  bool first_intersecting_region(
      const std::string& contig, uint32_t real_start, size_t& first_region);

  /**
   * Processes the result cells from the last TileDB query. Returns false if,
   * during in-memory export, a user buffer filled up (which means it was an
   * incomplete read operation). Else, returns true.
   */
  bool process_query_results_v4();

  /**
   * Processes the result cells from the last TileDB query. Returns false if,
   * during in-memory export, a user buffer filled up (which means it was an
   * incomplete read operation). Else, returns true.
   */
  bool process_query_results_v3();

  /**
   * Processes the result cells from the last TileDB query. Returns false if,
   * during in-memory export, a user buffer filled up (which means it was an
   * incomplete read operation). Else, returns true.
   */
  bool process_query_results_v2();

  /**
   * Reports (exports or copies into external buffers) the cell in the current
   * query results at the given index. Returns false if, during in-memory
   * export, a user buffer filled up (which means it was an incomplete read
   * operation). Else, returns true.
   */
  bool report_cell(
      const Region& region, uint32_t contig_offset, uint64_t cell_idx);

  /** Initializes the TileDB context and VFS instances. */
  void init_tiledb();

  /** Checks that the partitioning values are valid. */
  static void check_partitioning(
      uint64_t partition_idx, uint64_t num_partitions);

  /**
   * Builds and sets a TileDB config for the query
   *
   * Currently used for setting things like the `sm.memory_budget` and
   * `sm.memory_buget_var`
   */
  void set_tiledb_query_config();

  void compute_memory_budget_details();
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_READER_H
