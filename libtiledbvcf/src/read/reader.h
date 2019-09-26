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

#ifndef TILEDB_VCF_READER_H
#define TILEDB_VCF_READER_H

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
#include "enums/attr_datatype.h"
#include "enums/read_status.h"
#include "read/exporter.h"
#include "read/in_memory_exporter.h"
#include "read/read_query_results.h"

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

/** Arguments/params for export. */
struct ExportParams {
  // Basic export params:
  std::string uri;
  std::string samples_file_uri;
  std::string regions_file_uri;
  std::vector<std::string> sample_names;
  std::vector<std::string> regions;
  std::string output_dir;
  std::string upload_dir;
  std::string tsv_output_path;
  std::vector<std::string> tsv_fields;
  PartitionInfo sample_partitioning;
  PartitionInfo region_partitioning;
  ExportFormat format = ExportFormat::CompressedBCF;
  bool verbose = false;
  bool export_to_disk = false;
  bool cli_count_only = false;
  bool sort_regions = true;
  uint64_t max_num_records = std::numeric_limits<uint64_t>::max();
  std::vector<std::string> tiledb_config;

  // Memory/performance params:
  unsigned memory_budget_mb = 2 * 1024;
};

/* ********************************* */
/*              READER               */
/* ********************************* */

class Reader {
 public:
  /* ********************************* */
  /*            PUBLIC API             */
  /* ********************************* */

  Reader();

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

  /** Returns the read status of the last read operation. */
  ReadStatus read_status() const;

  /** Returns the number of records last exported. */
  uint64_t num_records_exported() const;

  /** Gets the version number of the open dataset. */
  void dataset_version(int32_t* version) const;

  /**
   * Returns the size of the result last exported for the given
   * attribute.
   */
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

  void get_buffer_values(
      int32_t buffer_idx, const char** name, void** data_buff) const;

  void get_buffer_offsets(
      int32_t buffer_idx, const char** name, int32_t** buff) const;

  void get_buffer_list_offsets(
      int32_t buffer_idx, const char** name, int32_t** buff) const;

  void get_buffer_validity_bitmap(
      int32_t buffer_idx, const char** name, uint8_t** buff) const;

 private:
  /* ********************************* */
  /*           PRIVATE DATATYPES       */
  /* ********************************* */

  struct QueryRegion {
    uint32_t col_min;
    uint32_t col_max;
  };

  struct ReadState {
    ReadStatus status = ReadStatus::UNINITIALIZED;
    uint32_t sample_min = 0;
    uint32_t sample_max = 0;
    std::vector<SampleAndId> current_samples;
    std::vector<SafeBCFHdr> current_hdrs;
    std::vector<uint32_t> last_reported_end;

    size_t region_idx = 0;
    std::vector<Region> regions;
    std::vector<QueryRegion> query_regions;

    size_t batch_idx = 0;
    std::vector<std::vector<SampleAndId>> sample_batches;

    uint64_t total_num_records_exported = 0;
    uint64_t last_num_records_exported = 0;

    std::unique_ptr<Array> array;
    std::unique_ptr<Query> query;
    ReadQueryResults query_results;
    std::future<tiledb::Query::Status> async_query;
    uint64_t cell_idx = 0;
  };

  /* ********************************* */
  /*          PRIVATE ATTRIBUTES       */
  /* ********************************* */

  ExportParams params_;
  std::unique_ptr<tiledb::Context> ctx_;
  std::unique_ptr<tiledb::VFS> vfs_;
  std::unique_ptr<TileDBVCFDataset> dataset_;
  std::unique_ptr<Exporter> exporter_;
  ReadState read_state_;
  std::unique_ptr<AttributeBufferSet> buffers_a;
  std::unique_ptr<AttributeBufferSet> buffers_b;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  InMemoryExporter* set_in_memory_exporter();

  bool next_read_batch();

  void init_for_reads();

  void init_exporter();

  bool read_current_batch();

  std::vector<std::vector<SampleAndId>> prepare_sample_batches();

  std::vector<SampleAndId> prepare_sample_names() const;

  void prepare_regions(
      std::vector<Region>* regions,
      std::vector<QueryRegion>* query_regions) const;

  void prepare_attribute_buffers();

  bool process_query_results();

  static std::pair<size_t, size_t> get_intersecting_regions(
      const std::vector<Region>& regions,
      size_t region_idx,
      uint32_t start,
      uint32_t end,
      uint32_t real_end,
      size_t* new_region_idx);

  /**
   * Reports (exports or copies into external buffers) the cell in the current
   * query results at the given index.
   */
  bool report_cell(
      const Region& region, uint32_t contig_offset, uint64_t cell_idx);

  /** Initializes the TileDB context and VFS instances. */
  void init_tiledb();

  /** Checks that the partitioning values are valid. */
  static void check_partitioning(
      uint64_t partition_idx, uint64_t num_partitions);
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_READER_H
