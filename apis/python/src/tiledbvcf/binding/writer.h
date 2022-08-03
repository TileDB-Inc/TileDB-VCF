/**
 * @file   writer.h
 *
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

#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <tiledbvcf/tiledbvcf.h>

#include <map>
#include <set>

namespace py = pybind11;

namespace tiledbvcfpy {

/**
 * The Writer class is the main interface to the TileDB-VCF writer C API.
 */
class Writer {
 public:
  /** Constructor. */
  Writer();

  /** Initializes the writer for creating or writing to the given dataset. */
  void init(const std::string& dataset_uri);

  /** Enables or disables internal TileDB statistics. */
  void set_tiledb_stats_enabled(const bool stats_enabled);

  /** Sets a CSV list of samples be registered or ingested. */
  void set_samples(const std::string& samples);

  /**
   * [Creation only] Sets the info and fmt fields that should be extracted as
   * separate TileDB attributes. Expects a CSV string.
   */
  void set_extra_attributes(const std::string& attributes);

  /**
   * [Creation only] Sets the info and fmt fields that should be extracted as
   * separate TileDB attributes using all fields in the provided VCF file.
   */
  void set_vcf_attributes(const std::string& vcf_uri);

  /**
    [Creation only] Sets the checksum type to be used of the arrays
  */
  void set_checksum(const std::string& checksum);

  /**
    [Creation only] Sets whether duplicates are allowed in the array
  */
  void set_allow_duplicates(const bool& allow_duplicates);

  /**
     [Creation only] Sets the data array's tile capacity
   */
  void set_tile_capacity(const uint64_t tile_capacity);

  /**
     [Creation only] Set the length of gaps between inserted anchor records.
   */
  void set_anchor_gap(const uint32_t anchor_gap);

  /**
    [Store only] Set the number of threads used for ingestion.
  */
  void set_num_threads(const uint32_t threads);

  /**
    [Store only] Set the total memory budget for ingestion (MiB)
  */
  void set_total_memory_budget_mb(const uint32_t total_memory_budget_mb);

  /**
    [Store only] Set the percentage of total system memory used for ingestion
    (overrides 'total_memory_budget_mb')
  */
  void set_total_memory_percentage(const float total_memory_percentage);

  /**
    [Store only] Set the ratio of memory budget allocated to
    TileDB::sm.mem.total_budget
  */
  void set_ratio_tiledb_memory(const float ratio_tiledb_memory);

  /**
    [Store only] Set the maximum memory allocated to TileDB::sm.mem.total_budget
    (MiB)
  */
  void set_max_tiledb_memory_mb(const uint32_t max_tiledb_memory_mb);

  /**
    [Store only] Set the size of input record buffer for each sample file (MiB)
  */
  void set_input_record_buffer_mb(const uint32_t input_record_buffer_mb);

  /**
    [Store only] Set the average VCF record size (bytes)
  */
  void set_avg_vcf_record_size(const uint32_t avg_vcf_record_size);

  /**
    [Store only] Set the ratio of worker task size to computed task size
  */
  void set_ratio_task_size(const float ratio_task_size);

  /**
    [Store only] Set the ratio of output buffer capacity that triggers a flush
    to TileDB
  */
  void set_ratio_output_flush(const float ratio_output_flush);

  /**
    [Store only] Set the max size of an ingestion task.
  */
  void set_thread_task_size(const uint32_t size);

  /**
    [Store only] Set the max size of TileDB buffers before flushing.
  */
  void set_memory_budget(const uint32_t memory_mb);

  /**
    [Store only] Allocates scratch space for downloading sample files
  */
  void set_scratch_space(const std::string& path, uint64_t size);

  /**
    [Store only] Limits the number of VCF records to buffer per file
  */
  void set_max_num_records(const uint64_t max_num_records);

  void create_dataset();

  void register_samples();

  void ingest_samples();

  /** Returns schema version number of the TileDB VCF dataset */
  int32_t get_schema_version();

  /**
   * Set writer verbose output mode
   *
   * @param verbose mode
   */
  void set_verbose(bool verbose);

  /** Sets CSV TileDB config parameters. */
  void set_tiledb_config(const std::string& config_str);

  /**
    [Store only] Sets the number of samples per batch for ingestion
  */
  void set_sample_batch_size(const uint64_t size);

  /**
    [Store only] Checks whether internal TileDB Statistics are enabled
  */
  bool get_tiledb_stats_enabled();

  /**
    Fetches internal TileDB statistics
  */
  std::string get_tiledb_stats();

  /** Get Version info for TileDB VCF and TileDB. */
  std::string version();

  /**
    [Store only] Sets whether to enable resuming partial sample ingestion
  */
  void set_resume(const bool resume);

  /**
    [Store only] Sets whether to enable merging of contigs into super fragments
  */
  void set_contig_fragment_merging(const bool contig_fragment_merging);

  /**
    [Store only] Set list of contigs to keep separate and not merge
  */
  void set_contigs_to_keep_separate(
      const std::vector<std::string>& contigs_to_keep_separate);

  /**
    [Store only] Set list of contigs to allow merging into super fragments
  */
  void set_contigs_to_allow_merging(
      const std::vector<std::string>& contigs_to_allow_merging);

  /**
    [Store only] Set contig ingestion mode
  */
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
  /** Helper function to free a C writer instance */
  static void deleter(tiledb_vcf_writer_t* w);

  /** The underlying C writer object. */
  std::unique_ptr<tiledb_vcf_writer_t, decltype(&deleter)> ptr;
};

}  // namespace tiledbvcfpy
