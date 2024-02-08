/**
 * @file   reader.h
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

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <tiledbvcf/tiledbvcf.h>

#include "vcf_arrow.h"

namespace py = pybind11;

namespace tiledbvcfpy {

void config_logging(const std::string& level, const std::string& logfile);

/**
 * The Reader class is the main interface to the TileDB-VCF reader C API.
 */
class Reader {
 public:
  /** Constructor. */
  Reader();

  /** Initializes the reader for reading from the given dataset. */
  void init(const std::string& dataset_uri);

  /** Resets the reader so that another read can be done. */
  void reset();

  /** Sets the list of attributes that will be included in the read. */
  void set_attributes(const std::vector<std::string>& attributes);

  /** Sets a CSV list of samples to include in the read. */
  void set_samples(const std::string& samples);

  /** Sets a URI of a file containing sample names to include in the read. */
  void set_samples_file(const std::string& uri);

  /** Sets a CSV list of genomic regions to include in the read. */
  void set_regions(const std::string& regions);

  /** Sets a URI of a BED file containing regions to include in the read. */
  void set_bed_file(const std::string& uri);

  /** Sets a URI of a BED array containing regions to include in the read. */
  void set_bed_array(const std::string& uri);

  /** Sets the region partition of this reader. */
  void set_region_partition(int32_t partition, int32_t num_partitions);

  /** Sets the sample partition of this reader. */
  void set_sample_partition(int32_t partition, int32_t num_partitions);

  /** Sets the sort regions parameter of this reader. */
  void set_sort_regions(bool sort_regions);

  /** Sets the internal memory budget for the TileDB-VCF library. */
  void set_memory_budget(int32_t memory_mb);

  /** Sets the max number of records that will be read. */
  void set_max_num_records(int64_t max_num_records);

  /** Sets CSV TileDB config parameters. */
  void set_tiledb_config(const std::string& config_str);

  /** Sets whether internal TileDB Statistics are Enabled or Disabled*/
  void set_tiledb_stats_enabled(const bool stats_enabled);

  /** Performs a blocking read operation. */
  void read(const bool release_buffs = true);

  /**
   * Returns a PyArrow table containing the results from the last read
   * operation.
   */
  py::object get_results_arrow();

  /** Returns the number of records in the last read operation's results. */
  int64_t result_num_records();

  /** Returns true if the last read operation was complete. */
  bool completed();

  /** Gets whether internal TileDB Statistics are Enabled or Disabled*/
  bool get_tiledb_stats_enabled();

  /** Fetches TileDB statistics */
  std::string get_tiledb_stats();

  /** Returns schema version number of the TileDB VCF dataset */
  int32_t get_schema_version();

  /** Returns fmt attribute names */
  std::vector<std::string> get_fmt_attributes();

  /** Returns info attribute names */
  std::vector<std::string> get_info_attributes();

  /** Returns all queryable attribute names */
  std::vector<std::string> get_queryable_attributes();

  /** Returns all materialized attribute names */
  std::vector<std::string> get_materialized_attributes();

  /** Returns number of registered samples in the dataset */
  int32_t get_sample_count();

  /** Retrieve list of registered samples names */
  std::vector<std::string> get_sample_names();

  /** Get Stats Results **/
  py::object get_variant_stats_results();

  /** Get Allele Count Results **/
  py::object get_allele_count_results();

  /** Set reader verbose output mode */
  void set_verbose(bool verbose);

  /** Set export to disk mode */
  void set_export_to_disk(bool export_to_disk);

  /** Set export merge mode */
  void set_merge(bool merge);

  /** Set export output format */
  void set_output_format(const std::string& output_format);

  /** Set export output path */
  void set_output_path(const std::string& output_path);

  /** Set export output directory */
  void set_output_dir(const std::string& output_dir);

  /** Set internal allele frequency filtering expression */
  void set_af_filter(const std::string& af_filter);

  /** Set whether to scan all samples in the dataset when computing frequency */
  void set_scan_all_samples(const bool scan_all_samples);

  /** Set the TileDB query buffer memory percentage */
  void set_buffer_percentage(float buffer_percentage);

  /** Set the TileDB tile cache memory percentage */
  void set_tiledb_tile_cache_percentage(float tile_percentage);

  /** Set to check if samples requested exist and error if not. */
  void set_check_samples_exist(bool check_samples_exist);

  /** Get Version info for TileDB VCF and TileDB. */
  std::string version();

  /** Set disable progress estimation */
  void set_enable_progress_estimation(const bool& enable_progress_estimation);

  /** Set print vcf regions in verbose mode */
  void set_debug_print_vcf_regions(const bool& print_vcf_regions);

  /** Set print sample list in verbose mode */
  void set_debug_print_sample_list(const bool& print_sample_list);

  /** Set print TileDB query ranges in verbose mode */
  void set_debug_print_tiledb_query_ranges(const bool& tiledb_query_ranges);

 private:
  /** Helper function to free a C reader instance */
  static void deleter(tiledb_vcf_reader_t* r);

  /** The underlying C reader object. */
  std::unique_ptr<tiledb_vcf_reader_t, decltype(&deleter)> ptr;

  /** The size (in MB) of the memory budget parameter. */
  int64_t mem_budget_mb_;

  /** The set of attribute names included in the read query. */
  std::vector<std::string> attributes_;

  /** List of attribute buffers. */
  std::vector<std::shared_ptr<BufferInfo>> buffers_;

  /** Allocate buffers for the read. */
  void alloc_buffers(const bool release_buffs = true);

  /** Sets the allocated buffers on the reader object. */
  void set_buffers();

  /** Releases references on allocated buffers and clears the buffers list. */
  void release_buffers();
};
}  // namespace tiledbvcfpy
