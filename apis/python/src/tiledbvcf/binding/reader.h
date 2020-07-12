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

#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <tiledbvcf/tiledbvcf.h>

#include <map>
#include <set>

namespace py = pybind11;

namespace tiledbvcfpy {

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
  void read();

  /**
   * Returns a map of attribute name -> (offsets_buff, data_buff) containing the
   * data from the last read operation.
   */
  std::map<std::string, std::pair<py::array, py::array>> get_buffers();

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

  /** Returns number of queryable fmt attributes */
  int32_t get_fmt_attribute_count();

  /** Returns fmt attribute name */
  std::string get_fmt_attribute_name(int32_t index);

  /** Returns number of queryable info attributes */
  int32_t get_info_attribute_count();

  /** Returns info attribute name */
  std::string get_info_attribute_name(int32_t index);

  /** Returns number of queryable attributes */
  int32_t get_queryable_attribute_count();

  /** Returns queryable attribute name */
  std::string get_queryable_attribute_name(int32_t index);

  /**
   * Set reader verbose output mode
   *
   * @param verbose mode
   */
  void set_verbose(bool verbose);

 private:
  /** Buffer struct to hold attribute data read from the dataset. */
  struct BufferInfo {
    /** Name of attribute. */
    std::string attr_name;
    /** Offsets buffer, for var-len attributes. */
    py::array offsets;
    /** List offsets buffer, for list var-len attributes. */
    py::array list_offsets;
    /** Data buffer. */
    py::array data;
    /** Null-value bitmap, for nullable attributes. */
    py::array bitmap;
  };

  /** Helper function to free a C reader instance */
  static void deleter(tiledb_vcf_reader_t* r);

  /** Convert the given datatype to a numpy dtype */
  static py::dtype to_numpy_dtype(tiledb_vcf_attr_datatype_t datatype);

  /** The underlying C reader object. */
  std::unique_ptr<tiledb_vcf_reader_t, decltype(&deleter)> ptr;

  /** The size (in MB) of the memory budget parameter. */
  int64_t mem_budget_mb_;

  /** The set of attribute names included in the read query. */
  std::vector<std::string> attributes_;

  /** List of attribute buffers. */
  std::vector<BufferInfo> buffers_;

  /** Allocate buffers for the read. */
  void alloc_buffers();

  /** Sets the allocated buffers on the reader object. */
  void set_buffers();

  /** Releases references on allocated buffers and clears the buffers list. */
  void release_buffers();
};

}  // namespace tiledbvcfpy
