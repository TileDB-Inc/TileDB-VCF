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

  /** Sets the allocation size for the Python attribute buffers. */
  void set_buffer_alloc_size(int64_t nbytes);

  /** Sets a CSV list of samples to include in the read. */
  void set_samples(const std::string& samples);

  /** Sets a CSV list of genomic regions to include in the read. */
  void set_regions(const std::string& regions);

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

 private:
  /** Buffer pair to hold attribute data read from the dataset. */
  struct BufferPair {
    /** Offsets buffer, for var-len attributes. */
    py::array offsets;
    /** Data buffer. */
    py::array data;
  };

  /** Helper function to free a C reader instance */
  static void deleter(tiledb_vcf_reader_t* r);

  /** Convert the given datatype to a numpy dtype */
  static py::dtype to_numpy_dtype(tiledb_vcf_attr_datatype_t datatype);

  /** The underlying C reader object. */
  std::unique_ptr<tiledb_vcf_reader_t, decltype(&deleter)> ptr;

  /** The size (in bytes) to use for Python buffer allocations. */
  int64_t alloc_size_bytes_;

  /** The set of attribute names included in the read query. */
  std::set<std::string> attributes_;

  /** Map of attribute -> Python buffer pair. */
  std::map<std::string, BufferPair> buffers_;

  /** Allocate buffers for the read. */
  void alloc_buffers();

  /** Sets the allocated buffers on the reader object. */
  void set_buffers();

  /** Resizes the Python result buffers according to the number of results. */
  void prepare_result_buffers();
};

}  // namespace tiledbvcfpy