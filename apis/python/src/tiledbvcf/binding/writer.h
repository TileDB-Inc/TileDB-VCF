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

  /** Sets a CSV list of samples be registered or ingested. */
  void set_samples(const std::string& samples);

  /**
   * [Creation only] Sets the info and fmt fields that should be extracted as
   * separate TileDB attributes. Expects a CSV string.
   */
  void set_extra_attributes(const std::string& attributes);

  /**
    [Creation only] Sets the checksum type to be used of the arrays
  */
  void set_checksum(const std::string& checksum);

  /**
    [Creation only] Sets whether duplicates are allowed in the array
  */
  void set_allow_duplicates(const bool& allow_duplicates);

  void create_dataset();

  void register_samples();

  void ingest_samples();

 private:
  /** Helper function to free a C writer instance */
  static void deleter(tiledb_vcf_writer_t* w);

  /** The underlying C writer object. */
  std::unique_ptr<tiledb_vcf_writer_t, decltype(&deleter)> ptr;
};

}  // namespace tiledbvcfpy
