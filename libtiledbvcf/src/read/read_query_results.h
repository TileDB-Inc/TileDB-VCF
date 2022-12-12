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

#ifndef TILEDB_VCF_READ_QUERY_RESULTS_H
#define TILEDB_VCF_READ_QUERY_RESULTS_H

#include <string>
#include <unordered_map>
#include <vector>

#include "dataset/attribute_buffer_set.h"
#include "utils/buffer.h"

namespace tiledb {
namespace vcf {

/**
 * Helper class holding information about TileDB query results when reading
 * from a TileDBVCFDataset.
 */
struct ReadQueryResults {
 public:
  /** Constructor. */
  ReadQueryResults();

  /** Initializes this instance with the given query results. */
  void set_results(
      const TileDBVCFDataset& dataset,
      const AttributeBufferSet* buffers,
      const tiledb::Query& query);

  /** Returns a pointer to the set of buffers actually holding the data. */
  const AttributeBufferSet* buffers() const;

  /** Returns the number of cells in the query results. */
  uint64_t num_cells() const;

  /** Returns the query status. */
  tiledb::Query::Status query_status() const;

  /** Returns the size of the sample dimension results. */
  const std::pair<uint64_t, uint64_t>& sample_size() const;

  /** Returns the size of the contig dimension results. */
  const std::pair<uint64_t, uint64_t>& contig_size() const;

  /** Returns the size of the alleles attribute results. */
  const std::pair<uint64_t, uint64_t>& alleles_size() const;

  /** Returns the size of the id attribute results. */
  const std::pair<uint64_t, uint64_t>& id_size() const;

  /** Returns the size of the filter ids attribute results. */
  const std::pair<uint64_t, uint64_t>& filter_ids_size() const;

  /** Returns the size of the info attribute results. */
  const std::pair<uint64_t, uint64_t>& info_size() const;

  /** Returns the size of the fmt attribute results. */
  const std::pair<uint64_t, uint64_t>& fmt_size() const;

  /** Returns a map of the size of the "extra" attribute results. */
  const std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>&
  extra_attrs_size() const;

  /** TileDB Internal Allele Frequency values*/
  std::vector<float> af_values;

 private:
  /** Pointer to buffer set holding the actual data. */
  const AttributeBufferSet* buffers_;

  /** TileDB query status */
  tiledb::Query::Status query_status_;

  /** Number of cells in the query results */
  uint64_t num_cells_;

  std::pair<uint64_t, uint64_t> sample_size_;
  std::pair<uint64_t, uint64_t> contig_size_;
  std::pair<uint64_t, uint64_t> alleles_size_;
  std::pair<uint64_t, uint64_t> id_size_;
  std::pair<uint64_t, uint64_t> filter_ids_size_;
  std::pair<uint64_t, uint64_t> info_size_;
  std::pair<uint64_t, uint64_t> fmt_size_;
  std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>
      extra_attrs_size_;
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_READ_QUERY_RESULTS_H
