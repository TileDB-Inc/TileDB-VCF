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
  ReadQueryResults();

  void set_results(
      const TileDBVCFDataset& dataset,
      const AttributeBufferSet* buffers,
      const tiledb::Query& query);

  const AttributeBufferSet* buffers() const;

  uint64_t num_cells() const;

  tiledb::Query::Status query_status() const;

  const std::pair<uint64_t, uint64_t>& alleles_size() const;
  const std::pair<uint64_t, uint64_t>& id_size() const;
  const std::pair<uint64_t, uint64_t>& filter_ids_size() const;
  const std::pair<uint64_t, uint64_t>& info_size() const;
  const std::pair<uint64_t, uint64_t>& fmt_size() const;
  const std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>&
  extra_attrs_size() const;

 private:
  const AttributeBufferSet* buffers_;
  tiledb::Query::Status query_status_;
  uint64_t num_cells_;
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
