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

#include "read/read_query_results.h"

namespace tiledb {
namespace vcf {

ReadQueryResults::ReadQueryResults()
    : buffers_(nullptr)
    , num_cells_(0)
    , sample_size_(0, 0)
    , contig_size_(0, 0) {
}

void ReadQueryResults::set_results(
    const TileDBVCFDataset& dataset,
    const AttributeBufferSet* buffers,
    const tiledb::Query& query) {
  buffers_ = buffers;
  query_status_ = query.query_status();

  auto result_el = query.result_buffer_elements();

  if (dataset.metadata().version == TileDBVCFDataset::Version::V4)
    num_cells_ = result_el["sample"].first;
  else
    num_cells_ = result_el["sample"].second;
  // Set config sizes for v4
  if (dataset.metadata().version == TileDBVCFDataset::Version::V4) {
    contig_size_ = result_el["contig"];
    sample_size_ = result_el["sample"];
  }

  alleles_size_ = result_el["alleles"];
  id_size_ = result_el["id"];
  filter_ids_size_ = result_el["filter_ids"];
  info_size_ = result_el["info"];
  fmt_size_ = result_el["fmt"];

  extra_attrs_size_.clear();
  for (const auto& attr : dataset.metadata().extra_attributes)
    extra_attrs_size_[attr] = result_el[attr];
}

tiledb::Query::Status ReadQueryResults::query_status() const {
  return query_status_;
}

const AttributeBufferSet* ReadQueryResults::buffers() const {
  return buffers_;
}

uint64_t ReadQueryResults::num_cells() const {
  return num_cells_;
}

const std::pair<uint64_t, uint64_t>& ReadQueryResults::sample_size() const {
  return sample_size_;
}

const std::pair<uint64_t, uint64_t>& ReadQueryResults::contig_size() const {
  return contig_size_;
}

const std::pair<uint64_t, uint64_t>& ReadQueryResults::alleles_size() const {
  return alleles_size_;
}

const std::pair<uint64_t, uint64_t>& ReadQueryResults::id_size() const {
  return id_size_;
}

const std::pair<uint64_t, uint64_t>& ReadQueryResults::filter_ids_size() const {
  return filter_ids_size_;
}

const std::pair<uint64_t, uint64_t>& ReadQueryResults::info_size() const {
  return info_size_;
}

const std::pair<uint64_t, uint64_t>& ReadQueryResults::fmt_size() const {
  return fmt_size_;
}

const std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>&
ReadQueryResults::extra_attrs_size() const {
  return extra_attrs_size_;
}

}  // namespace vcf
}  // namespace tiledb
