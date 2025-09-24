/**
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB, Inc.
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

#include "write/datasources/dataset_datasource.h"

namespace tiledb {
namespace vcf {

DatasetDatasource::DatasetDatasource(
    const std::string& uri,
    const std::vector<std::string>& sample_names,
    const Config& config)
    : uri(uri)
    , sample_names(sample_names) {
  // TODO: instantiate Reader, including memory budget
}

std::vector<SampleAndIndex> DatasetDatasource::get_sample_list(
    const TileDBVCFDataset* dataset) const {
  // TODO
  std::vector<SampleAndIndex> samples;
  return samples;
}

std::vector<SampleAndIndex> DatasetDatasource::get_sample_list_v4(
    const TileDBVCFDataset* dataset) const {
  // TODO
  std::vector<SampleAndIndex> samples;
  return samples;
}

std::vector<SampleAndIndex> DatasetDatasource::prepare_samples(
    const std::vector<SampleAndIndex>& samples) {
  // TODO: no-op?
  return samples;
}

std::set<std::string> DatasetDatasource::get_nonempty_contigs(
    const std::vector<SampleAndIndex>& samples,
    const std::vector<std::string>& contigs,
    const unsigned version) const {
  // TODO
  std::set<std::string> nonempty_contigs;
  return nonempty_contigs;
}

std::set<std::string> DatasetDatasource::get_nonempty_contigs_v4(
    const std::vector<SampleAndIndex>& samples,
    const ContigMode contig_mode,
    const std::set<std::string>& contigs_to_keep_separate,
    std::map<std::string, std::string>& sample_headers,
    std::map<std::string, uint32_t>& total_contig_records,
    size_t& total_records_expected,
    std::vector<Region>& regions_v4) const {
  // TODO
  std::set<std::string> nonempty_contigs;
  return nonempty_contigs;
}

void DatasetDatasource::cleanup() {
  // TODO: is there anything to cleanup?
}

}  // namespace vcf
}  // namespace tiledb
