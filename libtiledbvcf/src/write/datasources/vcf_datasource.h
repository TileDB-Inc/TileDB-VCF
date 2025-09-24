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

#ifndef TILEDB_VCF_VCF_DATASOURCE_H
#define TILEDB_VCF_VCF_DATASOURCE_H

#include "write/datasources/datasource.h"

namespace tiledb {
namespace vcf {

/**
 * An implementation of the Datasource abstract class that loads samples from
 * VCF files.
 */
class VCFDatasource : public Datasource {
 public:
  struct Config {
    std::vector<std::string> tiledb_config;
    ScratchSpaceInfo scratch_space;
  };

  VCFDatasource(
      const std::string& samples_file_uri,
      const bool remove_samples_file,
      const Config& config);

  VCFDatasource(
      const std::vector<std::string>& sample_uris, const Config& config);

  std::vector<SampleAndIndex> get_sample_list(
      const TileDBVCFDataset* dataset) const;

  std::vector<SampleAndIndex> get_sample_list_v4(
      const TileDBVCFDataset* dataset) const;

  std::vector<SampleAndIndex> prepare_samples(
      const std::vector<SampleAndIndex>& samples);

  std::set<std::string> get_nonempty_contigs(
      const std::vector<SampleAndIndex>& samples,
      const std::vector<std::string>& contigs,
      const unsigned version) const;

  std::set<std::string> get_nonempty_contigs_v4(
      const std::vector<SampleAndIndex>& samples,
      const ContigMode contig_mode,
      const std::set<std::string>& contigs_to_keep_separate,
      std::map<std::string, std::string>& sample_headers,
      std::map<std::string, uint32_t>& total_contig_records,
      size_t& total_records_expected,
      std::vector<Region>& regions_v4) const;

  void cleanup();

 private:
  std::string samples_file_uri;
  bool remove_samples_file;
  std::vector<std::string> sample_uris;

  bool download_samples = false;
  uint64_t scratch_size_mb = 0;

  ScratchSpaceInfo scratch_space_a;
  ScratchSpaceInfo scratch_space_b;

  // TODO: do tiledb_config_, vfs_config_, and ctx_ need to be save outside of
  // init_vfs?
  std::unique_ptr<tiledb::Config> tiledb_config_;
  std::unique_ptr<tiledb::Config> vfs_config_;
  std::shared_ptr<Context> ctx_;
  std::unique_ptr<VFS> vfs_;

  void init_vfs(const std::vector<std::string>& tiledb_config);

  void init_scratch_space(const ScratchSpaceInfo& scratch_space);

  void cleanup_scratch_space();

  void cleanup_samples_file();
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_VCF_DATASOURCE_H
