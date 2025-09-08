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

#include "write/datasources/vcf_datasource.h"
#include "utils/logger_public.h"

namespace tiledb {
namespace vcf {

VCFDatasource::VCFDatasource(
    const std::string& samples_file_uri,
    const bool remove_samples_file,
    const Config& config)
    : samples_file_uri(samples_file_uri)
    , remove_samples_file(remove_samples_file) {
  init_scratch_space(config.scratch_space);
  init_vfs(config.tiledb_config);
}

VCFDatasource::VCFDatasource(
    const std::vector<std::string>& sample_uris, const Config& config)
    : sample_uris(sample_uris) {
  init_scratch_space(config.scratch_space);
  init_vfs(config.tiledb_config);
}

VCFDatasource::~VCFDatasource() {
}

std::vector<SampleAndIndex> VCFDatasource::get_sample_list(
    const TileDBVCFDataset* dataset) const {
  auto samples =
      SampleUtils::build_samples_uri_list(*vfs_, samples_file_uri, sample_uris);

  // Get sample names
  auto sample_names =
      SampleUtils::get_sample_names(*vfs_, samples, scratch_space_a);

  // Sort by sample ID
  std::vector<std::pair<SampleAndIndex, std::string>> sorted;
  for (size_t i = 0; i < samples.size(); i++)
    sorted.emplace_back(samples[i], sample_names[i]);

  std::sort(
      sorted.begin(),
      sorted.end(),
      [dataset](
          const std::pair<SampleAndIndex, std::string>& a,
          const std::pair<SampleAndIndex, std::string>& b) {
        return dataset->metadata().sample_ids.at(a.second) <
               dataset->metadata().sample_ids.at(b.second);
      });

  std::vector<SampleAndIndex> result;
  // Set sample id for later use
  for (const auto& pair : sorted) {
    auto s = pair.first;
    if (dataset->metadata().version == TileDBVCFDataset::Version::V2 ||
        dataset->metadata().version == TileDBVCFDataset::Version::V3)
      s.sample_id = dataset->metadata().sample_ids.at(pair.second);
    result.push_back(s);
  }

  return result;
}

std::vector<SampleAndIndex> VCFDatasource::get_sample_list_v4(
    const TileDBVCFDataset* dataset) const {
  auto samples =
      SampleUtils::build_samples_uri_list(*vfs_, samples_file_uri, sample_uris);

  // Get sample names
  auto sample_names =
      SampleUtils::get_sample_names(*vfs_, samples, scratch_space_a);

  // Sort by sample ID
  std::vector<std::pair<SampleAndIndex, std::string>> sorted(samples.size());
  for (size_t i = 0; i < samples.size(); i++)
    sorted[i] = std::make_pair(samples[i], sample_names[i]);
  std::sort(
      sorted.begin(),
      sorted.end(),
      [](const std::pair<SampleAndIndex, std::string>& a,
         const std::pair<SampleAndIndex, std::string>& b) {
        return a.second < b.second;
      });

  std::vector<SampleAndIndex> result;
  // Set sample id for later use
  for (const auto& pair : sorted) {
    auto s = pair.first;
    if (dataset->metadata().version == TileDBVCFDataset::Version::V2 ||
        dataset->metadata().version == TileDBVCFDataset::Version::V3)
      s.sample_id = 0;
    result.push_back(s);
  }

  return result;
}

void VCFDatasource::init_vfs(const std::vector<std::string>& tiledb_config) {
  tiledb_config_.reset(new tiledb::Config);
  // TODO: do we actually need this for VFS?
  //(*tiledb_config_)["vfs.s3.multipart_part_size"] = params.part_size_mb << 20;
  //(*tiledb_config_)["sm.mem.total_budget"] =
  //  params.tiledb_memory_budget_mb << 20;
  //(*tiledb_config_)["sm.compute_concurrency_level"] = params.num_threads;
  //(*tiledb_config_)["sm.io_concurrency_level"] = params.num_threads;
  ctx_.reset(new Context(*tiledb_config_));

  std::vector<std::string> vfs_config = tiledb_config;
  try {
    auto vcf_region = tiledb_config_->get("vcf.s3.region");
    vfs_config.push_back("vfs.s3.region=" + vcf_region);
    LOG_INFO("VFS and htslib reading data from S3 region: {}", vcf_region);
  } catch (...) {
    // tiledb_config_ is not defined in "vcf.s3.region", no action required
  }
  vfs_config_.reset(new tiledb::Config);
  utils::set_tiledb_config(vfs_config, vfs_config_.get());
  vfs_.reset(new VFS(*ctx_, *vfs_config_));
}

void VCFDatasource::init_scratch_space(const ScratchSpaceInfo& scratch_space) {
  scratch_space_a = scratch_space;
  scratch_space_b = scratch_space;
  download_samples = !scratch_space.path.empty();
  if (download_samples) {
    // Set up parameters for two scratch spaces
    scratch_size_mb = scratch_space.size_mb / 2;
    scratch_space_a.size_mb = scratch_size_mb;
    scratch_space_a.path = utils::uri_join(scratch_space_a.path, "ingest-a");
    if (!vfs_->is_dir(scratch_space_a.path))
      vfs_->create_dir(scratch_space_a.path);
    scratch_space_b.size_mb = scratch_size_mb;
    scratch_space_b.path = utils::uri_join(scratch_space_b.path, "ingest-b");
    if (!vfs_->is_dir(scratch_space_b.path))
      vfs_->create_dir(scratch_space_b.path);
  }
}

std::vector<SampleAndIndex> VCFDatasource::prepare_samples(
    const std::vector<SampleAndIndex>& samples) {
  // Reset scratch_space_a and swap with scratch_space_b
  if (download_samples) {
    if (vfs_->is_dir(scratch_space_a.path))
      vfs_->remove_dir(scratch_space_a.path);
    vfs_->create_dir(scratch_space_a.path);
    scratch_space_a.size_mb = scratch_size_mb;
    if (vfs_->is_dir(scratch_space_b.path))
      std::swap(scratch_space_a, scratch_space_b);
  }
  // Fetch the samples
  return SampleUtils::get_samples(*vfs_, samples, &scratch_space_b);
}

void VCFDatasource::cleanup() {
  cleanup_scratch_space();
  cleanup_samples_file();
}

void VCFDatasource::cleanup_scratch_space() {
  if (download_samples) {
    if (vfs_->is_dir(scratch_space_a.path))
      vfs_->remove_dir(scratch_space_a.path);
    if (vfs_->is_dir(scratch_space_b.path))
      vfs_->remove_dir(scratch_space_b.path);
  }
}

void VCFDatasource::cleanup_samples_file() {
  if (remove_samples_file && vfs_->is_file(samples_file_uri))
    vfs_->remove_file(samples_file_uri);
}

}  // namespace vcf
}  // namespace tiledb
