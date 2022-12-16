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

#include "utils/sample_utils.h"
#include "utils/buffer.h"
#include "utils/utils.h"

namespace tiledb {
namespace vcf {

std::vector<SampleAndIndex> SampleUtils::get_samples(
    const tiledb::VFS& vfs,
    const std::vector<SampleAndIndex>& samples,
    ScratchSpaceInfo* scratch_space) {
  // If the user doesn't have scratch space, use VFS htslib plugin to read
  // remote samples
  if (scratch_space->path.empty()) {
    return build_vfs_plugin_sample_list(samples);
  }

  // Set up some local scratch space
  const auto download_dest_dir =
      utils::uri_join(scratch_space->path, "sample-dl");
  if (scratch_space->size_mb > 0 && !vfs.is_dir(download_dest_dir))
    vfs.create_dir(download_dest_dir);

  Buffer buffer;
  std::vector<SampleAndIndex> local_paths;
  for (const auto& s : samples) {
    if (utils::is_local_uri(s.sample_uri)) {
      local_paths.push_back(
          {.sample_uri = s.sample_uri, .index_uri = s.index_uri});
      continue;
    }

    // Download sample file
    const auto sample_path =
        utils::uri_join(download_dest_dir, utils::uri_filename(s.sample_uri));
    if (!utils::download_file(
            vfs, s.sample_uri, sample_path, 0, scratch_space->size_mb, buffer))
      throw std::runtime_error(
          "Error downloading sample '" + s.sample_uri +
          "'; not enough scratch disk space configured.");
    scratch_space->size_mb -= vfs.file_size(sample_path) / (1024 * 1024);

    // Download index file
    std::string idx_path;
    if (!s.index_uri.empty()) {
      idx_path =
          utils::uri_join(download_dest_dir, utils::uri_filename(s.sample_uri));
      if (!utils::download_file(
              vfs, s.index_uri, idx_path, 0, scratch_space->size_mb, buffer))
        throw std::runtime_error(
            "Error downloading sample index '" + s.index_uri +
            "'; not enough scratch disk space configured.");
      scratch_space->size_mb -= vfs.file_size(idx_path) / (1024 * 1024);
    } else if (vfs.is_file(s.sample_uri + ".csi")) {
      std::string idx_uri = s.sample_uri + ".csi";
      idx_path =
          utils::uri_join(download_dest_dir, utils::uri_filename(idx_uri));
      if (!utils::download_file(
              vfs, idx_uri, idx_path, 0, scratch_space->size_mb, buffer))
        throw std::runtime_error(
            "Error downloading sample index '" + s.sample_uri + ".csi" +
            "'; not enough scratch disk space configured.");
    } else if (vfs.is_file(s.sample_uri + ".tbi")) {
      std::string idx_uri = s.sample_uri + ".tbi";
      idx_path =
          utils::uri_join(download_dest_dir, utils::uri_filename(idx_uri));
      if (!utils::download_file(
              vfs, idx_uri, idx_path, 0, scratch_space->size_mb, buffer))
        throw std::runtime_error(
            "Error downloading sample index '" + s.sample_uri + ".tbi" +
            "'; not enough scratch disk space configured.");
    } else {
      throw std::runtime_error(
          "Error downloading index for sample '" + s.sample_uri +
          "'; could not find index.");
    }

    local_paths.push_back({.sample_uri = sample_path, .index_uri = idx_path});
  }

  return local_paths;
}

std::vector<SafeBCFHdr> SampleUtils::get_sample_headers(
    const tiledb::VFS& vfs,
    const std::vector<SampleAndIndex>& samples,
    const ScratchSpaceInfo& scratch_space) {
  return process_sample_headers<SafeBCFHdr>(
      vfs, samples, scratch_space, [](SafeBCFHdr hdr) { return hdr; });
}

std::vector<std::string> SampleUtils::get_sample_names(
    const tiledb::VFS& vfs,
    const std::vector<SampleAndIndex>& samples,
    const ScratchSpaceInfo& scratch_space) {
  return process_sample_headers<std::string>(
      vfs, samples, scratch_space, [](SafeBCFHdr hdr) {
        int sample_count = bcf_hdr_nsamples(hdr.get());
        if (sample_count > 1)
          throw std::runtime_error("Combined VCFs are current not suppported");
        else if (sample_count == 0)
          return std::string();
        else
          return std::string(hdr->samples[0]);
      });
}

std::vector<SampleAndIndex> SampleUtils::build_samples_uri_list(
    const tiledb::VFS& vfs,
    const std::string& samples_file_uri,
    const std::vector<std::string>& samples_uri_list) {
  if (samples_uri_list.empty() && samples_file_uri.empty()) {
    throw std::runtime_error(
        "Can not proceed with an empty sample list, without a sample file.");
  }
  std::vector<SampleAndIndex> result;

  // First add samples from the given samples file, if present.
  if (!samples_file_uri.empty()) {
    auto per_line = [&result](std::string* line) {
      auto pair = utils::split(*line, '\t');
      if (pair.empty())
        return;
      if (pair.size() >= 2)
        result.push_back({.sample_uri = pair[0], .index_uri = pair[1]});
      else
        result.push_back({.sample_uri = pair[0]});
    };
    utils::read_file_lines(vfs, samples_file_uri, per_line);
  }

  // Add any explicitly passed samples.
  for (const auto& uri : samples_uri_list)
    result.push_back({.sample_uri = uri});

  return result;
}

std::string SampleUtils::build_vfs_plugin_uri(const std::string& uri) {
  // Local URIs can be skipped and read directly with htslib
  if (utils::is_local_uri(uri)) {
    return uri;
  }
  std::string adjusted_uri = std::string(HFILE_TILEDB_VFS_SCHEME) + "://" + uri;
  return adjusted_uri;
}

SampleAndIndex SampleUtils::build_vfs_plugin_sample_and_index(
    const SampleAndIndex& sample) {
  return {
      .sample_uri = build_vfs_plugin_uri(sample.sample_uri),
      .index_uri = build_vfs_plugin_uri(sample.index_uri)};
}

std::vector<SampleAndIndex> SampleUtils::build_vfs_plugin_sample_list(
    const std::vector<SampleAndIndex>& samples) {
  std::vector<SampleAndIndex> local_paths;
  for (const auto& s : samples) {
    // Local URIs can be skipped and read directly with htslib
    if (utils::is_local_uri(s.sample_uri)) {
      local_paths.push_back(
          {.sample_uri = s.sample_uri, .index_uri = s.index_uri});
      continue;
    }
    local_paths.push_back(build_vfs_plugin_sample_and_index(s));
  }

  return local_paths;
}

std::vector<std::vector<SampleAndIndex>> batch_elements_by_tile(
    const std::vector<SampleAndIndex>& vec, uint64_t tile_size) {
  std::vector<std::vector<SampleAndIndex>> result;
  std::vector<SampleAndIndex> batch;
  // Set last seen tile extent to max, as an initialized value
  uint32_t last_seen_tile_extent = std::numeric_limits<uint32_t>::max();
  for (unsigned vec_idx = 0; vec_idx < vec.size(); vec_idx++) {
    auto sample = vec[vec_idx];
    // When batching we must include samples only in the same tile extent
    if (last_seen_tile_extent != sample.sample_id / tile_size) {
      // reset last_seen_tile_extent
      last_seen_tile_extent = sample.sample_id / tile_size;
      result.emplace_back(batch);
      batch = std::vector<SampleAndIndex>();
    }
    batch.emplace_back(vec[vec_idx]);
  }
  // Add last batch if it exists
  if (!batch.empty())
    result.emplace_back(batch);

  return result;
}

std::vector<std::vector<SampleAndIndex>> batch_elements_by_tile_v4(
    const std::vector<SampleAndIndex>& vec, uint64_t tile_size) {
  std::vector<std::vector<SampleAndIndex>> result;
  std::vector<SampleAndIndex> batch;
  // Set last seen tile extent to max, as an initialized value
  uint64_t count = 0;
  for (unsigned vec_idx = 0; vec_idx < vec.size(); vec_idx++) {
    auto sample = vec[vec_idx];
    // When batching we must include samples only in the same tile extent
    if (count >= tile_size) {
      // reset count
      count = 0;
      result.emplace_back(batch);
      batch = std::vector<SampleAndIndex>();
    }
    batch.emplace_back(vec[vec_idx]);
    ++count;
  }
  // Add last batch if it exists
  if (!batch.empty())
    result.emplace_back(batch);

  return result;
}

}  // namespace vcf
}  // namespace tiledb
