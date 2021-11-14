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
 *
 * @section DESCRIPTION
 *
 * This file declares utility functions for fetching information about (possibly
 * remote) sample VCF/BCF files.
 *
 */

#ifndef TILEDB_VCF_SAMPLE_UTILS_H
#define TILEDB_VCF_SAMPLE_UTILS_H

#include <memory>
#include <string>
#include <vector>

#include <htslib/vcf.h>
#include <tiledb/vfs.h>

#include "htslib_plugin/hfile_tiledb_vfs.h"
#include "utils/buffer.h"
#include "utils/utils.h"
#include "vcf/vcf_utils.h"

namespace tiledb {
namespace vcf {

/** Struct holding information about available scratch disk space. */
struct ScratchSpaceInfo {
  std::string path = "";
  uint64_t size_mb = 0;
};

/** URI of a sample (VCF) file, and optionally the URI of its index file. */
struct SampleAndIndex {
  std::string sample_uri;
  std::string index_uri;
  uint32_t sample_id;
};

/** Pair of sample name and ID (row coord). */
struct SampleAndId {
  std::string sample_name;
  uint32_t sample_id;
};

/**
 * Utility class implementing helper methods for getting information about
 * BCF/VCF files.
 */
class SampleUtils {
 public:
  /**
   * Downloads remote BCF/VCF sample files from S3 as necessary.
   *
   * @param vfs TileDB VFS instance to use
   * @param samples List of samples to download
   * @param scratch_space Scratch space info
   * @return List of local paths for downloaded samples.
   */
  static std::vector<SampleAndIndex> get_samples(
      const tiledb::VFS& vfs,
      const std::vector<SampleAndIndex>& samples,
      ScratchSpaceInfo* scratch_space);

  /**
   * Downloads headers for the given samples and returns a vector of the sample
   * name in each sample.
   *
   * @param vfs TileDB VFS instance to use
   * @param samples List of samples to fetch names for
   * @param scratch_space Scratch space info
   * @return Vector of sample names
   */
  static std::vector<std::string> get_sample_names(
      const tiledb::VFS& vfs,
      const std::vector<SampleAndIndex>& samples,
      const ScratchSpaceInfo& scratch_space);

  /**
   * Downloads headers for the given samples and return the HTSlib header
   * instance for each sample.
   *
   * @param vfs TileDB VFS instance to use
   * @param samples List of samples to fetch headers for
   * @param scratch_space Scratch space info
   * @return Vector of sample header instances
   */
  static std::vector<SafeBCFHdr> get_sample_headers(
      const tiledb::VFS& vfs,
      const std::vector<SampleAndIndex>& samples,
      const ScratchSpaceInfo& scratch_space);
  /**
   * Aggregates sample URIs passed explicitly and contained in a file into a
   * single list.
   */
  static std::vector<SampleAndIndex> build_samples_uri_list(
      const tiledb::VFS& vfs,
      const std::string& samples_file_uri,
      const std::vector<std::string>& samples_uri_list);

  /**
   * Changes remote BCF/VCF sample files to vfs plugin path as necessary.
   *
   * @param samples List of samples to download
   * @return List of local paths for downloaded samples.
   */
  static std::vector<SampleAndIndex> build_vfs_plugin_sample_list(
      const std::vector<SampleAndIndex>& samples);

  /**
   * Builds a SampleAndIndex object with VFS htslib plugin prefix
   * @param uri to adjust
   * @return uri string with htslib prefix
   */
  static std::string build_vfs_plugin_uri(const std::string& uri);

  /**
   * Builds a SampleAndIndex object with VFS htslib plugin prefix
   * @param sample to adjust
   * @return SampleAndIndex object
   */
  static SampleAndIndex build_vfs_plugin_sample_and_index(
      const SampleAndIndex& sample);

 private:
  /**
   * Helper method that downloads the header for each sample and performs a
   * 'process' callback on each header instance, returning the results.
   *
   * Note: If no scratch space is configured we will default to using vfs htslib
   * plugin to avoid downloading
   *
   * @tparam T Return type of process function
   * @param vfs TileDB VFS instance to use
   * @param samples List of samples to fetch headers for
   * @param scratch_space Scratch space info
   * @param process Callback invoked on each header
   * @return Results of callbacks.
   */
  template <typename T>
  static std::vector<T> process_sample_headers(
      const tiledb::VFS& vfs,
      const std::vector<SampleAndIndex>& samples,
      const ScratchSpaceInfo& scratch_space,
      const std::function<T(SafeBCFHdr)>& process) {
    // Disable HTSlib error messages, as we may cause some benign read errors.
    const auto old_log_level = hts_get_log_level();
    hts_set_log_level(HTS_LOG_OFF);

    bool download_samples = !scratch_space.path.empty();
    // Set up some local scratch space
    const auto download_dest_dir =
        utils::uri_join(scratch_space.path, "sample-hdr-dl");

    bool cleanup_dir = false;
    if (download_samples) {
      if (scratch_space.size_mb > 0 && !vfs.is_dir(download_dest_dir)) {
        vfs.create_dir(download_dest_dir);
        cleanup_dir = true;
      }
    }

    Buffer buff;
    std::vector<T> result;
    for (const auto& s : samples) {
      if (!vfs.is_file(s.sample_uri))
        throw std::runtime_error(
            "Error processing sample; URI '" + s.sample_uri +
            "' does not exist.");

      auto file_size = vfs.file_size(s.sample_uri);
      // Repeatedly download more and more of the file until the header can be
      // successfully parsed. Start by downloading 32KB.
      uint64_t dl_num_bytes = std::min<uint64_t>(32 * 1024, file_size);

      // Sample-specific temporary file to download to
      std::string download_dest_file = utils::uri_join(
          download_dest_dir, utils::split(s.sample_uri, '/').back());

      bool found_hdr = false;
      while (true) {
        if (vfs.is_file(download_dest_file))
          vfs.remove_file(download_dest_file);

        // Download the file from S3 if necessary.
        std::string path;
        if (!utils::is_local_uri(s.sample_uri)) {
          if (download_samples) {
            if (!utils::download_file(
                    vfs,
                    s.sample_uri,
                    download_dest_file,
                    dl_num_bytes,
                    scratch_space.size_mb,
                    buff))
              throw std::runtime_error(
                  "Could not download header for " + s.sample_uri +
                  ". Increase scratch space from current setting of " +
                  std::to_string(scratch_space.size_mb) + " MB.");
            path = download_dest_file;
          } else {
            path = build_vfs_plugin_uri(s.sample_uri);
          }
        } else {
          path = s.sample_uri;
        }

        // Allocate a header struct and try to parse from the local file.
        SafeBCFHdr hdr(VCFUtils::hdr_read_header(path), bcf_hdr_destroy);
        if (hdr != nullptr) {
          result.push_back(process(std::move(hdr)));
          found_hdr = true;
          break;
        }

        // Double the download size and try again. Don't infinite loop on error.
        auto doubled = std::min<uint64_t>(dl_num_bytes * 2, file_size);
        if (doubled == dl_num_bytes)
          break;
        dl_num_bytes = doubled;
      }

      if (!found_hdr) {
        throw std::runtime_error("Invalid VCF file: " + s.sample_uri);
      }
    }

    // Restore old logging
    hts_set_log_level(old_log_level);

    // Clean up
    if (cleanup_dir && vfs.is_dir(download_dest_dir))
      vfs.remove_dir(download_dest_dir);

    return result;
  }
};

/**
 * Batches the given vector into a vector of vectors based on the tile_size .
 * Ideally the vectors will be fixed (even) sized. However if the sample ids
 * cross tile extents we must stop the batch as we can not load across tile
 * extents.
 *
 * In the worst case, if a user tried to load multiple samples but all from
 * unique tile extents this will devolve into single sample batches
 *
 *
 * @param vec Vector to batch
 * @param batch_size Number of elements per tile extent
 * @return Batched result
 */
std::vector<std::vector<SampleAndIndex>> batch_elements_by_tile(
    const std::vector<SampleAndIndex>& vec, uint64_t tile_size);

/**
 * Batches the given vector into a vector of vectors based on the tile_size.
 * Ideally the vectors will be fixed (even) sized. If there is not an evenly
 * divisible number of samples the last batch will be smaller.
 *
 * @param vec Vector to batch
 * @param batch_size Number of elements per tile extent
 * @return Batched result
 */
std::vector<std::vector<SampleAndIndex>> batch_elements_by_tile_v4(
    const std::vector<SampleAndIndex>& vec, uint64_t tile_size);

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_SAMPLE_UTILS_H
