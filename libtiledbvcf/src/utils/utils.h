/**
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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

#ifndef TILEDB_VCF_UTILS_H
#define TILEDB_VCF_UTILS_H

#include <algorithm>
#include <chrono>
#include <functional>
#include <numeric>
#include <string>
#include <vector>

#include <tiledb/vfs.h>

#include "utils/buffer.h"

#define TRY_CATCH_THROW(stmt)                                      \
  [&]() {                                                          \
    try {                                                          \
      (stmt);                                                      \
    } catch (const std::exception& e) {                            \
      auto err = std::string("TileDB-VCF exception: ") + e.what(); \
      LOG_ERROR(err);                                              \
      throw std::runtime_error(err);                               \
    }                                                              \
  }()

namespace tiledb {
namespace vcf {

namespace utils {

/** Commit hash of TileDB-VCF (#defined by CMake) */
extern const std::string TILEDB_VCF_COMMIT_HASH;

extern std::string version;

/** Returns the value of x/y (integer division) rounded up. */
uint32_t ceil(uint32_t x, uint32_t y);

/** Returns the value of x/y (integer division) rounded up. */
uint64_t ceil(uint64_t x, uint64_t y);

/** Returns the value of x/y (integer division) rounded down. */
uint32_t floor(uint32_t x, uint32_t y);

/** Returns the value of x/y (integer division) rounded down. */
uint64_t floor(uint64_t x, uint64_t y);

/**
 * Apply binary_op to each token in [in_begin,in_end], split by any element in
 * [d_begin, d_end]. Adapted from
 * http://tristanbrindle.com/posts/a-quicker-study-on-tokenising/
 *
 * @tparam InIter Type of input iterator
 * @tparam DelimIter Type of split iterator
 * @tparam BinOp Binary Op to apply fo each token
 * @param in_begin Input begin iterator
 * @param in_end Input end iterator
 * @param d_begin Delimiter begin iterator
 * @param d_end Delimiter end iterator
 * @param binary_op operation to apply to each token
 */
template <typename InIter, typename DelimIter, class BinOp>
void for_each_token(
    InIter in_begin,
    InIter in_end,
    DelimIter d_begin,
    DelimIter d_end,
    BinOp binary_op) {
  while (in_begin != in_end) {
    const auto pos = std::find_first_of(in_begin, in_end, d_begin, d_end);
    binary_op(in_begin, pos);
    if (pos == in_end)
      break;
    in_begin = std::next(pos);
  }
}

/**
 * Sets a locale on the given output stream that causes numbers to be
 * pretty-printed.
 *
 * Example:
 * @code{.cc}
 * int x = 12345;
 *
 * auto old_locale = std::cout.getloc();
 * enable_pretty_print_numbers(std::cout);
 * std::cout << x << "\n";
 *
 * std::cout.imbue(old_locale);
 * std::cout << x << "\n";
 * @endcode
 *
 * The above example prints "12,345" followed by "12345".
 *
 * @param os Stream to modify
 */
void enable_pretty_print_numbers(std::ostream& os);

/**
 * Batches the given vector into a vector of fixed-size vectors. The last batch
 * may be smaller if the batch size does not evenly divide the vector size.
 *
 * @param vec Vector to batch
 * @param batch_size Number of elements per batch
 * @return Batched result
 */
template <typename T>
std::vector<std::vector<T>> batch_elements(
    const std::vector<T>& vec, uint64_t batch_size) {
  std::vector<std::vector<T>> result;
  const uint64_t num_batches = ceil(vec.size(), batch_size);
  unsigned vec_idx = 0;
  for (uint64_t i = 0; i < num_batches; i++) {
    result.push_back({});
    std::vector<T>& batch = result.back();
    for (unsigned j = 0; j < batch_size && vec_idx < vec.size();
         j++, vec_idx++) {
      batch.emplace_back(vec[vec_idx]);
    }
  }
  return result;
}

/**
 * Partitions the given vector in-place.
 *
 * @tparam T Vector element type
 * @param partition_idx Index of partition
 * @param num_partitions Total number of partitions
 * @param vec Vector that will be partitioned.
 */
template <typename T>
void partition_vector(
    uint64_t partition_idx, uint64_t num_partitions, std::vector<T>* vec) {
  const uint64_t num_elements = vec->size();
  if (num_partitions == 0 || num_partitions > num_elements)
    throw std::runtime_error(
        "Error partitioning vector; cannot partition " +
        std::to_string(num_elements) + " elements into " +
        std::to_string(num_partitions) + " partitions.");
  if (partition_idx >= num_partitions)
    throw std::runtime_error(
        "Error partitioning vector; partition index " +
        std::to_string(partition_idx) + " >= num partitions " +
        std::to_string(num_partitions) + ".");
  uint64_t elts_per_partition = utils::floor(num_elements, num_partitions);
  uint64_t idx_min =
      std::min<uint64_t>(partition_idx * elts_per_partition, num_elements);
  uint64_t idx_max =
      std::min<uint64_t>(idx_min + elts_per_partition, num_elements);

  // Handle any left overs by giving them to the first n partitions
  uint64_t left_overs = num_elements % num_partitions;
  idx_min += std::min<uint64_t>(partition_idx, left_overs);
  idx_max += std::min<uint64_t>(partition_idx + 1, left_overs);

  // Check for empty partition assignment.
  if (idx_min == idx_max) {
    vec->clear();
    return;
  }

  // Partition list
  auto beg_it = vec->begin() + idx_min;
  auto end_it = vec->begin() + idx_max;
  std::vector<T> new_vec;
  for (auto it = beg_it; it != end_it; ++it)
    new_vec.emplace_back(std::move(*it));
  vec->swap(new_vec);
}

/**
 * @brief
 * Split a string into tokens with any delimiter in delims.
 * @param str String to split
 * @param delims List of delimiters to split by
 * @param skip_empty Skip empty elements
 * @return vector of tokens
 */
std::vector<std::string> split(
    const std::string& str,
    const std::string& delims = ",",
    bool skip_empty = true);

/**
 * @brief
 * Split a string into tokens with any delimiter in delims.
 * @param str String to split
 * @param delims List of delimiters to split by
 * @param skip_empty Skip empty elements
 * @return set of tokens
 */
std::set<std::string> split_set(const std::string& s, char delim);

/**
 * @brief
 * Splits a string into a vector given some character delimiter.
 * @param s string to split
 * @param delim split string at delim, discarding the delim
 * @return vector to store results in
 */
std::vector<std::string> split(const std::string& s, char delim);

/**
 * @tparam T
 * @param start_time
 * @return Time between start time and now.
 */
template <typename T>
double chrono_duration(const std::chrono::time_point<T>& start_time) {
  auto now = std::chrono::steady_clock::now();
  return std::chrono::duration_cast<std::chrono::duration<double>>(
             now - start_time)
      .count();
}

/**
 * Checks if a string starts with some substring
 *
 * @param value
 * @param prefix
 * @return True if full_string starts with prefix
 */
bool starts_with(const std::string& value, const std::string& prefix);

/**
 * Checks if a string ends with some substring
 *
 * @param full_string
 * @param ending
 * @return True if full_string ends with ending
 */
bool ends_with(std::string const& full_string, std::string const& ending);

/** Trims leading and trailing whitespace in-place. */
void trim(std::string* s);

/** Ensure URI ends in / if a dir */
void normalize_uri(std::string& uri, bool is_dir);

/**
 * Returns the filename (path after last trailing '/') from the given URI.
 *
 * If the URI ends in '/', empty string is returned as URI refers to a
 * directory.
 */
std::string uri_filename(const std::string& uri);

/**
 * Joins a filename to a directory URI (adds a '/' between them).
 */
std::string uri_join(
    const std::string& dir,
    const std::string& filename,
    const char delimiter = '/');

/**
 * Downloads a file to local storage.
 *
 * @param vfs TileDB VFS instance to use
 * @param src_uri URI of file to download
 * @param dest_path Destination path for downloaded file
 * @param download_bytes If 0, download the whole file. If non-zero, download
 *      only the given number of bytes, from the beginning of the file.
 * @param max_download_mb Do not download if the file exceeds this size (MB).
 * @param buffer Buffer to use for downloading
 * @return True if the file was downloaded
 *
 * @throws std::runtime_error if an error occurred during downloading.
 */
bool download_file(
    const tiledb::VFS& vfs,
    const std::string& src_uri,
    const std::string& dest_path,
    uint64_t download_bytes,
    uint64_t max_download_mb,
    Buffer& buffer);

/**
 * Uploads a file to a S3 URI (or copies if to a local directory).
 *
 * Note: this buffers the entire file in memory while uploading.
 *
 * @param vfs TileDB VFS instance to use
 * @param src_path URI of file to upload
 * @param dest_uri Destination URI for downloaded file
 * @param buffer Buffer to use for uploading.
 *
 * @throws std::runtime_error if an error occurred during uploading.
 */
void upload_file(
    const tiledb::VFS& vfs,
    const std::string& src_path,
    const std::string& dest_uri,
    Buffer& buffer);

/**
 * Buffers the contents of the given URI into memory (using the given VFS
 * instance) and makes the given callback for each textual line in the file.
 *
 * @param vfs TileDB VFS instance to use for buffering the file
 * @param uri URI of file to read
 * @param callback Callback made, for each line in the file.
 */
void read_file_lines(
    const tiledb::VFS& vfs,
    const std::string& uri,
    std::function<void(std::string*)> callback);

/**
 * Reads the given file and appends all lines to the given vector.
 */
void append_from_file(const std::string& uri, std::vector<std::string>* lines);

/** Return size (in bytes) of an htslib type. */
int bcf_type_size(const int type);

/**
 * Parse the give list of params (of the format 'param.name=value') and set them
 * on the unordered map
 * @param params vector of params
 * @param cfg unordered_map to set values on
 */
void set_tiledb_config_map(
    const std::vector<std::string>& params,
    std::unordered_map<std::string, std::string>* cfg);

/**
 * Parses the given list of params (of the format 'param.name=value') and sets
 * them on the given Config instance.
 */
void set_tiledb_config(
    const std::vector<std::string>& params, tiledb::Config* cfg);

/**
 * Parses the given list of params (of the format 'param.name=value') and sets
 * them on the given Config instance.
 *
 * @param params vector of params
 * @param cfg c_api tiledb_config_t*
 */
void set_tiledb_config(
    const std::vector<std::string>& params, tiledb_config_t* cfg);

/**
 * Sets a map of parameters to a TileDB Config
 * @param params unordered map of configuration parameters
 * @param cfg TileDB Config to set
 */
void set_tiledb_config(
    const std::unordered_map<std::string, std::string>& params,
    tiledb::Config* cfg);

/**
 * Sets a map of parameters to a TileDB Config
 * @param params unordered map of configuration parameters
 * @param cfg TileDB Config to set
 */
void set_tiledb_config(
    const std::unordered_map<std::string, std::string>& params,
    tiledb_config_t* cfg);

/**
 * Set the htslib global config and context. We use this c++ function to provide
 * a thread-safe implementation
 *
 * @param tiledb_config config vector to parse
 */
void set_htslib_tiledb_context(const std::vector<std::string>& tiledb_config);

/**
 * Free the htslib plugin context
 * a thread-safe implementation
 *
 */
void free_htslib_tiledb_context();

/**
 * Help function to initialize the htslib plugin
 */
void init_htslib();

/**
 * compares two config
 * @param rhs
 * @param lhs
 * @return true is identical, false otherwise
 */
bool compare_configs(const tiledb::Config& rhs, const tiledb::Config& lhs);

/**
 * Checks if a file path is local or remote
 * @param uri to check
 * @return true if file is local path (file:// or no prefix), else false
 */
bool is_local_uri(const std::string& uri);

/**
 * Returns TileDB-VCF and TileDB version information in string form.
 * @return
 */
const std::string& version_info();

/**
 * Returns total system memory in MiB
 */
uint32_t system_memory_mb();

/**
 * @brief Return memory usage by the current process.
 *
 * @return std::string Memory usage for log file.
 */
std::string memory_usage_str();

/**
 *
 * Given a vector of T return an a vector if indexes, sorted by ascending values
 *
 * inspired by: https://stackoverflow.com/a/12399290/11562375
 * @tparam T
 * @param v
 * @return
 */
template <typename T, typename U>
std::vector<size_t> sort_indexes_pvcf(
    const T& start_pos, const U& sample_names) {
  assert(start_pos.size() == sample_names.size());
  // initialize original index locations
  std::vector<size_t> idx(start_pos.size());
  std::iota(idx.begin(), idx.end(), 0);

  // sort indexes based on comparing values in v
  // using std::stable_sort instead of std::sort
  // to avoid unnecessary index re-orderings
  // when v contains elements of equal values
  std::stable_sort(
      idx.begin(),
      idx.end(),
      [&start_pos, &sample_names](size_t i1, size_t i2) {
        if (start_pos[i1] == start_pos[i2])
          return sample_names[i1] < sample_names[i2];

        return start_pos[i1] < start_pos[i2];
      });

  return idx;
}

/**
 * @brief Search for item in vec. If not found push item to the back of vec.
 *
 * @tparam T type of item
 * @param vec vector of type T
 * @param item item to add to vector
 * @return int index of item in vec
 */
template <class T>
int push_unique(std::vector<T>& vec, T item) {
  auto find_it = std::find(vec.begin(), vec.end(), item);
  if (find_it == vec.end()) {
    vec.push_back(item);
    return vec.size() - 1;
  }
  return find_it - vec.begin();
}

/**
 * @brief Search for item in vec. Return true if found, false otherwise.
 *
 * @tparam T type of item
 * @param vec vector of type T
 * @param item item to search for
 * @return true item found in vec
 * @return false item not found in vec
 */
template <class T>
bool contains(std::vector<T>& vec, T& item) {
  return std::find(vec.begin(), vec.end(), item) != vec.end();
}

}  // namespace utils
}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_UTILS_H
