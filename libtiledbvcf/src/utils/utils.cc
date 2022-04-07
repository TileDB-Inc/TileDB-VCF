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

#include <htslib/vcf.h>
#include <tiledb/version.h>
#include <unistd.h>
#include <cerrno>
#include <fstream>
#include <mutex>

#include "htslib_plugin/hfile_tiledb_vfs.h"
#include "utils/logger_public.h"
#include "utils/utils.h"

namespace tiledb {
namespace vcf {
namespace utils {

/** Commit hash of TileDB-VCF (#defined by CMake) */
const std::string TILEDB_VCF_COMMIT_HASH = BUILD_COMMIT_HASH;

std::string version;

std::vector<std::string> split(
    const std::string& str, const std::string& delims, bool skip_empty) {
  std::vector<std::string> output;
  for_each_token(
      str.cbegin(),
      str.cend(),
      delims.cbegin(),
      delims.cend(),
      [&](std::string::const_iterator first,
          std::string::const_iterator second) {
        if (first != second || !skip_empty) {
          output.emplace_back(first, second);
        }
      });
  return output;
}

void enable_pretty_print_numbers(std::ostream& os) {
  struct custom_punct : std::numpunct<char> {
    char do_thousands_sep() const {
      return ',';
    }
    std::string do_grouping() const {
      return "\3";
    }
  };
  os.imbue(std::locale(os.getloc(), new custom_punct));
}

std::vector<std::string> split(const std::string& s, char delim) {
  return split(s, std::string(1, delim));
}

std::set<std::string> split_set(const std::string& s, char delim) {
  std::set<std::string> results;
  for (const auto& string : split(s, std::string(1, delim))) {
    results.emplace(string);
  }

  return results;
}

bool starts_with(const std::string& value, const std::string& prefix) {
  if (prefix.size() > value.size())
    return false;
  return std::equal(prefix.begin(), prefix.end(), value.begin());
}

bool ends_with(std::string const& full_string, std::string const& ending) {
  if (full_string.length() >= ending.length())
    return full_string.compare(
               full_string.length() - ending.length(),
               ending.length(),
               ending) == 0;
  else
    return false;
}

void trim(std::string* s) {
  while (s->size() && isspace(s->back()))
    s->pop_back();
  while (s->size() && isspace(s->front()))
    s->erase(s->begin());
}

void normalize_uri(std::string& uri, bool is_dir) {
  if (is_dir) {
    if (uri.back() != '/')
      uri.push_back('/');
  } else {
    if (uri.back() == '/')
      uri.pop_back();
  }
}

std::string uri_filename(const std::string& uri) {
  if (ends_with(uri, "/"))
    return "";
  auto path_parts = split(uri, "/");
  assert(!path_parts.empty());
  return path_parts.back();
}

std::string uri_join(
    const std::string& dir, const std::string& filename, const char delimiter) {
  std::string result = dir;
  if (!ends_with(result, std::string(1, delimiter)) && !result.empty())
    result += delimiter;
  result += filename;
  return result;
}

bool download_file(
    const tiledb::VFS& vfs,
    const std::string& src_uri,
    const std::string& dest_path,
    uint64_t download_bytes,
    uint64_t max_download_mb,
    Buffer& buffer) {
  // Open the remote file using VFS
  tiledb::VFS::filebuf sbuf(vfs);
  sbuf.open(src_uri, std::ios::in);
  std::istream is(&sbuf);
  if (!is.good() || is.fail() || is.bad()) {
    const char* err_c_str = strerror(errno);
    throw std::runtime_error(
        "Error downloading file '" + src_uri + "'; " + std::string(err_c_str));
  }

  // See if it will fit
  const auto file_bytes = vfs.file_size(src_uri);
  const auto to_download =
      download_bytes > 0 ? std::min(download_bytes, file_bytes) : file_bytes;
  if ((to_download / (1024.0 * 1024.0)) > max_download_mb)
    return false;

  // Download the file
  buffer.clear();
  buffer.resize(to_download);
  is.read(buffer.data<char>(), to_download);

  // Write the file locally
  std::ofstream os(dest_path, std::ios::binary);
  if (!os.good() || os.fail() || os.bad()) {
    const char* err_c_str = strerror(errno);
    throw std::runtime_error(
        "Error copying downloaded file to " + dest_path + "; " +
        std::string(err_c_str));
  }
  os.write(buffer.data<char>(), to_download);

  return true;
}

void upload_file(
    const tiledb::VFS& vfs,
    const std::string& src_path,
    const std::string& dest_uri,
    Buffer& buffer) {
  // Open the source file using VFS
  tiledb::VFS::filebuf sbuf_in(vfs);
  sbuf_in.open(src_path, std::ios::in);
  std::istream is(&sbuf_in);
  if (!is.good() || is.fail() || is.bad()) {
    const char* err_c_str = strerror(errno);
    throw std::runtime_error(
        "Error reading '" + src_path + "' for upload to destination '" +
        dest_uri + "'; " + std::string(err_c_str));
  }

  // Copy the file into memory
  const auto file_bytes = vfs.file_size(src_path);
  buffer.clear();
  buffer.resize(file_bytes);
  is.read(buffer.data<char>(), file_bytes);

  // Open the remote file using VFS
  tiledb::VFS::filebuf sbuf(vfs);
  sbuf.open(dest_uri, std::ios::out);
  std::ostream os(&sbuf);
  if (!os.good() || os.fail() || os.bad()) {
    const char* err_c_str = strerror(errno);
    throw std::runtime_error(
        "Error uploading file '" + src_path + "' to destination '" + dest_uri +
        "'; " + std::string(err_c_str));
  }
  os.write(buffer.data<char>(), file_bytes);
}

void append_from_file(const std::string& uri, std::vector<std::string>* lines) {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  auto per_line = [&lines](std::string* line) {
    if (line->size())
      lines->push_back(*line);
  };

  read_file_lines(vfs, uri, per_line);
}

void read_file_lines(
    const tiledb::VFS& vfs,
    const std::string& uri,
    std::function<void(std::string*)> callback) {
  if (!vfs.is_file(uri))
    throw std::invalid_argument(
        "Cannot read file lines; URI " + uri + " does not exist.");

  VFS::filebuf filebuf(vfs);
  filebuf.open(uri, std::ios::in);
  std::istream is(&filebuf);
  if (!is.good() || is.fail() || is.bad()) {
    const char* err_c_str = strerror(errno);
    throw std::runtime_error(
        "Cannot read file lines; " + std::string(err_c_str));
  }

  const auto total_size = vfs.file_size(uri);
  const unsigned buff_size = 500 * 1024 * 1024;  // 500 MB
  uint64_t bytes_remaining = total_size;
  Buffer buff;
  while (bytes_remaining > 0) {
    uint64_t to_read = std::min<uint64_t>(buff_size, bytes_remaining);
    buff.clear();
    buff.resize(to_read);

    is.read(buff.data<char>(), to_read);
    if (is.bad() || static_cast<uint64_t>(is.gcount()) != to_read) {
      const char* err_c_str = strerror(errno);
      throw std::runtime_error(
          "Error reading lines from file '" + uri + "'; " +
          std::string(err_c_str));
    }

    bytes_remaining -= to_read;

    std::string line;
    for (uint64_t i = 0; i < to_read;) {
      char c = buff.value<char>(i);
      line.push_back(c);
      bool eol =
          c == '\n' || c == '\r' || (bytes_remaining == 0 && i == to_read - 1);
      if (eol) {
        trim(&line);
        callback(&line);
        line.clear();
        do {
          i++;
        } while (i < to_read &&
                 (buff.value<char>(i) == '\n' || buff.value<char>(i) == '\r'));
      } else {
        i++;
      }
    }
  }
}

int bcf_type_size(const int type) {
  switch (type) {
    case BCF_HT_STR:
      return 1;
    case BCF_HT_REAL:
      return sizeof(float);
    default:
      return sizeof(int32_t);
  }
}

void set_tiledb_config_map(
    const std::vector<std::string>& params,
    std::unordered_map<std::string, std::string>* cfg) {
  for (const auto& s : params) {
    auto kv = utils::split(s, '=');
    if (kv.size() != 2)
      throw std::runtime_error(
          "Error setting TileDB config parameter; bad value '" + s + "'");

    utils::trim(&kv[0]);
    utils::trim(&kv[1]);
    cfg->emplace(kv[0], kv[1]);
  }
}

void set_tiledb_config(
    const std::vector<std::string>& params, tiledb::Config* cfg) {
  set_tiledb_config(params, cfg->ptr().get());
}

void set_tiledb_config(
    const std::vector<std::string>& params, tiledb_config_t* cfg) {
  tiledb_error_t* err;
  for (const auto& s : params) {
    auto kv = utils::split(s, '=');
    if (kv.size() != 2)
      throw std::runtime_error(
          "Error setting TileDB config parameter; bad value '" + s + "'");
    utils::trim(&kv[0]);
    utils::trim(&kv[1]);
    tiledb_config_set(cfg, kv[0].c_str(), kv[1].c_str(), &err);
    tiledb::impl::check_config_error(err);
  }
}

void set_tiledb_config(
    const std::unordered_map<std::string, std::string>& params,
    tiledb::Config* cfg) {
  set_tiledb_config(params, cfg->ptr().get());
}

void set_tiledb_config(
    const std::unordered_map<std::string, std::string>& params,
    tiledb_config_t* cfg) {
  tiledb_error_t* err;
  for (const auto& kv : params) {
    tiledb_config_set(cfg, kv.first.c_str(), kv.second.c_str(), &err);
    tiledb::impl::check_config_error(err);
  }
}

uint32_t ceil(uint32_t x, uint32_t y) {
  if (y == 0)
    return 0;
  return x / y + (x % y != 0);
}

uint64_t ceil(uint64_t x, uint64_t y) {
  if (y == 0)
    return 0;
  return x / y + (x % y != 0);
}

uint32_t floor(uint32_t x, uint32_t y) {
  if (y == 0)
    return 0;
  return x / y;
}

uint64_t floor(uint64_t x, uint64_t y) {
  if (y == 0)
    return 0;
  return x / y;
}

// Mutexs to make htslib plugin initialization thread-safe
std::mutex cfg_mutex;
std::mutex init_mutex;

// Store config and context in unique_ptr so we don't leak
std::vector<std::string> last_set_config;
void set_htslib_tiledb_context(const std::vector<std::string>& tiledb_config) {
  const std::lock_guard<std::mutex> lock(cfg_mutex);
  tiledb::Config cfg, existing_config;
  set_tiledb_config(tiledb_config, &cfg);
  if (!last_set_config.empty())
    set_tiledb_config(last_set_config, &existing_config);
  // Only set hts lib plugin config if not already initialized or update the
  // config if it is different
  if (hfile_tiledb_vfs_config == nullptr || last_set_config.empty() ||
      !compare_configs(existing_config, cfg)) {
    if (hfile_tiledb_vfs_config != nullptr)
      tiledb_config_free(&hfile_tiledb_vfs_config);

    tiledb_error_t* error;
    int32_t rc = tiledb_config_alloc(&hfile_tiledb_vfs_config, &error);
    if (rc != TILEDB_OK) {
      const char* msg;
      tiledb_error_message(error, &msg);
      throw std::runtime_error(msg);
    }

    set_tiledb_config(tiledb_config, hfile_tiledb_vfs_config);

    // Set default sizes for read ahead cache, 256KiB for read size and 1GB for
    // cache size. These values were emperically determined by running
    // experiments using the 20 sample synthetic dataset
    std::string hfile_tiledb_read_ahead_size = std::to_string(1024UL * 256);
    std::string hfile_tiledb_read_ahead_cache_size =
        std::to_string(1024UL * 1024 * 1024);
    // Check for if the user set the tiledb read ahead size or cache
    // If they set it we want to us their values instead of the defaults
    for (const auto& s : tiledb_config) {
      auto kv = utils::split(s, '=');
      utils::trim(&kv[0]);
      utils::trim(&kv[1]);
      if (kv[0] == "vfs.read_ahead_size")
        hfile_tiledb_read_ahead_size = kv[1];
      if (kv[0] == "vfs.read_ahead_cache_size")
        hfile_tiledb_read_ahead_size = kv[1];
    }

    // Set read-ahead cache used for remote sample file reading/loading
    // read ahead 256kib
    std::string read_ahead_size = hfile_tiledb_read_ahead_size;
    rc = tiledb_config_set(
        hfile_tiledb_vfs_config,
        "vfs.read_ahead_size",
        hfile_tiledb_read_ahead_size.c_str(),
        &error);
    if (rc != TILEDB_OK) {
      const char* msg;
      tiledb_error_message(error, &msg);
      throw std::runtime_error(msg);
    }

    rc = tiledb_config_set(
        hfile_tiledb_vfs_config,
        "vfs.read_ahead_cache_size",
        hfile_tiledb_read_ahead_cache_size.c_str(),
        &error);
    if (rc != TILEDB_OK) {
      const char* msg;
      tiledb_error_message(error, &msg);
      throw std::runtime_error(msg);
    }

    // Always set parallel size so we avoid breaking htslib reads down into
    // smaller chunks HTSLIB has a max read size of 32KiB so we don't need
    // TileDB to try to optimize here Breaking things down just results in more
    // overhead
    std::string min_parallel_size = std::to_string(99999999);
    rc = tiledb_config_set(
        hfile_tiledb_vfs_config,
        "vfs.min_parallel_size",
        min_parallel_size.c_str(),
        &error);
    if (rc != TILEDB_OK) {
      const char* msg;
      tiledb_error_message(error, &msg);
      throw std::runtime_error(msg);
    }

    // Similar to above, we want only 1 parallel operations per S3 read request.
    // The reads are already capped at 32 KiB so splitting it into parallel
    // reads is disadvantageous due to the latency
    std::string max_parallel_ops = std::to_string(1);
    rc = tiledb_config_set(
        hfile_tiledb_vfs_config,
        "vfs.s3.max_parallel_ops",
        max_parallel_ops.c_str(),
        &error);
    if (rc != TILEDB_OK) {
      const char* msg;
      tiledb_error_message(error, &msg);
      throw std::runtime_error(msg);
    }

    if (hfile_tiledb_vfs_ctx != nullptr)
      tiledb_ctx_free(&hfile_tiledb_vfs_ctx);

    rc = tiledb_ctx_alloc(hfile_tiledb_vfs_config, &hfile_tiledb_vfs_ctx);
    if (rc != TILEDB_OK) {
      throw std::runtime_error("Error creating context for htslib plugin");
    }

    // Store the last config so it's easy to create c++ tiledb::Config object
    // for comparison
    last_set_config = tiledb_config;
  }
}

void free_htslib_tiledb_context() {
  const std::lock_guard<std::mutex> lock(cfg_mutex);
  last_set_config.clear();
  if (hfile_tiledb_vfs_ctx != nullptr)
    tiledb_ctx_free(&hfile_tiledb_vfs_ctx);
  if (hfile_tiledb_vfs_config != nullptr)
    tiledb_config_free(&hfile_tiledb_vfs_config);
}

void init_htslib() {
  const std::lock_guard<std::mutex> lock(init_mutex);
  // trick to init some of htslib's internal data structures for plugins
  bool isremote = hisremote("vfs:///ignored");
  (void)isremote;
  // This might need a mutex for thread safety? Is there a better place to init
  // this?
  hfile_add_scheme_handler(HFILE_TILEDB_VFS_SCHEME, &tiledb_vfs_handler);
}

bool compare_configs(const tiledb::Config& rhs, const tiledb::Config& lhs) {
  // Check every parameter to see if they are the same or different
  for (const auto& it : const_cast<tiledb::Config&>(rhs)) {
    try {
      if (lhs.get(it.first) != it.second) {
        return false;
      }
    } catch (tiledb::TileDBError& e) {
      return false;
    }
  }

  // Now check that lhs is in rhs
  for (const auto& it : const_cast<tiledb::Config&>(lhs)) {
    try {
      if (rhs.get(it.first) != it.second) {
        return false;
      }
    } catch (tiledb::TileDBError& e) {
      return false;
    }
  }

  return true;
}

bool is_local_uri(const std::string& uri) {
  if (starts_with(uri, "s3://"))
    return false;
  if (starts_with(uri, "azure://"))
    return false;
  if (starts_with(uri, "gcs://"))
    return false;
  if (starts_with(uri, "hdfs://"))
    return false;

  return true;
}

static std::mutex version_creation_mtx_;
const std::string& version_info() {
  std::unique_lock<std::mutex> lck(version_creation_mtx_);
  if (version.empty()) {
    std::stringstream ss;
    ss << "TileDB-VCF version " << utils::TILEDB_VCF_COMMIT_HASH << std::endl;
    auto v = tiledb::version();
    ss << "TileDB version " << std::get<0>(v) << "." << std::get<1>(v) << "."
       << std::get<2>(v) << std::endl;
    ss << "htslib version " << hts_version();
    version = ss.str();
  }

  return version;
}

#ifdef _WIN32
#include <windows.h>
#elif __APPLE__
#include <sys/sysctl.h>
#include <sys/types.h>
#endif

uint32_t system_memory_mb() {
#ifdef _WIN32
  // https://docs.microsoft.com/en-us/windows/win32/api/sysinfoapi/nf-sysinfoapi-globalmemorystatusex
  MEMORYSTATUSEX statex;
  statex.dwLength = sizeof(statex);
  GlobalMemoryStatusEx(&statex);
  return statex.ullTotalPhys >> 20;
#elif __APPLE__
  uint64_t mem;
  size_t len = sizeof(mem);
  sysctlbyname("hw.memsize", &mem, &len, NULL, 0);
  return mem >> 20;
#else
  return (sysconf(_SC_PHYS_PAGES) * sysconf(_SC_PAGE_SIZE)) >> 20;
#endif
}

std::string memory_usage_str() {
#ifdef __linux__
  std::string filename = "/proc/self/statm";
  std::ifstream ifs(filename);
  if (!ifs.is_open()) {
    return "NA";
  }
  std::string line;
  getline(ifs, line);
  float usage_gb = 4.0 * std::stoi(split(line, " ")[1]) / (1 << 20);
  return fmt::format("{:.3f} GiB", usage_gb);
#else
  return "NA";
#endif
}

}  // namespace utils
}  // namespace vcf
}  // namespace tiledb
