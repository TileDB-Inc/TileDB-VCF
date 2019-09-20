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
#include <cerrno>
#include <fstream>

#include "utils/utils.h"

namespace tiledb {
namespace vcf {
namespace utils {

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

std::string uri_join(const std::string& dir, const std::string& filename) {
  std::string result = dir;
  if (!ends_with(result, "/") && !result.empty())
    result += "/";
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

}  // namespace utils
}  // namespace vcf
}  // namespace tiledb