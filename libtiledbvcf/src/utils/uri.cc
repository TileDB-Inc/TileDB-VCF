/**
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2025 TileDB, Inc.
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

#include "utils/uri.h"
#include "utils/utils.h"

namespace tiledb {
namespace vcf {
namespace utils {

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

std::string group_uri(const Group& group, const std::string& array) {
  try {
    auto member = group.member(array);
    return member.uri();
  } catch (const TileDBError& ex) {
    return "";
  }
}

std::string root_uri(
    const std::string& root_uri, const std::string& array, bool relative) {
  auto root = relative ? "" : root_uri;
  return utils::uri_join(root, array);
}

}  // namespace utils
}  // namespace vcf
}  // namespace tiledb
