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

#include <ranges>
#include <regex>

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

std::string uri_filename(std::string_view uri) {
  if (uri.ends_with("/"))
    return "";

  auto path_parts = split(std::string(uri), '/');

  assert(!path_parts.empty());
  return std::string(path_parts.back());
}

std::string uri_join(
    std::string_view dir, std::string_view filename, const char delimiter) {
  std::string result = std::string(dir);
  if (!result.ends_with(delimiter) && !result.empty())
    result += delimiter;
  result += filename;
  return result;
}

bool is_local_uri(std::string_view uri) {
  if (uri.starts_with("s3://"))
    return false;
  if (uri.starts_with("azure://"))
    return false;
  if (uri.starts_with("gcs://"))
    return false;
  if (uri.starts_with("hdfs://"))
    return false;

  return true;
}

std::string group_uri(const Group& group, std::string_view array) {
  try {
    auto member = group.member(array.data());
    return member.uri();
  } catch (const TileDBError& ex) {
    return "";
  }
}

std::string root_uri(
    std::string_view root_uri, std::string_view array, bool relative) {
  auto root = relative ? "" : root_uri;
  return utils::uri_join(root, array);
}

TileDBDataProtocol detect_tiledb_data_protocol(
    std::string_view uri, const Context& ctx) {
  if (!uri.starts_with("tiledb")) {
    return TileDBDataProtocol::TILEDBV2;
  }

  if (ctx.config().contains("rest.server_address")) {
    auto rest_server = ctx.config().get("rest.server_address");

    if (rest_server == "https://api.tiledb.com" ||
        rest_server == "https://api.dev.tiledb.io") {
      return TileDBDataProtocol::TILEDBV2;
    }

    return TileDBDataProtocol::TILEDBV3;
  }

  throw std::runtime_error(
      "Unknown TileDB Data Protocol. Missing rest server address.");
}

void validate_uri(std::string_view uri, const Context& ctx) {
  if (detect_tiledb_data_protocol(uri, ctx) == TileDBDataProtocol::TILEDBV2) {
    return;
  }

  std::regex storage_uri_regex(
      "^tiledb://.*/.*://.*$", std::regex_constants::ECMAScript);

  if (std::regex_match(uri.data(), storage_uri_regex)) {
    throw std::runtime_error(
        "Unsupported URI format - storage URI specification not supported on "
        "Carrara.");
  }
}

}  // namespace utils
}  // namespace vcf
}  // namespace tiledb
