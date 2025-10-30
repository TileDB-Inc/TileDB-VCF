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

#ifndef TILEDB_VCF_URI_H
#define TILEDB_VCF_URI_H

#include <string>

namespace tiledb {
namespace vcf {

namespace utils {

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
 * Checks if a file path is local or remote
 * @param uri to check
 * @return true if file is local path (file:// or no prefix), else false
 */
bool is_local_uri(const std::string& uri);

}  // namespace utils
}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_URI_H
