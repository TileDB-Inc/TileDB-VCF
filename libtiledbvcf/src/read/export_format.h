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

// since #define of DELETE can occur after original inclusion of export_format.h
// and can still cause problems with references elsewhere later, address outside
// of header guards, so might be included multiple times to 'cleanup' the anomaly
// (if clang format will allow that.)
//  #if _MSC_VER
  #if (defined(DELETE))
// note: 'DELETE' is #define'd somewhere within windows headers as
// something resolving to '(0x00010000L)', which causes problems.
#undef DELETE
// If this is encountered 'too often', further consideration might be given to
// simply qualifying the currently unqualified definition of
// TILEDB_QUERY_TYPE_ENUM in query_type.h so 'DELETE' and any other enum items
// here would not collide with this windows definition known to be in conflict.
#endif
//  #endif

#ifndef TILEDB_VCF_EXPORT_FORMAT_H
#define TILEDB_VCF_EXPORT_FORMAT_H

namespace tiledb {
namespace vcf {

/** For to-disk exports, the output format. */
enum class ExportFormat { CompressedBCF, BCF, VCFGZ, VCF, TSV, DELETE };

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_EXPORT_FORMAT_H
