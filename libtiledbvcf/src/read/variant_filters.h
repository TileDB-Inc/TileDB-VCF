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

#ifndef TILEDB_VCF_VARIANT_FILTERS_H
#define TILEDB_VCF_VARIANT_FILTERS_H

#include "read/read_query_results.h"

namespace tiledb {
namespace vcf {

/**
 * Helper class that can perform filtering on records during export for types of
 * variants. Implements similar functionality to `bcftools view -v/-V`.
 */
class VariantFilters {
 public:
  enum class Variant { SNP, Indel, MNP, Ref, Bnd };
  enum class Type { Include, Exclude };

  explicit VariantFilters(VariantFilters::Type type);

 private:
  Variant variant_;
  Type type_;
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_VARIANT_FILTERS_H
