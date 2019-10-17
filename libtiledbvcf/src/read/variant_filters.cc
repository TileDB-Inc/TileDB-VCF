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

#include "read/variant_filters.h"

namespace tiledb {
namespace vcf {

VariantFilter::VariantFilter() {
  type_ = Type::Include;
  variants_.push_back(Variant::Any);
}

VariantFilter::VariantFilter(Type type)
    : type_(type) {
}

void VariantFilter::get_required_attributes(
    std::set<std::string>* attrs) const {
  for (const auto v : variants_) {
    switch (v) {
      case Variant::Ref:
        attrs->insert(TileDBVCFDataset::AttrNames::alleles);
        break;
      case Variant::Any:
        // None required.
        break;
      default:
        throw std::runtime_error("Unimplemented variant type for filtering");
    }
  }
}

void VariantFilter::add_variant(Variant variant) {
  variants_.push_back(variant);
}

bool VariantFilter::evaluate(
    const ReadQueryResults& results, uint64_t cell_idx) const {
  if (variants_.size() == 1 && variants_[0] == Variant::Any)
    return type_ == Type::Include;

  bool is_one_of_variants = true;
  for (const auto v : variants_) {
    switch (v) {
      case Variant::Ref:
        is_one_of_variants |= is_ref(results, cell_idx);
        break;
      case Variant::Any:
        // Do nothing.
        break;
      default:
        throw std::runtime_error("Unimplemented variant type for filtering");
    }
  }

  return type_ == Type::Include ? is_one_of_variants : !is_one_of_variants;
}

bool VariantFilter::is_ref(const ReadQueryResults& results, uint64_t cell_idx) {
  // Get alleles attribute value for the cell.
  const uint64_t num_cells = results.num_cells();
  const uint64_t buff_var_size = results.alleles_size().second;
  const auto& offsets = results.buffers()->alleles().offsets();
  const uint64_t offset = offsets[cell_idx];
  const uint64_t next_offset =
      cell_idx == num_cells - 1 ? buff_var_size : offsets[cell_idx + 1];
  const uint64_t nbytes = next_offset - offset;
  const char* data = results.buffers()->alleles().data<char>() + offset;

  // Find the first comma (alleles attribute is CSV).
  const char* p = data;
  while (*p != ',' && (p - data) < nbytes)
    ++p;
  if (*p != ',')
    throw std::runtime_error(
        "Variant filter error; alleles value '" + std::string(data, nbytes) +
        "' has unexpected format.");
  ++p;

  // Check the second element (ALT) against the constant string <NON_REF>.
  return strncmp("<NON_REF>", p, 9) == 0;
}

}  // namespace vcf
}  // namespace tiledb
