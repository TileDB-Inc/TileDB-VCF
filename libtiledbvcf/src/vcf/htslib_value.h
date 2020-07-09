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

#ifndef TILEDB_VCF_HTSLIB_VALUE_H
#define TILEDB_VCF_HTSLIB_VALUE_H

#include "utils/utils.h"

namespace tiledb {
namespace vcf {

/**
 * Helper struct for reusing memory when calling:
 *   - bcf_get_info_values
 *   - bcf_get_format_values
 * As otherwise those functions will always allocate new memory.
 */
struct HtslibValueMem {
  HtslibValueMem()
      : dst(nullptr)
      , ndst(0)
      , type_for_ndst(-1) {
  }

  ~HtslibValueMem() {
    if (dst != nullptr)
      free(dst);
  }

  HtslibValueMem(const HtslibValueMem&) = delete;
  HtslibValueMem(HtslibValueMem&&) = delete;
  HtslibValueMem& operator=(const HtslibValueMem&) = delete;
  HtslibValueMem& operator=(HtslibValueMem&&) = delete;

  void* dst;
  int ndst;
  int type_for_ndst;

  /**
   * The functions bcf_get_info_values() and bcf_get_format_values() take an
   * `ndst` parameter which is the number of elements allocated in the `dst`
   * buffer argument. We can reuse `dst` buffers as long as we inform htslib how
   * big the buffer allocation is so it knows if it needs to realloc.
   *
   * This function converts a number of elements of an old type (ndst) into the
   * corresponding number of elements of a new type.
   *
   * Example: if the old type was char, and ndst is 7, that corresponds to a
   * buffer allocation of 7 bytes. If the new type is int32, the new ndst value
   * is 7 / sizeof(int32) = 1, because the buffer allocation can only fit 1
   * int32 value.
   *
   * @param ndst Number of buffer elements in a buffer of the old type.
   * @param new_type New buffer type
   * @param old_type_for_ndst Pointer to the old buffer type, which will be set
   *      to the new type by this function.
   * @return (ndst * sizeof(old_type) / sizeof(new_type)). If old_type_for_ndst
   *      is -1, just return ndst unmodified.
   */
  static int convert_ndst_for_type(
      int ndst, int new_type, int* old_type_for_ndst) {
    assert(old_type_for_ndst != nullptr);

    if (*old_type_for_ndst < 0) {
      *old_type_for_ndst = new_type;
      return ndst;
    } else {
      auto old_nbytes = ndst * utils::bcf_type_size(*old_type_for_ndst);
      auto new_ndst = old_nbytes / utils::bcf_type_size(new_type);
      *old_type_for_ndst = new_type;
      return new_ndst;
    }
  }
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_HTSLIB_VALUE_H
