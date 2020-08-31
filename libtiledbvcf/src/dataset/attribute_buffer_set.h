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

#ifndef TILEDB_VCF_ATTRIBUTE_BUFFER_SET_H
#define TILEDB_VCF_ATTRIBUTE_BUFFER_SET_H

#include <string>
#include <vector>

#include "dataset/tiledbvcfdataset.h"
#include "utils/buffer.h"

namespace tiledb {
namespace vcf {

/**
 * Class holding a set of columnar TileDB attribute buffers, suitable for
 * reading/writing TileDBVCFDatasets. This is not the same buffer setup as used
 * for in-memory exports, as the persisted representation is not the same as
 * what is exported for in-memory.
 */
class AttributeBufferSet {
 public:
  /**
   * Resize buffers for the given set of attributes using the given allocation
   * budget.
   *
   * Used when reading.
   *
   * @param extra List of attributes to allocate buffers for.
   * @param mem_budget_mb Memory budget (MB) of sum of allocations.
   */
  void allocate_fixed(
      const std::unordered_set<std::string>& attr_names,
      unsigned mem_budget_mb);

  /**
   * Returns the sum of sizes of all buffers (in bytes). This includes the size
   * of the offset buffers.
   */
  uint64_t total_size() const;

  /** Sets these buffers on the given TileDB query. */
  void set_buffers(tiledb::Query* query, unsigned version) const;

  /** Clears all buffers. */
  void clear();

  /** sample buffer. */
  const Buffer& sample() const;

  /** sample buffer. */
  Buffer& sample();

  /** start_pos buffer. */
  const Buffer& start_pos() const;

  /** start_pos buffer. */
  Buffer& start_pos();

  /** real_start_pos buffer. */
  const Buffer& real_start_pos() const;

  /** real_start_pos buffer. */
  Buffer& real_start_pos();

  /** pos buffer. */
  const Buffer& pos() const;

  /** pos buffer. */
  Buffer& pos();

  /** real_end buffer. */
  const Buffer& real_end() const;

  /** real_end buffer. */
  Buffer& real_end();

  /** end_pos buffer. */
  const Buffer& end_pos() const;

  /** end_pos buffer. */
  Buffer& end_pos();

  /** qual buffer. */
  const Buffer& qual() const;

  /** qual buffer. */
  Buffer& qual();

  /** id buffer. */
  const Buffer& id() const;

  /** id buffer. */
  Buffer& id();

  /** alleles buffer. */
  const Buffer& alleles() const;

  /** alleles buffer. */
  Buffer& alleles();

  /** filter_ids buffer. */
  const Buffer& filter_ids() const;

  /** filter_ids buffer. */
  Buffer& filter_ids();

  /** info buffer. */
  const Buffer& info() const;

  /** info buffer. */
  Buffer& info();

  /** fmt buffer. */
  const Buffer& fmt() const;

  /** fmt buffer. */
  Buffer& fmt();

  /** Set of buffers for optional "extracted"/"extra" info/fmt attributes. */
  const std::unordered_map<std::string, Buffer>& extra_attrs() const;

  /** Set of buffers for optional "extracted"/"extra" info/fmt attributes. */
  std::unordered_map<std::string, Buffer>& extra_attrs();

  /**
   * Gets the optional buffer for the given attribute. Returns true if a buffer
   * is found.
   */
  bool extra_attr(const std::string& name, const Buffer** buffer) const;

  /**
   * Gets the optional buffer for the given attribute. Returns true if a buffer
   * is found.
   */
  bool extra_attr(const std::string& name, Buffer** buffer);

 private:
  /** sample v3/v2 dimension (uint32_t) */
  Buffer sample_;

  /** start_pos v3 dimension (uint32_t) */
  Buffer start_pos_;

  /** pos v2 dimension (uint32_t) */
  Buffer pos_;

  /** real_end v2 attribute (uint32_t) */
  Buffer real_end_;

  /** real_start_pos v3 attribute (uint32_t) */
  Buffer real_start_pos_;

  /** end_pos v3 attribute, v2 dimension (uint32_t) */
  Buffer end_pos_;

  /** qual v3/v2 attribute (float) */
  Buffer qual_;

  /** CSV alleles v3/v2 attribute list (var-len char) */
  Buffer alleles_;

  /** ID string v3/v2 attribute (var-len char) */
  Buffer id_;

  /** Filter IDs v3/v2 attribute list (var-len int32_t) */
  Buffer filter_ids_;

  /** info v3/v2 attribute (var-len uint8_t) */
  Buffer info_;

  /** fmt v3/v2 attribute (var-len uint8_t) */
  Buffer fmt_;

  /** Optional extra extracted info/fmt attributes (all var-len uint8_t). */
  std::unordered_map<std::string, Buffer> extra_attrs_;

  /**
   * List of (var_num, name, buffer, datatype_size) for all fixed-alloced
   * attributes.
   */
  std::vector<std::tuple<bool, std::string, Buffer*, unsigned>> fixed_alloc_;
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_ATTRIBUTE_BUFFER_SET_H
