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
   * Resize buffer for the given set of attributes to the given size.
   *
   * Used when reading.
   *
   * @param extra List of attributes to allocate buffers for.
   * @param size_mb Size (MB) of allocation.
   */
  void allocate_fixed(
      const std::set<std::string>& attr_names, unsigned size_mb);

  /**
   * Returns the sum of sizes of all buffers (in bytes). This includes the size
   * of the offset buffers.
   */
  uint64_t total_size() const;

  /** Sets these buffers on the given TileDB query. */
  void set_buffers(tiledb::Query* query) const;

  /** Clears all buffers. */
  void clear();

  /** Coords buffer. */
  const Buffer& coords() const;

  /** Coords buffer. */
  Buffer& coords();

  /** pos buffer. */
  const Buffer& pos() const;

  /** pos buffer. */
  Buffer& pos();

  /** real_end buffer. */
  const Buffer& real_end() const;

  /** real_end buffer. */
  Buffer& real_end();

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
  const std::map<std::string, Buffer>& extra_attrs() const;

  /** Set of buffers for optional "extracted"/"extra" info/fmt attributes. */
  std::map<std::string, Buffer>& extra_attrs();

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
  /** coords (uint32_t) */
  Buffer coords_;

  /** pos attribute (uint32_t) */
  Buffer pos_;

  /** real_end attribute (uint32_t) */
  Buffer real_end_;

  /** qual attribute (float) */
  Buffer qual_;

  /** CSV alleles list (var-len char) */
  Buffer alleles_;

  /** ID string (var-len char) */
  Buffer id_;

  /** Filter IDs list (var-len int32_t) */
  Buffer filter_ids_;

  /** info (var-len uint8_t) */
  Buffer info_;

  /** fmt (var-len uint8_t) */
  Buffer fmt_;

  /** Optional extra extracted info/fmt attributes (all var-len uint8_t). */
  std::map<std::string, Buffer> extra_attrs_;

  /**
   * List of (var_num, name, buffer, datatype_size) for all fixed-alloced
   * attributes.
   */
  std::vector<std::tuple<bool, std::string, Buffer*, unsigned>> fixed_alloc_;
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_ATTRIBUTE_BUFFER_SET_H
