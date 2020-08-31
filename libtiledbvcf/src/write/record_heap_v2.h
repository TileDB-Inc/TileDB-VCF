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

#ifndef TILEDB_VCF_RECORD_HEAP_V2_H
#define TILEDB_VCF_RECORD_HEAP_V2_H

#include <htslib/vcf.h>
#include <queue>

#include "vcf/vcf_v2.h"

namespace tiledb {
namespace vcf {

class RecordHeapV2 {
 public:
  enum class NodeType { Record, Anchor };
  struct Node {
    Node()
        : vcf(nullptr)
        , type(NodeType::Record)
        , record(nullptr)
        , sort_end_pos(std::numeric_limits<uint32_t>::max())
        , sample_id(std::numeric_limits<uint32_t>::max()) {
    }

    VCFV2* vcf;
    NodeType type;
    bcf1_t* record;
    uint32_t sort_end_pos;
    uint32_t sample_id;
  };

  void clear();

  bool empty() const;

  void insert(
      VCFV2* vcf,
      NodeType type,
      bcf1_t* record,
      uint32_t sort_end_pos,
      uint32_t sample_id);

  const Node& top() const;

  void pop();

 private:
  /**
   * Performs a greater-than comparison on two RecordHeapV2 Node structs. This
   * results in a min-heap sorted on end position first, breaking ties by sample
   * ID.
   */
  struct NodeCompareGT {
    bool operator()(
        const std::unique_ptr<Node>& a, const std::unique_ptr<Node>& b) const {
      auto a_end = a->sort_end_pos, b_end = b->sort_end_pos;
      return a_end > b_end || (a_end == b_end && a->sample_id > b->sample_id);
    }
  };

  /** A min-heap of BCF record structs, sorted on end pos. */
  typedef std::priority_queue<
      std::unique_ptr<Node>,
      std::vector<std::unique_ptr<Node>>,
      NodeCompareGT>
      record_heap_t;

  /** The heap. */
  record_heap_t heap_;
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_RECORD_HEAP_V2_H