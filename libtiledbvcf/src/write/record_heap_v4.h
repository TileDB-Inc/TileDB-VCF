/**
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020 TileDB, Inc.
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

#ifndef TILEDB_VCF_RECORD_HEAP_V4_H
#define TILEDB_VCF_RECORD_HEAP_V4_H

#include <htslib/vcf.h>
#include <queue>

#include "vcf/vcf_v4.h"

namespace tiledb {
namespace vcf {

class RecordHeapV4 {
 public:
  enum class NodeType { Record, Anchor };
  struct Node {
    Node()
        : vcf(nullptr)
        , type(NodeType::Record)
        , record(nullptr)
        , start_pos(std::numeric_limits<uint32_t>::max())
        , end_pos(std::numeric_limits<uint32_t>::max())
        , sample_name() {
    }

    std::shared_ptr<VCFV4> vcf;
    NodeType type;
    SafeSharedBCFRec record;
    std::string contig;
    uint32_t start_pos;
    uint32_t end_pos;
    std::string sample_name;
  };

  void clear();

  bool empty() const;

  void insert(
      std::shared_ptr<VCFV4> vcf,
      NodeType type,
      SafeSharedBCFRec record,
      const std::string& contig,
      uint32_t start_pos,
      uint32_t end_pos,
      const std::string& sample_name);

  void insert(const Node& node);

  const Node& top() const;

  void pop();

  size_t size();

 private:
  /**
   * Performs a greater-than comparison on two RecordHeapV4 Node structs. This
   * results in a min-heap sorted on start position first, breaking ties by
   * sample ID.
   */
  struct NodeCompareGT {
    bool operator()(
        const std::unique_ptr<Node>& a, const std::unique_ptr<Node>& b) const {
      auto a_start = a->start_pos, b_start = b->start_pos;
      auto a_contig = a->contig, b_contig = b->contig;
      return a_contig > b_contig ||
             (a_contig == b_contig && a_start > b_start) ||
             (a_contig == b_contig && a_start == b_start &&
              a->sample_name > b->sample_name);
    }
  };

  /** A min-heap of BCF record structs, sorted on start pos. */
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

#endif  // TILEDB_VCF_RECORD_HEAP_V4_H
