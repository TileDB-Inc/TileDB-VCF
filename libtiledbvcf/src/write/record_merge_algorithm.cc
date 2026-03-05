/**
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2026 TileDB, Inc.
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

#include "utils/logger_public.h"
#include "write/writer_worker_v4.h"

namespace tiledb {
namespace vcf {

inline bool RecordMergeAlgorithm::head_comparator_gt(
    const std::shared_ptr<RecordHeapV4::Node>& a,
    const std::shared_ptr<RecordHeapV4::Node>& b) const {
  return a->contig > b->contig ||
         (a->contig == b->contig && a->start_pos > b->start_pos) ||
         (a->contig == b->contig && a->start_pos == b->start_pos &&
          a->sample_name > b->sample_name);
}

void RecordMergeAlgorithm::insert_head(
    std::shared_ptr<RecordHeapV4::Node> node, size_t i) {
  // Iterate the head list and add the node in the correct position
  auto itr = head_list_.begin();
  for (; itr != head_list_.end(); ++itr) {
    if (head_comparator_gt(itr->node, node)) {
      head_list_.insert(itr, {std::move(node), i});
      break;
    }
  }
  if (itr == head_list_.end()) {
    head_list_.push_back({std::move(node), i});
  }
}

void RecordMergeAlgorithm::initialize_merge_head_list(size_t n) {
  for (size_t i = 0; i < n; i++) {
    std::shared_ptr<RecordHeapV4::Node> head = get_head(i);
    // There's no records for this source, skip it
    if (head == nullptr)
      continue;
    insert_head(std::move(head), i);
  }
}

std::shared_ptr<RecordHeapV4::Node> RecordMergeAlgorithm::next_head() {
  // Get the next node in the global order and remove it from the head list
  Head& next = head_list_.front();
  std::shared_ptr<RecordHeapV4::Node> node = std::move(next.node);
  size_t i = next.stream_index;
  head_list_.pop_front();

  // Replace the record in the list with the head record from the same VCF
  std::shared_ptr<RecordHeapV4::Node> head = get_head(i);
  if (head != nullptr) {
    insert_head(std::move(head), i);
  }

  return node;
}

bool RecordMergeAlgorithm::merged_records_empty() {
  return head_list_.empty();
}

}  // namespace vcf
}  // namespace tiledb
