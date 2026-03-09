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

#include "write/record_merge_algorithm.h"
#include "utils/logger_public.h"

namespace tiledb {
namespace vcf {

void RecordMergeAlgorithm::insert_head(SharedWriterRecordV4 node, size_t i) {
  // Iterate the head list and add the node in the correct position
  auto itr = head_list_.begin();
  for (; itr != head_list_.end(); ++itr) {
    if (writer_record_v4_gt(*(itr->node), *node)) {
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
    SharedWriterRecordV4 head = get_head(i);
    // There's no records for this source, skip it
    if (head == nullptr)
      continue;
    insert_head(std::move(head), i);
  }
}

SharedWriterRecordV4 RecordMergeAlgorithm::next_head() {
  // Get the next node in the global order and remove it from the head list
  Head& next = head_list_.front();
  SharedWriterRecordV4 node = std::move(next.node);
  size_t i = next.stream_index;
  head_list_.pop_front();

  // Replace the record in the list with the head record from the same VCF
  SharedWriterRecordV4 head = get_head(i);
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
