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

#include "write/record_heap_v2.h"
#include "vcf/vcf_utils.h"

namespace tiledb {
namespace vcf {

void RecordHeapV2::clear() {
  while (!heap_.empty())
    heap_.pop();
}

bool RecordHeapV2::empty() const {
  return heap_.empty();
}

void RecordHeapV2::insert(
    VCFV2* vcf,
    NodeType type,
    bcf1_t* record,
    uint32_t sort_end_pos,
    uint32_t sample_id) {
  // Sanity check start <= end
  if (sort_end_pos < (uint32_t)record->pos) {
    HtslibValueMem val;
    std::string contig(bcf_seqname(vcf->hdr(), record));
    std::string str_type = type == NodeType::Record ? "record" : "anchor";
    throw std::runtime_error(
        "Error inserting " + str_type + " '" + contig + ":" +
        std::to_string(record->pos + 1) + "-" +
        std::to_string(VCFUtils::get_end_pos(vcf->hdr(), record, &val) + 1) +
        "' into ingestion heap from sample ID " + std::to_string(sample_id) +
        "; sort end position " + std::to_string(sort_end_pos + 1) +
        " cannot be less than start.");
  }

  auto node = std::unique_ptr<Node>(new Node);
  node->vcf = vcf;
  node->type = type;
  node->record = record;
  node->sort_end_pos = sort_end_pos;
  node->sample_id = sample_id;
  heap_.push(std::move(node));
}

const RecordHeapV2::Node& RecordHeapV2::top() const {
  return *heap_.top();
}

void RecordHeapV2::pop() {
  heap_.pop();
}

}  // namespace vcf
}  // namespace tiledb
