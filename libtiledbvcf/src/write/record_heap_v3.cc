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

#include "write/record_heap_v3.h"
#include "vcf/vcf_utils.h"

namespace tiledb {
namespace vcf {

void RecordHeapV3::clear() {
  while (!heap_.empty())
    heap_.pop();
}

bool RecordHeapV3::empty() const {
  return heap_.empty();
}

void RecordHeapV3::insert(
    VCFV3* vcf,
    NodeType type,
    SafeSharedBCFRec record,
    uint32_t start_pos,
    uint32_t end_pos,
    uint32_t sample_id) {
  // Sanity check start_pos is greater than the record start position.
  if (start_pos < (uint32_t)record->pos) {
    HtslibValueMem val;
    std::string contig(bcf_seqname(vcf->hdr(), record.get()));
    std::string str_type = type == NodeType::Record ? "record" : "anchor";
    throw std::runtime_error(
        "Error inserting " + str_type + " '" + contig + ":" +
        std::to_string(record->pos + 1) + "-" +
        std::to_string(
            VCFUtils::get_end_pos(vcf->hdr(), record.get(), &val) + 1) +
        "' into ingestion heap from sample ID " + std::to_string(sample_id) +
        "; sort start position " + std::to_string(start_pos + 1) +
        " cannot be less than start.");
  }

  auto node = std::unique_ptr<Node>(new Node);
  node->vcf = vcf;
  node->type = type;
  node->record = std::move(record);
  node->start_pos = start_pos;
  node->end_pos = end_pos;
  node->sample_id = sample_id;
  heap_.push(std::move(node));
}

const RecordHeapV3::Node& RecordHeapV3::top() const {
  return *heap_.top();
}

void RecordHeapV3::pop() {
  heap_.pop();
}

}  // namespace vcf
}  // namespace tiledb
