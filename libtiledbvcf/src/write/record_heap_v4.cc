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

#include "write/record_heap_v4.h"
#include "vcf/vcf_utils.h"

namespace tiledb {
namespace vcf {

void RecordHeapV4::clear() {
  while (!heap_.empty())
    heap_.pop();
}

bool RecordHeapV4::empty() const {
  return heap_.empty();
}

void RecordHeapV4::insert(
    std::shared_ptr<VCFV4> vcf,
    NodeType type,
    SafeSharedBCFRec record,
    const std::string& contig,
    uint32_t start_pos,
    uint32_t end_pos,
    const std::string& sample_name) {
  // Sanity check start_pos is greater than the record start position.
  if (start_pos < (uint32_t)record->pos) {
    HtslibValueMem val;
    std::string str_type = type == NodeType::Record ? "record" : "anchor";
    throw std::runtime_error(
        "Error inserting " + str_type + " '" + contig + ":" +
        std::to_string(record->pos + 1) + "-" +
        std::to_string(
            VCFUtils::get_end_pos(vcf->hdr(), record.get(), &val) + 1) +
        "' into ingestion heap from sample " + sample_name +
        "; sort start position " + std::to_string(start_pos + 1) +
        " cannot be less than start.");
  }

  auto node = std::unique_ptr<Node>(new Node);
  node->vcf = vcf;
  node->type = type;
  node->record = std::move(record);
  node->contig = contig;
  node->start_pos = start_pos;
  node->end_pos = end_pos;
  node->sample_name = sample_name;
  heap_.push(std::move(node));
}

void RecordHeapV4::insert(const Node& node) {
  insert(
      node.vcf,
      node.type,
      node.record,
      node.contig,
      node.start_pos,
      node.end_pos,
      node.sample_name);
}

const RecordHeapV4::Node& RecordHeapV4::top() const {
  return *heap_.top();
}

void RecordHeapV4::pop() {
  heap_.pop();
}

size_t RecordHeapV4::size() {
  return heap_.size();
}

}  // namespace vcf
}  // namespace tiledb
