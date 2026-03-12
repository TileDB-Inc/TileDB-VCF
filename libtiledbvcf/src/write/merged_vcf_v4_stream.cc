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

#include "write/merged_vcf_v4_stream.h"
#include "merged_vcf_v4_stream.h"
#include "utils/logger_public.h"

namespace tiledb {
namespace vcf {

MergedVCFV4Stream::MergedVCFV4Stream(
    const std::vector<SampleAndIndex>& samples,
    uint32_t queue_size,
    uint64_t vcf_buffer_size,
    uint32_t anchor_gap,
    SharingMode mode)
    : SharedPtrPool<WriterRecordV4>(mode)
    , queue_(queue_size)
    , anchor_gap_(anchor_gap) {
  for (const auto& s : samples) {
    auto vcf = std::make_shared<VCFV4>(VCFV4::SharingMode::AUTOMATIC);
    vcf->set_max_record_buff_size(vcf_buffer_size);
    vcf->open(s.sample_uri, s.index_uri);
    vcfs_.push_back(vcf);
    vcf_has_records_.push_back(false);
  }
}

MergedVCFV4Stream::~MergedVCFV4Stream() {
  for (auto& vcf : vcfs_) {
    vcf->close();
  }
}

SharedWriterRecordV4 MergedVCFV4Stream::get_head(size_t i) {
  // Check if the VCF has records for this region
  if (!vcf_has_records_[i]) {
    return nullptr;
  }

  // Get the record
  auto& vcf = vcfs_[i];
  SafeSharedBCFRec record = vcf->front_record();
  if (record == nullptr) {
    return nullptr;
  }

  // Get the start and end positions of the record
  const uint32_t start_pos = record->pos;
  const uint32_t end_pos =
      VCFUtils::get_end_pos(vcf->hdr(), record.get(), &val_);

  // Create a new node for the record
  SharedWriterRecordV4 node = get_ptr_from_pool();
  node->vcf = vcf;
  node->type = WriterRecordV4::Type::Record;
  node->record = std::move(record);
  node->contig = region_.seq_name;
  node->start_pos = start_pos;
  node->end_pos = end_pos;
  node->sample_name = vcf->sample_name();

  // Pop the record from the VCF buffer
  vcf->pop_record();

  return node;
}

void MergedVCFV4Stream::parse(const Region& region) {
  if (!queue_.was_empty())
    throw std::runtime_error(
        "Error in parsing; record queue was unexpectedly not empty.");

  region_ = region;

  // Prepare to parse the VCF files
  for (size_t i = 0; i < vcfs_.size(); i++) {
    auto& vcf = vcfs_[i];
    // If seek returns false there is no records for this contig
    if (vcf->seek(region.seq_name, region.min)) {
      vcf_has_records_[i] = true;
    } else {
      vcf_has_records_[i] = false;
    }
  }

  // Initialize the merged head list
  initialize_merge_head_list(vcfs_.size());

  // Buffer records until there's no variants left to parse in any of the VCFs
  while (!merged_records_empty()) {
    SharedWriterRecordV4 node = next_head();
    WriterRecordV4& record = *node;
    // Push anchors from the heap that precede the head
    push_anchors(record);
    // Push the head to the queue; push() will block if the queue is full
    queue_.push(std::move(node));
    // Generate anchors and add them to the heap
    generate_anchors(record);
  }
  // Push any remaining anchors from the heap to the queue
  push_anchors();
  // Signal that the parse is complete by pushing a null pointer
  queue_.push(nullptr);
}

SharedWriterRecordV4 MergedVCFV4Stream::pop() {
  return queue_.pop();
}

void MergedVCFV4Stream::return_node(SharedWriterRecordV4& node) {
  return_ptr_to_pool(node);
}

void MergedVCFV4Stream::generate_anchors(const WriterRecordV4& node) {
  // Early exit if start and end are the same
  if (node.start_pos == node.end_pos) {
    return;
  }
  // Generate anchors between start and end position of node
  for (uint32_t start_pos = node.start_pos + anchor_gap_;
       start_pos <= node.end_pos;
       start_pos += anchor_gap_) {
    // Create a new anchor for the record
    SharedWriterRecordV4 anchor = get_ptr_from_pool();
    anchor->vcf = node.vcf;
    anchor->type = WriterRecordV4::Type::Anchor;
    anchor->record = node.record;
    anchor->contig = node.contig;
    anchor->start_pos = start_pos;
    anchor->end_pos = node.end_pos;
    anchor->sample_name = node.sample_name;
    // Add the anchor to the heap
    anchor_heap_.insert(std::move(anchor));
  }
}

void MergedVCFV4Stream::push_anchors() {
  while (!anchor_heap_.empty()) {
    // Push the anchor to the queue; push() will block if the queue is full
    queue_.push(anchor_heap_.top());
    anchor_heap_.pop();
  }
}

void MergedVCFV4Stream::push_anchors(const WriterRecordV4& node) {
  while (!anchor_heap_.empty()) {
    const SharedWriterRecordV4& top = anchor_heap_.top();
    if (top->start_pos <= node.start_pos) {
      // Push the anchor to the queue; push() will block if the queue is full
      queue_.push(top);
      anchor_heap_.pop();
    } else {
      break;
    }
  }
}

}  // namespace vcf
}  // namespace tiledb
