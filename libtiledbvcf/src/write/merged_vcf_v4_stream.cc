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
#include "utils/logger_public.h"

namespace tiledb {
namespace vcf {

MergedVCFV4Stream::MergedVCFV4Stream(
    const std::vector<SampleAndIndex>& samples,
    uint32_t queue_size,
    uint64_t vcf_buffer_size)
    : queue_(queue_size) {
  for (const auto& s : samples) {
    auto vcf = std::make_shared<VCFV4>();
    vcf->set_max_record_buff_size(vcf_buffer_size);
    vcf->open(s.sample_uri, s.index_uri);
    vcfs_.push_back(vcf);
  }
}

void MergedVCFV4Stream::parse(const Region& region) {
  LOG_DEBUG(
      "MergedVCFV4Stream: parse {}:{}-{}",
      region.seq_name,
      region.min,
      region.max);

  if (!queue_.was_empty())
    throw std::runtime_error(
        "Error in parsing; record queue was unexpectedly not empty.");

  // Initialize the head list with the first record from each sample
  for (auto& vcf : vcfs_) {
    // If seek returns false there is no records for this contig
    if (!vcf->seek(region.seq_name, region.min))
      continue;
    SafeSharedBCFRec r = vcf->front_record();
    // Sample has no records at this region, skip it
    if (r == nullptr)
      continue;
    insert_head(r, vcf, region.seq_name, vcf->sample_name());
  }

  // Buffer records until there's no variants left to parse in any of the VCFs
  while (!head_list_.empty()) {
    // Get the next record in the global order and remove it from the head list
    std::unique_ptr<RecordHeapV4::Node> next = std::move(head_list_.front());
    head_list_.pop_front();
    // Replace the record in the list with the head record from the same VCF
    auto& vcf = next->vcf;
    SafeSharedBCFRec head = vcf->front_record();
    if (head != nullptr) {
      insert_head(head, vcf, vcf->contig_name(head.get()), next->sample_name);
    }
    // Add the next record to the queue; push() will block if the queue is full
    queue_.push(std::move(next));
  }
}

std::unique_ptr<RecordHeapV4::Node> MergedVCFV4Stream::pop() {
  return queue_.pop();
}

inline bool MergedVCFV4Stream::head_comparator_gt(
    const std::unique_ptr<RecordHeapV4::Node>& a,
    const std::unique_ptr<RecordHeapV4::Node>& b) const {
  auto a_start = a->start_pos, b_start = b->start_pos;
  auto a_contig = a->contig, b_contig = b->contig;
  return a_contig > b_contig || (a_contig == b_contig && a_start > b_start) ||
         (a_contig == b_contig && a_start == b_start &&
          a->sample_name > b->sample_name);
}

void MergedVCFV4Stream::insert_head(
    SafeSharedBCFRec& record,
    std::shared_ptr<VCFV4> vcf,
    const std::string& contig,
    const std::string& sample_name) {
  // Get the start and end positions of the record
  const uint32_t start_pos = record->pos;
  const uint32_t end_pos =
      VCFUtils::get_end_pos(vcf->hdr(), record.get(), &val_);
  // Duplicate the record so the original can be reused by vcf
  SafeSharedBCFRec record_copy(bcf_dup(record.get()), bcf_destroy);
  bcf_unpack(record_copy.get(), BCF_UN_ALL);
  // Create a new node for the record
  auto node = std::unique_ptr<RecordHeapV4::Node>(new RecordHeapV4::Node);
  node->vcf = vcf;
  node->type = RecordHeapV4::NodeType::Record;
  node->record = std::move(record_copy);
  node->contig = contig;
  node->start_pos = start_pos;
  node->end_pos = end_pos;
  node->sample_name = sample_name;
  // Iterate the head list and add the node in the correct position
  auto itr = head_list_.begin();
  for (; itr != head_list_.end(); ++itr) {
    if (head_comparator_gt(*itr, node)) {
      head_list_.insert(itr, std::move(node));
      break;
    }
  }
  if (itr == head_list_.end()) {
    head_list_.push_back(std::move(node));
  }
  // Pop the record from the VCF buffer and return it to the pool at the same
  // time
  vcf->pop_record();
  vcf->return_record(record);
}

}  // namespace vcf
}  // namespace tiledb
