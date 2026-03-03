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

#include "write/writer_worker_v4.h"
#include "merged_vcf_v4_stream.h"
#include "utils/logger_public.h"
#include "writer_worker_v4.h"

namespace tiledb {
namespace vcf {

WriterWorkerV4::WriterWorkerV4(int id, int num_vcf_streams)
    : id_(id)
    , num_vcf_streams_(num_vcf_streams)
    , dataset_(nullptr)
    , records_buffered_(0)
    , anchors_buffered_(0) {
}

void WriterWorkerV4::init(
    const TileDBVCFDataset& dataset,
    const IngestionParams& params,
    const std::vector<SampleAndIndex>& samples) {
  dataset_ = &dataset;

  // Partition the samples into equal sized parts
  const int num_vcf_streams =
      std::min(num_vcf_streams_, static_cast<int>(samples.size()));
  std::vector<std::vector<SampleAndIndex>> partitioned_samples(num_vcf_streams);
  for (int i = 0; i < samples.size(); i++) {
    const int j = i % num_vcf_streams;
    partitioned_samples[j].push_back(samples[i]);
  }

  // Create the VCF streams
  for (auto& stream_samples : partitioned_samples) {
    // TODO: make queue_size configurable
    MergedVCFV4Stream* stream = new MergedVCFV4Stream(
        stream_samples, 1024, params.max_record_buffer_size);
    vcf_streams_.emplace_back(stream);
  }

  for (const auto& attr : dataset.metadata().extra_attributes) {
    record_buffers_.extra_attrs()[attr] = Buffer();
    anchor_buffers_.extra_attrs()[attr] = Buffer();
  }
}

uint64_t WriterWorkerV4::records_buffered() const {
  return records_buffered_;
}

uint64_t WriterWorkerV4::anchors_buffered() const {
  return anchors_buffered_;
}

inline bool WriterWorkerV4::head_comparator_gt(
    const std::unique_ptr<RecordHeapV4::Node>& a,
    const std::unique_ptr<RecordHeapV4::Node>& b) const {
  auto a_start = a->start_pos, b_start = b->start_pos;
  auto a_contig = a->contig, b_contig = b->contig;
  return a_contig > b_contig || (a_contig == b_contig && a_start > b_start) ||
         (a_contig == b_contig && a_start == b_start &&
          a->sample_name > b->sample_name);
}

void WriterWorkerV4::insert_head(
    std::unique_ptr<RecordHeapV4::Node> node, int i) {
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

bool WriterWorkerV4::parse(const Region& region) {
  if (records_buffered_ > 0)
    throw std::runtime_error("Error in parsing; record buffers aren't empy.");
  if (!anchor_heap_.empty())
    throw std::runtime_error("Error in parsing; anchor heap not empty.");

  region_ = region;

  LOG_DEBUG(
      "WriteWorker4(id={})::parse {}:{}-{}",
      id_,
      region.seq_name,
      region.min,
      region.max);

  // Initialize the head list with the first record from each stream
  for (int i = 0; i < vcf_streams_.size(); i++) {
    MergedVCFV4Stream* vcf = vcf_streams_[i].get();
    TRY_CATCH_THROW(vcf_stream_tasks.push_back(std::async(
        std::launch::async, &MergedVCFV4Stream::parse, vcf, region)));
    std::unique_ptr<RecordHeapV4::Node> head = vcf->pop();
    // The stream has no records at this region, skip it
    if (head == nullptr)
      continue;
    insert_head(std::move(head), i);
  }

  // Start buffering records
  return resume();
}

bool WriterWorkerV4::resume() {
  // Buffer VCF records in-memory for writing to the TileDB array. The records
  // are expected to be sorted in ascending order by there start position and
  // duplicate start positions are allowed. Record ranges may overlap. Records
  // are sorted by a merge algorithm and anchor points are ordered by using a
  // small record heap.
  //
  // 1. Pop the top record from `head_list_` and buffer the record.
  //     a. This includes generating allele count, variant stats, and sample
  //        stats data
  // 2. Generate anchors for the record and push them onto the `anchor_heap_`.
  // 3. Buffer anchors from the `anchor_heap_`
  //     a. Any anchor on the heap with start position up to the current
  //        record's start position can be buffered.
  // 3. Repeat step (1) until the `head_list_` is empty or the buffers are full.
  //
  // When a region has been parsed or the buffers are full, upstream code
  // flushes all of the buffers, with the possible exception of samples stats.
  // These buffers are only flushed when the buffers are flushed AND finalized.

  record_buffers_.clear();
  anchor_buffers_.clear();
  records_buffered_ = 0;
  anchors_buffered_ = 0;

  // Buffer records until there's no variants left to parse in any of the VCF
  // streams or until the buffer is full
  while (!head_list_.empty()) {
    // Get the next node in the global order and remove it from the head list
    Head& next = head_list_.front();
    std::unique_ptr<RecordHeapV4::Node> node = std::move(next.node);
    int i = next.stream_index;
    head_list_.pop_front();
    if (node->start_pos > region_.max) {
      LOG_FATAL(
          "WriterWorkerV4(id={})::resume Next record starts outside of the "
          "region: {} > {}",
          id_,
          node->start_pos,
          region_.max);
    }

    // Replace the record in the list with the head record from the same VCF
    auto& vcf = vcf_streams_[i];
    std::unique_ptr<RecordHeapV4::Node> head = vcf->pop();
    if (head != nullptr) {
      insert_head(std::move(head), i);
    }

    // Buffer the node's record
    buffer_record(record_buffers_, *node);
    // Generate and buffer anchors
    generate_anchors(*node);
    buffer_anchors(*node);

    // Check if the buffers are full
    const uint64_t buffer_size_mb = total_size() >> 20;
    if (buffer_size_mb > max_total_buffer_size_mb_) {
      LOG_DEBUG(
          "WriterWorkerV4(id={})::resume buffers full, total buffers size = {} "
          "MiB",
          id_,
          buffer_size_mb);
      return false;
    }
  }
  // Drain the anchor heap for returning for the final time
  buffer_anchors();
  LOG_TRACE(
      "WriterWorkerV4(id={})::resume all records parsed, total buffers size = "
      "{} MiB",
      id_,
      total_size() >> 20);
  return true;
}

const AttributeBufferSet& WriterWorkerV4::buffers() const {
  return record_buffers_;
}

void WriterWorkerV4::flush_ingestion_tasks(bool finalize) {
  ac_.flush(finalize);
  vs_.flush(finalize);
  if (finalize) {
    ss_.flush(finalize);
  }
}

void WriterWorkerV4::write_buffers(
    std::unique_ptr<Query>& record_query,
    std::unique_ptr<Query>& anchor_query,
    bool finalize) {
  // Write the buffered records
  if (records_buffered_ > 0) {
    write_buffers(record_query, record_buffers_, finalize);
  }
  if (anchors_buffered_ > 0) {
    write_buffers(anchor_query, anchor_buffers_, finalize);
  }
  // Flush the stats arrays
  flush_ingestion_tasks(finalize);
}

uint64_t WriterWorkerV4::total_size() const {
  return record_buffers_.total_size() + anchor_buffers_.total_size() +
         ac_.total_size() + vs_.total_size();
}

void WriterWorkerV4::buffer_record(
    AttributeBufferSet& buffers, const RecordHeapV4::Node& node) {
  auto vcf = node.vcf;
  bcf1_t* r = node.record.get();
  bcf_hdr_t* hdr = vcf->hdr();
  const std::string contig = vcf->contig_name(r);
  const std::string sample_name = node.sample_name;
  const uint32_t col = node.start_pos;
  const uint32_t pos = r->pos;
  const uint32_t end_pos = VCFUtils::get_end_pos(hdr, r, &val_);

  // Ingestion tasks process only NodeType::Record
  if (node.type == RecordHeapV4::NodeType::Record) {
    ac_.process(hdr, sample_name, contig, pos, r);
    ss_.process(hdr, sample_name, contig, pos, r);
    vs_.process(hdr, sample_name, contig, pos, r);
  }

  buffers.sample_name().offsets().push_back(buffers.sample_name().size());
  buffers.sample_name().append(sample_name.c_str(), sample_name.length());
  buffers.contig().offsets().push_back(buffers.contig().size());
  buffers.contig().append(contig.c_str(), contig.length());
  buffers.start_pos().append(&col, sizeof(uint32_t));
  buffers.qual().append(&r->qual, sizeof(float));
  buffers.real_start_pos().append(&pos, sizeof(uint32_t));
  buffers.end_pos().append(&end_pos, sizeof(uint32_t));

  // ID string (include null terminator)
  const size_t id_size = strlen(r->d.id) + 1;
  buffers.id().offsets().push_back(buffers.id().size());
  buffers.id().append(r->d.id, id_size);

  // Alleles
  buffer_alleles(r, &buffers.alleles());

  // Filter IDs
  buffers.filter_ids().offsets().push_back(buffers.filter_ids().size());
  buffers.filter_ids().append(&(r->d.n_flt), sizeof(int32_t));
  buffers.filter_ids().append(r->d.flt, sizeof(int32_t) * r->d.n_flt);

  // Start expecting info on all the extra buffers
  for (auto& it : buffers.extra_attrs())
    it.second.start_expecting();

  // Extract INFO fields into separate attributes
  std::vector<bool> infos_extracted(r->n_info, false);
  unsigned n_info_as_attr = 0;
  for (unsigned i = 0; i < r->n_info; i++) {
    bcf_info_t* info = r->d.info + i;
    int info_id = info->key;
    const char* key = bcf_hdr_int2id(hdr, BCF_DT_ID, info_id);

    Buffer* buff;
    if (buffers.extra_attr(std::string("info_") + key, &buff)) {
      // No need to store the string key, as it's an extracted attribute.
      const bool include_key = false;
      buffer_info_field(hdr, r, info, include_key, &val_, buff);
      // Mark key so we don't add it again in the info blob attribute
      infos_extracted[i] = true;
      n_info_as_attr++;
    }
  }

  // Extract FMT fields into separate attributes
  std::vector<bool> fmts_extracted(r->n_fmt, false);
  unsigned n_fmt_as_attr = 0;
  for (unsigned i = 0; i < r->n_fmt; i++) {
    bcf_fmt_t* fmt = r->d.fmt + i;
    int fmt_id = fmt->id;
    const char* key = bcf_hdr_int2id(hdr, BCF_DT_ID, fmt_id);
    Buffer* buff;
    if (buffers.extra_attr(std::string("fmt_") + key, &buff)) {
      // No need to store the string key, as it's an extracted attribute.
      const bool include_key = false;
      buffer_fmt_field(hdr, r, fmt, include_key, &val_, buff);
      // Mark key so we don't add it again in the fmt blob attribute
      fmts_extracted[i] = true;
      n_fmt_as_attr++;
    }
  }

  // Remaining INFO/FMT fields go into blob attributes
  Buffer& info = buffers.info();
  info.offsets().push_back(info.size());
  const uint32_t non_attr_info = r->n_info - n_info_as_attr;
  info.append(&non_attr_info, sizeof(uint32_t));
  for (unsigned i = 0; i < r->n_info; i++) {
    if (!infos_extracted[i]) {
      bcf_info_t* info_field = r->d.info + i;
      buffer_info_field(hdr, r, info_field, true, &val_, &info);
    }
  }

  Buffer& fmt = buffers.fmt();
  fmt.offsets().push_back(fmt.size());
  const uint32_t non_attr_fmt = r->n_fmt - n_fmt_as_attr;
  fmt.append(&non_attr_fmt, sizeof(uint32_t));
  for (unsigned i = 0; i < r->n_fmt; i++) {
    if (!fmts_extracted[i]) {
      bcf_fmt_t* fmt_field = r->d.fmt + i;
      buffer_fmt_field(hdr, r, fmt_field, true, &val_, &fmt);
    }
  }

  // Make sure any extra attributes get dummy values if no info was written.
  for (auto& it : buffers.extra_attrs())
    it.second.stop_expecting();

  if (node.type == RecordHeapV4::NodeType::Record)
    records_buffered_++;
  else
    anchors_buffered_++;
}

void WriterWorkerV4::buffer_alleles(bcf1_t* record, Buffer* buffer) {
  buffer->offsets().push_back(buffer->size());

  // Alleles list is null-separated but we store as null-terminated CSV to make
  // it easier to interact with on export.
  char* allele_ptr = record->d.als;
  for (unsigned i = 0; i < record->n_allele; i++) {
    const char comma = ',';
    auto len = std::strlen(allele_ptr);
    buffer->append(allele_ptr, len);
    if (i < static_cast<unsigned>(record->n_allele - 1))
      buffer->append(&comma, sizeof(char));
    allele_ptr += len + 1;
  }

  const char nul = '\0';
  buffer->append(&nul, sizeof(char));
}

void WriterWorkerV4::buffer_info_field(
    const bcf_hdr_t* hdr,
    bcf1_t* r,
    const bcf_info_t* info,
    bool include_key,
    HtslibValueMem* val,
    Buffer* buff) {
  int info_id = info->key;
  const char* key = bcf_hdr_int2id(hdr, BCF_DT_ID, info_id);
  int type = bcf_hdr_id2type(hdr, BCF_HL_INFO, info->key);
  val->ndst = HtslibValueMem::convert_ndst_for_type(
      val->ndst, type, &val->type_for_ndst);
  int num_vals = bcf_get_info_values(hdr, r, key, &val->dst, &val->ndst, type);
  if (num_vals < 0)
    throw std::runtime_error(
        "Error reading INFO value for '" + std::string(key) + "'; " +
        std::to_string(num_vals));

  if (buff->expecting())
    buff->offsets().push_back(buff->size());

  // Write an INFO value: [keystr],type,nvalues,values
  if (include_key)
    buff->append(key, strlen(key) + 1);
  buff->append(&type, sizeof(int));
  buff->append(&num_vals, sizeof(int));
  if (val->dst) {
    if (type == BCF_HT_FLAG) {
      // Write a dummy value for flags.
      int flag = 1;
      buff->append(&flag, num_vals * utils::bcf_type_size(type));
    } else {
      buff->append(val->dst, num_vals * utils::bcf_type_size(type));
    }
  } else {
    // val->dst can be NULL if the only INFO value is a flag
    assert(num_vals == 1);
    int dummy = 0;
    buff->append(&dummy, num_vals * utils::bcf_type_size(type));
  }
}

void WriterWorkerV4::buffer_fmt_field(
    const bcf_hdr_t* hdr,
    bcf1_t* r,
    const bcf_fmt_t* fmt,
    bool include_key,
    HtslibValueMem* val,
    Buffer* buff) {
  const char* key = bcf_hdr_int2id(hdr, BCF_DT_ID, fmt->id);

  // Header says GT is str, but it's encoded as an int (index into alleles)
  int type = std::strcmp("GT", key) == 0 ?
                 BCF_HT_INT :
                 bcf_hdr_id2type(hdr, BCF_HL_FMT, fmt->id);
  val->ndst = HtslibValueMem::convert_ndst_for_type(
      val->ndst, type, &val->type_for_ndst);
  int num_vals =
      bcf_get_format_values(hdr, r, key, &val->dst, &val->ndst, type);
  if (num_vals < 0)
    throw std::runtime_error(
        "Error reading FMT field '" + std::string(key) + "'; " +
        std::to_string(num_vals));

  if (buff->expecting())
    buff->offsets().push_back(buff->size());

  // Write a FMT value: [keystr],type,nvalues,values
  if (include_key)
    buff->append(key, strlen(key) + 1);
  buff->append(&type, sizeof(int));
  buff->append(&num_vals, sizeof(int));
  buff->append(val->dst, num_vals * utils::bcf_type_size(type));
}

void WriterWorkerV4::generate_anchors(const RecordHeapV4::Node& node) {
  // Early exit if start and end are the same
  if (node.start_pos == node.end_pos) {
    return;
  }
  // Generate anchors between start and end position of node
  const auto& metadata = dataset_->metadata();
  for (uint32_t start_pos = node.start_pos + metadata.anchor_gap;
       start_pos < node.end_pos - metadata.anchor_gap - 1;
       start_pos += metadata.anchor_gap) {
    anchor_heap_.insert(
        node.vcf,
        RecordHeapV4::NodeType::Anchor,
        node.record,
        node.contig,
        start_pos,
        node.end_pos,
        node.sample_name);
  }
}

void WriterWorkerV4::buffer_anchors() {
  while (!anchor_heap_.empty()) {
    buffer_record(anchor_buffers_, anchor_heap_.top());
    anchor_heap_.pop();
  }
}

void WriterWorkerV4::buffer_anchors(const RecordHeapV4::Node& node) {
  while (!anchor_heap_.empty()) {
    const RecordHeapV4::Node& top = anchor_heap_.top();
    if (top.start_pos <= node.start_pos) {
      buffer_record(anchor_buffers_, top);
      anchor_heap_.pop();
    } else {
      break;
    }
  }
}

void WriterWorkerV4::write_buffers(
    std::unique_ptr<Query>& query, AttributeBufferSet& buffers, bool finalize) {
  // Prepare the query to write the buffers
  buffers.set_buffers(query.get(), dataset_->metadata().version);

  // Submit the write query
  if (finalize) {
    query->submit_and_finalize();
  } else {
    query->submit();
  }

  // Check that the query didn't fail
  auto status = query->query_status();
  if (status == Query::Status::FAILED) {
    LOG_FATAL("Error submitting TileDB write query: status = FAILED");
  }
}

}  // namespace vcf
}  // namespace tiledb
