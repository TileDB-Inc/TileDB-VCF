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
#include "utils/logger_public.h"

namespace tiledb {
namespace vcf {

WriterWorkerV4::WriterWorkerV4(int id)
    : id_(id)
    , dataset_(nullptr)
    , records_buffered_(0)
    , anchors_buffered_(0) {
}

void WriterWorkerV4::init(
    const TileDBVCFDataset& dataset,
    const IngestionParams& params,
    const std::vector<SampleAndIndex>& samples) {
  dataset_ = &dataset;

  for (const auto& s : samples) {
    auto vcf = std::make_shared<VCFV4>();
    vcf->set_max_record_buff_size(params.max_record_buffer_size);
    vcf->open(s.sample_uri, s.index_uri);
    vcfs_.push_back(vcf);
  }

  for (const auto& attr : dataset.metadata().extra_attributes)
    buffers_.extra_attrs()[attr] = Buffer();
}

const AttributeBufferSet& WriterWorkerV4::buffers() const {
  return buffers_;
}

uint64_t WriterWorkerV4::records_buffered() const {
  return records_buffered_;
}

uint64_t WriterWorkerV4::anchors_buffered() const {
  return anchors_buffered_;
}

void WriterWorkerV4::insert_record(
    const SafeSharedBCFRec& record,
    std::shared_ptr<VCFV4> vcf,
    const std::string& contig,
    const std::string& sample_name) {
  // If a record starts outside the region max, skip it.
  const uint32_t start_pos = record->pos;
  if (start_pos > region_.max)
    return;

  const uint32_t end_pos =
      VCFUtils::get_end_pos(vcf->hdr(), record.get(), &val_);
  record_heap_.insert(
      vcf,
      RecordHeapV4::NodeType::Record,
      record,
      contig,
      start_pos,
      end_pos,
      sample_name);
}

bool WriterWorkerV4::parse(const Region& region) {
  if (!record_heap_.empty())
    throw std::runtime_error(
        "Error in parsing; record heap unexpectedly not empty.");

  region_ = region;

  LOG_DEBUG(
      "WriteWorker4: parse {}:{}-{}", region.seq_name, region.min, region.max);

  // Initialize the record heap with the first record from each sample.
  for (auto& vcf : vcfs_) {
    // If seek returns false there is no records for this contig
    if (!vcf->seek(region.seq_name, region.min))
      continue;

    SafeSharedBCFRec r = vcf->front_record();
    if (r == nullptr) {
      // Sample has no records at this region, skip it.
      continue;
    }
    vcf->pop_record();

    insert_record(r, vcf, region.seq_name, vcf->sample_name());
  }

  // Start buffering records (which can possibly be incomplete if the buffers
  // run out of space).
  return resume();
}

bool WriterWorkerV4::resume() {
  buffers_.clear();
  records_buffered_ = 0;
  anchors_buffered_ = 0;

  const auto& metadata = dataset_->metadata();

  // Buffer VCF records in-memory for writing to the TileDB array. The records
  // are expected to be sorted in ascending order by there start position and
  // duplicate start positions are allowed. Record ranges may overlap. Records
  // are sorted on the `record_heap_` because anchor points may not be ordered
  // for overlapping records.
  //
  // 1. Pop the top record from `record_heap_`. If the record is a
  //    NodeType::Record, add it to the attribute buffers.
  // 2. If there are no remaining bytes for the record (an "end node"):
  //     a. Pop the next record from the VCF reader and insert it on the heap.
  //    Else
  //     a. Re-insert the record on the record_heap_ as NodeType::Anchor with a
  //        start position advanced by the length of the anchor gap.
  //     b. Insert the record on the anchor_heap_ as NodeType::Anchor with a
  //        start position advanced by the length of the anchor gap.
  //     c. front the next record from the VCF read. If it has a start position
  //        less than the start position of the anchor in step (a), insert it
  //        on the heap.
  // 3. Repeat step (1) until the heap is empty.
  //
  // When this worker is done processing the region, upstream logic will use an
  // "anchor worker" to:
  //
  // 1. Drain anchors from this anchor_heap_ and add them to the anchor worker's
  //    anchor_heap_, along with all anchors in the current (or merged) contig.
  // 2. Drain anchors from the anchor worker's anchor_heap_ and add them to the
  //    attribute buffers.
  // 3. Write all anchors for the current (or merged) contig to a TileDB
  //    fragment.
  //
  while (!record_heap_.empty()) {
    RecordHeapV4::Node& top =
        const_cast<RecordHeapV4::Node&>(record_heap_.top());
    const std::string sample_name = top.sample_name;
    auto vcf = top.vcf;

    // If top is type Record and inside the region, copy the record into the
    // buffers. If the record caused the buffers to exceed the max memory
    // allocation, we'll stop processing at this record.
    bool overflowed = false;
    if (top.type == RecordHeapV4::NodeType::Record) {
      if (top.start_pos > region_.max) {
        LOG_FATAL(
            "WriterWorker4: Top record starts outside of the region: {} > {}",
            top.start_pos,
            region_.max);
      }
      overflowed = !buffer_record(top);
    }

    // Determine if this is the last node for the record.
    const bool is_end_node =
        top.end_pos == top.start_pos ||
        (top.end_pos - top.start_pos - 1) < metadata.anchor_gap;

    if (is_end_node) {
      // After buffering the end node, we're done with its record and it may
      // returned to the vcf record pool for re-use. This is strictly an
      // optimization.
      vcf->return_record(top.record);

      // We're done with the top node. Remove it from the heap.
      record_heap_.pop();

      // If there is a next record, insert it on the heap.
      if (vcf->is_open()) {
        SafeSharedBCFRec next_r = vcf->front_record();
        if (next_r != nullptr) {
          vcf->pop_record();
          insert_record(
              next_r, vcf, vcf->contig_name(next_r.get()), sample_name);
        }
      }
    } else {
      // Insert the next anchor for the current record.
      const uint32_t anchor_start = top.start_pos + metadata.anchor_gap;
      record_heap_.insert(
          vcf,
          RecordHeapV4::NodeType::Anchor,
          top.record,
          top.contig,
          anchor_start,
          top.end_pos,
          sample_name);

      // Add the anchor to the anchor heap for buffering and writing at the end
      // of the contig. Duplicate top.record because the worker deletes the
      // record contents when it is done processing the region.
      SafeSharedBCFRec dup_record(bcf_dup(top.record.get()), bcf_destroy);
      bcf_unpack(dup_record.get(), BCF_UN_ALL);
      anchor_heap_.insert(
          vcf,
          RecordHeapV4::NodeType::Anchor,
          std::move(dup_record),
          top.contig,
          anchor_start,
          top.end_pos,
          sample_name);

      // We're done with the top node. Remove it from the heap.
      record_heap_.pop();

      if (vcf->is_open()) {
        // If there is a next record and it proceeds the anchor, insert it
        // on the heap.
        SafeSharedBCFRec next_r = vcf->front_record();
        if (next_r != nullptr &&
            static_cast<uint32_t>(next_r->pos) < anchor_start) {
          vcf->pop_record();
          insert_record(
              next_r, vcf, vcf->contig_name(next_r.get()), sample_name);
        }
      }
    }

    if (overflowed) {
      LOG_DEBUG(
          "Worker {}: buffer full, output buffer size = {} MiB",
          id_,
          buffers_.total_size() >> 20);
      return false;
    }
  }

  LOG_TRACE(
      "Worker {}: record heap empty, output buffer size = {} MiB",
      id_,
      buffers_.total_size() >> 20);
  return true;
}

void WriterWorkerV4::flush_ingestion_tasks() {
  ac_.flush();
  vs_.flush();
}

void WriterWorkerV4::drain_anchors(WriterWorkerV4& anchor_worker) {
  int count = 0;

  // Drain anchors from the anchor_heap and add them to the anchor worker
  while (!anchor_heap_.empty()) {
    anchor_worker.anchor_heap_.insert(anchor_heap_.top());
    anchor_heap_.pop();
    count++;
  }
  if (count) {
    LOG_DEBUG("Worker {}: Drained {} anchors.", id_, count);
  }
}

size_t WriterWorkerV4::buffer_anchors() {
  buffers_.clear();
  anchors_buffered_ = 0;

  size_t records = anchor_heap_.size();

  // Drain all records in the anchor_heap and add them to the buffers
  while (!anchor_heap_.empty()) {
    buffer_record(anchor_heap_.top());
    anchor_heap_.pop();
  }

  return records;
}

bool WriterWorkerV4::buffer_record(const RecordHeapV4::Node& node) {
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
    vs_.process(hdr, sample_name, contig, pos, r);
  }

  buffers_.sample_name().offsets().push_back(buffers_.sample_name().size());
  buffers_.sample_name().append(sample_name.c_str(), sample_name.length());
  buffers_.contig().offsets().push_back(buffers_.contig().size());
  buffers_.contig().append(contig.c_str(), contig.length());
  buffers_.start_pos().append(&col, sizeof(uint32_t));
  buffers_.qual().append(&r->qual, sizeof(float));
  buffers_.real_start_pos().append(&pos, sizeof(uint32_t));
  buffers_.end_pos().append(&end_pos, sizeof(uint32_t));

  // ID string (include null terminator)
  const size_t id_size = strlen(r->d.id) + 1;
  buffers_.id().offsets().push_back(buffers_.id().size());
  buffers_.id().append(r->d.id, id_size);

  // Alleles
  buffer_alleles(r, &buffers_.alleles());

  // Filter IDs
  buffers_.filter_ids().offsets().push_back(buffers_.filter_ids().size());
  buffers_.filter_ids().append(&(r->d.n_flt), sizeof(int32_t));
  buffers_.filter_ids().append(r->d.flt, sizeof(int32_t) * r->d.n_flt);

  // Start expecting info on all the extra buffers
  for (auto& it : buffers_.extra_attrs())
    it.second.start_expecting();

  // Extract INFO fields into separate attributes
  std::vector<bool> infos_extracted(r->n_info, false);
  unsigned n_info_as_attr = 0;
  for (unsigned i = 0; i < r->n_info; i++) {
    bcf_info_t* info = r->d.info + i;
    int info_id = info->key;
    const char* key = bcf_hdr_int2id(hdr, BCF_DT_ID, info_id);

    // No need to store END.
    if (strcmp("END", key) == 0) {
      infos_extracted[i] = true;
      n_info_as_attr++;
      continue;
    }

    Buffer* buff;
    if (buffers_.extra_attr(std::string("info_") + key, &buff)) {
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
    if (buffers_.extra_attr(std::string("fmt_") + key, &buff)) {
      // No need to store the string key, as it's an extracted attribute.
      const bool include_key = false;
      buffer_fmt_field(hdr, r, fmt, include_key, &val_, buff);
      // Mark key so we don't add it again in the fmt blob attribute
      fmts_extracted[i] = true;
      n_fmt_as_attr++;
    }
  }

  // Remaining INFO/FMT fields go into blob attributes
  Buffer& info = buffers_.info();
  info.offsets().push_back(info.size());
  const uint32_t non_attr_info = r->n_info - n_info_as_attr;
  info.append(&non_attr_info, sizeof(uint32_t));
  for (unsigned i = 0; i < r->n_info; i++) {
    if (!infos_extracted[i]) {
      bcf_info_t* info_field = r->d.info + i;
      buffer_info_field(hdr, r, info_field, true, &val_, &info);
    }
  }

  Buffer& fmt = buffers_.fmt();
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
  for (auto& it : buffers_.extra_attrs())
    it.second.stop_expecting();

  if (node.type == RecordHeapV4::NodeType::Record)
    records_buffered_++;
  else
    anchors_buffered_++;

  // Return false if buffers are full
  const uint64_t buffer_size_mb = buffers_.total_size() >> 20;
  if (buffer_size_mb > max_total_buffer_size_mb_) {
    return false;
  }

  return true;
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
    buff->append(val->dst, num_vals * utils::bcf_type_size(type));
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

}  // namespace vcf
}  // namespace tiledb
