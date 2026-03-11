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

#include "write/parallel_writer_worker_v4.h"
#include "merged_vcf_v4_stream.h"
#include "stats_worker.h"
#include "utils/logger_public.h"
#include "writer_worker_v4.h"

namespace tiledb {
namespace vcf {

ParallelWriterWorkerV4::ParallelWriterWorkerV4(
    int id, size_t num_vcf_streams, size_t num_buffers)
    : id_(id)
    , num_vcf_streams_(num_vcf_streams)
    , num_buffers_(num_buffers)
    , buffers_(num_buffers)
    , dataset_(nullptr) {
}

void ParallelWriterWorkerV4::init(
    const TileDBVCFDataset& dataset,
    const IngestionParams& params,
    const std::vector<SampleAndIndex>& samples) {
  dataset_ = &dataset;
  const auto& metadata = dataset_->metadata();

  // Partition the samples into equal sized parts
  const size_t num_vcf_streams = std::min(num_vcf_streams_, samples.size());
  std::vector<std::vector<SampleAndIndex>> partitioned_samples(num_vcf_streams);
  for (size_t i = 0, j = 0; i < samples.size(); j = ++i % num_vcf_streams) {
    partitioned_samples[j].push_back(samples[i]);
  }

  // Create the VCF streams
  for (auto& stream_samples : partitioned_samples) {
    // TODO: make queue_size configurable
    MergedVCFV4Stream* stream = new MergedVCFV4Stream(
        stream_samples,
        1024,
        params.max_record_buffer_size,
        metadata.anchor_gap);
    vcf_streams_.emplace_back(stream);
  }

  // Create the stats worker
  // TODO: make queue_size configurable
  stats_worker_.reset(new StatsWorker(1024, num_buffers_));

  // Reset the buffers
  for (size_t i = 0; i < num_buffers_; i++) {
    Buffers& buffers = buffers_[i];
    for (const auto& attr : metadata.extra_attributes) {
      buffers.record_buffers.extra_attrs()[attr] = Buffer();
      buffers.anchor_buffers.extra_attrs()[attr] = Buffer();
    }
    buffers.records_buffered = 0;
    buffers.anchors_buffered = 0;
  }
}

uint64_t ParallelWriterWorkerV4::records_buffered(size_t i) const {
  return buffers_[i].records_buffered;
}

uint64_t ParallelWriterWorkerV4::anchors_buffered(size_t i) const {
  return buffers_[i].anchors_buffered;
}

SharedWriterRecordV4 ParallelWriterWorkerV4::get_head(size_t i) {
  MergedVCFV4Stream* vcf = vcf_streams_[i].get();
  return vcf->pop();
}

bool ParallelWriterWorkerV4::parse(const Region& region, size_t i) {
  if (buffers_[i].records_buffered > 0)
    LOG_ERROR(
        "ParallelWriterWorker4(id={})::parse Error in parsing; record buffers "
        "{} aren't empty.",
        id_,
        i);

  region_ = region;

  LOG_DEBUG(
      "ParallelWriterWorker4(id={})::parse {}:{}-{}",
      id_,
      region.seq_name,
      region.min,
      region.max);

  // Start parsing the VCF files
  for (int j = 0; j < vcf_streams_.size(); j++) {
    MergedVCFV4Stream* vcf = vcf_streams_[j].get();
    TRY_CATCH_THROW(vcf_stream_tasks_.push_back(std::async(
        std::launch::async, &MergedVCFV4Stream::parse, vcf, region)));
  }

  // Initialize the merged head list
  initialize_merge_head_list(vcf_streams_.size());

  // Start buffering records
  return resume(i);
}

bool ParallelWriterWorkerV4::resume(size_t i) {
  // Buffer VCF records (and anchors) in-memory for writing to the TileDB array.
  // The records are expected to be sorted in ascending order by their start
  // position and duplicate start positions are allowed. Record ranges may
  // overlap. Records are sorted by a merge algorithm.
  //
  // 1. Pop the top record/anchor from `head_list_` and buffer it.
  //     a. If the reord is not an anchor, generate allele count, variant stats,
  //     and sample stats data using a stats worker running in another thread
  // 2. Repeat step (1) until the `head_list_` is empty or the buffers are full.
  //
  // When a region has been parsed or the buffers are full, upstream code
  // flushes all of the buffers, with the possible exception of samples stats.
  // Sample stats buffers are only flushed when the buffers are flushed AND
  // finalized.

  // Start the stats task
  TRY_CATCH_THROW(stats_task_ = std::async(std::launch::async, [this, i]() {
                    stats_worker_->run(i);
                  }));

  // Get the buffers to be filled
  Buffers& buffers = buffers_[i];
  if (buffers.records_buffered > 0)
    LOG_FATAL(
        "ParallelWriterWorkerV4(id={})::resume Record buffers aren't empty",
        id_);

  // Buffer records until there's no variants left to parse in any of the VCF
  // streams or until the buffer is full
  while (!merged_records_empty()) {
    // Get the next node in the global order from the head list
    SharedWriterRecordV4 node = next_head();
    if (node->start_pos > region_.max) {
      LOG_FATAL(
          "ParallelWriterWorkerV4(id={})::resume Next record starts outside of "
          "the region: {} > {}",
          id_,
          node->start_pos,
          region_.max);
    }

    // Buffer the record and compute stats
    if (node->type == WriterRecordV4::Type::Record) {
      // The stats queue has finite size so `push(node)` blocks when it's full
      stats_worker_->push(node);
      buffer_record(buffers.record_buffers, *node);
      buffers.records_buffered++;
    } else {
      buffer_record(buffers.anchor_buffers, *node);
      buffers.anchors_buffered++;
    }

    // Check if the buffers are full
    const uint64_t buffer_size_mb = total_size(i) >> 20;
    if (buffer_size_mb > max_total_buffer_size_mb_) {
      // Signal the stats worker to stop
      stats_worker_->push(nullptr);
      LOG_DEBUG(
          "ParallelWriterWorkerV4(id={})::resume buffers {} full, total "
          "buffers size = {} MiB",
          id_,
          i,
          buffer_size_mb);
      return false;
    }
  }
  // Signal the stats worker to stop
  stats_worker_->push(nullptr);
  LOG_TRACE(
      "ParallelWriterWorkerV4(id={})::resume all records parsed, total buffers "
      "{} size = {} MiB",
      id_,
      i,
      total_size() >> 20);
  return true;
}

const AttributeBufferSet& ParallelWriterWorkerV4::buffers(size_t i) const {
  return buffers_[i].record_buffers;
}

void ParallelWriterWorkerV4::pre_finalize(size_t i) {
  // Wait for the stats worker to finish parsing before flushing
  stats_task_.wait();
  stats_worker_->buffer_sample_stats(i);
}

void ParallelWriterWorkerV4::flush_ingestion_tasks(bool finalize, size_t i) {
  stats_task_.wait();
  stats_worker_->flush(finalize, i);
}

void ParallelWriterWorkerV4::write_buffers(
    std::unique_ptr<Query>& record_query,
    std::unique_ptr<Query>& anchor_query,
    bool finalize,
    size_t i) {
  Buffers& buffers = buffers_[i];

  // Write the buffered records
  if (buffers.records_buffered > 0) {
    write_buffers(record_query, buffers.record_buffers, finalize);
  }
  if (buffers.anchors_buffered > 0) {
    write_buffers(anchor_query, buffers.anchor_buffers, finalize);
  }
  // Flush the stats arrays
  flush_ingestion_tasks(finalize, i);

  // Clear the buffers
  buffers.record_buffers.clear();
  buffers.anchor_buffers.clear();
  buffers.records_buffered = 0;
  buffers.anchors_buffered = 0;
}

uint64_t ParallelWriterWorkerV4::total_size(size_t i) const {
  // TODO: Include stats in the size
  return buffers_[i].record_buffers.total_size() +
         buffers_[i].anchor_buffers.total_size();
}

void ParallelWriterWorkerV4::buffer_record(
    AttributeBufferSet& buffers, const WriterRecordV4& node) {
  auto vcf = node.vcf;
  bcf1_t* r = node.record.get();
  bcf_hdr_t* hdr = vcf->hdr();
  const std::string contig = vcf->contig_name(r);
  const std::string sample_name = node.sample_name;
  const uint32_t col = node.start_pos;
  const uint32_t pos = r->pos;
  const uint32_t end_pos = VCFUtils::get_end_pos(hdr, r, &val_);

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
}

void ParallelWriterWorkerV4::buffer_alleles(bcf1_t* record, Buffer* buffer) {
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

void ParallelWriterWorkerV4::buffer_info_field(
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

void ParallelWriterWorkerV4::buffer_fmt_field(
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

void ParallelWriterWorkerV4::write_buffers(
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
    LOG_FATAL(
        "ParallelWriterWorkerV4(id={})::write_buffers Error submitting TileDB "
        "write query: status = FAILED",
        id_);
  }
}

}  // namespace vcf
}  // namespace tiledb
