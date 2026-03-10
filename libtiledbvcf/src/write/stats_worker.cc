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

#include <htslib/vcf.h>

#include "stats_worker.h"
#include "write/stats_worker.h"

namespace tiledb {
namespace vcf {

StatsWorker::StatsWorker(uint32_t queue_size, size_t num_buffers)
    : queue_(queue_size)
    , num_buffers_(num_buffers)
    , buffers_(num_buffers) {
}

void StatsWorker::run(size_t i) {
  // NOTE: We don't check if `queue_` still has records because it's shared in
  // the multiple buffer scenario and the caller may push records before `run()`
  // is called

  Buffers& buffers = buffers_[i];
  VariantStats& vs = buffers.variant_stats;
  AlleleCount& ac = buffers.allele_count;

  SharedWriterRecordV4 node = queue_.pop();
  // Buffer records until there's no variants left to parse in any of the VCFs
  while (node != nullptr) {
    buffer_record(*node, vs, ac, sample_stats_);
    node = queue_.pop();
  }
}

void StatsWorker::push(const SharedWriterRecordV4& node) {
  queue_.push(node);
}

void StatsWorker::buffer_record(
    const WriterRecordV4& node,
    VariantStats& vs,
    AlleleCount& ac,
    SampleStats& ss) {
  auto vcf = node.vcf;
  bcf1_t* r = node.record.get();
  bcf_hdr_t* hdr = vcf->hdr();
  const std::string contig = vcf->contig_name(r);
  const std::string sample_name = node.sample_name;
  const uint32_t pos = r->pos;
  vs.process(hdr, sample_name, contig, pos, r);
  ac.process(hdr, sample_name, contig, pos, r);
  ss.process(hdr, sample_name, contig, pos, r);
}

void StatsWorker::flush(bool finalize, size_t i) {
  Buffers& buffers = buffers_[i];
  buffers.variant_stats.flush(finalize);
  buffers.allele_count.flush(finalize);
  if (finalize) {
    sample_stats_.flush(finalize);
  }
}

uint64_t StatsWorker::total_size(size_t i) const {
  const Buffers& buffers = buffers_[i];
  return buffers.allele_count.total_size() + buffers.variant_stats.total_size();
}

}  // namespace vcf
}  // namespace tiledb
