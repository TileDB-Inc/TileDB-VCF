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

StatsWorker::StatsWorker(uint32_t queue_size)
    : queue_(queue_size) {
}

void StatsWorker::run() {
  if (!queue_.was_empty())
    throw std::runtime_error(
        "Error in run; record queue was unexpectedly not empty.");

  SharedWriterRecordV4 node = queue_.pop();
  // Buffer records until there's no variants left to parse in any of the VCFs
  while (node != nullptr) {
    buffer_record(*node);
    node = queue_.pop();
  }
}

void StatsWorker::push(const SharedWriterRecordV4& node) {
  queue_.push(node);
}

bool StatsWorker::is_idle() {
  return queue_.was_empty();
}

void StatsWorker::buffer_record(const WriterRecordV4& node) {
  auto vcf = node.vcf;
  bcf1_t* r = node.record.get();
  bcf_hdr_t* hdr = vcf->hdr();
  const std::string contig = vcf->contig_name(r);
  const std::string sample_name = node.sample_name;
  const uint32_t pos = r->pos;
  vs_.process(hdr, sample_name, contig, pos, r);
  ac_.process(hdr, sample_name, contig, pos, r);
  ss_.process(hdr, sample_name, contig, pos, r);
}

void StatsWorker::flush(bool finalize) {
  vs_.flush(finalize);
  ac_.flush(finalize);
  if (finalize) {
    ss_.flush(finalize);
  }
}

uint64_t StatsWorker::total_size() const {
  return ac_.total_size() + vs_.total_size();
}

}  // namespace vcf
}  // namespace tiledb
