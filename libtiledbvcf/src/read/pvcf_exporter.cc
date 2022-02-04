/**
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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

#include <memory>

#include "htslib_plugin/hfile_tiledb_vfs.h"
#include "read/pvcf_exporter.h"
#include "read/reader.h"
#include "utils/logger_public.h"

namespace tiledb {
namespace vcf {

PVCFExporter::PVCFExporter(const std::string& output_uri, ExportFormat fmt)
    : uri_(output_uri)
    , fp_(nullptr, hts_close) {
  need_headers_ = true;
  switch (fmt) {
    case ExportFormat::CompressedBCF:
      fmt_code_ = "b";
      break;
    case ExportFormat::BCF:
      fmt_code_ = "bu";
      break;
    case ExportFormat::VCFGZ:
      fmt_code_ = "z";
      break;
    case ExportFormat::VCF:
      fmt_code_ = "";
      break;
    default:
      LOG_FATAL("Error initializing PVCFExporter: unknown format.");
  }
}

PVCFExporter::~PVCFExporter() {
}

void PVCFExporter::init(
    const std::unordered_map<std::string, size_t>& hdrs_lookup,
    const std::unordered_map<uint32_t, SafeBCFHdr>& hdrs) {
  // sort sample names to match the order returned by tiledb
  std::vector<std::pair<std::string, size_t>> sorted_hdrs(
      hdrs_lookup.begin(), hdrs_lookup.end());
  std::sort(sorted_hdrs.begin(), sorted_hdrs.end());

  merger_.init(sorted_hdrs, hdrs);

  std::string mode = "w" + fmt_code_;
  fp_.reset(bcf_open(uri_.c_str(), mode.c_str()));
  if (fp_.get() == nullptr) {
    LOG_FATAL("Error creating VCF output file '{}'", uri_);
  }

  int rc = bcf_hdr_write(fp_.get(), merger_.get_header());
  if (rc < 0) {
    LOG_FATAL("Error writing VCF header to '{}'", uri_);
  }
}

void PVCFExporter::reset() {
  Exporter::reset();
  uri_ = "";
  fp_.reset(nullptr);
  merger_.reset();
}

void PVCFExporter::write_records() {
  while (!merger_.is_empty()) {
    auto out_rec = merger_.read();

    int rc = bcf_write(fp_.get(), merger_.get_header(), out_rec.get());
    if (rc < 0) {
      LOG_FATAL("Error writing VCF records to '{}'", uri_);
    }
  }
}

void PVCFExporter::close() {
  // finish merging records
  merger_.finish();
  // write last records to the vcf file
  write_records();
  // close the merger
  merger_.close();
  // close the the vcf file
  fp_.reset(nullptr);
}

bool PVCFExporter::export_record(
    const SampleAndId& sample,
    const bcf_hdr_t* hdr,
    const Region& query_region,
    uint32_t contig_offset,
    const ReadQueryResults& query_results,
    uint64_t cell_idx) {
  SafeBCFRec rec(bcf_init(), bcf_destroy);

  recover_record(
      hdr,
      query_results,
      cell_idx,
      query_region.seq_name,
      contig_offset,
      rec.get());

  // Add record to vcf merger
  merger_.write(sample.sample_name, std::move(rec));

  write_records();

  return true;
}

std::set<std::string> PVCFExporter::array_attributes_required() const {
  // TODO: currently we require all attributes for record recovery.
  return dataset_->all_attributes();
}

}  // namespace vcf
}  // namespace tiledb
