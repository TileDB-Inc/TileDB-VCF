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

#include <memory>

#include "htslib_plugin/hfile_tiledb_vfs.h"
#include "read/bcf_exporter.h"
#include "read/reader.h"

namespace tiledb {
namespace vcf {

BCFExporter::BCFExporter(ExportFormat fmt) {
  need_headers_ = true;
  switch (fmt) {
    case ExportFormat::CompressedBCF:
      extension_ = ".bcf";
      fmt_code_ = "b";
      break;
    case ExportFormat::BCF:
      extension_ = ".bcf";
      fmt_code_ = "bu";
      break;
    case ExportFormat::VCFGZ:
      extension_ = ".vcf.gz";
      fmt_code_ = "z";
      break;
    case ExportFormat::VCF:
      extension_ = ".vcf";
      fmt_code_ = "";
      break;
    default:
      throw std::runtime_error(
          "Error initializing BCFExporter: unknown format.");
  }
}

void BCFExporter::reset() {
  Exporter::reset();
  file_info_.clear();
  record_buffers_v4_.clear();
}

bool BCFExporter::export_record(
    const SampleAndId& sample,
    const bcf_hdr_t* hdr,
    const Region& query_region,
    uint32_t contig_offset,
    const ReadQueryResults& query_results,
    uint64_t cell_idx) {
  // Can't do anything directly with the intersection regions when writing BCF.
  (void)query_region;

  recover_record(
      hdr,
      query_results,
      cell_idx,
      query_region.seq_name,
      contig_offset,
      reusable_rec_.get());

  buffer_record(sample, hdr, reusable_rec_.get());

  return true;
}

void BCFExporter::finalize_export(
    const SampleAndId& sample, const bcf_hdr_t* hdr) {
  auto buff_map_it = record_buffers_v4_.find(sample.sample_name);
  if (buff_map_it != record_buffers_v4_.end()) {
    flush_record_buffer(sample, hdr, &buff_map_it->second);
    record_buffers_v4_.erase(buff_map_it);
  }

  auto file_it = file_info_.find(sample.sample_name);
  if (file_it != file_info_.end())
    file_info_.erase(file_it);
}

std::set<std::string> BCFExporter::array_attributes_required() const {
  // TODO: currently we require all attributes for record recovery.
  return dataset_->all_attributes();
}

void BCFExporter::buffer_record(
    const SampleAndId& sample, const bcf_hdr_t* hdr, bcf1_t* rec) {
  Buffer& buffer = record_buffers_v4_[sample.sample_name];

  uint64_t num_buffered = buffer.size() / sizeof(bcf1_t*);
  if (num_buffered >= RECORD_BUFFER_LIMIT)
    flush_record_buffer(sample, hdr, &buffer);

  bcf1_t* dup = bcf_dup(rec);
  buffer.append(&dup, sizeof(bcf1_t*));
}

void BCFExporter::flush_record_buffer(
    const SampleAndId& sample, const bcf_hdr_t* hdr, Buffer* buffer) {
  init_export_for_sample(sample, hdr);

  std::string path = output_path(sample);
  htsFile* fp = bcf_open(path.c_str(), ("a" + fmt_code_).c_str());
  if (fp == nullptr)
    throw std::runtime_error(
        "Error flushing record buffer for '" + path + "'; error opening file.");

  // Using hts_close because bcf_close is a macro.
  std::unique_ptr<htsFile, decltype(&hts_close)> fp_ptr(fp, hts_close);

  uint64_t num_buffered = buffer->size() / sizeof(bcf1_t*);
  for (uint64_t i = 0; i < num_buffered; i++) {
    bcf1_t* rec = buffer->data<bcf1_t*>()[i];
    if (bcf_write(fp, const_cast<bcf_hdr_t*>(hdr), rec) < 0)
      throw std::runtime_error(
          "Error flushing record buffer for '" + path +
          "'; error writing record.");
    bcf_destroy(rec);
  }

  buffer->clear();
}

void BCFExporter::init_export_for_sample(
    const SampleAndId& sample, const bcf_hdr_t* hdr) {
  if (file_info_.count(sample.sample_name) > 0)
    return;

  std::string path = output_path(sample);
  htsFile* fp = bcf_open(path.c_str(), ("w" + fmt_code_).c_str());
  if (fp == nullptr)
    throw std::runtime_error(
        "Error creating BCF output file '" + path +
        "'; could not create file.");

  // Using hts_close because bcf_close is a macro.
  std::unique_ptr<htsFile, decltype(&hts_close)> fp_ptr(fp, hts_close);

  int rc = bcf_hdr_write(fp, const_cast<bcf_hdr_t*>(hdr));
  if (rc < 0)
    throw std::runtime_error(
        "Error creating BCF output file '" + path +
        "'; error writing header: ");

  file_info_[sample.sample_name] = path;
  all_exported_files_.push_back(path);
}

std::string BCFExporter::output_path(const SampleAndId& sample) const {
  std::string filename = sample.sample_name + extension_;
  return output_dir_.empty() ? filename :
                               utils::uri_join(output_dir_, filename);
}

}  // namespace vcf
}  // namespace tiledb
