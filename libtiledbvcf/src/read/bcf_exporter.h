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

#ifndef TILEDB_VCF_BCF_EXPORTER_H
#define TILEDB_VCF_BCF_EXPORTER_H

#include "read/exporter.h"

namespace tiledb {
namespace vcf {

/** Export to BCF/VCF. Note this class is currently not threadsafe. */
class BCFExporter : public Exporter {
 public:
  explicit BCFExporter(ExportFormat fmt);

  void reset() override;

  bool export_record(
      const SampleAndId& sample,
      const bcf_hdr_t* hdr,
      const Region& query_region,
      uint32_t contig_offset,
      const ReadQueryResults& query_results,
      uint64_t cell_idx) override;

  void finalize_export(
      const SampleAndId& sample, const bcf_hdr_t* hdr) override;

  std::set<std::string> array_attributes_required() const override;

 private:
  /** Number of records to buffer for a file before flushing to disk. */
  const unsigned RECORD_BUFFER_LIMIT = 10000;

  std::map<std::string, std::string> file_info_;
  std::unordered_map<std::string, Buffer> record_buffers_v4_;
  std::string extension_;
  std::string fmt_code_;

  void buffer_record(
      const SampleAndId& sample, const bcf_hdr_t* hdr, bcf1_t* rec);

  void flush_record_buffer(
      const SampleAndId& sample, const bcf_hdr_t* hdr, Buffer* buffer);

  void init_export_for_sample(const SampleAndId& sample, const bcf_hdr_t* hdr);

  std::string output_path(const SampleAndId& sample) const;
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_BCF_EXPORTER_H
