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

#ifndef TILEDB_PVCF_BCF_EXPORTER_H
#define TILEDB_PVCF_BCF_EXPORTER_H

#include "read/exporter.h"
#include "vcf/vcf_merger.h"

namespace tiledb {
namespace vcf {

/** Export to pVCF. Note this class is currently not threadsafe. */
class PVCFExporter : public Exporter {
 public:
  explicit PVCFExporter(const std::string& output_uri, ExportFormat fmt);

  ~PVCFExporter();

  void init(
      const std::unordered_map<std::string, size_t>& hdrs_lookup,
      const std::unordered_map<uint32_t, SafeBCFHdr>& hdrs);

  void reset() override;

  void close() override;

  bool export_record(
      const SampleAndId& sample,
      const bcf_hdr_t* hdr,
      const Region& query_region,
      uint32_t contig_offset,
      const ReadQueryResults& query_results,
      uint64_t cell_idx) override;

  std::set<std::string> array_attributes_required() const override;

 private:
  std::string uri_;
  std::string fmt_code_;
  SafeBCFFh fp_;
  VCFMerger merger_;

  // read merged records and write the records to the output
  void write_records();
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_PVCF_BCF_EXPORTER_H
