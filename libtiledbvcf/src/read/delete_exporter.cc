/**
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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

#include "read/delete_exporter.h"

namespace tiledb::vcf {

bool DeleteExporter::export_record(
    const SampleAndId& sample,
    const bcf_hdr_t* hdr,
    const Region& query_region,
    uint32_t contig_offset,
    const ReadQueryResults& query_results,
    uint64_t cell_idx) {
  SafeBCFRec rec(bcf_init(), bcf_destroy);

  // Populate the htslib record
  const field_filter no_fields = {};
  const field_filter genotype_fields = {"GT"};
  recover_record(
      hdr,
      query_results,
      cell_idx,
      query_region.seq_name,
      contig_offset,
      rec.get(),
      &no_fields,
      &genotype_fields);

  // Flush and finalize the stats array querys when moving to a new contig
  if (contig_ != query_region.seq_name && !contig_.empty()) {
    ac_.flush(true);
    vs_.flush(true);
  }
  contig_ = query_region.seq_name;

  // Update the stats arrays
  ac_.process(
      hdr, sample.sample_name, query_region.seq_name, rec->pos, rec.get());
  vs_.process(
      hdr, sample.sample_name, query_region.seq_name, rec->pos, rec.get());

  // Return true to indicate no overflow
  return true;
}

}  // namespace tiledb::vcf
