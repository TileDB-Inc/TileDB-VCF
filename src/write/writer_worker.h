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

#ifndef TILEDB_VCF_WRITER_WORKER_H
#define TILEDB_VCF_WRITER_WORKER_H

#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <htslib/vcf.h>
#include <tiledb/tiledb>

#include "dataset/attribute_buffer_set.h"
#include "dataset/tiledbvcfdataset.h"
#include "vcf/htslib_value.h"
#include "write/record_heap.h"
#include "write/writer.h"

namespace tiledb {
namespace vcf {

class WriterWorker {
 public:
  WriterWorker();

  void init(
      const TileDBVCFDataset& dataset,
      const IngestionParams& params,
      const std::vector<SampleAndIndex>& samples);

  bool parse(const Region& region);

  bool resume();

  const AttributeBufferSet& buffers() const;

  uint64_t records_buffered() const;

  uint64_t anchors_buffered() const;

 private:
  AttributeBufferSet buffers_;

  const TileDBVCFDataset* dataset_;

  std::vector<std::unique_ptr<VCF>> vcfs_;

  HtslibValueMem val_;

  uint64_t records_buffered_;
  uint64_t anchors_buffered_;

  Region region_;

  RecordHeap record_heap_;

  bool buffer_record(uint32_t contig_offset, const RecordHeap::Node& node);

  static void buffer_alleles(bcf1_t* record, Buffer* buffer);

  static void buffer_info_field(
      const bcf_hdr_t* hdr,
      bcf1_t* r,
      const bcf_info_t* info,
      bool include_key,
      HtslibValueMem* val,
      Buffer* buff);

  static void buffer_fmt_field(
      const bcf_hdr_t* hdr,
      bcf1_t* r,
      const bcf_fmt_t* fmt,
      bool include_key,
      HtslibValueMem* val,
      Buffer* buff);
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_WRITER_WORKER_H
