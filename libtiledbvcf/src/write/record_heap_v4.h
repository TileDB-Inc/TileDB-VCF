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

#ifndef TILEDB_VCF_RECORD_HEAP_V4_H
#define TILEDB_VCF_RECORD_HEAP_V4_H

#include <htslib/vcf.h>
#include <queue>

#include "vcf/vcf_v4.h"
#include "write/writer_record_v4.h"

namespace tiledb {
namespace vcf {

class RecordHeapV4 {
 public:
  void clear();

  bool empty() const;

  void insert(
      std::shared_ptr<VCFV4> vcf,
      WriterRecordV4::Type type,
      SafeSharedBCFRec record,
      const std::string& contig,
      uint32_t start_pos,
      uint32_t end_pos,
      const std::string& sample_name);

  void insert(const WriterRecordV4& node);

  const WriterRecordV4& top() const;

  void pop();

  size_t size();

 private:
  /** A min-heap of UniqueWriterRecordV4 structs. */
  typedef std::priority_queue<
      UniqueWriterRecordV4,
      std::vector<UniqueWriterRecordV4>,
      WriterRecordV4GT>
      record_heap_t;

  /** The heap. */
  record_heap_t heap_;
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_RECORD_HEAP_V4_H
