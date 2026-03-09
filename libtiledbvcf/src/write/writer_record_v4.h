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

#ifndef TILEDB_VCF_WRITER_RECORD_V4_H
#define TILEDB_VCF_WRITER_RECORD_V4_H

#include <htslib/vcf.h>
#include <memory>

#include "vcf/vcf_v4.h"

namespace tiledb {
namespace vcf {

/**
 * `WriterRecordV4` wraps `SafeSharedBCFRec` for use with `Writer` V4 codepaths.
 */
struct WriterRecordV4 {
  enum class Type { Record, Anchor };

  WriterRecordV4()
      : vcf(nullptr)
      , type(Type::Record)
      , record(nullptr)
      , start_pos(std::numeric_limits<uint32_t>::max())
      , end_pos(std::numeric_limits<uint32_t>::max())
      , sample_name() {
  }

  std::shared_ptr<VCFV4> vcf;
  WriterRecordV4::Type type;
  SafeSharedBCFRec record;
  std::string contig;
  uint32_t start_pos;
  uint32_t end_pos;
  std::string sample_name;
};

typedef std::unique_ptr<WriterRecordV4> UniqueWriterRecordV4;
typedef std::shared_ptr<WriterRecordV4> SharedWriterRecordV4;

/**
 * Performs a greater-than comparison on two `WriterRecordV4` structs.
 * Specifically, records are sorted on contig, then start position, and lastly
 * sample ID.
 *
 * @param a The first record to compare
 * @param b The second record to compare
 * @return Whether or not the first record is greater than the second record
 */
inline bool writer_record_v4_gt(
    const WriterRecordV4& a, const WriterRecordV4& b) {
  return (a.contig == b.contig &&
          (a.start_pos > b.start_pos ||
           (a.start_pos == b.start_pos && a.sample_name > b.sample_name))) ||
         strcmp(a.contig.c_str(), b.contig.c_str()) > 0;
};

struct UniqueWriterRecordV4GT {
  bool operator()(
      const UniqueWriterRecordV4& a, const UniqueWriterRecordV4& b) const {
    return writer_record_v4_gt(*a, *b);
  }
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_WRITER_RECORD_V4_H
