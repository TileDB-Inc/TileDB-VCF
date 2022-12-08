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

#ifndef TILEDB_VCF_WRITER_WORKER_V4_H
#define TILEDB_VCF_WRITER_WORKER_V4_H

#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <htslib/vcf.h>
#include <tiledb/tiledb>

#include "dataset/attribute_buffer_set.h"
#include "dataset/tiledbvcfdataset.h"
#include "stats/allele_count.h"
#include "stats/variant_stats.h"
#include "vcf/htslib_value.h"
#include "vcf/vcf_utils.h"
#include "write/record_heap_v4.h"
#include "write/writer.h"
#include "write/writer_worker.h"

namespace tiledb {
namespace vcf {

/**
 * A WriterWorkerV4 is responsible for parsing a particular genomic region from
 * a set of VCFs, into a set of attribute buffers that will be used to submit a
 * TileDB query.
 *
 * The buffering process ensures that the cells are sorted according to the
 * TileDB-VCF array schema's global order, which is column-major (no tiling
 * across columns).
 */
class WriterWorkerV4 : public WriterWorker {
 public:
  /** Constructor. */
  WriterWorkerV4(int id = 0);

  /**
   * Initializes: opens the specified VCF files and allocates empty attribute
   * buffers.
   */
  void init(
      const TileDBVCFDataset& dataset,
      const IngestionParams& params,
      const std::vector<SampleAndIndex>& samples);

  /**
   * Parse the given region from all samples into the attribute buffers.
   *
   * @param region Genomic region to read
   * @return True if all records from all samples were loaded into the buffers.
   *    False if the buffers ran out of space, and there are more records
   *    to be read.
   */
  bool parse(const Region& region);

  /**
   * Resumes parsing from the current state. This is used if the buffers are too
   * small to fit all records in the genomic region in memory.
   *
   * @return True if the last record from all samples was buffered. False if the
   *    buffers ran out of space, and there are more records to be read.
   */
  bool resume();

  /** Return a handle to the attribute buffers */
  const AttributeBufferSet& buffers() const;

  /** Returns the number of records buffered by the last parse operation. */
  uint64_t records_buffered() const;

  /** Returns the number of anchors buffered by the last parse operation. */
  uint64_t anchors_buffered() const;

  /** Initialize ingestion tasks, like allele count ingestion. */
  void init_ingestion_tasks(std::shared_ptr<Context> ctx, std::string uri);

  /** Flush ingestion tasks. */
  void flush_ingestion_tasks();

  /** Drain local heap of anchors into the anchor worker. */
  void drain_anchors(WriterWorkerV4& anchor_worker);

  /** Move all records in the anchor heap to the local buffers and return the
   * number of records buffered. */
  size_t buffer_anchors();

 private:
  /** Worker id */
  int id_;

  /** Attribute buffers holding parsed data. */
  AttributeBufferSet buffers_;

  /** The destination dataset. */
  const TileDBVCFDataset* dataset_;

  /** Vector of VCF files being parsed. */
  std::vector<std::shared_ptr<VCFV4>> vcfs_;

  /** Reusable memory allocation for getting record field values from htslib. */
  HtslibValueMem val_;

  /** Current number of records buffered. */
  uint64_t records_buffered_;

  /** Current number of anchors buffered. */
  uint64_t anchors_buffered_;

  /** Record heap for sorting records across samples. */
  RecordHeapV4 record_heap_;

  /** Record heap for storing anchors. */
  RecordHeapV4 anchor_heap_;

  // Allele count ingestion task object
  AlleleCount ac_;

  // Variant stats ingestion task object
  VariantStats vs_;

  /**
   * Inserts a record (non-anchor) into the heap if it fits
   * in `region_`.
   *
   * @param record The record to insert
   * @param vcf The VCF state that contains `record`.
   * @param contig_offset The VCF contig offset
   * @param sample_id The sample id for the record.
   */
  void insert_record(
      const SafeSharedBCFRec& record,
      std::shared_ptr<VCFV4> vcf,
      const std::string& contig,
      const std::string& sample_name);

  /**
   * Copies all fields of a VCF record or anchor into the attribute buffers.
   *
   * @param contig_offset Offset of the record's contig
   * @param node Record to buffer
   * @return True if copy succeeded; false on buffer overflow.
   */
  bool buffer_record(const RecordHeapV4::Node& node);

  /** Helper function to buffer the alleles attribute. */
  static void buffer_alleles(bcf1_t* record, Buffer* buffer);

  /** Helper function to buffer an INFO field. */
  static void buffer_info_field(
      const bcf_hdr_t* hdr,
      bcf1_t* r,
      const bcf_info_t* info,
      bool include_key,
      HtslibValueMem* val,
      Buffer* buff);

  /** Helper function to buffer a FMT field. */
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

#endif  // TILEDB_VCF_WRITER_WORKER_V4_H
