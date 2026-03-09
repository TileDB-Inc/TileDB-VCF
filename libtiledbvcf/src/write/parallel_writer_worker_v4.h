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

#ifndef TILEDB_VCF_PARALLEL_WRITER_WORKER_V4_H
#define TILEDB_VCF_PARALLEL_WRITER_WORKER_V4_H

#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <htslib/vcf.h>
#include <tiledb/tiledb>

#include "dataset/attribute_buffer_set.h"
#include "dataset/tiledbvcfdataset.h"
#include "stats_worker.h"
#include "vcf/htslib_value.h"
#include "vcf/vcf_utils.h"
#include "write/merged_vcf_v4_stream.h"
#include "write/stats_worker.h"
#include "write/writer.h"
#include "write/writer_record_v4.h"
#include "write/writer_worker.h"

namespace tiledb {
namespace vcf {

/**
 * A `ParallelWriterWorkerV4` is responsible for parsing a particular genomic
 * region from a set of VCFs, into a set of attribute buffers that will be used
 * to submit a TileDB query.
 *
 * The buffering process ensures that the cells are sorted according to the
 * TileDB-VCF array schema's global order, which is column-major (no tiling
 * across columns).
 *
 * As with `WriterWorkerV4`, `ParallelWriterWorkerV4` works on V4 datasets and
 * multiple instances can be run in parallel. However, `ParallelWriterWorkerV4`
 * uses a parallel merge algorithm when parsing, so the primary method of
 * parallelization should be to instantiate a single instance of
 * `ParallelWriterWorkerV4` and use it to parse an entire contig at a time,
 * using the `num_vcf_streams` constructor parameter to determine the level of
 * parallelism. Instantiating multiple instances and running them in parallel
 * is only recommended when the ratio of VCF files being parsed versus the
 * number of threads available is relatively low. Even then, it's recommended
 * that each instances is responsible for parsing an entire contig.
 */
class ParallelWriterWorkerV4 : public WriterWorker,
                               public RecordMergeAlgorithm {
 public:
  /**
   * Constructor.
   *
   * @param id The ID the worker will use when logging
   * @param num_vcf_streams The number of VCF streams/threads the worker should
   * use
   */
  ParallelWriterWorkerV4(int id = 0, int num_vcf_streams = 0);

  /**
   * Initializes: opens the specified VCF files and allocates empty attribute
   * buffers.
   *
   * @param dataset The dataset variants are being parsed for
   * @param params The ingestion params to use when parsing
   * @param samples The VCFs to parse
   */
  void init(
      const TileDBVCFDataset& dataset,
      const IngestionParams& params,
      const std::vector<SampleAndIndex>& samples);

  /**
   * Parse the given region from all samples into the worker's buffers.
   *
   * @param region Genomic region to parse
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

  /**
   * Returns a handle to the attribute buffers of the parsed records.
   *
   * @return The attribute buffers
   */
  const AttributeBufferSet& buffers() const;

  /**
   * Returns the number of records buffered by the last parse operation.
   *
   * @return The number of records buffered
   */
  uint64_t records_buffered() const;

  /**
   * Returns the number of anchors buffered by the last parse operation.
   *
   * @return The number of anchors buffered
   */
  uint64_t anchors_buffered() const;

  /**
   * Flushes stats data to write buffers and submits write queries. Sample
   * stats are only flushed if finalizing.
   *
   * @param finalize Whether or not the write queries should be finalized
   */
  void flush_ingestion_tasks(bool finalize);

  /**
   * Writes all buffered data, i.e. records, anchors, allele counts, variant
   * stats, and sample stats. Note that sample stats are only written when
   * finalizing.
   *
   * @param record_query The query to use for writing records
   * @param anchor_query The query to use for writing anchors
   * @param finalize Whether or not the write queries should be finalized
   */
  void write_buffers(
      std::unique_ptr<Query>& record_query,
      std::unique_ptr<Query>& anchor_query,
      bool finalize);

 private:
  struct Head {
    SharedWriterRecordV4 node;
    int stream_index;
  };

  /** Worker id */
  int id_;

  /** Number of VCF streams to use */
  int num_vcf_streams_;

  /** Attribute buffers holding parsed data. */
  AttributeBufferSet record_buffers_;

  /** Attribute buffers holding generated anchor data. */
  AttributeBufferSet anchor_buffers_;

  /** The destination dataset. */
  const TileDBVCFDataset* dataset_;

  /** Vector of merged VCF streams. */
  std::vector<std::unique_ptr<MergedVCFV4Stream>> vcf_streams_;

  /** Vector of VCF parse tasks that run the streams. */
  std::vector<std::future<void>> vcf_stream_tasks;

  /** Reusable memory allocation for getting record field values from htslib. */
  HtslibValueMem val_;

  /** Current number of records buffered. */
  uint64_t records_buffered_;

  /** Current number of anchors buffered. */
  uint64_t anchors_buffered_;

  /** A worker for computing sample stats in a separate thread. */
  std::unique_ptr<StatsWorker> stats_worker_;

  /** The stats task that buffers record stats. */
  std::future<void> stats_task_;

  /**
   * Pops the head record from the ith `MergedVCFV4Stream`.
   *
   * @param i The index of the `MergedVCFV4Stream`
   * @return The head record that was popped
   */
  SharedWriterRecordV4 get_head(size_t i);

  /**
   * Returns the sum of sizes of all buffers (in bytes).
   *
   * @return The total size of the buffers
   */
  uint64_t total_size() const;

  /**
   * Copies all fields of a VCF record or anchor into the attribute buffers and
   * buffers stats for records.
   *
   * @param buffers The attribute buffer set to add the record to
   * @param node Record to buffer
   */
  void buffer_record(AttributeBufferSet& buffers, const WriterRecordV4& node);

  /**
   * Helper function to buffer the alleles attribute.
   *
   * @param record The record to be buffered
   * @param buffer The buffer to add the record to
   */
  static void buffer_alleles(bcf1_t* record, Buffer* buffer);

  /**
   * Helper function to buffer an INFO field.
   *
   * @param hdr The header for the field being buffered
   * @param r The record for the field being buffered
   * @param info The info field being buffered
   * @param include_key Whether or not to incude the key in the buffered data
   * @param val The value of the field being buffered
   * @param buff The buffer to add the data to
   */
  static void buffer_info_field(
      const bcf_hdr_t* hdr,
      bcf1_t* r,
      const bcf_info_t* info,
      bool include_key,
      HtslibValueMem* val,
      Buffer* buff);

  /**
   * Helper function to buffer a FMT field.
   *
   * @param hdr The header for the field being buffered
   * @param r The record for the field being buffered
   * @param fmt The format field being buffered
   * @param include_key Whether or not to incude the key in the buffered data
   * @param val The value of the field being buffered
   * @param buff The buffer to add the data to
   */
  static void buffer_fmt_field(
      const bcf_hdr_t* hdr,
      bcf1_t* r,
      const bcf_fmt_t* fmt,
      bool include_key,
      HtslibValueMem* val,
      Buffer* buff);

  /**
   * Uses the given query to write all data in the given buffer.
   *
   * @param query The query to use when writing data
   * @param buffers The attribute buffers to write
   * @param finalize Whether or not the write queries should be finalized
   */
  void write_buffers(
      std::unique_ptr<Query>& query,
      AttributeBufferSet& buffers,
      bool finalize);
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_PARALLEL_WRITER_WORKER_V4_H
