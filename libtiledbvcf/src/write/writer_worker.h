/**
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2019-2020 TileDB, Inc.
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
#include "write/writer.h"

namespace tiledb {
namespace vcf {

// Forward declaration
struct IngestionParams;

/**
 * A WriterWorker is responsible for parsing a particular genomic region from a
 * set of VCFs, into a set of attribute buffers that will be used to submit
 * a TileDB query.
 *
 * The buffering process ensures that the cells are sorted according to the
 * TileDB-VCF array schema's global order, which is column-major (no tiling
 * across columns).
 */
class WriterWorker {
 public:
  /** Destructor. */
  virtual ~WriterWorker() = default;

  /**
   * Initializes: opens the specified VCF files and allocates empty attribute
   * buffers.
   */
  virtual void init(
      const TileDBVCFDataset& dataset,
      const IngestionParams& params,
      const std::vector<SampleAndIndex>& samples) = 0;

  /**
   * Parse the given region from all samples into the attribute buffers.
   *
   * @param region Genomic region to read
   * @return True if all records from all samples were loaded into the buffers.
   *    False if the buffers ran out of space, and there are more records
   *    to be read.
   */
  virtual bool parse(const Region& region) = 0;

  /**
   * Resumes parsing from the current state. This is used if the buffers are too
   * small to fit all records in the genomic region in memory.
   *
   * @return True if the last record from all samples was buffered. False if the
   *    buffers ran out of space, and there are more records to be read.
   */
  virtual bool resume() = 0;

  /** Flush ingestion tasks. */
  virtual void flush_ingestion_tasks() {
  }

  /** Return a handle to the attribute buffers */
  virtual const AttributeBufferSet& buffers() const = 0;

  /** Returns the number of records buffered by the last parse operation. */
  virtual uint64_t records_buffered() const = 0;

  /** Returns the number of anchors buffered by the last parse operation. */
  virtual uint64_t anchors_buffered() const = 0;

  /**
   * Return region set for worker
   * @return Region
   */
  Region region() const {
    return region_;
  }

  /** Region being parsed. */
  Region region_;

  /** max MiB to buffer before flushing to TileDB. */
  uint64_t max_total_buffer_size_mb_;

  /**
   * Set the max buffer size in MiB for worker
   * @param size
   */
  void set_max_total_buffer_size_mb(uint64_t size) {
    max_total_buffer_size_mb_ = size;
  }
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_WRITER_WORKER_H
