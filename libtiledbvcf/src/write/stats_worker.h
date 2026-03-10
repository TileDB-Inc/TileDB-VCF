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

#ifndef TILEDB_VCF_STATS_WORKER_H
#define TILEDB_VCF_STATS_WORKER_H

#include <atomic_queue/atomic_queue.h>
#include <memory>

#include "stats/allele_count.h"
#include "stats/sample_stats.h"
#include "stats/variant_stats.h"
#include "write/writer_record_v4.h"

namespace tiledb {
namespace vcf {

/**
 * The StatsWorker is responsible for computing variant stats, allele counts,
 * and sample stats. It is specifically designed to do this in a dedicated
 * thread in a non-blocking manner.
 */
class StatsWorker {
 public:
  /**
   * Constructs the class, including setting up the record queue.
   *
   * @param queue_size The size of the queue (will be rounded up to the nearest
   * power of 2)
   * @param num_buffers The number of buffers to instantiate
   */
  StatsWorker(uint32_t queue_size, size_t num_buffers = 1);

  /**
   * Gets the number of buffers in the stats worker.
   *
   * @return The number of buffers
   */
  size_t num_buffers() const {
    return num_buffers_;
  }

  /**
   * Parses records from the queue until a `nullptr` is popped.
   *
   * @param i Which buffers to parse data into; this can be different each time
   *    `run(i)` is called
   */
  void run(size_t i = 0);

  /**
   * Pushes a VCF record onto the queue. Note that this will block if the queue
   * is full so it is critical that `run()` has been called in a separate
   * thread to prevent deadlock.
   *
   * @param node The record to push
   */
  void push(const SharedWriterRecordV4& node);

  /**
   * Flushes stats data to write buffers and submits write queries. Sample
   * stats are only flushed if finalizing.
   *
   * @param i Which buffers to flush
   * @param finalize Whether or not the write queries should be finalized
   */
  void flush(bool finalize, size_t i = 0);

  /**
   * Returns the sum of sizes of all buffers (in bytes).
   *
   * @param i Which buffers to get the size of
   * @return The total size of the buffers
   */
  uint64_t total_size(size_t i = 0) const;

 private:
  struct Buffers {
    /** Variant stats ingestion task object. */
    VariantStats variant_stats;
    /** Allele count ingestion task object. */
    AlleleCount allele_count;
    /** SampleStats is excluded because they're only flushed when finalizing. */
  };

  /**
   * Fixed size queue for non-atomic elements, i.e. records. This is the
   * "OptimistAtomicQueueB2" configuration where the buffer size is specified as
   * an argument to the constructor and that busy-waits when empty (pull) or
   * full (push).
   *
   * @tparam T Type stored in the queue
   * @tparam A Allocator for type stored in the queue
   * @tparam MAXIMIZE_THROUGHPUT Enables throughput optimizations
   * @tparam TOTAL_ORDER Ensures messages are pulled in the same FIFO order they
   * were pushed
   * @tparam SPSC Enables single-producer-single-consumer mode, which is much
   * faster
   */
  typedef atomic_queue::AtomicQueueB2<
      SharedWriterRecordV4,
      std::allocator<SharedWriterRecordV4>,
      true,
      true,
      true>
      OptimistAtomicQueueB2;
  OptimistAtomicQueueB2 queue_;

  /** Number of buffers available */
  const int num_buffers_;

  /** Stats buffers available for concurrent parsing and writing. */
  std::vector<Buffers> buffers_;

  /** Sample stats ingestion task object. */
  SampleStats sample_stats_;

  /**
   * Computes stats for a VCF record.
   *
   * @param node The record to buffer
   * @param vs The variant stats buffer to put data in
   * @param ac The allele count buffer to put data in
   * @param ss The sample stats buffer to put data in
   */
  void buffer_record(
      const WriterRecordV4& node,
      VariantStats& vs,
      AlleleCount& ac,
      SampleStats& ss);
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_STATS_WORKER_H
