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
   */
  StatsWorker(uint32_t queue_size);

  /** Parses records from the queue until a `nullptr` is popped. */
  void run();

  /**
   * Pushes a VCF record onto the queue. Note that this will block if the queue
   * is full so it is critical that `run()` has been called in a separate
   * thread to prevent deadlock.
   *
   * @param node The record to push
   */
  void push(const SharedWriterRecordV4& node);

  /**
   * Checks if the worker is idle, i.e. there's no records in the queue.
   *
   * @return True if the idle, otherwise false
   */
  bool is_idle();

  /**
   * Flushes stats data to write buffers and submits write queries. Sample
   * stats are only flushed if finalizing.
   *
   * @param finalize Whether or not the write queries should be finalized
   */
  void flush(bool finalize);

  /**
   * Returns the sum of sizes of all buffers (in bytes).
   *
   * @return The total size of the buffers
   */
  uint64_t total_size() const;

 private:
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

  /** Variant stats ingestion task object. */
  VariantStats vs_;

  /** Allele count ingestion task object. */
  AlleleCount ac_;

  /** Sample stats ingestion task object. */
  SampleStats ss_;

  /**
   * Computes stats for a VCF record.
   *
   * @param node The record to buffer
   */
  void buffer_record(const WriterRecordV4& node);
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_STATS_WORKER_H
