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

#ifndef TILEDB_VCF_MERGED_VCF_V4_STREAM_H
#define TILEDB_VCF_MERGED_VCF_V4_STREAM_H

#include <vector>

#include <atomic_queue/atomic_queue.h>

#include "utils/sample_utils.h"
#include "vcf/vcf_utils.h"
#include "vcf/vcf_v4.h"
#include "write/record_heap_v4.h"

namespace tiledb {
namespace vcf {

/**
 * A MergedVCFV4Stream uses the VCFV4 class to parse a set of VCF files into a
 * single stream of variants in global order.
 *
 * A lock-free queue built on a circular buffer is used as the stream to ensure
 * that variants can be continuously written and read in a thread-safe manner.
 */
class MergedVCFV4Stream {
 public:
  /**
   * @param samples The VCF files to be parsed
   * @param queue_size The size of the (will be rounded up to the nearest power
   * of 2)
   * @param vcf_buffer_size The number of records that can be buffered for each
   * VCF file
   */
  MergedVCFV4Stream(
      const std::vector<SampleAndIndex>& samples,
      uint32_t queue_size,
      uint64_t vcf_buffer_size);

  /**
   * Parses the given region in each VCF file and continuously fills the queue
   * with a merged global ordering of the variants until there's no variants
   * left to parse. It is critical that the queue is consumed by another thread,
   * otherwise this method will block indefinitely.
   *
   * @param region The region to be parsed
   */
  void parse(const Region& region);

  /**
   * Pops the node at the front of the queue and returns it. It is critical that
   * the queue is being filled by another thread (i.e. via the parse() method),
   * otherwise this method will block indefinitely.
   *
   * @return A unique pointer wrapping the pooped node
   */
  std::unique_ptr<RecordHeapV4::Node> pop();

 private:
  /** A list that stores the next record (i.e. head) of each VCF file in sorted
   * order. */
  std::list<std::unique_ptr<RecordHeapV4::Node>> head_list_;

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
      std::unique_ptr<RecordHeapV4::Node>,
      std::allocator<std::unique_ptr<RecordHeapV4::Node>>,
      true,
      true,
      true>
      OptimistAtomicQueueB2;
  OptimistAtomicQueueB2 queue_;

  /** Vector of VCF files being parsed. */
  std::vector<std::shared_ptr<VCFV4>> vcfs_;

  /** Reusable memory allocation for getting record field values from htslib. */
  HtslibValueMem val_;

  /** A compartor used to order nodes in the head list. */
  bool head_comparator_gt(
      const std::unique_ptr<RecordHeapV4::Node>& a,
      const std::unique_ptr<RecordHeapV4::Node>& b) const;

  /**
   * Inserts a record into the head list.
   *
   * @param record The record to insert
   * @param vcf The VCF state that contains the record
   * @param contig_offset The VCF contig offset
   * @param sample_id The sample id for the record.
   */
  void insert_head(
      SafeSharedBCFRec& record,
      std::shared_ptr<VCFV4> vcf,
      const std::string& contig,
      const std::string& sample_name);
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_MERGED_VCF_V4_STREAM_H
