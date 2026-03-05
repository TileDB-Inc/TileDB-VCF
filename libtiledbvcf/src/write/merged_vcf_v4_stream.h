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

#include "record_merge_algorithm.h"
#include "utils/sample_utils.h"
#include "utils/shared_ptr_pool.h"
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
class MergedVCFV4Stream : public RecordMergeAlgorithm,
                          public SharedPtrPool<RecordHeapV4::Node> {
 public:
  typedef SharedPtrPool::SharingMode SharingMode;

  /**
   * Constructs the class and opens the VCF files for reading.
   *
   * @param samples The VCF files to be parsed
   * @param queue_size The size of the (will be rounded up to the nearest power
   * of 2)
   * @param vcf_buffer_size The number of records that can be buffered for each
   * VCF file
   */
  MergedVCFV4Stream(
      const std::vector<SampleAndIndex>& samples,
      uint32_t queue_size,
      uint64_t vcf_buffer_size,
      SharingMode mode = SharingMode::AUTOMATIC);

  /** Closes the VCF files and detructs the class. */
  ~MergedVCFV4Stream();

  /**
   * Pops the head record from the ith `VCFV4`.
   *
   * @param i The index of the `VCFV4`
   * @return The head record that was popped
   */
  std::shared_ptr<RecordHeapV4::Node> get_head(size_t i);

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
  std::shared_ptr<RecordHeapV4::Node> pop();

  /**
   * As an optimization, nodes returned from `get_head()` may be passed
   * to this routine for reuse to reduce memory allocation overhead.
   *
   * @param node The processed node to return into the allocation pool
   */
  void return_node(std::shared_ptr<RecordHeapV4::Node>& node);

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
      std::shared_ptr<RecordHeapV4::Node>,
      std::allocator<std::shared_ptr<RecordHeapV4::Node>>,
      true,
      true,
      true>
      OptimistAtomicQueueB2;
  OptimistAtomicQueueB2 queue_;

  /** The region current being parsed. */
  Region region_;

  /** Vector of VCF files being parsed. */
  std::vector<std::shared_ptr<VCFV4>> vcfs_;

  /** Vector of VCF files being parsed. */
  std::vector<bool> vcf_has_records_;

  /** Reusable memory allocation for getting record field values from htslib. */
  HtslibValueMem val_;
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_MERGED_VCF_V4_STREAM_H
