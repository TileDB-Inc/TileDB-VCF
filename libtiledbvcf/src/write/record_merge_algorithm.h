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

#ifndef TILEDB_VCF_RECORD_MERGE_ALGORITHM_H
#define TILEDB_VCF_RECORD_MERGE_ALGORITHM_H

#include "write/record_heap_v4.h"

namespace tiledb {
namespace vcf {

/**
 * RecordMergeAlgorithm implements an algorithm that takes VCF records (i.e.
 * RecordHeapV4::Node instances) from multuiple sources and emits them in global
 * order.
 *
 * Assuming each VCF source emits records in global order and that sources emit
 * a nullptr when there are no more records to emit, the merge algorithm works
 * by taking the first record, i.e. head, from each source and adding it and the
 * index of the source it came from to the `head_list_`. Entries in the
 * `head_list_` are kept in sorted order. When a record is emited, the front
 * record is popped from the `head_list_` and replaced with the next head record
 * from the same VCF source.
 *
 * The algorithm can be used as follows:
 * ```
 * class RecordMergeImplementation : public RecordMergeAlgorithm {
 *   std::unique_ptr<RecordHeapV4::Node> get_head(size_t i) {
 *     // Get the head record from VCF source i
 *   }
 *   void merge_records() {
 *     // Loop until `head_list_` is empty
 *     while (!merged_records_empty()) {
 *       std::unique_ptr<RecordHeapV4::Node> node = next_head();
 *       // Do something with node
 *     }
 *   }
 * }
 * ```
 */
class RecordMergeAlgorithm {
 public:
  virtual ~RecordMergeAlgorithm() = default;

 protected:
  struct Head {
    std::unique_ptr<RecordHeapV4::Node> node;
    size_t stream_index;
  };

  /**
   * Pops the head record from the VCF source at the given index.
   *
   * @param i The index of the VCF source
   * @return The head record that was popped
   */
  virtual std::unique_ptr<RecordHeapV4::Node> get_head(size_t i) = 0;

  /**
   * Initializes the head list with the head record from each VCF source.
   *
   * @param n The number of VCF sources in the class
   */
  void initialize_merge_head_list(size_t n);

  /**
   * Returns the next ordered record from the head list and replaces it with the
   * next head from the record's VCF source.
   *
   * @return The next ordered record from the head list
   */
  std::unique_ptr<RecordHeapV4::Node> next_head();

  /**
   * Checks if there are any merged ordered records.
   *
   * @return True if there are no merged ordered records; false otherwise
   */
  bool merged_records_empty();

 private:
  /** A list that stores the next node (i.e. head) of each VCF source in sorted
   * order. */
  std::list<Head> head_list_;

  /**
   * A compartor used to order nodes in the head list.
   *
   * @param The first node to be compared
   * @param The second node to be compared
   * @return Whether or not the first node if greater than the second node
   */
  bool head_comparator_gt(
      const std::unique_ptr<RecordHeapV4::Node>& a,
      const std::unique_ptr<RecordHeapV4::Node>& b) const;

  /**
   * Inserts a node into the head list.
   *
   * @param node The node to insert
   * @param i The index of VCF source the node is from
   */
  void insert_head(std::unique_ptr<RecordHeapV4::Node>, size_t i);
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_RECORD_MERGE_ALGORITHM_H
