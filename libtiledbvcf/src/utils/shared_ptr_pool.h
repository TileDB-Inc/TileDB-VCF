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

#ifndef TILEDB_VCF_SHARED_PTR_POOL_H
#define TILEDB_VCF_SHARED_PTR_POOL_H

#include <memory>
#include <queue>

#include "utils/logger_public.h"

namespace tiledb {
namespace vcf {

/**
 * Manages a pool of `std::shared_ptr<T>` instances, enabling instances of `T`
 * to be reused. The pool can be managed either manually or automatically. In
 * MANUAL mode, shared pointers must be returned using `return_ptr_to_pool()` to
 * prevent them from being automatically destroyed when the pointer's use count
 * reaches 0. In AUTOMATIC mode, `SharedPtrPool` keeps a copy of each
 * `std::shared_ptr<T>` and releases it for reuse when its use count reaches 1.
 *
 * Note that AUTOMATIC mode uses a lazy algorithm for releasing pointers, so
 * it is only appropriate when pointers can be released in roughly the same
 * order that they were added to the pool's queue.
 *
 * The class can be used as follows:
 * ```
 * class SharedPtrUser : public SharedPtrPool<RecordHeapV4> {
 *   std::shared_ptr<RecordHeapV4::Node> automatic_create_or_reuse() {
 *     return get_ptr_from_pool();
 *   }
 *   std::shared_ptr<RecordHeapV4::Node> manual_create_or_reuse() {
 *     if (ptr_pool_empty()) {
 *       return create_pool_ptr(new RecordHeapV4::Node());
 *     } else {
 *       return reuse_pool_ptr();
 *     }
 *   }
 * }
 * ```
 */
template <typename T>
class SharedPtrPool {
 public:
  enum SharingMode {
    MANUAL,
    AUTOMATIC,
  };

  /**
   * Returns the string equivalent of the given `SharingMode`.
   *
   * @param mode The mode to convert to a string
   */
  inline const std::string modeToString(SharingMode mode) {
    switch (mode) {
      case MANUAL:
        return "MANUAL";
      case AUTOMATIC:
        return "AUTOMATIC";
    }
    LOG_ERROR("{} is not a valid SharingMode", mode);
  }

  /**
   * Constructor that determines the management mode to be used.
   *
   * @param mode The mode to use for managing SafeSharedBCFRec pointers
   */
  SharedPtrPool(SharingMode mode = SharingMode::AUTOMATIC)
      : mode_(mode) {
  }

 protected:
  /**
   * Return pointer to pool for reuse.
   *
   * @param ptr The pointer to return to the pool.
   */
  void return_ptr_to_pool(std::shared_ptr<T>& ptr) {
    if (mode_ == SharingMode::MANUAL) {
      shared_ptr_pool_.emplace(std::move(ptr));
    } else {
      LOG_ERROR(
          "VCFV4::return_record records cannot be manually returned for mode "
          "{}",
          modeToString(mode_));
    }
  }

  /**
   * Creates a new record and adds it to the `shared_ptr_pool_`.
   *
   * @tparam Args The types of arguments that will be passed to the constructor
   * @param args The arguments to pass to the constructor
   * @return The pointer created
   */
  template <typename... Args>
  std::shared_ptr<T> create_pool_ptr(Args... args) {
    std::shared_ptr<T> ptr(args...);
    if (mode_ == SharingMode::AUTOMATIC) {
      shared_ptr_pool_.push(ptr);
    }
    return ptr;
  }

  /**
   * Gets the front pointer from the pool returns it. This method provides no
   * safety checks and should only be used after using `ptr_pool_empty()` to
   * determine if there's a pointer ready to be reused.
   *
   * @return The pointer being reused
   */
  std::shared_ptr<T> reuse_pool_ptr() {
    std::shared_ptr<T> ptr = shared_ptr_pool_.front();
    shared_ptr_pool_.pop();
    if (mode_ == SharingMode::AUTOMATIC) {
      shared_ptr_pool_.push(ptr);
    }
    return ptr;
  }

  /**
   * Checks if the pool is empty, i.e. there isn't a pointer ready to be reused.
   *
   * @return True if the pool is empty
   */
  bool ptr_pool_empty() {
    if (shared_ptr_pool_.empty()) {
      return true;
    } else if (!shared_ptr_pool_.empty() && mode_ == SharingMode::AUTOMATIC) {
      std::shared_ptr<T>& ptr = shared_ptr_pool_.front();
      if (ptr.use_count() > 1) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks if there's a pointer ready to be reused and, if yes, returns it.
   * Otherwise, a new pointer is created.
   *
   * @return The `std::shared_ptr<T>` that was created or reused
   */
  std::shared_ptr<T> get_ptr_from_pool() {
    if (ptr_pool_empty()) {
      return create_pool_ptr(new T());
    }
    return reuse_pool_ptr();
  }

  /** Clears the pointers from the pool. */
  void clear_ptr_pool() {
    std::queue<std::shared_ptr<T>>().swap(shared_ptr_pool_);
  }

  /** Swap all fields with the given `SharedPtrPool` instance. */
  void swap(SharedPtrPool<T>& other) {
    std::swap(mode_, other.mode_);
    std::swap(shared_ptr_pool_, other.shared_ptr_pool_);
  }

 private:
  /** The mode used to manage the SafeSharedBCFRec pointers. */
  SharingMode mode_;

  /** Stale pointers available for reuse. */
  std::queue<std::shared_ptr<T>> shared_ptr_pool_;
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_SHARED_PTR_POOL_H
