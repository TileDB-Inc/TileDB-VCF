/**
 * @file   unique_rwlock.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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
 *
 * @section DESCRIPTION
 *
 * This file defines and implements the UniqueRWLock<R> class and
 * type-defintions for UniqueReadLock and UniqueWriteLock.
 */

#ifndef TILEDB_VCF_UNIQUE_RWLOCK_H
#define TILEDB_VCF_UNIQUE_RWLOCK_H

#include "utils/rwlock.h"

namespace tiledb {
namespace vcf {
namespace utils {

template <bool R>
class UniqueRWLock final {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /*
   * Value constructor. Locks `rwlock` and releases
   * it in the destructor. If template-type `R` is
   * true, this class performs read-locking. Otherwise,
   * this performs write-locking.
   *
   * @param rwlock the read-write lock.
   */
  explicit UniqueRWLock(RWLock* const rwlock)
      : rwlock_(rwlock)
      , locked_(false) {
    assert(rwlock_);

    lock();
  }

  /** Destructor. Releases the read-write lock. */
  ~UniqueRWLock() {
    if (locked_)
      unlock();
  }

  UniqueRWLock(const UniqueRWLock&) = delete;

  UniqueRWLock& operator=(const UniqueRWLock&) = delete;

  UniqueRWLock(UniqueRWLock&&) = delete;

  UniqueRWLock& operator=(UniqueRWLock&&) = delete;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Acquires the read-write lock. */
  void lock() {
    assert(!locked_);

    if (R)
      rwlock_->read_lock();
    else
      rwlock_->write_lock();

    locked_ = true;
  }

  /** Releases the read-write lock. */
  void unlock() {
    assert(locked_);

    if (R)
      rwlock_->read_unlock();
    else
      rwlock_->write_unlock();

    locked_ = false;
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The read-write lock. */
  RWLock* const rwlock_;

  /** True if holding a lock on `rwlock_`. */
  bool locked_;
};

// Type-define UniqueReadLock and UniqueWriteLock.
using UniqueReadLock = UniqueRWLock<true>;
using UniqueWriteLock = UniqueRWLock<false>;

}  // namespace utils
}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_UNIQUE_RWLOCK_H