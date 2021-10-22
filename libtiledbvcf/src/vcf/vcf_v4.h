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

#ifndef TILEDB_VCF_VCF_V4_H
#define TILEDB_VCF_VCF_V4_H

#include <htslib/hts.h>
#include <htslib/synced_bcf_reader.h>
#include <htslib/vcf.h>
#include <htslib/vcfutils.h>
#include <algorithm>
#include <cstdio>
#include <iostream>
#include <map>
#include <queue>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "utils/utils.h"
#include "vcf/htslib_value.h"
#include "vcf/region.h"
#include "vcf/vcf_utils.h"

namespace tiledb {
namespace vcf {

/**
 * Class wrapping a BCF/VCF file to allow iteration over records.
 */
class VCFV4 {
 public:
  VCFV4();
  ~VCFV4();

  VCFV4(VCFV4&& other) = delete;
  VCFV4(const VCFV4&) = delete;
  VCFV4& operator=(const VCFV4&) = delete;
  VCFV4& operator=(VCFV4&&) = delete;

  bool init(const std::string& contig_name, uint32_t pos);

  /**
   * Opens the specified VCF or BCF file and load the header.
   *
   * @param file Path to VCF file.
   * @param index_file Path to VCF index file. If empty, HTSlib will attempt
   *   to locate the index file automatically.
   */
  void open(const std::string& file, const std::string& index_file = "");

  /** Closes the VCF file and invalidates the iterator. */
  void close();

  /** Returns true if the file is open. */
  bool is_open() const;

  /**
   * Sets the current iterator position to the first record at or after the
   * given region. Getting `curr_rec` after seek will return the first record in
   * the region.
   *
   * @param contig_name Name of contig to seek to
   * @param pos 0-based starting position within contig to seek to
   * @return True if there is at least one record in the given region.
   */
  bool seek(const std::string& contig_name, uint32_t pos);

  /**
   * Returns true if the given contig has any records in the VCF file.
   */
  bool contig_has_records(const std::string& contig_name) const;

  /**
   * Returns the number of records found in the VCF file for the given contig
   */
  size_t record_count(const std::string& contig_name) const;

  /** Returns the header instance of the currently open file. */
  bcf_hdr_t* hdr() const;

  /**
   * Returns the next buffered record but does not remove it from the queue.
   * Note you must first call seek() to initialize the iterator position.
   *
   * @return null if all records have been read.
   */
  SafeSharedBCFRec front_record();

  /**
   * Removes the next buffered record from the internal queue. This does
   * not invalidate records returned from `front_record`.
   */
  void pop_record();

  /**
   * As an optimization, records returned from `next()` may be passed
   * to this routine for re-use to reduce memory allocation overhead.
   *
   * @param record The processed record to return into the allocation pool.
   */
  void return_record(SafeSharedBCFRec& record);

  /** Returns the name of the contig of record `r`. */
  std::string contig_name(bcf1_t* r) const;

  /** Returns the normalized name of the sample in the currently open file. */
  std::string sample_name() const;

  /** Sets the max number of records that can be buffered in memory. */
  void set_max_record_buff_size(uint64_t max_record_buffer_size);

 private:
  /** BCF/VCF iterator wrapper. */
  class Iter {
   public:
    Iter();
    ~Iter();
    bool init_bcf(
        SafeBCFFh&& fh,
        bcf_hdr_t* hdr,
        hts_idx_t* index,
        const std::string& contig_name,
        uint32_t pos);
    bool init_tbx(
        SafeBCFFh&& fh,
        bcf_hdr_t* hdr,
        tbx_t* index,
        const std::string& contig_name,
        uint32_t pos);
    void reset();
    void swap(Iter& other);
    bool next(bcf1_t* rec);
    bool seek(const std::string& contig_name, uint32_t pos);

   private:
    SafeBCFFh fh_;
    bcf_hdr_t* hdr_;
    hts_itr_t* hts_iter_;
    tbx_t* tbx_;
    hts_idx_t* hts_idx_;
    kstring_t tmps_ = {0, 0, nullptr};
  };

  /** True if the file is open. */
  bool open_;

  /** True if the file is init'ed by seek. */
  bool inited_;

  /** Full path of the VCF file being read. */
  std::string path_;

  /** Full path of the index file (may be empty). */
  std::string index_path_;

  /** The buffered records. */
  std::queue<SafeSharedBCFRec> record_queue_;

  /** Stale records available for re-use in `record_queue_`. */
  std::queue<SafeSharedBCFRec> record_queue_pool_;

  /** The BCF/TBX record iterator. */
  Iter record_iter_;

  /** Number of records to buffer in memory. */
  unsigned max_record_buffer_size_;

  /** The HTSlib file header handle. */
  bcf_hdr_t* hdr_;

  /** The TBX index handle, if the index format is TBX. */
  tbx_t* index_tbx_;

  /** The HTS index handle, if the index format is HTS. */
  hts_idx_t* index_hts_;

  /** Reads records into the record buffer using `iter_`. */
  void read_records();

  /** Swap all fields with the given VCFV3 instance. */
  void swap(VCFV4& other);

  /** name of contig last seeked to. */
  std::string seeked_contig_name_;
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_VCF_V4_H
