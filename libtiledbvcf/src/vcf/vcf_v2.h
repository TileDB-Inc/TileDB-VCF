/**
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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

#ifndef TILEDB_VCFV2_VCFV2_V2_H
#define TILEDB_VCFV2_VCFV2_V2_H

#include <htslib/hts.h>
#include <htslib/synced_bcf_reader.h>
#include <htslib/vcf.h>
#include <htslib/vcfutils.h>
#include <algorithm>
#include <cstdio>
#include <iostream>
#include <map>
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
class VCFV2 {
 public:
  VCFV2();
  ~VCFV2();

  VCFV2(VCFV2&& other) = delete;
  VCFV2(const VCFV2&) = delete;
  VCFV2& operator=(const VCFV2&) = delete;
  VCFV2& operator=(VCFV2&&) = delete;

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
   * Returns the record at the current iterator position. Note you must first
   * call seek() to initialize the iterator position.
   */
  bcf1_t* curr_rec() const;

  /** Returns the header instance of the currently open file. */
  bcf_hdr_t* hdr() const;

  /**
   * Advances the current iterator position to the next record. Note you must
   * first call seek() to initialize the iterator position.
   *
   * @return false on read error or if the file has no more records.
   */
  bool next();

  /** Returns the name of the contig of the current record. */
  std::string contig_name() const;

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

   private:
    SafeBCFFh fh_;
    bcf_hdr_t* hdr_;
    hts_itr_t* hts_iter_;
    tbx_t* tbx_;
    kstring_t tmps_ = {0, 0, nullptr};
  };

  /** True if the file is open. */
  bool open_;

  /** Full path of the VCF file being read. */
  std::string path_;

  /** Full path of the index file (may be empty). */
  std::string index_path_;

  /** Record buffer. */
  Buffer buffer_;

  /** Record buffer offset. */
  uint64_t buffer_offset_;

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

  /** Frees all buffered records and then resets the record buffer. */
  void destroy_buffer();

  /**
   * Resets the record buffer size to 0, but does not free the buffered records
   * themselves.
   */
  void reset_buffer();

  /** Reads records into the record buffer using `iter_`. */
  void read_records();

  /** Swap all fields with the given VCFV2 instance. */
  void swap(VCFV2& other);
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCFV2_VCFV2_V2_H
