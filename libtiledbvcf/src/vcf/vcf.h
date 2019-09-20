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

#ifndef TILEDB_VCF_VCF_H
#define TILEDB_VCF_VCF_H

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

namespace tiledb {
namespace vcf {

/** HTSFile index type. */
enum class Idx { HTS, TBX };

/**
 * Class wrapping a BCF/VCF file to allow iteration over records.
 */
class VCF {
 public:
  VCF();
  ~VCF();

  VCF(VCF&& other) = delete;
  VCF(const VCF&) = delete;
  VCF& operator=(const VCF&) = delete;
  VCF& operator=(VCF&&) = delete;

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

  /**
   * Helper function that returns the 0-based END position of the given record.
   * If the record does not contain an END info field, this returns POS +
   * len(REF) - 1.
   *
   * @param hdr Header instance for the record
   * @param rec Record to get END position of
   * @param val Reusable memory location for htslib values
   * @return 0-based END position
   */
  static uint32_t get_end_pos(bcf_hdr_t* hdr, bcf1_t* rec, HtslibValueMem* val);

  /**
   * Helper function that reads an HTSlib header instance from the VCF/BCF file
   * at the given path.
   *
   * @param path Path of VCF file
   * @return Header instance read from the file, or null if a header cannot be
   * read.
   */
  static bcf_hdr_t* hdr_read_header(const std::string& path);

  /**
   * Helper function that returns the normalized sample names from a header
   * instance.
   *
   * @param hdr Header instance
   * @return Sample names in the header.
   */
  static std::vector<std::string> hdr_get_samples(bcf_hdr_t* hdr);

  /**
   * Helper function that converts an HTSlib header instance to a string.
   *
   * @param hdr Header instance
   * @return Stringified header
   */
  static std::string hdr_to_string(bcf_hdr_t* hdr);

  /**
   * Helper function that constructs a map of contig name -> global genomic
   * offset.
   *
   * @param hdr Header instance
   * @param contig_lengths Populated to store contig name -> length.
   * @return Map of contig name -> global genomic position.
   */
  static std::map<std::string, uint32_t> hdr_get_contig_offsets(
      bcf_hdr_t* hdr, std::map<std::string, uint32_t>* contig_lengths);

  /**
   * Helper function that normalizes a sample name:
   *  - Remove leading/trailing whitespace
   *  - Error on invalid chars (comma)
   *
   * @param sample Sample name to normalize
   * @param normalized Set to the normalized sample name
   * @return True if the sample name was normalized successfully
   */
  static bool normalize_sample_name(
      const std::string& sample, std::string* normalized);

 private:
  /** BCF/VCF iterator wrapper. */
  class Iter {
   public:
    Iter(htsFile* fh, bcf_hdr_t* hdr);
    ~Iter();
    bool init(
        bcf_hdr_t* hdr,
        hts_idx_t* index,
        const std::string& contig_name,
        uint32_t pos);
    bool init(tbx_t* index, const std::string& contig_name, uint32_t pos);
    bool next(bcf1_t* rec);

   private:
    htsFile* fh_;
    bcf_hdr_t* hdr_;
    hts_itr_t* iter_;
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

  /**
   * The contig name and min POS value of the current buffered records.
   */
  Region buffered_region_;

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

  /** Reads records into the record buffer using the given iterator. */
  void read_records(Iter* iter);

  /** Swap all fields with the given VCF instance. */
  void swap(VCF& other);
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_VCF_H
