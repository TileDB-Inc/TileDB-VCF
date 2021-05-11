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
 *
 * @section DESCRIPTION
 *
 * Utility routines for VCF.
 */

#ifndef TILEDB_VCF_VCF_UTILS_H
#define TILEDB_VCF_VCF_UTILS_H

#include <htslib/hts.h>
#include <htslib/synced_bcf_reader.h>
#include <htslib/vcf.h>
#include <htslib/vcfutils.h>
#include <map>

#include "region.h"
#include "vcf/htslib_value.h"

namespace tiledb {
namespace vcf {

/** Alias for unique_ptr to bcf_hdr_t. */
typedef std::unique_ptr<bcf_hdr_t, decltype(&bcf_hdr_destroy)> SafeBCFHdr;

/** Alias for unique_ptr to bcf1_t. */
typedef std::unique_ptr<bcf1_t, decltype(&bcf_destroy)> SafeBCFRec;

/** Alias for shared_ptr to bcf1_t. */
typedef std::shared_ptr<bcf1_t> SafeSharedBCFRec;

/** Alias for unique_ptr to htsFile. */
typedef std::unique_ptr<htsFile, decltype(&hts_close)> SafeBCFFh;

/** Alias for unique_ptr to bcf_sr_regions_t. */
typedef std::unique_ptr<bcf_sr_regions_t, decltype(&bcf_sr_regions_destroy)>
    SafeRegionFh;

// Forward declare region
struct Region;

/** HTSFile index type. */
enum class Idx { HTS, TBX };

class VCFUtils {
 public:
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
   * Helper function that returns the sample name by passing in the sample uri
   * which it will read the header from
   *
   * @param path Path of VCF file
   * @return Sample names in the header.
   */
  static std::vector<std::string> get_sample_name_from_vcf(
      const std::string& path);

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
   * Helper function to get a set of contigs from a header
   * @param hdr Header instance
   * @return Vector of contig regions
   */
  static std::vector<tiledb::vcf::Region> hdr_get_contigs_regions(
      bcf_hdr_t* hdr);

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
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_VCF_UTILS_H
