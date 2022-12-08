/**
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2019 TileDB, Inc.
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

#ifndef TILEDB_VCF_EXPORTER_H
#define TILEDB_VCF_EXPORTER_H

#include "dataset/tiledbvcfdataset.h"
#include "read/export_format.h"
#include "read_query_results.h"
#include "vcf/region.h"

namespace tiledb {
namespace vcf {

/** Abstract interface for exporting extracted records to disk or in-memory. */
class Exporter {
 public:
  /** Constructor */
  Exporter();

  /** Destructor */
  virtual ~Exporter() {
  }

  /** Resets any export state (but not configuration) stored by the Exporter. */
  virtual void reset() {
    all_exported_files_.clear();
  }

  /** Sets the local output prefix where any output files should be stored. */
  void set_output_dir(const std::string& output_dir);

  /** Sets the dataset being exported. */
  void set_dataset(const TileDBVCFDataset* dataset);

  /** Upload any/all exported files to the given local or remote (S3) URI. */
  void upload_exported_files(
      const VFS& vfs, const std::string& upload_dir) const;

  /**
   * Exports a BCF record.
   *
   * @param sample Sample that the record belongs to
   * @param sample_hdr Header of the sample that the record belongs to
   * @param query_region The query region that intersected the record.
   * @param query_contig_offset Global offset of contig of query_region.
   * @param query_results From TileDB, the query results for all cells.
   * @param cell_idx The index of the cell of the record to export.
   * @return False if in-memory buffers are full and the record could not be
   *    exported.
   */
  virtual bool export_record(
      const SampleAndId& sample,
      const bcf_hdr_t* sample_hdr,
      const Region& query_region,
      uint32_t query_contig_offset,
      const ReadQueryResults& query_results,
      uint64_t cell_idx) = 0;

  /**
   * Finalize export of the given sample (after which no more records for the
   * sample are allowed to be exported).
   */
  virtual void finalize_export(const SampleAndId&, const bcf_hdr_t*) {
  }

  /** Close the exporter, flushing any buffers. */
  virtual void close() {
  }

  /**
   * Returns a list of dataset array attribute names are required to be read to
   * satisfy the particular exporter requirements.
   */
  virtual std::set<std::string> array_attributes_required() const = 0;

  bool need_headers() const;

  /**
   * Enable internal allele frequency generation
   */
  void enable_iaf();

 protected:
  /** The dataset. */
  const TileDBVCFDataset* dataset_;

  /** List tracking all file paths that are created during export. */
  std::vector<std::string> all_exported_files_;

  /** Output prefix (local) for all exported files. */
  std::string output_dir_;

  /** Reusable htslib record struct. */
  SafeBCFRec reusable_rec_;

  /** Does the exporter need headers */
  bool need_headers_ = false;

  /**
   * Given the TileDB query results, populates the htslib record struct with
   * the corresponding attribute values for a particular cell.
   *
   * @param hdr Header of sample containing record
   * @param query_results TileDB query results for all cells
   * @param cell_idx Cell whose record to reconstruct
   * @param contig_name Name of contig of record
   * @param contig_offset Global offset of contig of record
   * @param dst Record instance to be populated.
   */
  void recover_record(
      const bcf_hdr_t* hdr,
      const ReadQueryResults& query_results,
      uint64_t cell_idx,
      const std::string& contig_name,
      uint32_t contig_offset,
      bcf1_t* dst) const;

  bool add_iaf = false;
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_EXPORTER_H
