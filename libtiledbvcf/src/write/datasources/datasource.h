/**
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB, Inc.
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

#ifndef TILEDB_VCF_DATASOURCE_H
#define TILEDB_VCF_DATASOURCE_H

#include "dataset/tiledbvcfdataset.h"
#include "utils/sample_utils.h"
#include "write/writer.h"

namespace tiledb {
namespace vcf {

// Forward declaration
enum class ContigMode;

/**
 * The Datasource class is an abstract class that defines the API a source of
 * data must provide to be ingestable.
 */
class Datasource {
 public:
  virtual ~Datasource() = default;

  /**
   * Prepares the samples list to be ingested, using the given dataset to
   * assign and sort by sample IDs.
   * @param dataset The dataset that provides sample IDs
   * @return A vector containing the sample list
   */
  virtual std::vector<SampleAndIndex> get_sample_list(
      const TileDBVCFDataset* dataset) const = 0;

  /**
   * Prepares the samples list to be ingested, sorted by sample name, using the
   * given dataset to assign sample IDs.
   * @param dataset The dataset that provides sample IDs
   * @return A vector containing the sample list
   */
  virtual std::vector<SampleAndIndex> get_sample_list_v4(
      const TileDBVCFDataset* dataset) const = 0;

  /**
   * Prepares the given list of samples for ingestion.
   * @param samples List of samples to prepare for ingestion.
   * TODO: this needs to be generalized to not use SampleAndIndex
   * @return List of local paths for downloaded samples.
   */
  virtual std::vector<SampleAndIndex> prepare_samples(
      const std::vector<SampleAndIndex>& samples) = 0;

  /**
   * Given a set of samples and contigs, creates a set containing all of the
   * contigs that are non-empty in at least one sample.
   * @param samples The samples to get non-empty contigs for.
   * @param contigs The contigs to check are non-empty.
   * @param version The dataset version the set should be compatible with.
   * @return The set of non-empty contigs for the given samples.
   */
  virtual std::set<std::string> get_nonempty_contigs(
      const std::vector<SampleAndIndex>& samples,
      const std::vector<std::string>& contigs,
      const unsigned version) const = 0;

  /**
   * Given a set of samples and contigs to be ingested, creates a set containing
   * all of the contigs that are non-empty in at least one sample.
   * @param samples The samples to get non-empty contigs for.
   * @param contig_mode Which contigs should be ingested.
   * @param contigs_to_keep_separate Set of contigs that should not be merged.
   * @param sample_headers Outputs the headers for the given samples.
   * @param total_contig_records Outputs the number of records per contig.
   * @param total_records_expected Outputs the total number of records.
   * @param regions_v4 Outputs the regions on the contigs.
   * @return The set of non-empty contigs for the given samples.
   */
  virtual std::set<std::string> get_nonempty_contigs_v4(
      const std::vector<SampleAndIndex>& samples,
      const ContigMode contig_mode,
      const std::set<std::string>& contigs_to_keep_separate,
      std::map<std::string, std::string>& sample_headers,
      std::map<std::string, uint32_t>& total_contig_records,
      size_t& total_records_expected,
      std::vector<Region>& regions_v4) const = 0;

  /**
   * Called after an ingestion completes. This is different from a teardown
   * because a teardown sould always be called whether or not an ingesation
   * completes, e.g. when the object's descructor is called.
   */
  virtual void cleanup() = 0;
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_DATASOURCE_H
