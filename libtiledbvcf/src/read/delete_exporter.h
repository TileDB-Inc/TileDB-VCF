/**
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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

#ifndef TILEDB_DELETE_EXPORTER_H
#define TILEDB_DELETE_EXPORTER_H

#include "read/exporter.h"
#include "stats/allele_count.h"
#include "stats/variant_stats.h"
#include "utils/logger_public.h"

namespace tiledb::vcf {

class DeleteExporter : public Exporter {
 public:
  /**
   * @brief Construct a new DeleteExporter object.
   *
   * @param ctx TileDB Context
   * @param root_uri TileDB-VCF dataset URI
   */
  explicit DeleteExporter(
      std::shared_ptr<Context> ctx, const std::string& root_uri)
      : ac_(true)
      , vs_(true) {
    need_headers_ = true;
    Group group(*ctx, root_uri, TILEDB_READ);
    ac_.init(ctx, group);
    vs_.init(ctx, group);
  }

  ~DeleteExporter() = default;

  /**
   * @brief Convert a record in the query results to a htslib record and
   * processes the record with the stats array ingestors in delete mode.
   *
   * @param sample Sample name
   * @param hdr VCF header for sample
   * @param query_region Query egion
   * @param contig_offset Global offset of contig of record
   * @param query_results TileDB query results for all cells
   * @param cell_idx Index of cell to export
   * @return true Always return true to indicate no overflow
   */
  bool export_record(
      const SampleAndId& sample,
      const bcf_hdr_t* hdr,
      const Region& query_region,
      uint32_t contig_offset,
      const ReadQueryResults& query_results,
      uint64_t cell_idx) override;

  /**
   * @brief Return the set of attributes required by the ingestor.
   *
   * @return std::set<std::string> Required attributes
   */
  std::set<std::string> array_attributes_required() const override {
    return dataset_->all_attributes();
  }

  /**
   * @brief Flush and close the stats array writers.
   */
  void close() override {
    ac_.flush(true);
    ac_.close();
    vs_.flush(true);
    vs_.close();
  }

 private:
  // Allele count ingestion task object
  AlleleCount ac_;

  // Variant stats ingestion task object
  VariantStats vs_;

  // Contig of last record exported
  std::string contig_ = "";
};

}  // namespace tiledb::vcf

#endif  // TILEDB_DELETE_EXPORTER_H
