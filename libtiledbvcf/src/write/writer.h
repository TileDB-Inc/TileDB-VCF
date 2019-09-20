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

#ifndef TILEDB_VCF_WRITER_H
#define TILEDB_VCF_WRITER_H

#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <htslib/vcf.h>
#include <tiledb/tiledb>

#include "dataset/attribute_buffer_set.h"
#include "dataset/tiledbvcfdataset.h"
#include "vcf/htslib_value.h"
#include "write/record_heap.h"

namespace tiledb {
namespace vcf {

/* ********************************* */
/*       AUXILIARY DATATYPES         */
/* ********************************* */

/** Arguments/params for dataset ingestion. */
struct IngestionParams {
  std::string uri;
  std::string samples_file_uri;
  std::vector<std::string> sample_uris;
  unsigned num_threads = std::thread::hardware_concurrency();
  unsigned part_size_mb = 50;
  bool verbose = false;
  ScratchSpaceInfo scratch_space;
  bool remove_samples_file = false;
  uint64_t max_record_buffer_size = 50000;

  /**
   * Max length (# columns) of an ingestion "task". This value is derived
   * empirically to optimize ingestion performance. It affects available
   * parallelism as well as load balancing of ingestion work across threads.
   */
  unsigned thread_task_size = 5000000;
};

/* ********************************* */
/*              WRITER               */
/* ********************************* */

class Writer {
 public:
  /* ********************************* */
  /*            PUBLIC API             */
  /* ********************************* */

  Writer();

  /** Convenience function to set all parameters from the given struct. */
  void set_all_params(const IngestionParams& params);

  void set_dataset_uri(const std::string& uri);

  void set_sample_uris(const std::string& sample_uris);

  void set_extra_attributes(const std::string& attributes);

  void create_dataset();

  void register_samples();

  void ingest_samples();

 private:
  /* ********************************* */
  /*          PRIVATE ATTRIBUTES       */
  /* ********************************* */

  std::unique_ptr<Config> tiledb_config_;
  std::unique_ptr<Context> ctx_;
  std::unique_ptr<VFS> vfs_;
  std::unique_ptr<Array> array_;
  std::unique_ptr<Query> query_;
  RecordHeap record_heap_;

  CreationParams creation_params_;
  RegistrationParams registration_params_;
  IngestionParams ingestion_params_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  void init(const TileDBVCFDataset& dataset, const IngestionParams& params);

  std::vector<SampleAndIndex> prepare_sample_list(
      const TileDBVCFDataset& dataset, const IngestionParams& params) const;

  std::vector<Region> prepare_region_list(
      const TileDBVCFDataset& dataset, const IngestionParams& params) const;

  std::pair<uint64_t, uint64_t> ingest_samples(
      const TileDBVCFDataset& dataset,
      const IngestionParams& params,
      const std::vector<SampleAndIndex>& samples,
      const std::vector<Region>& regions);
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_WRITER_H
