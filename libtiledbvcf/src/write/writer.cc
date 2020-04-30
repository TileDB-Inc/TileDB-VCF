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

#include <future>

#include "dataset/attribute_buffer_set.h"
#include "dataset/tiledbvcfdataset.h"
#include "utils/sample_utils.h"
#include "write/writer.h"
#include "write/writer_worker.h"

namespace tiledb {
namespace vcf {

Writer::Writer() {
}

void Writer::init(
    const TileDBVCFDataset& dataset, const IngestionParams& params) {
  // Clean up old query and array objects first, if any.
  query_.reset(nullptr);
  array_.reset(nullptr);

  tiledb_config_.reset(new Config);
  (*tiledb_config_)["vfs.s3.multipart_part_size"] =
      params.part_size_mb * 1024 * 1024;

  // User overrides
  utils::set_tiledb_config(params.tiledb_config, tiledb_config_.get());

  ctx_.reset(new Context(*tiledb_config_));
  vfs_.reset(new VFS(*ctx_, *tiledb_config_));
  array_.reset(new Array(*ctx_, dataset.data_uri(), TILEDB_WRITE));
  query_.reset(new Query(*ctx_, *array_));
  query_->set_layout(TILEDB_GLOBAL_ORDER);

  creation_params_.checksum = TILEDB_FILTER_CHECKSUM_SHA256;
  creation_params_.allow_duplicates = true;
}

void Writer::set_all_params(const IngestionParams& params) {
  ingestion_params_ = params;
}

void Writer::set_dataset_uri(const std::string& uri) {
  creation_params_.uri = uri;
  registration_params_.uri = uri;
  ingestion_params_.uri = uri;
}

void Writer::set_sample_uris(const std::string& sample_uris) {
  auto uris = utils::split(sample_uris, ",");
  registration_params_.sample_uris = uris;
  ingestion_params_.sample_uris = uris;
}

void Writer::set_extra_attributes(const std::string& attributes) {
  auto attrs = utils::split(attributes, ",");
  creation_params_.extra_attributes = attrs;
}

void Writer::set_checksum_type(const int& checksum) {
  set_checksum_type((tiledb_filter_type_t)checksum);
}

void Writer::set_checksum_type(const tiledb_filter_type_t& checksum) {
  creation_params_.checksum = checksum;
}

void Writer::set_allow_duplicates(const bool& allow_duplicates) {
  creation_params_.allow_duplicates = allow_duplicates;
}

void Writer::create_dataset() {
  TileDBVCFDataset::create(creation_params_);
}

void Writer::register_samples() {
  TileDBVCFDataset dataset;
  dataset.open(registration_params_.uri, ingestion_params_.tiledb_config);
  dataset.register_samples(registration_params_);
}

void Writer::ingest_samples() {
  auto start_all = std::chrono::steady_clock::now();

  TileDBVCFDataset dataset;
  dataset.open(ingestion_params_.uri, ingestion_params_.tiledb_config);
  init(dataset, ingestion_params_);

  // Get the list of samples to ingest, sorted on ID
  auto samples = prepare_sample_list(dataset, ingestion_params_);

  // Get a list of regions to ingest, covering the whole genome. The list of
  // disjoint region is used to divvy up work across ingestion threads.
  auto regions = prepare_region_list(dataset, ingestion_params_);

  // Batch the list of samples per space tile.
  auto batches =
      batch_elements_by_tile(samples, dataset.metadata().row_tile_extent);

  // Set up parameters for two scratch spaces.
  const auto scratch_size_mb = ingestion_params_.scratch_space.size_mb / 2;
  ScratchSpaceInfo scratch_space_a = ingestion_params_.scratch_space;
  scratch_space_a.size_mb = scratch_size_mb;
  scratch_space_a.path = utils::uri_join(scratch_space_a.path, "ingest-a");
  if (!vfs_->is_dir(scratch_space_a.path))
    vfs_->create_dir(scratch_space_a.path);
  ScratchSpaceInfo scratch_space_b = ingestion_params_.scratch_space;
  scratch_space_b.size_mb = scratch_size_mb;
  scratch_space_b.path = utils::uri_join(scratch_space_b.path, "ingest-b");
  if (!vfs_->is_dir(scratch_space_b.path))
    vfs_->create_dir(scratch_space_b.path);

  if (ingestion_params_.verbose)
    std::cout << "Initialization completed in "
              << utils::chrono_duration(start_all) << " sec." << std::endl;

  // Start the first batch downloading.
  auto future_paths = std::async(
      std::launch::async,
      SampleUtils::download_samples,
      *vfs_,
      batches[0],
      &scratch_space_a);
  uint64_t records_ingested = 0, anchors_ingested = 0;
  uint64_t samples_ingested = 0;
  for (unsigned i = 1; i < batches.size(); i++) {
    // Block until current batch downloads.
    auto local_samples = future_paths.get();

    // Start the next batch downloading.
    future_paths = std::async(
        std::launch::async,
        SampleUtils::download_samples,
        *vfs_,
        batches[i],
        &scratch_space_b);

    // Ingest the batch.
    auto start_batch = std::chrono::steady_clock::now();
    auto result =
        ingest_samples(dataset, ingestion_params_, local_samples, regions);
    records_ingested += result.first;
    anchors_ingested += result.second;
    samples_ingested += local_samples.size();

    if (ingestion_params_.verbose) {
      std::cout << "Finished ingesting " << samples_ingested << " / "
                << samples.size() << " samples ("
                << utils::chrono_duration(start_batch) << " sec)..."
                << std::endl;
    }

    // Reset current scratch space and swap.
    if (vfs_->is_dir(scratch_space_a.path))
      vfs_->remove_dir(scratch_space_a.path);
    vfs_->create_dir(scratch_space_a.path);
    scratch_space_a.size_mb = scratch_size_mb;
    std::swap(scratch_space_a, scratch_space_b);
  }

  // Ingest the last batch
  auto local_samples = future_paths.get();
  auto result =
      ingest_samples(dataset, ingestion_params_, local_samples, regions);
  records_ingested += result.first;
  anchors_ingested += result.second;

  query_->finalize();
  array_->close();

  // Clean up
  if (vfs_->is_dir(scratch_space_a.path))
    vfs_->remove_dir(scratch_space_a.path);
  if (vfs_->is_dir(scratch_space_b.path))
    vfs_->remove_dir(scratch_space_b.path);
  if (ingestion_params_.remove_samples_file &&
      vfs_->is_file(ingestion_params_.samples_file_uri))
    vfs_->remove_file(ingestion_params_.samples_file_uri);

  if (ingestion_params_.verbose) {
    auto loc = std::cout.getloc();
    utils::enable_pretty_print_numbers(std::cout);
    std::cout << "Done. Ingested " << records_ingested << " records (+ "
              << anchors_ingested << " anchors) from " << samples.size()
              << " samples in " << utils::chrono_duration(start_all)
              << " seconds." << std::endl;
    std::cout.imbue(loc);
  }
}

std::pair<uint64_t, uint64_t> Writer::ingest_samples(
    const TileDBVCFDataset& dataset,
    const IngestionParams& params,
    const std::vector<SampleAndIndex>& samples,
    const std::vector<Region>& regions) {
  uint64_t records_ingested = 0, anchors_ingested = 0;
  if (samples.empty() || regions.empty())
    return {0, 0};

  // TODO: workers can be reused across space tiles
  std::vector<WriterWorker> workers(params.num_threads);
  for (auto& worker : workers)
    worker.init(dataset, params, samples);

  // First compose the set of contigs that are nonempty.
  // This can significantly speed things up in the common case that the sample
  // headers list many contigs that do not actually have any records.
  const auto& metadata = dataset.metadata();
  std::set<std::string> nonempty_contigs;
  for (const auto& s : samples) {
    VCF vcf;
    vcf.open(s.sample_uri, s.index_uri);
    for (const auto& p : metadata.contig_offsets) {
      if (vcf.contig_has_records(p.first))
        nonempty_contigs.insert(p.first);
    }
  }

  const size_t nregions = regions.size();
  size_t region_idx = 0;
  std::vector<std::future<bool>> tasks;
  for (unsigned i = 0; i < workers.size(); i++) {
    WriterWorker* worker = &workers[i];
    while (region_idx < nregions) {
      Region reg = regions[region_idx++];
      if (nonempty_contigs.count(reg.seq_name) > 0) {
        tasks.push_back(std::async(std::launch::async, [worker, reg]() {
          return worker->parse(reg);
        }));
        break;
      }
    }
  }

  bool finished = tasks.empty();
  while (!finished) {
    finished = true;

    for (unsigned i = 0; i < tasks.size(); i++) {
      if (!tasks[i].valid())
        continue;

      WriterWorker* worker = &workers[i];
      bool task_complete = false;
      while (!task_complete) {
        task_complete = tasks[i].get();

        // Write worker buffers, if any data.
        if (worker->records_buffered() > 0) {
          worker->buffers().set_buffers(query_.get());
          auto st = query_->submit();
          if (st != Query::Status::COMPLETE)
            throw std::runtime_error(
                "Error submitting TileDB write query; unexpected query "
                "status.");
        }
        records_ingested += worker->records_buffered();
        anchors_ingested += worker->anchors_buffered();

        // Repeatedly resume the same worker where it left off until it
        // is able to complete.
        if (!task_complete)
          tasks[i] = std::async(
              std::launch::async, [worker]() { return worker->resume(); });
      }

      // Start next region parsing using the same worker.
      while (region_idx < nregions) {
        Region reg = regions[region_idx++];
        if (nonempty_contigs.count(reg.seq_name) > 0) {
          tasks[i] = std::async(std::launch::async, [worker, reg]() {
            return worker->parse(reg);
          });
          finished = false;
          break;
        }
      }
    }
  }

  return {records_ingested, anchors_ingested};
}

std::vector<SampleAndIndex> Writer::prepare_sample_list(
    const TileDBVCFDataset& dataset, const IngestionParams& params) const {
  auto samples = SampleUtils::build_samples_uri_list(
      *vfs_, params.samples_file_uri, params.sample_uris);

  // Get sample names
  auto sample_names =
      SampleUtils::download_sample_names(*vfs_, samples, params.scratch_space);

  // Sort by sample ID
  std::vector<std::pair<SampleAndIndex, std::string>> sorted;
  for (size_t i = 0; i < samples.size(); i++)
    sorted.emplace_back(samples[i], sample_names[i]);
  std::sort(
      sorted.begin(),
      sorted.end(),
      [&dataset](
          const std::pair<SampleAndIndex, std::string>& a,
          const std::pair<SampleAndIndex, std::string>& b) {
        return dataset.metadata().sample_ids.at(a.second) <
               dataset.metadata().sample_ids.at(b.second);
      });

  std::vector<SampleAndIndex> result;
  // Set sample id for later use
  for (const auto& pair : sorted) {
    auto s = pair.first;
    s.sample_id = dataset.metadata().sample_ids.at(pair.second);
    result.push_back(s);
  }

  return result;
}

std::vector<Region> Writer::prepare_region_list(
    const TileDBVCFDataset& dataset, const IngestionParams& params) const {
  std::vector<Region> all_contigs = dataset.all_contigs();
  std::vector<Region> result;

  for (const auto& r : all_contigs) {
    const uint32_t contig_len = r.max - r.min + 1;
    const uint32_t ntasks = utils::ceil(contig_len, params.thread_task_size);
    for (uint32_t i = 0; i < ntasks; i++) {
      uint32_t task_min = r.min + i * params.thread_task_size;
      uint32_t task_max =
          std::min(task_min + params.thread_task_size - 1, r.max);
      result.emplace_back(r.seq_name, task_min, task_max);
    }
  }

  return result;
}

}  // namespace vcf
}  // namespace tiledb
