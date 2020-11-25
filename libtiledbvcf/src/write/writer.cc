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
#include "write/writer_worker_v2.h"
#include "write/writer_worker_v3.h"
#include "write/writer_worker_v4.h"

namespace tiledb {
namespace vcf {

Writer::Writer() {
}

void Writer::init(const std::string& uri, const std::string& config_str) {
  if (!config_str.empty())
    set_tiledb_config(config_str);

  set_dataset_uri(uri);

  // Clean up old query and array objects first, if any.
  query_.reset(nullptr);
  array_.reset(nullptr);

  dataset_.reset(new TileDBVCFDataset);

  std::vector<std::string> tiledb_config;
  if (!ingestion_params_.tiledb_config.empty())
    tiledb_config = ingestion_params_.tiledb_config;
  else if (!creation_params_.tiledb_config.empty())
    tiledb_config = creation_params_.tiledb_config;
  else if (!registration_params_.tiledb_config.empty())
    tiledb_config = registration_params_.tiledb_config;

  try {
    dataset_->open(uri, ingestion_params_.tiledb_config);
  } catch (std::exception& e) {
    // If the dataset doesn't exist lets not error out, the user might be
    // creating a new dataset
  }
}

void Writer::init(const IngestionParams& params) {
  // Clean up old query and array objects first, if any.
  query_.reset(nullptr);
  array_.reset(nullptr);

  dataset_.reset(new TileDBVCFDataset);
  dataset_->open(ingestion_params_.uri, ingestion_params_.tiledb_config);

  tiledb_config_.reset(new Config);
  (*tiledb_config_)["vfs.s3.multipart_part_size"] =
      params.part_size_mb * 1024 * 1024;

  // User overrides
  utils::set_tiledb_config(params.tiledb_config, tiledb_config_.get());

  ctx_.reset(new Context(*tiledb_config_));

  // Set htslib global config and context based on user passed TileDB config
  // options
  utils::set_htslib_tiledb_context(params.tiledb_config);

  vfs_.reset(new VFS(*ctx_, *tiledb_config_));
  array_.reset(new Array(*ctx_, dataset_->data_uri(), TILEDB_WRITE));
  query_.reset(new Query(*ctx_, *array_));
  query_->set_layout(TILEDB_GLOBAL_ORDER);

  creation_params_.checksum = TILEDB_FILTER_CHECKSUM_SHA256;
  creation_params_.allow_duplicates = true;
}

void Writer::set_tiledb_config(const std::string& config_str) {
  creation_params_.tiledb_config = utils::split(config_str, ',');
  registration_params_.tiledb_config = utils::split(config_str, ',');
  ingestion_params_.tiledb_config = utils::split(config_str, ',');
  // Attempt to set config to check validity
  // cfg object will be discarded as a later call to tiledb_init will properly
  // create config/context
  tiledb::Config cfg;
  utils::set_tiledb_config(ingestion_params_.tiledb_config, &cfg);
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
  dataset_.reset(new TileDBVCFDataset);
  dataset_->open(registration_params_.uri, ingestion_params_.tiledb_config);
  if (dataset_->metadata().version == TileDBVCFDataset::Version::V2 ||
      dataset_->metadata().version == TileDBVCFDataset::Version::V3)
    dataset_->register_samples(registration_params_);
  else {
    assert(dataset_->metadata().version == TileDBVCFDataset::Version::V4);
    throw std::runtime_error(
        "Only v2 and v3 datasets require registration. V4 and newer are "
        "capable of ingestion without registration.");
  }
}

void Writer::ingest_samples() {
  auto start_all = std::chrono::steady_clock::now();

  // If the user requests stats, enable them on read
  // Multiple calls to enable stats has no effect
  if (this->ingestion_params_.tiledb_stats_enabled) {
    tiledb::Stats::enable();
  } else {
    // Else we will make sure they are disable and reset
    tiledb::Stats::disable();
    tiledb::Stats::reset();
  }

  init(ingestion_params_);

  // Get the list of samples to ingest, sorted on ID (v2/v3) or name (v4)
  std::vector<SampleAndIndex> samples;
  if (dataset_->metadata().version == TileDBVCFDataset::V2 ||
      dataset_->metadata().version == TileDBVCFDataset::Version::V3)
    samples = prepare_sample_list(ingestion_params_);
  else
    samples = prepare_sample_list_v4(ingestion_params_);

  // Get a list of regions to ingest, covering the whole genome. The list of
  // disjoint region is used to divvy up work across ingestion threads.
  auto regions = prepare_region_list(ingestion_params_);

  // Batch the list of samples per space tile.
  std::vector<std::vector<SampleAndIndex>> batches;
  if (dataset_->metadata().version == TileDBVCFDataset::V2 ||
      dataset_->metadata().version == TileDBVCFDataset::Version::V3)
    batches =
        batch_elements_by_tile(samples, dataset_->metadata().row_tile_extent);
  else
    batches = batch_elements_by_tile_v4(
        samples, dataset_->metadata().row_tile_extent);

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
    auto result = ingest_samples(ingestion_params_, local_samples, regions);
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
  auto result = ingest_samples(ingestion_params_, local_samples, regions);
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
    const IngestionParams& params,
    const std::vector<SampleAndIndex>& samples,
    std::vector<Region>& regions) {
  uint64_t records_ingested = 0, anchors_ingested = 0;
  if (samples.empty() ||
      (regions.empty() &&
       (dataset_->metadata().version == TileDBVCFDataset::Version::V3 ||
        dataset_->metadata().version == TileDBVCFDataset::Version::V2)))
    return {0, 0};

  // TODO: workers can be reused across space tiles
  std::vector<std::unique_ptr<WriterWorker>> workers(params.num_threads);
  for (size_t i = 0; i < workers.size(); ++i) {
    if (dataset_->metadata().version == TileDBVCFDataset::Version::V2) {
      workers[i] = std::unique_ptr<WriterWorker>(new WriterWorkerV2());
    } else if (dataset_->metadata().version == TileDBVCFDataset::Version::V3) {
      workers[i] = std::unique_ptr<WriterWorker>(new WriterWorkerV3());
    } else {
      assert(dataset_->metadata().version == TileDBVCFDataset::Version::V4);
      workers[i] = std::unique_ptr<WriterWorker>(new WriterWorkerV4());
    }

    workers[i]->init(*dataset_, params, samples);
  }

  // First compose the set of contigs that are nonempty.
  // This can significantly speed things up in the common case that the sample
  // headers list many contigs that do not actually have any records.
  const auto& metadata = dataset_->metadata();
  std::set<std::string> nonempty_contigs;
  std::map<std::string, std::string> sample_headers;
  std::vector<Region> regions_v4;
  for (const auto& s : samples) {
    if (dataset_->metadata().version == TileDBVCFDataset::Version::V2) {
      VCFV2 vcf;
      vcf.open(s.sample_uri, s.index_uri);
      for (const auto& p : metadata.contig_offsets) {
        if (vcf.contig_has_records(p.first))
          nonempty_contigs.insert(p.first);
      }
    } else if (dataset_->metadata().version == TileDBVCFDataset::Version::V3) {
      VCFV3 vcf;
      vcf.open(s.sample_uri, s.index_uri);
      for (const auto& p : metadata.contig_offsets) {
        if (vcf.contig_has_records(p.first))
          nonempty_contigs.insert(p.first);
      }
    } else {
      assert(dataset_->metadata().version == TileDBVCFDataset::Version::V4);
      VCFV4 vcf;
      vcf.open(s.sample_uri, s.index_uri);
      // For V4 we also need to check the header, collect and write them

      // Allocate a header struct and try to parse from the local file.
      SafeBCFHdr hdr(VCFUtils::hdr_read_header(s.sample_uri), bcf_hdr_destroy);

      std::vector<std::string> hdr_samples =
          VCFUtils::hdr_get_samples(hdr.get());
      if (hdr_samples.size() > 1)
        throw std::invalid_argument(
            "Error registering samples; a file has more than 1 sample. "
            "Ingestion "
            "from cVCF is not supported.");

      const auto& sample_name = hdr_samples[0];
      sample_headers[sample_name] = VCFUtils::hdr_to_string(hdr.get());

      // Loop over all contigs in the header, store the nonempty and also the
      // regions
      for (auto& contig_region : VCFUtils::hdr_get_contigs_regions(hdr.get())) {
        nonempty_contigs.emplace(contig_region.seq_name);

        // regions
        bool region_found = false;
        for (auto& region : regions_v4) {
          if (region.seq_name == contig_region.seq_name) {
            region.max = std::max(region.max, contig_region.max);
            region_found = true;
            break;
          }
        }

        if (!region_found)
          regions_v4.emplace_back(contig_region);
      }
    }
  }

  // For V4 lets write the headers for this batch and also prepare the region
  // list specific to this batch
  if (dataset_->metadata().version == TileDBVCFDataset::Version::V4) {
    // If there were no regions in the VCF files return early
    if (regions_v4.empty())
      return {0, 0};

    dataset_->write_vcf_headers_v4(*ctx_, sample_headers);
    regions = prepare_region_list(regions_v4, ingestion_params_);
  }

  const size_t nregions = regions.size();
  size_t region_idx = 0;
  std::vector<std::future<bool>> tasks;
  for (unsigned i = 0; i < workers.size(); i++) {
    WriterWorker* worker = workers[i].get();
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

      WriterWorker* worker = workers[i].get();
      bool task_complete = false;
      while (!task_complete) {
        task_complete = tasks[i].get();

        // Write worker buffers, if any data.
        if (worker->records_buffered() > 0) {
          worker->buffers().set_buffers(
              query_.get(), dataset_->metadata().version);
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
    const IngestionParams& params) const {
  auto samples = SampleUtils::build_samples_uri_list(
      *vfs_, params.samples_file_uri, params.sample_uris);

  // Get sample names
  auto sample_names =
      SampleUtils::download_sample_names(*vfs_, samples, params.scratch_space);

  // Sort by sample ID
  std::vector<std::pair<SampleAndIndex, std::string>> sorted;
  for (size_t i = 0; i < samples.size(); i++)
    sorted.emplace_back(samples[i], sample_names[i]);

  TileDBVCFDataset* dataset = dataset_.get();
  std::sort(
      sorted.begin(),
      sorted.end(),
      [dataset](
          const std::pair<SampleAndIndex, std::string>& a,
          const std::pair<SampleAndIndex, std::string>& b) {
        return dataset->metadata().sample_ids.at(a.second) <
               dataset->metadata().sample_ids.at(b.second);
      });

  std::vector<SampleAndIndex> result;
  // Set sample id for later use
  for (const auto& pair : sorted) {
    auto s = pair.first;
    if (dataset_->metadata().version == TileDBVCFDataset::Version::V2 ||
        dataset_->metadata().version == TileDBVCFDataset::Version::V3)
      s.sample_id = dataset_->metadata().sample_ids.at(pair.second);
    result.push_back(s);
  }

  return result;
}

std::vector<SampleAndIndex> Writer::prepare_sample_list_v4(
    const IngestionParams& params) const {
  auto samples = SampleUtils::build_samples_uri_list(
      *vfs_, params.samples_file_uri, params.sample_uris);

  // Get sample names
  auto sample_names =
      SampleUtils::download_sample_names(*vfs_, samples, params.scratch_space);

  // Sort by sample ID
  std::vector<std::pair<SampleAndIndex, std::string>> sorted(samples.size());
  for (size_t i = 0; i < samples.size(); i++)
    sorted[i] = std::make_pair(samples[i], sample_names[i]);
  std::sort(
      sorted.begin(),
      sorted.end(),
      [](const std::pair<SampleAndIndex, std::string>& a,
         const std::pair<SampleAndIndex, std::string>& b) {
        return a.second < b.second;
      });

  std::vector<SampleAndIndex> result;
  // Set sample id for later use
  for (const auto& pair : sorted) {
    auto s = pair.first;
    if (dataset_->metadata().version == TileDBVCFDataset::Version::V2 ||
        dataset_->metadata().version == TileDBVCFDataset::Version::V3)
      s.sample_id = 0;
    result.push_back(s);
  }

  return result;
}

std::vector<Region> Writer::prepare_region_list(
    const IngestionParams& params) const {
  std::vector<Region> all_contigs = dataset_->all_contigs();
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

std::vector<Region> Writer::prepare_region_list(
    const std::vector<Region>& all_contigs,
    const IngestionParams& params) const {
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

void Writer::set_scratch_space(const std::string& path, uint64_t size) {
  ScratchSpaceInfo scratchSpaceInfo;
  scratchSpaceInfo.path = path;
  scratchSpaceInfo.size_mb = size;
  this->registration_params_.scratch_space = scratchSpaceInfo;
  this->ingestion_params_.scratch_space = scratchSpaceInfo;
}

void Writer::set_verbose(const bool& verbose) {
  ingestion_params_.verbose = verbose;
}

void Writer::set_tiledb_stats_enabled(bool stats_enabled) {
  this->ingestion_params_.tiledb_stats_enabled = stats_enabled;
}

void Writer::tiledb_stats_enabled(bool* enabled) {
  *enabled = this->ingestion_params_.tiledb_stats_enabled;
}

void Writer::tiledb_stats(char** stats) {
  auto rc = tiledb_stats_dump_str(stats);
  if (rc != TILEDB_OK)
    throw std::runtime_error("Error dumping tiledb statistics");
}

void Writer::dataset_version(int32_t* version) const {
  if (dataset_ == nullptr)
    throw std::runtime_error("Error getting dataset version");
  *version = dataset_->metadata().version;
}

}  // namespace vcf
}  // namespace tiledb
