/**
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2019-2021 TileDB, Inc.
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

#include <sys/resource.h>
#include <future>

#include "dataset/attribute_buffer_set.h"
#include "dataset/tiledbvcfdataset.h"
#include "utils/logger_public.h"
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

Writer::~Writer() {
  utils::free_htslib_tiledb_context();
  AlleleCount::close();
  VariantStats::close();
}

void Writer::init(const std::string& uri, const std::string& config_str) {
  if (!config_str.empty())
    set_tiledb_config(config_str);

  set_dataset_uri(uri);

  // Clean up old query and array objects first, if any.
  query_.reset(nullptr);
  array_.reset(nullptr);

  if (tiledb_config_ != nullptr) {
    ctx_.reset(new Context(*tiledb_config_));
  } else {
    ctx_.reset(new Context);
  }
  dataset_.reset(new TileDBVCFDataset(ctx_));

  std::vector<std::string> tiledb_config;
  if (!ingestion_params_.tiledb_config.empty())
    tiledb_config = ingestion_params_.tiledb_config;
  else if (!creation_params_.tiledb_config.empty())
    tiledb_config = creation_params_.tiledb_config;
  else if (!registration_params_.tiledb_config.empty())
    tiledb_config = registration_params_.tiledb_config;

  try {
    dataset_->open(uri, tiledb_config);
  } catch (std::exception& e) {
    // If the dataset doesn't exist lets not error out, the user might be
    // creating a new dataset
  }
}

void Writer::init(const IngestionParams& params) {
  // Clean up old query and array objects first, if any.
  query_.reset(nullptr);
  array_.reset(nullptr);

  tiledb_config_.reset(new Config);
  (*tiledb_config_)["vfs.s3.multipart_part_size"] = params.part_size_mb << 20;
  (*tiledb_config_)["sm.mem.total_budget"] = params.tiledb_memory_budget_mb
                                             << 20;
  (*tiledb_config_)["sm.compute_concurrency_level"] = params.num_threads;
  (*tiledb_config_)["sm.io_concurrency_level"] = params.num_threads;

  // User overrides
  utils::set_tiledb_config(params.tiledb_config, tiledb_config_.get());

  ctx_.reset(new Context(*tiledb_config_));
  dataset_.reset(new TileDBVCFDataset(ctx_));

  dataset_->set_tiledb_stats_enabled(params.tiledb_stats_enabled);
  dataset_->set_tiledb_stats_enabled_vcf_header(
      params.tiledb_stats_enabled_vcf_header_array);

  dataset_->open(
      params.uri, params.tiledb_config, params.load_data_array_fragment_info);

  // Set htslib global config and context based on user passed TileDB config
  // options
  std::vector<std::string> vfs_config = params.tiledb_config;
  try {
    auto vcf_region = tiledb_config_->get("vcf.s3.region");
    vfs_config.push_back("vfs.s3.region=" + vcf_region);
    LOG_INFO("VFS and htslib reading data from S3 region: {}", vcf_region);
  } catch (...) {
    // tiledb_config_ is not defined in "vcf.s3.region", no action required
  }
  utils::set_htslib_tiledb_context(vfs_config);

  vfs_config_.reset(new Config);
  utils::set_tiledb_config(vfs_config, vfs_config_.get());

  vfs_.reset(new VFS(*ctx_, *vfs_config_));
  array_.reset(new Array(*ctx_, dataset_->data_uri(), TILEDB_WRITE));
  query_.reset(new Query(*ctx_, *array_));
  query_->set_layout(TILEDB_GLOBAL_ORDER);

  creation_params_.checksum = TILEDB_FILTER_CHECKSUM_SHA256;
  creation_params_.allow_duplicates = true;

  AlleleCount::init(ctx_, params.uri);
  VariantStats::init(ctx_, params.uri);
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

void Writer::set_vcf_attributes(const std::string& vcf_uri) {
  creation_params_.vcf_uri = vcf_uri;
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

void Writer::set_tile_capacity(const uint64_t tile_capacity) {
  creation_params_.tile_capacity = tile_capacity;
}

void Writer::set_anchor_gap(const uint32_t anchor_gap) {
  creation_params_.anchor_gap = anchor_gap;
}

void Writer::create_dataset() {
  TileDBVCFDataset::create(creation_params_);
}

void Writer::register_samples() {
  if (tiledb_config_ != nullptr) {
    ctx_.reset(new Context(*tiledb_config_));
  } else {
    ctx_.reset(new Context);
  }
  dataset_.reset(new TileDBVCFDataset(ctx_));
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

void Writer::update_params(IngestionParams& params) {
  // Override total memory budget if total_memory_percentage is provided
  if (params.total_memory_percentage > 0) {
    params.total_memory_budget_mb =
        utils::system_memory_mb() * params.total_memory_percentage;
  }

  // Distribute memory budget
  uint32_t total_mb = params.total_memory_budget_mb;
  uint32_t tiledb_mb = total_mb * params.ratio_tiledb_memory;
  tiledb_mb = std::min(tiledb_mb, params.max_tiledb_memory_mb);
  uint32_t input_mb = params.input_record_buffer_mb * params.num_threads *
                      params.sample_batch_size;
  float output_ratio = 0.5;
  uint32_t output_mb = (total_mb - tiledb_mb - input_mb) * output_ratio;
  uint32_t stats_mb = (total_mb - tiledb_mb - input_mb) * (1.0 - output_ratio);

  params.tiledb_memory_budget_mb = tiledb_mb;
  params.output_memory_budget_mb = output_mb;

  LOG_INFO(
      "Memory budget: total={} MiB = tiledb={} MiB + input={} MiB + "
      "output={} MiB + stats={} MiB",
      total_mb,
      tiledb_mb,
      input_mb,
      output_mb,
      stats_mb);

  // Set per thread, per sample vcf input buffer size
  if (params.use_legacy_max_record_buffer_size) {
    LOG_INFO(
        "Using legacy option: --max-record-buff={}",
        params.max_record_buffer_size);
    params.max_record_buffer_size =
        params.max_record_buffer_size * params.avg_vcf_record_size;
  } else {
    params.max_record_buffer_size = params.input_record_buffer_mb << 20;
  }

  LOG_INFO(
      "Input buffers = {} threads * {} samples * {} MiB",
      params.num_threads,
      params.sample_batch_size,
      params.max_record_buffer_size >> 20);

  // Set per thread output buffer size
  if (params.use_legacy_max_tiledb_buffer_size_mb) {
    LOG_INFO(
        "Using legacy option: --mem-budget-mb={}",
        params.max_tiledb_buffer_size_mb);
    LOG_INFO("Output buffer flush = {} MiB", params.max_tiledb_buffer_size_mb);

  } else {
    params.max_tiledb_buffer_size_mb = params.ratio_output_flush *
                                       params.output_memory_budget_mb /
                                       params.num_threads;
    LOG_INFO(
        "Output buffers = {} threads * {} MiB (flush = {} MiB)",
        params.num_threads,
        output_mb / params.num_threads,
        params.max_tiledb_buffer_size_mb);
  }
}

void Writer::ingest_samples() {
  auto start_all = std::chrono::steady_clock::now();

  // If the user requests stats, enable them on read
  // Multiple calls to enable stats has no effect
  if (ingestion_params_.tiledb_stats_enabled) {
    tiledb::Stats::enable();
  } else {
    // Else we will make sure they are disable and reset
    tiledb::Stats::disable();
    tiledb::Stats::reset();
  }

  if (ingestion_params_.resume_sample_partial_ingestion) {
    ingestion_params_.load_data_array_fragment_info = true;
  }

  // Reset expected total record count
  total_records_expected_ = 0;

  // Set open file soft limit to hard limit
  struct rlimit limit;
  if (getrlimit(RLIMIT_NOFILE, &limit) != 0) {
    LOG_WARN("Unable to read open file limit");
  } else {
    LOG_DEBUG("Open file limit = {}", limit.rlim_cur);
    limit.rlim_cur = limit.rlim_max;
    if (setrlimit(RLIMIT_NOFILE, &limit) != 0) {
      LOG_WARN("Unable to set open file limit");
    } else {
      if (getrlimit(RLIMIT_NOFILE, &limit) != 0) {
        LOG_WARN("Unable to read new open file limit");
      } else {
        LOG_DEBUG("New open file limit = {}", limit.rlim_cur);
      }
    }
  }

  update_params(ingestion_params_);
  init(ingestion_params_);

  if (ingestion_params_.resume_sample_partial_ingestion &&
      (dataset_->metadata().version == TileDBVCFDataset::V2 ||
       dataset_->metadata().version == TileDBVCFDataset::Version::V3)) {
    throw std::runtime_error(
        "Resume support only support for v4 or higher datasets");
  }

  std::unordered_map<
      std::pair<std::string, std::string>,
      std::vector<std::pair<std::string, std::string>>,
      tiledb::vcf::pair_hash>
      existing_fragments;
  if (ingestion_params_.resume_sample_partial_ingestion) {
    LOG_DEBUG(
        "Starting fetching of contig to sample list for resumption checking");
    existing_fragments = dataset_->fragment_sample_contig_list();
    LOG_DEBUG(
        "Finished fetching of contig sample list for resumption checking");
  }

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
    batches = batch_elements_by_tile(
        samples, dataset_->metadata().ingestion_sample_batch_size);
  else
    batches =
        batch_elements_by_tile_v4(samples, ingestion_params_.sample_batch_size);

  std::vector<SampleAndIndex> local_samples;
  ScratchSpaceInfo scratch_space_a = ingestion_params_.scratch_space;
  ScratchSpaceInfo scratch_space_b = ingestion_params_.scratch_space;
  std::future<std::vector<tiledb::vcf::SampleAndIndex>> future_paths;
  uint64_t scratch_size_mb = 0;

  bool download_samples = !ingestion_params_.scratch_space.path.empty();
  if (download_samples) {
    // Set up parameters for two scratch spaces.
    scratch_size_mb = ingestion_params_.scratch_space.size_mb / 2;
    scratch_space_a.size_mb = scratch_size_mb;
    scratch_space_a.path = utils::uri_join(scratch_space_a.path, "ingest-a");
    if (!vfs_->is_dir(scratch_space_a.path))
      vfs_->create_dir(scratch_space_a.path);
    scratch_space_b.size_mb = scratch_size_mb;
    scratch_space_b.path = utils::uri_join(scratch_space_b.path, "ingest-b");
    if (!vfs_->is_dir(scratch_space_b.path))
      vfs_->create_dir(scratch_space_b.path);
  }

  // Start fetching first batch, either downloading of using remote vfs plugin
  // if no scratch space.
  TRY_CATCH_THROW(
      future_paths = std::async(
          std::launch::async,
          SampleUtils::get_samples,
          *vfs_,
          batches[0],
          &scratch_space_a));

  LOG_DEBUG(
      "Initialization completed in {:.3f} seconds.",
      utils::chrono_duration(start_all));
  uint64_t records_ingested = 0, anchors_ingested = 0;
  uint64_t samples_ingested = 0;
  for (unsigned i = 1; i < batches.size(); i++) {
    // Block until current batch is fetched.
    TRY_CATCH_THROW(local_samples = future_paths.get());

    // Start the next batch fetching.
    TRY_CATCH_THROW(
        future_paths = std::async(
            std::launch::async,
            SampleUtils::get_samples,
            *vfs_,
            batches[i],
            &scratch_space_b));

    // Ingest the batch.
    auto start_batch = std::chrono::steady_clock::now();
    std::pair<uint64_t, uint64_t> result;
    if (dataset_->metadata().version == TileDBVCFDataset::Version::V3 ||
        dataset_->metadata().version == TileDBVCFDataset::Version::V2) {
      result = ingest_samples(ingestion_params_, local_samples, regions);
    } else {
      assert(dataset_->metadata().version == TileDBVCFDataset::Version::V4);
      result = ingest_samples_v4(
          ingestion_params_, local_samples, regions, existing_fragments);
    }
    records_ingested += result.first;
    anchors_ingested += result.second;
    samples_ingested += local_samples.size();

    LOG_INFO(
        "Finished ingesting {} / {} samples ({} sec)...",
        samples_ingested,
        samples.size(),
        utils::chrono_duration(start_batch));

    if (download_samples) {
      // Reset current scratch space and swap.
      if (vfs_->is_dir(scratch_space_a.path))
        vfs_->remove_dir(scratch_space_a.path);
      vfs_->create_dir(scratch_space_a.path);
      scratch_space_a.size_mb = scratch_size_mb;
      std::swap(scratch_space_a, scratch_space_b);
    }
  }

  // Ingest the last batch
  TRY_CATCH_THROW(local_samples = future_paths.get());
  std::pair<uint64_t, uint64_t> result;
  if (dataset_->metadata().version == TileDBVCFDataset::Version::V3 ||
      dataset_->metadata().version == TileDBVCFDataset::Version::V2) {
    result = ingest_samples(ingestion_params_, local_samples, regions);

    // Make sure to finalize for v2/v3
    query_->finalize();
  } else {
    assert(dataset_->metadata().version == TileDBVCFDataset::Version::V4);
    result = ingest_samples_v4(
        ingestion_params_, local_samples, regions, existing_fragments);
  }
  records_ingested += result.first;
  anchors_ingested += result.second;

  auto t0 = std::chrono::steady_clock::now();
  LOG_DEBUG("Making sure all finalize tasks completed...");
  for (auto& finalize_task : finalize_tasks_) {
    if (finalize_task.valid()) {
      TRY_CATCH_THROW(finalize_task.get());
    }
  }
  LOG_DEBUG(
      "All finalize tasks successfully completed. Waited for {} sec.",
      utils::chrono_duration(t0));

  AlleleCount::close();
  VariantStats::close();
  array_->close();

  // Clean up
  if (download_samples) {
    if (vfs_->is_dir(scratch_space_a.path))
      vfs_->remove_dir(scratch_space_a.path);
    if (vfs_->is_dir(scratch_space_b.path))
      vfs_->remove_dir(scratch_space_b.path);
  }
  if (ingestion_params_.remove_samples_file &&
      vfs_->is_file(ingestion_params_.samples_file_uri))
    vfs_->remove_file(ingestion_params_.samples_file_uri);

  auto time_sec = utils::chrono_duration(start_all);
  LOG_INFO(fmt::format(
      std::locale(""),
      "Done. Ingested {:L} records (+ {:L} anchors) from {:L} samples in "
      "{:.3f} seconds = {:.1f} records/sec",
      records_ingested,
      anchors_ingested,
      samples.size(),
      time_sec,
      records_ingested / time_sec));

  // Check records ingested matches total records in VCF files, unless resume
  // is enabled because resume may not ingest all records in the VCF files
  // (check not implemented for V2/V3)
  if (dataset_->metadata().version >= TileDBVCFDataset::Version::V4 &&
      !ingestion_params_.resume_sample_partial_ingestion) {
    if (records_ingested != total_records_expected_) {
      std::string message = fmt::format(
          "QACheck: [FAIL] Total records ingested ({}) != total records in VCF "
          "files ({})",
          records_ingested,
          total_records_expected_);
      LOG_ERROR(message);
      throw std::runtime_error(message);
    } else {
      LOG_INFO(
          "QACheck: [PASS] Total records ingested ({}) == total records in VCF "
          "files ({})",
          records_ingested,
          total_records_expected_);
    }
  }
}

std::pair<uint64_t, uint64_t> Writer::ingest_samples(
    const IngestionParams& params,
    const std::vector<SampleAndIndex>& samples,
    std::vector<Region>& regions) {
  assert(
      dataset_->metadata().version == TileDBVCFDataset::Version::V3 ||
      dataset_->metadata().version == TileDBVCFDataset::Version::V2);
  uint64_t records_ingested = 0, anchors_ingested = 0;
  if (samples.empty() || regions.empty())
    return {0, 0};

  // TODO: workers can be reused across space tiles
  std::vector<std::unique_ptr<WriterWorker>> workers(params.num_threads);
  for (size_t i = 0; i < workers.size(); ++i) {
    if (dataset_->metadata().version == TileDBVCFDataset::Version::V2) {
      workers[i] = std::unique_ptr<WriterWorker>(new WriterWorkerV2());
    } else {
      assert(dataset_->metadata().version == TileDBVCFDataset::Version::V3);
      workers[i] = std::unique_ptr<WriterWorker>(new WriterWorkerV3());
    }

    workers[i]->init(*dataset_, params, samples);
    workers[i]->set_max_total_buffer_size_mb(params.max_tiledb_buffer_size_mb);
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
    } else {
      assert(dataset_->metadata().version == TileDBVCFDataset::Version::V3);
      VCFV3 vcf;
      vcf.open(s.sample_uri, s.index_uri);
      for (const auto& p : metadata.contig_offsets) {
        if (vcf.contig_has_records(p.first))
          nonempty_contigs.insert(p.first);
      }
    }
  }

  const size_t nregions = regions.size();
  size_t region_idx = 0;
  std::vector<std::future<bool>> tasks;
  for (unsigned i = 0; i < workers.size(); i++) {
    WriterWorker* worker = workers[i].get();
    while (region_idx < nregions) {
      Region reg = regions[region_idx++];
      if (nonempty_contigs.count(reg.seq_name) > 0) {
        TRY_CATCH_THROW(
            tasks.push_back(std::async(std::launch::async, [worker, reg]() {
              return worker->parse(reg);
            })));
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
        TRY_CATCH_THROW(task_complete = tasks[i].get());

        // Write worker buffers, if any data.
        if (worker->records_buffered() > 0) {
          worker->buffers().set_buffers(
              query_.get(), dataset_->metadata().version);
          auto st = query_->submit();
          if (st != Query::Status::COMPLETE)
            throw std::runtime_error(
                "Error submitting TileDB write query; unexpected query "
                "status.");

          LOG_INFO(
              "Writing {} for contig {} (task {} / {})",
              worker->records_buffered(),
              worker->region().seq_name,
              i,
              tasks.size());
        }
        records_ingested += worker->records_buffered();
        anchors_ingested += worker->anchors_buffered();

        // Repeatedly resume the same worker where it left off until it
        // is able to complete.
        if (!task_complete) {
          TRY_CATCH_THROW(tasks[i] = std::async(std::launch::async, [worker]() {
                            return worker->resume();
                          }));
        }
      }

      // Start next region parsing using the same worker.
      while (region_idx < nregions) {
        Region reg = regions[region_idx++];
        if (nonempty_contigs.count(reg.seq_name) > 0) {
          TRY_CATCH_THROW(
              tasks[i] = std::async(std::launch::async, [worker, reg]() {
                return worker->parse(reg);
              }));
          finished = false;
          break;
        }
      }
    }
  }

  return {records_ingested, anchors_ingested};
}

std::pair<uint64_t, uint64_t> Writer::ingest_samples_v4(
    const IngestionParams& params,
    const std::vector<SampleAndIndex>& samples,
    std::vector<Region>& regions,
    std::unordered_map<
        std::pair<std::string, std::string>,
        std::vector<std::pair<std::string, std::string>>,
        pair_hash> existing_sample_contig_fragments) {
  assert(dataset_->metadata().version == TileDBVCFDataset::Version::V4);
  uint64_t records_ingested = 0, anchors_ingested = 0;

  if (ingestion_params_.contig_mode != IngestionParams::ContigMode::ALL &&
      ingestion_params_.contigs_to_allow_merging.size()) {
    LOG_FATAL("Cannot set contigs_to_allow_merging with contig_mode != all");
  }

  // TODO: workers can be reused across space tiles
  // TODO: use multiple threads for vcf open, currenly serial with num_threads *
  // samples.size() vcf open calls
  std::vector<std::unique_ptr<WriterWorker>> workers(params.num_threads);
  for (size_t i = 0; i < workers.size(); ++i) {
    workers[i] = std::unique_ptr<WriterWorker>(new WriterWorkerV4(i));

    workers[i]->init(*dataset_, params, samples);
    workers[i]->set_max_total_buffer_size_mb(params.max_tiledb_buffer_size_mb);
  }

  // Create a worker for buffering anchors
  WriterWorkerV4 anchor_worker(params.num_threads);
  anchor_worker.init(*dataset_, params, samples);
  anchor_worker.set_max_total_buffer_size_mb(params.max_tiledb_buffer_size_mb);

  // First compose the set of contigs that are nonempty.
  // This can significantly speed things up in the common case that the sample
  // headers list many contigs that do not actually have any records.
  std::set<std::string> nonempty_contigs;
  std::map<std::string, std::string> sample_headers;
  std::vector<Region> regions_v4;

  // Total number of records in each contig for all samples.
  std::map<std::string, uint32_t> total_contig_records;
  for (const auto& s : samples) {
    VCFV4 vcf;
    vcf.open(s.sample_uri, s.index_uri);
    // For V4 we also need to check the header, collect and write them

    // Allocate a header struct and try to parse from the local file.
    SafeBCFHdr hdr(VCFUtils::hdr_read_header(s.sample_uri), bcf_hdr_destroy);

    std::vector<std::string> hdr_samples = VCFUtils::hdr_get_samples(hdr.get());
    // Initially set sample_name to empty string to support annoated vcf's
    // without sample in the header
    std::string sample_name;
    if (hdr_samples.size() > 1)
      throw std::invalid_argument(
          "Error registering samples; a file has more than 1 sample. "
          "Ingestion "
          "from cVCF is not supported.");
    else if (hdr_samples.size() == 1)
      sample_name = hdr_samples[0];
    sample_headers[sample_name] = VCFUtils::hdr_to_string(hdr.get());

    // Loop over all contigs in the header, store the nonempty and also the
    // regions
    for (auto& contig_region : VCFUtils::hdr_get_contigs_regions(hdr.get())) {
      // Skip empty contigs
      if (!vcf.contig_has_records(contig_region.seq_name))
        continue;

      // Ingesting MERGED contigs, skip contigs in the
      // contigs_to_keep_separate list
      if (ingestion_params_.contig_mode ==
              IngestionParams::ContigMode::MERGED &&
          ingestion_params_.contigs_to_keep_separate.find(
              contig_region.seq_name) !=
              ingestion_params_.contigs_to_keep_separate.end()) {
        continue;
      }

      // Ingesting SEPARATE contigs, skip contigs not in the
      // contigs_to_keep_separate list
      if (ingestion_params_.contig_mode ==
              IngestionParams::ContigMode::SEPARATE &&
          ingestion_params_.contigs_to_keep_separate.find(
              contig_region.seq_name) ==
              ingestion_params_.contigs_to_keep_separate.end()) {
        continue;
      }

      LOG_TRACE(fmt::format(
          std::locale(""),
          "Sample {} contig {}: {:L} positions {:L} records",
          sample_name,
          contig_region.seq_name,
          contig_region.max + 1,
          vcf.record_count(contig_region.seq_name)));

      total_contig_records[contig_region.seq_name] +=
          vcf.record_count(contig_region.seq_name);

      total_records_expected_ += vcf.record_count(contig_region.seq_name);

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

  // If resuming, skip contigs that already exist in the array
  if (params.resume_sample_partial_ingestion &&
      !existing_sample_contig_fragments.empty()) {
    const std::string first_sample_name =
        VCFUtils::get_sample_name_from_vcf(samples.front().sample_uri)[0];
    const std::string last_sample_name =
        VCFUtils::get_sample_name_from_vcf(samples.back().sample_uri)[0];

    LOG_INFO("Resume: checking for regions to skip");
    LOG_DEBUG("Resume: regions before resume check = {}", regions_v4.size());

    // Loop over all regions and check if contig has already been ingested
    for (auto it = regions_v4.begin(); it != regions_v4.end();) {
      auto& contig = it->seq_name;
      bool skip = false;

      LOG_DEBUG(
          "Resume: Checking sample_range=({}, {}) contig={}",
          first_sample_name,
          last_sample_name,
          contig);

      // Check if the batch sample range exactly matches any fragment's sample
      // non-empty domain
      bool sample_match = existing_sample_contig_fragments.find(
                              {first_sample_name, last_sample_name}) !=
                          existing_sample_contig_fragments.end();

      if (sample_match) {
        auto frag_contigs = existing_sample_contig_fragments.at(
            {first_sample_name, last_sample_name});

        LOG_DEBUG(
            "Resume:   found fragments with sample_range=({}, {})",
            first_sample_name,
            last_sample_name);
        // Loop over contigs for the sample range
        for (auto& frag_contig : frag_contigs) {
          LOG_TRACE(
              "Resume:     check contig {} in fragment ({}, {})",
              contig,
              frag_contig.first,
              frag_contig.second);
          // If the batch contig is contained in the fragment's contig range,
          // skip this region
          if (contig >= frag_contig.first && contig <= frag_contig.second) {
            skip = true;
            break;
          }
        }
      }

      // Remove the region if marked to skip
      if (skip) {
        LOG_DEBUG("Resume:   skipping contig {}", contig);
        it = regions_v4.erase(it);
      } else {
        it++;
      }
    }
    LOG_DEBUG("Resume: regions after resume check = {}", regions_v4.size());
  }

  // If there were no regions in the VCF files return early
  if (regions_v4.empty())
    return {0, 0};

  // Estimate the number of records that will fill the output buffer
  float output_buffer_records = 1024.0 * 1024.0 *
                                params.max_tiledb_buffer_size_mb /
                                params.avg_vcf_record_size;

  LOG_DEBUG("Output buffer records = {}", output_buffer_records);
  assert(output_buffer_records > 0 && output_buffer_records < UINT32_MAX);

  if (params.use_legacy_thread_task_size) {
    LOG_INFO(
        "Using legacy option: --thread-task-size={}", params.thread_task_size);
  }

  // Set worker task size for each contig
  std::map<std::string, uint32_t> contig_task_size;
  for (auto& region : regions_v4) {
    // convert total records to task size
    uint32_t total_records = total_contig_records[region.seq_name];
    uint32_t task_size = region.max + 1;
    if (total_records > output_buffer_records) {
      task_size = params.ratio_task_size * region.max * output_buffer_records /
                  total_records;
    }
    contig_task_size[region.seq_name] = params.use_legacy_thread_task_size ?
                                            params.thread_task_size :
                                            task_size;
  }

  regions = prepare_region_list(regions_v4, contig_task_size);

  // For V4 lets write the headers for this batch and also prepare the region
  // list specific to this batch
  dataset_->write_vcf_headers_v4(*ctx_, sample_headers);

  const size_t nregions = regions.size();
  size_t region_idx = 0;
  std::vector<std::future<bool>> tasks;
  for (unsigned i = 0; i < workers.size(); i++) {
    WriterWorker* worker = workers[i].get();
    while (region_idx < nregions) {
      Region reg = regions[region_idx++];
      if (nonempty_contigs.count(reg.seq_name) > 0) {
        TRY_CATCH_THROW(
            tasks.push_back(std::async(std::launch::async, [worker, reg]() {
              return worker->parse(reg);
            })));
        break;
      }
    }
  }

  uint32_t last_start_pos = 0;
  int last_merged_fragment_index = 0;
  std::string last_region_contig = workers[0]->region().seq_name;
  std::string starting_region_contig_for_merge = workers[0]->region().seq_name;
  bool finished = tasks.empty();
  while (!finished) {
    finished = true;

    auto start = std::chrono::steady_clock::now();
    size_t prev_records = records_ingested;
    for (unsigned i = 0; i < tasks.size(); i++) {
      if (!tasks[i].valid())
        continue;

      WriterWorker* worker = workers[i].get();
      bool task_complete = false;
      while (!task_complete) {
        TRY_CATCH_THROW(task_complete = tasks[i].get());

        // Write worker buffers, if any data.
        if (worker->records_buffered() > 0) {
          const std::string& contig = worker->region().seq_name;
          // Check if finished contig is allowed to be merged
          const bool contig_mergeable = check_contig_mergeable(contig);
          const bool last_contig_mergeable =
              check_contig_mergeable(last_region_contig);
          bool finalize_merged_fragment = false;

          // If ingesting merged contigs only, finalize on fragment boundaries
          if (ingestion_params_.contig_mode ==
              IngestionParams::ContigMode::MERGED) {
            int merged_fragment_index = get_merged_fragment_index(contig);
            finalize_merged_fragment =
                merged_fragment_index != last_merged_fragment_index;
            last_merged_fragment_index = merged_fragment_index;
          }
          //  If the contig is different the last one we wrote, and we aren't
          //  suppose to merge this new one, then finalize the previous one
          if (last_region_contig != contig) {
            if (!last_contig_mergeable || !contig_mergeable ||
                finalize_merged_fragment) {
              if (!last_contig_mergeable) {
                LOG_DEBUG(
                    "Previous contig {} found to NOT be mergeable, "
                    "finalizing previous fragment and starting new write for "
                    "{}",
                    last_region_contig,
                    contig);
              } else if (!contig_mergeable) {
                LOG_DEBUG(
                    "Contig {0} found to NOT be mergeable, "
                    "finalizing previous fragment and starting new write for "
                    "{0}",
                    contig);
              } else if (finalize_merged_fragment) {
                LOG_DEBUG(
                    "Previous contig {} found to be end of mergeable fragment, "
                    "finalizing previous fragment and starting new write for "
                    "{}",
                    last_region_contig,
                    contig);
              }

              // If the contig is different the last one we wrote, and we
              // aren't suppose to merge this new one, then finalize the
              // previous one. If this is the first contig, last_region_contig
              // will be empty and we can skip the finalize.
              if (!last_region_contig.empty()) {
                LOG_INFO(
                    "Finalizing contig batch [{}, {}]",
                    starting_region_contig_for_merge,
                    last_region_contig);

                //=================================================================
                // Start finalize of previous contig.

                // Finalize stats arrays.
                AlleleCount::finalize();
                VariantStats::finalize();

                // Write and finalize anchors stored in the anchor worker.
                anchors_ingested += write_anchors(anchor_worker);

                // Finalize fragment for this contig async
                // it is okay to move the query because we reset it next.
                // NOTE: Finalize after the stats arrays and anchor fragment
                // are finalized to ensure a valid data fragment will have
                // corresponding valid stats and anchor fragments.
                TRY_CATCH_THROW(finalize_tasks_.emplace_back(std::async(
                    std::launch::async, finalize_query, std::move(query_))));

                // End finalize of previous contig.
                //=================================================================
              }

              // Start new query for new fragment for next contig
              query_.reset(new Query(*ctx_, *array_));
              query_->set_layout(TILEDB_GLOBAL_ORDER);

              // Set new contig
              last_region_contig = contig;
              starting_region_contig_for_merge = contig;
            } else {
              LOG_DEBUG(
                  "last contig {} and contig {} found to be mergeable, "
                  "combining into super fragment",
                  last_region_contig,
                  contig);
              // Set the last contig to the current one if we are merging
              last_region_contig = contig;
            }

            // Reset the global order checker moving to a new contig
            last_start_pos = 0;
          }

          worker->buffers().set_buffers(
              query_.get(), dataset_->metadata().version);
          auto st = query_->submit();
          if (st != Query::Status::COMPLETE)
            throw std::runtime_error(
                "Error submitting TileDB write query; unexpected query "
                "status.");
          {
            auto first = worker->buffers().start_pos().value<uint32_t>(0);
            auto nelts = worker->buffers().start_pos().nelts<uint32_t>();
            auto last =
                worker->buffers().start_pos().value<uint32_t>(nelts - 1);
            LOG_DEBUG(
                "Recorded {:L} cells from {}:{}-{} (task {} / {})",
                worker->records_buffered(),
                contig,
                first,
                last,
                i + 1,
                tasks.size());

            if (last_start_pos > first) {
              LOG_FATAL(
                  "VCF global order check failed: {} > {}",
                  last_start_pos,
                  first);
            }
            last_start_pos = last;
          }

          // Flush ingestion tasks
          worker->flush_ingestion_tasks();
        } else {
          LOG_DEBUG("No records found for {}", worker->region().seq_name);
        }
        records_ingested += worker->records_buffered();

        // Drain anchors from the worker into the anchor_worker
        static_cast<WriterWorkerV4*>(worker)->drain_anchors(anchor_worker);

        // Repeatedly resume the same worker where it left off until it
        // is able to complete.
        if (!task_complete) {
          TRY_CATCH_THROW(tasks[i] = std::async(std::launch::async, [worker]() {
                            return worker->resume();
                          }));
          LOG_DEBUG("Work for {} not complete, resuming", i);
        }
      }

      // Start next region parsing using the same worker.
      while (region_idx < nregions) {
        Region reg = regions[region_idx++];
        if (nonempty_contigs.count(reg.seq_name) > 0) {
          TRY_CATCH_THROW(
              tasks[i] = std::async(std::launch::async, [worker, reg]() {
                return worker->parse(reg);
              }));
          finished = false;
          break;
        }
      }
    }
    if (records_ingested > prev_records) {
      LOG_INFO(
          "Ingestion rate = {:.3f} records/sec (VmRSS = {})",
          (records_ingested - prev_records) / utils::chrono_duration(start),
          utils::memory_usage_str());
    }
  }

  LOG_DEBUG(
      "Finalizing last contig batch of [{}, {}]",
      starting_region_contig_for_merge,
      last_region_contig);

  //=================================================================
  // Start finalize of last contig.

  // Finalize stats arrays.
  AlleleCount::finalize();
  VariantStats::finalize();

  // Write and finalize anchors stored in the anchor worker.
  anchors_ingested += write_anchors(anchor_worker);

  // Finalize fragment for this contig.
  // NOTE: Finalize after the stats arrays and anchor fragment
  // are finalized to ensure a valid data fragment will have
  // corresponding valid stats and anchor fragments.
  TRY_CATCH_THROW(finalize_tasks_.emplace_back(
      std::async(std::launch::async, finalize_query, std::move(query_))));

  // End finalize of last contig.
  //=================================================================

  // Start new query for new fragment for next contig
  query_.reset(new Query(*ctx_, *array_));
  query_->set_layout(TILEDB_GLOBAL_ORDER);

  return {records_ingested, anchors_ingested};
}

size_t Writer::write_anchors(WriterWorkerV4& worker) {
  // Buffer anchor records in the anchor worker
  int records = worker.buffer_anchors();

  if (records) {
    LOG_DEBUG("Write {} anchor records.", records);

    // Create a new query object
    auto query = std::make_unique<Query>(*ctx_, *array_);
    query->set_layout(TILEDB_GLOBAL_ORDER);

    // Set the query buffers
    worker.buffers().set_buffers(query.get(), dataset_->metadata().version);

    // Submit and finalize the query
    auto st = query->submit();
    if (st != Query::Status::COMPLETE) {
      LOG_FATAL("Error submitting TileDB write query: status = {}", st);
    }
    query->finalize();
  }

  return records;
}

std::vector<SampleAndIndex> Writer::prepare_sample_list(
    const IngestionParams& params) const {
  auto samples = SampleUtils::build_samples_uri_list(
      *vfs_, params.samples_file_uri, params.sample_uris);

  // Get sample names
  auto sample_names =
      SampleUtils::get_sample_names(*vfs_, samples, params.scratch_space);

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
      SampleUtils::get_sample_names(*vfs_, samples, params.scratch_space);

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
    const std::map<std::string, uint32_t>& contig_task_size) const {
  std::vector<Region> result;

  for (const auto& r : all_contigs) {
    const uint32_t task_size = contig_task_size.at(r.seq_name);
    const uint32_t contig_len = r.max - r.min + 1;
    const uint32_t ntasks = utils::ceil(contig_len, task_size);
    LOG_DEBUG(fmt::format(
        std::locale(""),
        "Contig {}: task size = {:L} tasks = {:L}",
        r.seq_name,
        task_size,
        ntasks));
    for (uint32_t i = 0; i < ntasks; i++) {
      uint32_t task_min = r.min + i * task_size;
      uint32_t task_max = std::min(task_min + task_size - 1, r.max);
      result.emplace_back(r.seq_name, task_min, task_max);
    }
  }

  std::sort(result.begin(), result.end());

  return result;
}

void Writer::set_num_threads(const unsigned threads) {
  ingestion_params_.num_threads = threads;
}

void Writer::set_total_memory_budget_mb(const uint32_t total_memory_budget_mb) {
  ingestion_params_.total_memory_budget_mb = total_memory_budget_mb;
}

void Writer::set_total_memory_percentage(const float total_memory_percentage) {
  ingestion_params_.total_memory_percentage = total_memory_percentage;
}

void Writer::set_ratio_tiledb_memory(const float ratio_tiledb_memory) {
  ingestion_params_.ratio_tiledb_memory = ratio_tiledb_memory;
}

void Writer::set_max_tiledb_memory_mb(const uint32_t max_tiledb_memory_mb) {
  ingestion_params_.max_tiledb_memory_mb = max_tiledb_memory_mb;
}

void Writer::set_input_record_buffer_mb(const uint32_t input_record_buffer_mb) {
  ingestion_params_.input_record_buffer_mb = input_record_buffer_mb;
}

void Writer::set_avg_vcf_record_size(const uint32_t avg_vcf_record_size) {
  ingestion_params_.avg_vcf_record_size = avg_vcf_record_size;
}

void Writer::set_ratio_task_size(const float ratio_task_size) {
  ingestion_params_.ratio_task_size = ratio_task_size;
}

void Writer::set_ratio_output_flush(const float ratio_output_flush) {
  ingestion_params_.ratio_output_flush = ratio_output_flush;
}

void Writer::set_thread_task_size(const unsigned size) {
  ingestion_params_.use_legacy_thread_task_size = true;
  ingestion_params_.thread_task_size = size;
}

void Writer::set_memory_budget(const unsigned mb) {
  ingestion_params_.use_legacy_max_tiledb_buffer_size_mb = true;
  ingestion_params_.max_tiledb_buffer_size_mb = mb;
}

void Writer::set_record_limit(const uint64_t max_num_records) {
  ingestion_params_.use_legacy_max_record_buffer_size = true;
  ingestion_params_.max_record_buffer_size = max_num_records;
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
  if (verbose) {
    LOG_CONFIG("debug");
    LOG_INFO("Verbose mode enabled");
  }
}

void Writer::set_tiledb_stats_enabled(bool stats_enabled) {
  this->ingestion_params_.tiledb_stats_enabled = stats_enabled;
}

void Writer::tiledb_stats_enabled(bool* enabled) const {
  *enabled = this->ingestion_params_.tiledb_stats_enabled;
}

void Writer::set_tiledb_stats_enabled_vcf_header_array(bool stats_enabled) {
  this->ingestion_params_.tiledb_stats_enabled_vcf_header_array = stats_enabled;
}

void Writer::tiledb_stats_enabled_vcf_header_array(bool* enabled) const {
  *enabled = this->ingestion_params_.tiledb_stats_enabled_vcf_header_array;
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

void Writer::finalize_query(std::unique_ptr<tiledb::Query> query) {
  query->finalize();
}

void Writer::set_sample_batch_size(const uint64_t size) {
  ingestion_params_.sample_batch_size = size;
}

void Writer::set_resume_sample_partial_ingestion(const bool resume) {
  ingestion_params_.resume_sample_partial_ingestion = resume;
}

bool Writer::check_contig_mergeable(const std::string& contig) {
  // If merging is disabled always return false
  if (!ingestion_params_.contig_fragment_merging)
    return false;

  // If contig is in blacklist, then it can not be merged
  if (!ingestion_params_.contigs_to_keep_separate.empty() &&
      ingestion_params_.contigs_to_keep_separate.find(contig) !=
          ingestion_params_.contigs_to_keep_separate.end())
    return false;

  // If the whitelist is not empty and contig is not in the whitelist then it
  // can not be merged
  if (!ingestion_params_.contigs_to_allow_merging.empty() &&
      ingestion_params_.contigs_to_allow_merging.find(contig) ==
          ingestion_params_.contigs_to_allow_merging.end())
    return false;

  // In all other cases we are mergeable
  return true;
}

int Writer::get_merged_fragment_index(const std::string& contig) {
  int result = 0;
  for (auto& separate_contig : ingestion_params_.contigs_to_keep_separate) {
    if (contig > separate_contig) {
      result++;
    } else {
      break;
    }
  }
  return result;
}

void Writer::set_contig_fragment_merging(const bool contig_fragment_merging) {
  ingestion_params_.contig_fragment_merging = contig_fragment_merging;
}

void Writer::set_contigs_to_keep_separate(
    const std::set<std::string>& contigs_to_keep_separate) {
  ingestion_params_.contigs_to_keep_separate = contigs_to_keep_separate;
}

void Writer::set_contigs_to_allow_merging(
    const std::set<std::string>& contigs_to_allow_merging) {
  ingestion_params_.contigs_to_allow_merging = contigs_to_allow_merging;
}

void Writer::set_contig_mode(int contig_mode) {
  ingestion_params_.contig_mode =
      static_cast<IngestionParams::ContigMode>(contig_mode);
}

void Writer::set_enable_allele_count(bool enable) {
  creation_params_.enable_allele_count = enable;
}

void Writer::set_enable_variant_stats(bool enable) {
  creation_params_.enable_variant_stats = enable;
}

}  // namespace vcf
}  // namespace tiledb
