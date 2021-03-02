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
#include <iomanip>
#include <random>
#include <thread>

#include "dataset/attribute_buffer_set.h"
#include "read/bcf_exporter.h"
#include "read/in_memory_exporter.h"
#include "read/read_query_results.h"
#include "read/reader.h"
#include "read/tsv_exporter.h"

namespace tiledb {
namespace vcf {

Reader::Reader() {
}

Reader::~Reader() {
  if (read_state_.async_query.valid()) {
    // We must wait for the inflight query to finish before we destroy
    // everything. If we don't its possible to delete the buffers in the middle
    // of an active query
    ctx_->cancel_tasks();
    read_state_.async_query.wait();
  }

  utils::free_htslib_tiledb_context();
}

void Reader::open_dataset(const std::string& dataset_uri) {
  init_tiledb();

  dataset_.reset(new TileDBVCFDataset);
  dataset_->open(dataset_uri, params_.tiledb_config);
  read_state_.array = dataset_->data_array();
}

void Reader::reset() {
  read_state_ = ReadState();
  read_state_.array = dataset_->data_array();
  if (exporter_ != nullptr) {
    exporter_->reset();
    read_state_.need_headers = exporter_->need_headers();
  }
}

void Reader::reset_buffers() {
  auto exp = set_in_memory_exporter();
  exp->reset_buffers();
}

void Reader::set_all_params(const ExportParams& params) {
  params_ = params;
}

void Reader::set_samples(const std::string& samples) {
  params_.sample_names = utils::split(samples, ',');
}

void Reader::set_regions(const std::string& regions) {
  params_.regions = utils::split(regions, ',');
}

void Reader::set_sort_regions(bool sort_regions) {
  params_.sort_regions = sort_regions;
}

void Reader::set_samples_file(const std::string& uri) {
  if (vfs_ == nullptr)
    init_tiledb();

  if (!vfs_->is_file(uri))
    throw std::runtime_error(
        "Error setting samples file; '" + uri + "' does not exist.");
  params_.samples_file_uri = uri;
}

void Reader::set_bed_file(const std::string& uri) {
  if (vfs_ == nullptr)
    init_tiledb();

  if (!vfs_->is_file(uri))
    throw std::runtime_error(
        "Error setting BED file; '" + uri + "' does not exist.");
  params_.regions_file_uri = uri;
}

void Reader::set_region_partition(
    uint64_t partition_idx, uint64_t num_partitions) {
  check_partitioning(partition_idx, num_partitions);
  params_.region_partitioning.partition_index = partition_idx;
  params_.region_partitioning.num_partitions = num_partitions;
}

void Reader::set_sample_partition(
    uint64_t partition_idx, uint64_t num_partitions) {
  check_partitioning(partition_idx, num_partitions);
  params_.sample_partitioning.partition_index = partition_idx;
  params_.sample_partitioning.num_partitions = num_partitions;
}

void Reader::set_buffer_values(
    const std::string& attribute, void* buff, int64_t buff_size) {
  auto exp = set_in_memory_exporter();
  exp->set_buffer_values(attribute, buff, buff_size);

  if (dataset_->is_fmt_field(attribute) || dataset_->is_info_field(attribute)) {
    if (dataset_ != nullptr && !dataset_->is_attribute_materialized(attribute))
      read_state_.need_headers = true;
  } else if (attribute == "filters") {
    read_state_.need_headers = true;
  }
}

void Reader::set_buffer_offsets(
    const std::string& attribute, int32_t* buff, int64_t buff_size) {
  auto exp = set_in_memory_exporter();
  exp->set_buffer_offsets(attribute, buff, buff_size);

  if (dataset_->is_fmt_field(attribute) || dataset_->is_info_field(attribute)) {
    if (dataset_ != nullptr && !dataset_->is_attribute_materialized(attribute))
      read_state_.need_headers = true;
  } else if (attribute == "filters") {
    read_state_.need_headers = true;
  }
}

void Reader::set_buffer_list_offsets(
    const std::string& attribute, int32_t* buff, int64_t buff_size) {
  auto exp = set_in_memory_exporter();
  exp->set_buffer_list_offsets(attribute, buff, buff_size);

  if (dataset_->is_fmt_field(attribute) || dataset_->is_info_field(attribute)) {
    if (dataset_ != nullptr && !dataset_->is_attribute_materialized(attribute))
      read_state_.need_headers = true;
  } else if (attribute == "filters") {
    read_state_.need_headers = true;
  }
}

void Reader::set_buffer_validity_bitmap(
    const std::string& attribute, uint8_t* buff, int64_t buff_size) {
  auto exp = set_in_memory_exporter();
  exp->set_buffer_validity_bitmap(attribute, buff, buff_size);

  if (dataset_->is_fmt_field(attribute) || dataset_->is_info_field(attribute)) {
    if (dataset_ != nullptr && !dataset_->is_attribute_materialized(attribute))
      read_state_.need_headers = true;
  } else if (attribute == "filters") {
    read_state_.need_headers = true;
  }
}

InMemoryExporter* Reader::set_in_memory_exporter() {
  // On the first call to set_buffer(), swap out any existing exporter with an
  // InMemoryExporter.
  auto exp = dynamic_cast<InMemoryExporter*>(exporter_.get());
  if (exp == nullptr) {
    exp = new InMemoryExporter;
    exporter_.reset(exp);
  }
  return exp;
}

void Reader::set_memory_budget(unsigned mb) {
  params_.memory_budget_mb = mb;
  compute_memory_budget_details();
}

void Reader::set_record_limit(uint64_t max_num_records) {
  params_.max_num_records = max_num_records;
}

void Reader::set_tiledb_config(const std::string& config_str) {
  params_.tiledb_config = utils::split(config_str, ',');
  // Attempt to set config to check validity
  // cfg object will be discarded as a later call to tiledb_init will properly
  // create config/context
  tiledb::Config cfg;
  utils::set_tiledb_config(params_.tiledb_config, &cfg);
}

ReadStatus Reader::read_status() const {
  return read_state_.status;
}

uint64_t Reader::num_records_exported() const {
  return read_state_.last_num_records_exported;
}

void Reader::set_tiledb_stats_enabled(bool stats_enabled) {
  params_.tiledb_stats_enabled = stats_enabled;
}

void Reader::tiledb_stats_enabled(bool* enabled) const {
  *enabled = params_.tiledb_stats_enabled;
}

void Reader::set_tiledb_stats_enabled_vcf_header_array(bool stats_enabled) {
  params_.tiledb_stats_enabled_vcf_header_array = stats_enabled;
}

void Reader::tiledb_stats_enabled_vcf_header_array(bool* enabled) const {
  *enabled = params_.tiledb_stats_enabled_vcf_header_array;
}

void Reader::tiledb_stats(char** stats) {
  auto rc = tiledb_stats_dump_str(stats);
  if (rc != TILEDB_OK)
    throw std::runtime_error("Error dumping tiledb statistics");
}

void Reader::dataset_version(int32_t* version) const {
  if (dataset_ == nullptr)
    throw std::runtime_error("Error getting dataset version");
  *version = dataset_->metadata().version;
}

void Reader::result_size(
    const std::string& attribute,
    int64_t* num_offsets,
    int64_t* num_data_elements,
    int64_t* num_data_bytes) const {
  auto exp = dynamic_cast<InMemoryExporter*>(exporter_.get());
  if (exp == nullptr)
    throw std::runtime_error(
        "Error getting result size; improper or null exporter instance");
  return exp->result_size(
      attribute, num_offsets, num_data_elements, num_data_bytes);
}

void Reader::attribute_datatype(
    const std::string& attribute,
    AttrDatatype* datatype,
    bool* var_len,
    bool* nullable,
    bool* list) const {
  // Datatypes for attributes are defined by the in-memory export.
  return InMemoryExporter::attribute_datatype(
      dataset_.get(), attribute, datatype, var_len, nullable, list);
}

void Reader::num_buffers(int32_t* num_buffers) const {
  auto exp = dynamic_cast<InMemoryExporter*>(exporter_.get());
  if (exp == nullptr)
    throw std::runtime_error(
        "Error getting num buffers; improper or null exporter instance");
  exp->num_buffers(num_buffers);
}

void Reader::get_buffer_values(
    int32_t buffer_idx, const char** name, void** buff) const {
  auto exp = dynamic_cast<InMemoryExporter*>(exporter_.get());
  if (exp == nullptr)
    throw std::runtime_error(
        "Error getting buffer information; improper or null exporter instance");
  exp->get_buffer_values(buffer_idx, name, buff);
}

void Reader::get_buffer_offsets(
    int32_t buffer_idx, const char** name, int32_t** buff) const {
  auto exp = dynamic_cast<InMemoryExporter*>(exporter_.get());
  if (exp == nullptr)
    throw std::runtime_error(
        "Error getting buffer information; improper or null exporter instance");
  exp->get_buffer_offsets(buffer_idx, name, buff);
}

void Reader::get_buffer_list_offsets(
    int32_t buffer_idx, const char** name, int32_t** buff) const {
  auto exp = dynamic_cast<InMemoryExporter*>(exporter_.get());
  if (exp == nullptr)
    throw std::runtime_error(
        "Error getting buffer information; improper or null exporter instance");
  exp->get_buffer_list_offsets(buffer_idx, name, buff);
}

void Reader::get_buffer_validity_bitmap(
    int32_t buffer_idx, const char** name, uint8_t** buff) const {
  auto exp = dynamic_cast<InMemoryExporter*>(exporter_.get());
  if (exp == nullptr)
    throw std::runtime_error(
        "Error getting buffer information; improper or null exporter instance");
  exp->get_buffer_validity_bitmap(buffer_idx, name, buff);
}

void Reader::read() {
  dataset_->set_tiledb_stats_enabled(params_.tiledb_stats_enabled);
  dataset_->set_tiledb_stats_enabled_vcf_header(
      params_.tiledb_stats_enabled_vcf_header_array);
  // If the user requests stats, enable them on read
  // Multiple calls to enable stats has no effect
  if (params_.tiledb_stats_enabled) {
    tiledb::Stats::enable();
  } else {
    // Else we will make sure they are disable and reset
    tiledb::Stats::disable();
    tiledb::Stats::reset();
  }

  auto start_all = std::chrono::steady_clock::now();
  read_state_.last_num_records_exported = 0;
  if (dataset_ == nullptr)
    throw std::runtime_error(
        "Error exporting records; reader has not been initialized.");

  bool pending_work = true;
  switch (read_state_.status) {
    case ReadStatus::COMPLETED:
    case ReadStatus::FAILED:
      // Reset buffers as the are no longer needed
      buffers_a.reset(nullptr);
      buffers_b.reset(nullptr);
      return;
    case ReadStatus::INCOMPLETE:
      // Do nothing; read will resume.
      break;
    case ReadStatus::UNINITIALIZED:
      init_for_reads();
      pending_work = next_read_batch();
      read_state_.status = ReadStatus::FAILED;
      break;
  }

  // If we are using the InMemoryExporter we need to reset the user buffer
  // sizes at the start of each query
  if (!params_.export_to_disk && exporter_ != nullptr) {
    auto exp = dynamic_cast<InMemoryExporter*>(exporter_.get());
    exp->reset_current_sizes();
  }

  while (pending_work) {
    bool complete = read_current_batch();
    if (!complete) {
      read_state_.status = ReadStatus::INCOMPLETE;
      return;
    }
    pending_work = next_read_batch();
  }

  // If we get here, query is complete.
  read_state_.status = ReadStatus::COMPLETED;

  // Close the exporter (flushes any buffers), and upload files if specified.
  if (exporter_ != nullptr) {
    exporter_->close();
    exporter_->upload_exported_files(*vfs_, params_.upload_dir);
  }

  if (params_.cli_count_only) {
    std::cout << read_state_.last_num_records_exported << std::endl;
  } else if (params_.verbose) {
    auto old_locale = std::cout.getloc();
    utils::enable_pretty_print_numbers(std::cout);
    std::cout << "Done. Exported " << read_state_.last_num_records_exported
              << " records in " << utils::chrono_duration(start_all)
              << " seconds." << std::endl;
    std::cout.imbue(old_locale);
  }
}

void Reader::init_for_reads() {
  if (dataset_->metadata().version == TileDBVCFDataset::Version::V2) {
    return init_for_reads_v2();
  } else if (dataset_->metadata().version == TileDBVCFDataset::Version::V3) {
    return init_for_reads_v3();
  } else {
    assert(dataset_->metadata().version == TileDBVCFDataset::Version::V4);
    return init_for_reads_v4();
  }
}

void Reader::init_for_reads_v2() {
  assert(dataset_->metadata().version == TileDBVCFDataset::Version::V2);
  read_state_.batch_idx = 0;
  read_state_.sample_batches = prepare_sample_batches();
  read_state_.last_intersecting_region_idx_ = 0;

  init_exporter();

  prepare_regions_v2(&read_state_.regions, &read_state_.query_regions);

  prepare_attribute_buffers();
}

void Reader::init_for_reads_v3() {
  assert(dataset_->metadata().version == TileDBVCFDataset::Version::V3);
  read_state_.batch_idx = 0;
  read_state_.sample_batches = prepare_sample_batches();
  read_state_.last_intersecting_region_idx_ = 0;

  init_exporter();

  prepare_regions_v3(&read_state_.regions, &read_state_.query_regions);

  prepare_attribute_buffers();
}

void Reader::init_for_reads_v4() {
  assert(dataset_->metadata().version == TileDBVCFDataset::Version::V4);
  read_state_.batch_idx = 0;
  read_state_.sample_batches =
      prepare_sample_batches_v4(&read_state_.all_samples);
  read_state_.last_intersecting_region_idx_ = 0;

  init_exporter();

  prepare_regions_v4(
      &read_state_.regions,
      &read_state_.regions_index_per_contig,
      &read_state_.query_regions_v4);
  prepare_attribute_buffers();
}

bool Reader::next_read_batch() {
  if (dataset_->metadata().version == TileDBVCFDataset::V2 ||
      dataset_->metadata().version == TileDBVCFDataset::Version::V3)
    return next_read_batch_v2_v3();

  assert(dataset_->metadata().version == TileDBVCFDataset::Version::V4);
  return next_read_batch_v4();
}

bool Reader::next_read_batch_v2_v3() {
  // Check if we're done.
  if (read_state_.batch_idx >= read_state_.sample_batches.size() ||
      read_state_.total_num_records_exported >= params_.max_num_records)
    return false;

  // Handle edge case of an empty region partition
  if (read_state_.query_regions.empty() && read_state_.query_regions_v4.empty())
    return false;

  // Start the first batch, or advance to the next one if possible.
  if (read_state_.status == ReadStatus::UNINITIALIZED) {
    read_state_.batch_idx = 0;
    read_state_.query_contig_batch_idx = 0;
  } else if (read_state_.batch_idx + 1 < read_state_.sample_batches.size()) {
    read_state_.query_contig_batch_idx = 0;
    read_state_.batch_idx++;
  } else {
    return false;
  }

  // Sample row range
  read_state_.current_sample_batches =
      read_state_.sample_batches[read_state_.batch_idx];

  // Setup v2/v3 read details
  read_state_.sample_min = std::numeric_limits<uint32_t>::max() - 1;
  read_state_.sample_max = std::numeric_limits<uint32_t>::min();
  for (const auto& s : read_state_.current_sample_batches) {
    read_state_.sample_min = std::min(read_state_.sample_min, s.sample_id);
    read_state_.sample_max = std::max(read_state_.sample_max, s.sample_id);
  }

  // Sample handles
  read_state_.current_samples.clear();
  for (const auto& s : read_state_.current_sample_batches) {
    read_state_.current_samples[s.sample_id - read_state_.sample_min] = s;
  }

  // User query regions
  read_state_.region_idx = 0;

  // Headers
  read_state_.current_hdrs.clear();

  // Sample handles
  read_state_.current_samples.clear();
  for (const auto& s : read_state_.current_sample_batches) {
    read_state_.current_samples[s.sample_id] = s;
  }

  read_state_.current_hdrs =
      dataset_->fetch_vcf_headers(read_state_.current_sample_batches);

  // Set up the TileDB query
  read_state_.query.reset(new Query(*ctx_, *read_state_.array));
  set_tiledb_query_config();

  // Set ranges
  for (const auto& sample : read_state_.current_sample_batches)
    read_state_.query->add_range(0, sample.sample_id, sample.sample_id);
  for (const auto& query_region : read_state_.query_regions)
    read_state_.query->add_range(1, query_region.col_min, query_region.col_max);
  read_state_.query->set_layout(TILEDB_UNORDERED);
  if (params_.verbose) {
    std::cout << "Initialized TileDB query with "
              << read_state_.query_regions.size() << " start_pos ranges, "
              << read_state_.current_sample_batches.size() << " sample ranges."
              << std::endl;
  }

  // Get estimated records for verbose output
  read_state_.total_query_records_processed = 0;
  read_state_.query_estimated_num_records = 1;
  if (params_.verbose) {
    if (dataset_->metadata().version == TileDBVCFDataset::Version::V2) {
      read_state_.query_estimated_num_records =
          read_state_.query->est_result_size(
              TileDBVCFDataset::DimensionNames::V2::end_pos) /
          tiledb_datatype_size(
              dataset_->data_array()
                  ->schema()
                  .domain()
                  .dimension(TileDBVCFDataset::DimensionNames::V2::end_pos)
                  .type());
    } else {
      read_state_.query_estimated_num_records =
          read_state_.query->est_result_size(
              TileDBVCFDataset::DimensionNames::V3::start_pos) /
          tiledb_datatype_size(
              dataset_->data_array()
                  ->schema()
                  .domain()
                  .dimension(TileDBVCFDataset::DimensionNames::V3::start_pos)
                  .type());
    }
  }

  return true;
}

bool Reader::next_read_batch_v4() {
  assert(dataset_->metadata().version == TileDBVCFDataset::Version::V4);
  // Check if we're done. In v4 Sample batches are always one
  if (read_state_.batch_idx >= 1 ||
      read_state_.total_num_records_exported >= params_.max_num_records)
    return false;

  // Handle edge case of an empty region partition
  if (read_state_.query_regions.empty() && read_state_.query_regions_v4.empty())
    return false;

  // Start the first batch, or advance to the next one if possible.
  bool new_samples = true;
  if (read_state_.status == ReadStatus::UNINITIALIZED) {
    read_state_.batch_idx = 0;
    read_state_.query_contig_batch_idx = 0;
  } else if (
      read_state_.batch_idx + 1 < 1 ||
      read_state_.query_contig_batch_idx + 1 <
          read_state_.query_regions_v4.size()) {
    // If we have query contig batches left for this sample batch only increment
    // the contig batch
    if (read_state_.query_contig_batch_idx + 1 <
        read_state_.query_regions_v4.size()) {
      read_state_.query_contig_batch_idx++;
      new_samples = false;
    } else {
      read_state_.query_contig_batch_idx = 0;
      read_state_.batch_idx++;
    }
  } else {
    return false;
  }

  // User query region
  read_state_.region_idx = 0;

  // Headers
  if (new_samples) {
    // If all samples and no partitioning, there is no sample batch
    if (read_state_.all_samples &&
        params_.sample_partitioning.num_partitions == 1) {
      read_state_.current_sample_batches.clear();
    } else {
      // Sample row range
      read_state_.current_sample_batches =
          read_state_.sample_batches[read_state_.batch_idx];

      // Sample handles
      read_state_.current_samples.clear();
      for (const auto& s : read_state_.current_sample_batches) {
        read_state_.current_samples[s.sample_id] = s;
      }
    }

    // Fetch new headers for new sample batch
    if (read_state_.need_headers) {
      read_state_.current_hdrs.clear();
      read_state_.current_hdrs = dataset_->fetch_vcf_headers_v4(
          read_state_.current_sample_batches,
          &read_state_.current_hdrs_lookup,
          read_state_.all_samples,
          false);
    }
  }

  // Set up the TileDB query
  read_state_.query.reset(new Query(*ctx_, *read_state_.array));
  set_tiledb_query_config();

  // Set ranges
  // For samples we special case when we are looking at all samples. If so we
  // just need to set one range with the start/end sample id
  if (read_state_.all_samples) {
    if (params_.sample_partitioning.num_partitions == 1) {
      auto non_empty_domain = dataset_->data_array()->non_empty_domain_var(
          TileDBVCFDataset::DimensionNames::V4::sample);
      read_state_.query->add_range(
          2, non_empty_domain.first, non_empty_domain.second);
    } else {
      // if we have all samples but are partitioning we need to only use the
      // first/last sample of the partition partitions are sorted both globally
      // and in the vector so this is a shortcut to have less ranges
      read_state_.query->add_range(
          2,
          read_state_.current_sample_batches[0].sample_name,
          read_state_
              .current_sample_batches
                  [read_state_.current_sample_batches.size() - 1]
              .sample_name);
    }
  } else {
    // If we are not exporting all samples add the current partition/batch's
    // list
    for (const auto& sample : read_state_.current_sample_batches)
      read_state_.query->add_range(2, sample.sample_name, sample.sample_name);
  }

  for (const auto& query_region :
       read_state_.query_regions_v4[read_state_.query_contig_batch_idx].second)
    read_state_.query->add_range(1, query_region.col_min, query_region.col_max);

  read_state_.query->add_range(
      0,
      read_state_.query_regions_v4[read_state_.query_contig_batch_idx].first,
      read_state_.query_regions_v4[read_state_.query_contig_batch_idx].first);

  read_state_.query->set_layout(TILEDB_UNORDERED);
  if (params_.verbose) {
    std::stringstream ss;
    ss << "Initialized TileDB query with "
       << read_state_.query_regions_v4[read_state_.query_contig_batch_idx]
              .second.size()
       << " start_pos ranges, ";

    if (read_state_.all_samples)
      ss << " all samples";
    else
      ss << read_state_.current_sample_batches.size() << " samples";

    ss << " for contig "
       << read_state_.query_regions_v4[read_state_.query_contig_batch_idx].first
       << " (contig batch " << read_state_.query_contig_batch_idx + 1 << "/"
       << read_state_.query_regions_v4.size() << ", sample batch "
       << read_state_.batch_idx + 1 << "/" << read_state_.sample_batches.size()
       << ")." << std::endl;
    std::cout << ss.str();
  }

  // Get estimated records for verbose output
  read_state_.total_query_records_processed = 0;
  read_state_.query_estimated_num_records = 1;

  if (params_.verbose) {
    read_state_.query_estimated_num_records =
        read_state_.query->est_result_size(
            TileDBVCFDataset::DimensionNames::V4::start_pos) /
        tiledb_datatype_size(
            dataset_->data_array()
                ->schema()
                .domain()
                .dimension(TileDBVCFDataset::DimensionNames::V4::start_pos)
                .type());
  }

  return true;
}

void Reader::init_exporter() {
  if (params_.export_to_disk) {
    switch (params_.format) {
      case ExportFormat::CompressedBCF:
      case ExportFormat::BCF:
      case ExportFormat::VCFGZ:
      case ExportFormat::VCF:
        exporter_.reset(new BCFExporter(params_.format));
        break;
      case ExportFormat::TSV:
        exporter_.reset(
            new TSVExporter(params_.tsv_output_path, params_.tsv_fields));
        break;
      default:
        throw std::runtime_error(
            "Error exporting records; unknown export format.");
        break;
    }
    exporter_->set_output_dir(params_.output_dir);
  }

  // Note that exporter may be null if the user has specified no export to
  // disk but did not set any buffers for in-memory export. This reduces to a
  // count operation, and is supported.
  if (exporter_ != nullptr)
    exporter_->set_dataset(dataset_.get());

  // Set need_headers based on if the exporter needs a header and its not been
  // requested by an info/fmt field
  if (!read_state_.need_headers && exporter_ != nullptr)
    read_state_.need_headers = exporter_->need_headers();
}

bool Reader::read_current_batch() {
  tiledb::Query* query = read_state_.query.get();
  const bool verbose = params_.verbose;

  if (read_state_.status == ReadStatus::INCOMPLETE) {
    auto exp = dynamic_cast<InMemoryExporter*>(exporter_.get());
    if (exp == nullptr)
      throw std::runtime_error(
          "Error reading batch; incomplete query without user buffer "
          "exporter should not be possible.");

    // If the read status was incomplete, pick up processing the previous
    // TileDB query results.
    if (dataset_->metadata().version == TileDBVCFDataset::Version::V4) {
      if (!process_query_results_v4())
        return false;  // Still incomplete.
    } else if (dataset_->metadata().version == TileDBVCFDataset::Version::V3) {
      if (!process_query_results_v3())
        return false;  // Still incomplete.
    } else {
      assert(dataset_->metadata().version == TileDBVCFDataset::Version::V2);
      if (!process_query_results_v2())
        return false;  // Still incomplete.
    }

    // If we finished processing previous results and the TileDB query is now
    // complete, we are done. We check both the query_results and the query
    // itself to capture the case of a new underlying tiledb query for the
    // next range/sample partitioning since we partition samples on
    // tile_extent
    if (read_state_.query_results.query_status() !=
            tiledb::Query::Status::INCOMPLETE &&
        read_state_.query->query_status() !=
            tiledb::Query::Status::UNINITIALIZED) {
      return true;
    }
  }

  // If a past TileDB query was in-flight (from incomplete reads) and  B buffers
  // are not null indicating we are double buffers, it was using the B buffers,
  // so start off with that. Otherwise, submit a new async query.
  if (double_buffering_ && read_state_.async_query.valid()) {
    std::swap(buffers_a, buffers_b);
  } else {
    buffers_a->set_buffers(query, dataset_->metadata().version);
    read_state_.async_query =
        std::async(std::launch::async, [query, verbose]() {
          auto t0 = std::chrono::steady_clock::now();
          auto st = query->submit();
          if (verbose) {
            std::cout << "query completed in " << utils::chrono_duration(t0)
                      << " sec." << std::endl;
          }
          return st;
        });
  }

  do {
    // Block on query completion.
    auto query_status = read_state_.async_query.get();
    read_state_.query_results.set_results(*dataset_, buffers_a.get(), *query);

    if (dataset_->metadata().version == TileDBVCFDataset::Version::V4) {
      buffers_a->contig().effective_size(
          read_state_.query_results.contig_size().second * sizeof(char));
      buffers_a->contig().offset_nelts(
          read_state_.query_results.contig_size().first);
      buffers_a->sample_name().effective_size(
          read_state_.query_results.sample_size().second * sizeof(char));
      buffers_a->sample_name().offset_nelts(
          read_state_.query_results.sample_size().first);
    }

    read_state_.cell_idx = 0;

    if (read_state_.query_results.num_cells() == 0 &&
        read_state_.query_results.query_status() ==
            tiledb::Query::Status::INCOMPLETE)
      throw std::runtime_error(
          "Error exporting region on sample range " +
          std::to_string(read_state_.sample_min) + "-" +
          std::to_string(read_state_.sample_max) +
          "; incomplete TileDB query with 0 results.");

    // If the query was incomplete, submit it again while processing the
    // current results.
    if (query_status == tiledb::Query::Status::INCOMPLETE &&
        double_buffering_) {
      buffers_b->set_buffers(query, dataset_->metadata().version);
      read_state_.async_query =
          std::async(std::launch::async, [query, verbose]() {
            auto t0 = std::chrono::steady_clock::now();
            auto st = query->submit();
            if (verbose) {
              std::cout << "query completed in " << utils::chrono_duration(t0)
                        << " sec." << std::endl;
            }
            return st;
          });
    }

    // Process the query results.
    auto old_num_exported = read_state_.last_num_records_exported;
    read_state_.total_query_records_processed +=
        read_state_.query_results.num_cells();
    auto t0 = std::chrono::steady_clock::now();

    bool complete;
    if (dataset_->metadata().version == TileDBVCFDataset::Version::V4) {
      complete = process_query_results_v4();
    } else if (dataset_->metadata().version == TileDBVCFDataset::Version::V3) {
      complete = process_query_results_v3();
    } else {
      assert(dataset_->metadata().version == TileDBVCFDataset::Version::V2);
      complete = process_query_results_v2();
    }

    if (params_.verbose)
      std::cout << "Processed " << read_state_.query_results.num_cells()
                << " cells in " << utils::chrono_duration(t0)
                << " sec. Reported "
                << (read_state_.last_num_records_exported - old_num_exported)
                << " cells. Approximately " << std::fixed
                << std::setprecision(2)
                << (read_state_.total_query_records_processed /
                    static_cast<double>(
                        read_state_.query_estimated_num_records) *
                    100)
                << "% completed with query cells." << std::endl;

    // Return early if we couldn't process all the results.
    if (!complete)
      return false;

    // Swap the buffers if we are double buffering
    if (double_buffering_) {
      std::swap(buffers_a, buffers_b);
    } else if (
        query_status ==
        tiledb::Query::Status::INCOMPLETE) {  // resubmit existing buffers_a if
                                              // not double buffering
      buffers_a->set_buffers(query, dataset_->metadata().version);
      read_state_.async_query =
          std::async(std::launch::async, [query, verbose]() {
            auto t0 = std::chrono::steady_clock::now();
            auto st = query->submit();
            if (verbose) {
              std::cout << "query completed in " << utils::chrono_duration(t0)
                        << " sec." << std::endl;
            }
            return st;
          });
    }
  } while (read_state_.query_results.query_status() ==
               tiledb::Query::Status::INCOMPLETE &&
           read_state_.total_num_records_exported < params_.max_num_records);

  // Batch complete; finalize the export (if applicable).
  if (exporter_ != nullptr && read_state_.need_headers) {
    if (dataset_->metadata().version == TileDBVCFDataset::Version::V3 ||
        dataset_->metadata().version == TileDBVCFDataset::Version::V2) {
      for (const auto& s : read_state_.sample_batches[read_state_.batch_idx]) {
        bcf_hdr_t* hdr = read_state_.current_hdrs.at(s.sample_id).get();
        exporter_->finalize_export(s, hdr);
      }
    } else {
      assert(dataset_->metadata().version == TileDBVCFDataset::Version::V4);

      for (const auto& s : read_state_.current_hdrs_lookup) {
        std::string sample_name = s.first;
        bcf_hdr_t* hdr = read_state_.current_hdrs.at(s.second).get();
        exporter_->finalize_export(
            SampleAndId{.sample_name = sample_name, .sample_id = 0}, hdr);
      }
    }
  }

  return true;
}

/**
 * Comparator used to binary search across regions to find the first index to
 * start checking for intersections
 *
 * We know that any region whose end is before the real_start of a region
 * can't possibly intersect
 */
struct RegionComparator {
  /**
   * Compare for less than
   * @param left region
   * @param right real_start
   * @return
   */
  bool operator()(const Region& left, uint32_t right) {
    return left.max + left.seq_offset < right;
  }
} RegionComparator;

struct RegionComparatorV4 {
  std::vector<Region>* regions_;

  explicit RegionComparatorV4(std::vector<Region>* regions) {
    regions_ = regions;
  }

  /**
   * Compare for less than
   * @param left region index
   * @param right real_start
   * @return
   */
  bool operator()(const size_t& left, uint32_t right) {
    return (*regions_)[left].max < right;
  }
};

bool Reader::process_query_results_v4() {
  if (read_state_.regions.empty())
    throw std::runtime_error(
        "Error processing query results; empty regions list.");

  const auto& results = read_state_.query_results;
  const uint64_t num_cells = results.num_cells();
  if (num_cells == 0 || read_state_.cell_idx >= num_cells)
    return true;

  const uint32_t anchor_gap = dataset_->metadata().anchor_gap;

  // V4 querys are run on a single contig at a time, so we can grab it from
  // the batch
  std::string query_contig =
      read_state_.query_regions_v4[read_state_.query_contig_batch_idx].first;

  // This lets us limit the scope of intersections to only regions for this
  // query's contig
  const auto regions_indexes =
      read_state_.regions_index_per_contig.find(query_contig);
  // If we have a contig which isn't asked for error out
  if (regions_indexes == read_state_.regions_index_per_contig.end())
    throw std::runtime_error(
        "Error in query result processing; Could not lookup contig regions "
        "list for contig " +
        query_contig);

  // Report all intersections. If the previous read returned before
  // reporting all intersecting regions, 'last_intersecting_region_idx_'
  // will be non-zero. All regions with an index less-than
  // 'last_intersecting_region_idx_' have already been reported, so we
  // must avoid reporting them multiple times.
  std::vector<size_t> regions = regions_indexes->second;

  for (; read_state_.cell_idx < num_cells; read_state_.cell_idx++) {
    // For easy reference
    const uint64_t i = read_state_.cell_idx;

    // Get the start, real_start and end. We don't need the contig because we
    // know the query is limited to a single contig
    const uint32_t start = results.buffers()->start_pos().value<uint32_t>(i);
    const uint32_t real_start =
        results.buffers()->real_start_pos().value<uint32_t>(i);

    const uint32_t end = results.buffers()->end_pos().value<uint32_t>(i);

    // Perform a binary search to find first region we can intersection
    // This is an optimization to avoid a linear scan over all regions for
    // intersection This replaces the previous, incorrect, optimization of
    // trying to keep a minimum region as we iterate
    auto it = std::lower_bound(
        regions.begin(),
        regions.end(),
        real_start,
        RegionComparatorV4(&read_state_.regions));
    if (it == regions.end()) {
      continue;
    } else {
      read_state_.last_intersecting_region_idx_ =
          std::distance(regions.begin(), it);
    }

    for (size_t j = read_state_.last_intersecting_region_idx_;
         j < regions.size();
         j++) {
      const auto& reg = read_state_.regions[regions[j]];

      const uint32_t reg_min = reg.min;
      const uint32_t reg_max = reg.max;

      // If the vcf record is not contained in the region skip it
      if (real_start > reg_max)
        continue;

      // Exit early, in this case all regions are now passed this record
      if (end < reg_min)
        break;

      // Unless start is the real start (aka first record) then if we skip for
      // any record greater than the region min the goal is to only capture
      // starts which are within 1 anchor gap of the region start on the lower
      // side of the region start
      if (start != real_start && start >= reg_min)
        continue;

      // First lets make sure the anchor gap is smaller than the region
      // minimum, this avoid overflow in the next check.. second if the start
      // is further away from the region_start than the anchor gap discard
      if (anchor_gap < reg_min && start < reg_min - anchor_gap)
        continue;

      // If we overflow when reporting this cell, save the index of the
      // current region so that we restart from the same position on the
      // next read. Otherwise, we will re-report the cells in regions with
      // an index below 'j'.
      if (!report_cell(reg, reg.seq_offset, i)) {
        read_state_.last_intersecting_region_idx_ = j;
        return false;
      }

      // Return early if we've hit the record limit.
      if (read_state_.total_num_records_exported >= params_.max_num_records) {
        return true;
      }
    }

    // Clear 'last_intersecting_region_idx_' after successfully reporting
    // all cells in intersecting regions.
    read_state_.last_intersecting_region_idx_ = 0;

    read_state_.region_idx = 0;
  }

  return true;
}

bool Reader::process_query_results_v3() {
  if (read_state_.regions.empty())
    throw std::runtime_error(
        "Error processing query results; empty regions list.");

  const auto& results = read_state_.query_results;
  const uint64_t num_cells = results.num_cells();
  if (num_cells == 0 || read_state_.cell_idx >= num_cells)
    return true;

  // Get the contig offset and length of the first cell in the results.
  uint32_t first_col =
      results.buffers()->start_pos().value<uint32_t>(read_state_.cell_idx);
  auto contig_info = dataset_->contig_from_column(first_col);

  for (; read_state_.cell_idx < num_cells; read_state_.cell_idx++) {
    // For easy reference
    const uint64_t i = read_state_.cell_idx;
    const uint32_t start = results.buffers()->start_pos().value<uint32_t>(i);
    const uint32_t real_start =
        results.buffers()->real_start_pos().value<uint32_t>(i);
    const uint32_t end = results.buffers()->end_pos().value<uint32_t>(i);
    const uint32_t anchor_gap = dataset_->metadata().anchor_gap;

    // If the end position is before or after the config find the proper
    // contig
    if (end >= std::get<0>(contig_info) + std::get<1>(contig_info) ||
        end < std::get<0>(contig_info))
      contig_info = dataset_->contig_from_column(end);
    const uint32_t contig_offset = std::get<0>(contig_info);

    // Perform a binary search to find first region we can intersection
    // This is an optimization to avoid a linear scan over all regions for
    // intersection This replaces the previous, incorrect, optimization of
    // trying to keep a minimum region as we iterate
    auto it = std::lower_bound(
        read_state_.regions.begin(),
        read_state_.regions.end(),
        real_start,
        RegionComparator);
    if (it == read_state_.regions.end()) {
      continue;
    } else {
      read_state_.region_idx = std::distance(read_state_.regions.begin(), it);
    }

    // Report all intersections. If the previous read returned before
    // reporting all intersecting regions, 'last_intersecting_region_idx_'
    // will be non-zero. All regions with an index less-than
    // 'last_intersecting_region_idx_' have already been reported, so we
    // must avoid reporting them multiple times.
    size_t j = read_state_.last_intersecting_region_idx_ > 0 ?
                   read_state_.last_intersecting_region_idx_ :
                   read_state_.region_idx;
    for (; j < read_state_.regions.size(); j++) {
      const auto& reg = read_state_.regions[j];

      const uint32_t reg_min = reg.seq_offset + reg.min;
      const uint32_t reg_max = reg.seq_offset + reg.max;

      // If the vcf record is not contained in the region skip it
      if (real_start > reg_max)
        continue;

      // If the regions (sorted) are starting past the end of the record we
      // can safely exit out, as we will not intersect this record anymore
      if (end < reg_min)
        break;

      // Unless start is the real start (aka first record) then if we skip for
      // any record greater than the region min the goal is to only capture
      // starts which are within 1 anchor gap of the region start on the lower
      // side of the region start
      if (start != real_start && start >= reg_min)
        continue;
      // First lets make sure the anchor gap is smaller than the region
      // minimum, this avoid overflow in the next check. second if the start
      // is further away from the region_start than the anchor gap discard
      if (anchor_gap < reg_min && start < reg_min - anchor_gap)
        continue;

      // If the region does not match the contig skip
      if (reg.seq_name != std::get<2>(contig_info))
        continue;

      // If we overflow when reporting this cell, save the index of the
      // current region so that we restart from the same position on the
      // next read. Otherwise, we will re-report the cells in regions with
      // an index below 'j'.
      if (!report_cell(reg, contig_offset, i)) {
        read_state_.last_intersecting_region_idx_ = j;
        return false;
      }

      // Return early if we've hit the record limit.
      if (read_state_.total_num_records_exported >= params_.max_num_records)
        return true;
    }

    // Clear 'last_intersecting_region_idx_' after successfully reporting
    // all cells in intersecting regions.
    read_state_.last_intersecting_region_idx_ = 0;

    // Always need to reset to original index for next record
    // Records are unordered so we can't assume a minimum intersection region
    read_state_.region_idx = 0;
  }

  return true;
}

bool Reader::process_query_results_v2() {
  if (read_state_.regions.empty())
    throw std::runtime_error(
        "Error processing query results; empty regions list.");

  const auto& results = read_state_.query_results;
  const uint64_t num_cells = results.num_cells();
  if (num_cells == 0 || read_state_.cell_idx >= num_cells)
    return true;

  // Get the contig offset and length of the first cell in the results.
  uint32_t first_col =
      results.buffers()->end_pos().value<uint32_t>(read_state_.cell_idx);
  auto contig_info = dataset_->contig_from_column(first_col);

  for (; read_state_.cell_idx < num_cells; read_state_.cell_idx++) {
    // For easy reference
    const uint64_t i = read_state_.cell_idx;
    const uint32_t end = results.buffers()->end_pos().value<uint32_t>(i);
    const uint32_t start = results.buffers()->pos().value<uint32_t>(i);
    const uint32_t real_end = results.buffers()->real_end().value<uint32_t>(i);
    const uint32_t anchor_gap = dataset_->metadata().anchor_gap;

    // If the end position is before or after the config find the proper
    // contig
    if (end >= std::get<0>(contig_info) + std::get<1>(contig_info) ||
        end < std::get<0>(contig_info))
      contig_info = dataset_->contig_from_column(end);
    const uint32_t contig_offset = std::get<0>(contig_info);

    // Perform a binary search to find first region we can intersection
    // This is an optimization to avoid a linear scan over all regions for
    // intersection This replaces the previous, incorrect, optimization of
    // trying to keep a minimum region as we iterate
    auto it = std::lower_bound(
        read_state_.regions.begin(),
        read_state_.regions.end(),
        start,
        RegionComparator);
    if (it == read_state_.regions.end()) {
      continue;
    } else {
      read_state_.region_idx = std::distance(read_state_.regions.begin(), it);
    }

    // Report all intersections. If the previous read returned before
    // reporting all intersecting regions, 'last_intersecting_region_idx_'
    // will be non-zero. All regions with an index less-than
    // 'last_intersecting_region_idx_' have already been reported, so we
    // must avoid reporting them multiple times.
    size_t j = read_state_.last_intersecting_region_idx_ > 0 ?
                   read_state_.last_intersecting_region_idx_ :
                   read_state_.region_idx;
    for (; j < read_state_.regions.size(); j++) {
      const auto& reg = read_state_.regions[j];
      const uint32_t reg_min = reg.seq_offset + reg.min;
      const uint32_t reg_max = reg.seq_offset + reg.max;

      // If the vcf record is not contained in the region skip it
      if (start > reg_max)
        continue;

      // If the regions (sorted) are starting past the end of the record we
      // can safely exit out, as we will not intersect this record anymore
      if (real_end < reg_min)
        break;

      // Unless start is the real start (aka first record) then if we skip for
      // any record greater than the region min the goal is to only capture
      // starts which are within 1 anchor gap of the region start on the lower
      // side of the region start
      if (end != real_end && start >= reg_min)
        continue;
      // First lets make sure the anchor gap is smaller than the region
      // minimum, this avoid overflow in the next check. second if the start
      // is further away from the region_start than the anchor gap discard
      if (anchor_gap < reg_min && start < reg_min - anchor_gap)
        continue;

      // If the region does not match the contig skip
      if (reg.seq_name != std::get<2>(contig_info))
        continue;

      // If we overflow when reporting this cell, save the index of the
      // current region so that we restart from the same position on the
      // next read. Otherwise, we will re-report the cells in regions with
      // an index below 'j'.
      if (!report_cell(reg, contig_offset, i)) {
        read_state_.last_intersecting_region_idx_ = j;
        return false;
      }

      // Return early if we've hit the record limit.
      if (read_state_.total_num_records_exported >= params_.max_num_records)
        return true;
    }

    // Clear 'last_intersecting_region_idx_' after successfully reporting
    // all cells in intersecting regions.
    read_state_.last_intersecting_region_idx_ = 0;

    // Always need to reset to original index for next record
    // Records are unordered so we can't assume a minimum intersection region
    read_state_.region_idx = 0;
  }

  return true;
}

bool Reader::report_cell(
    const Region& region, uint32_t contig_offset, uint64_t cell_idx) {
  if (exporter_ == nullptr) {
    read_state_.last_num_records_exported++;
    read_state_.total_num_records_exported++;
    return true;
  }

  SampleAndId sample;
  uint64_t hdr_index = 0;
  const auto& results = read_state_.query_results;
  if (dataset_->metadata().version == TileDBVCFDataset::Version::V2 ||
      dataset_->metadata().version == TileDBVCFDataset::Version::V3) {
    uint32_t samp_idx = results.buffers()->sample().value<uint32_t>(cell_idx);

    // Skip this cell if we are not reporting its sample.
    if (read_state_.current_samples.count(samp_idx) == 0) {
      return true;
    }
    sample = read_state_.current_samples[samp_idx];
    hdr_index = samp_idx;
  } else {
    assert(dataset_->metadata().version == TileDBVCFDataset::Version::V4);
    uint64_t size = 0;
    const char* sample_name =
        results.buffers()->sample_name().value<char>(cell_idx, &size);
    sample = SampleAndId{std::string(sample_name, size)};
    hdr_index = read_state_.current_hdrs_lookup[sample.sample_name];
  }

  bcf_hdr_t* hdr_ptr = nullptr;
  if (read_state_.need_headers) {
    auto hdr_iter = read_state_.current_hdrs.find(hdr_index);
    if (hdr_iter == read_state_.current_hdrs.end())
      throw std::runtime_error(
          "Could not find VCF header for " + sample.sample_name +
          " in report_cell");

    const auto& hdr = read_state_.current_hdrs.at(hdr_index);
    hdr_ptr = hdr.get();
  }
  if (!exporter_->export_record(
          sample, hdr_ptr, region, contig_offset, results, cell_idx))
    return false;

  // If no overflow, increment num records count.
  read_state_.last_num_records_exported++;
  read_state_.total_num_records_exported++;
  return true;
}

std::vector<std::vector<SampleAndId>> Reader::prepare_sample_batches() const {
  // Get the list of all sample names and ID
  auto samples = prepare_sample_names();

  // Sort by sample ID
  std::sort(
      samples.begin(),
      samples.end(),
      [](const SampleAndId& a, const SampleAndId& b) {
        return a.sample_id < b.sample_id;
      });

  // Apply sample partitioning
  utils::partition_vector(
      params_.sample_partitioning.partition_index,
      params_.sample_partitioning.num_partitions,
      &samples);

  // Group partition into space tile batches.
  const uint32_t space_tile_extent =
      dataset_->metadata().ingestion_sample_batch_size;
  std::vector<std::vector<SampleAndId>> result;
  uint32_t curr_space_tile = std::numeric_limits<uint32_t>::max();
  for (const auto& s : samples) {
    uint32_t space_tile = s.sample_id / space_tile_extent;
    if (space_tile != curr_space_tile) {
      result.emplace_back();
      curr_space_tile = space_tile;
    }
    result.back().push_back(s);
  }

  return result;
}

std::vector<std::vector<SampleAndId>> Reader::prepare_sample_batches_v4(
    bool* all_samples) const {
  assert(all_samples);
  // Get the list of all sample names and ID
  auto samples = prepare_sample_names_v4(all_samples);
  // If we are fetching all samples and there is no partitioning we don't need
  // sample batches
  if (*all_samples && params_.sample_partitioning.num_partitions == 1)
    return {};

  // Sort by sample ID
  std::sort(
      samples.begin(),
      samples.end(),
      [](const SampleAndId& a, const SampleAndId& b) {
        return a.sample_name < b.sample_name;
      });

  // Apply sample partitioning
  utils::partition_vector(
      params_.sample_partitioning.partition_index,
      params_.sample_partitioning.num_partitions,
      &samples);

  return {samples};
}

std::vector<SampleAndId> Reader::prepare_sample_names() const {
  std::vector<SampleAndId> result;

  for (const std::string& s : params_.sample_names) {
    std::string name;
    if (!VCFUtils::normalize_sample_name(s, &name))
      throw std::runtime_error(
          "Error preparing sample list for export; sample name '" + s +
          "' is invalid.");

    const auto& sample_ids = dataset_->metadata().sample_ids;
    auto it = sample_ids.find(name);
    if (it == sample_ids.end())
      throw std::runtime_error(
          "Error preparing sample list for export; sample '" + s +
          "' has not been registered.");

    result.push_back({.sample_name = name, .sample_id = it->second});
  }

  if (!params_.samples_file_uri.empty()) {
    const auto& metadata = dataset_->metadata();
    auto per_line = [&metadata, &result](std::string* line) {
      std::string name;
      if (!VCFUtils::normalize_sample_name(*line, &name))
        throw std::runtime_error(
            "Error preparing sample list for export; sample name '" + *line +
            "' is invalid.");

      const auto& sample_ids = metadata.sample_ids;
      auto it = sample_ids.find(name);
      if (it == sample_ids.end())
        throw std::runtime_error(
            "Error preparing sample list for export; sample '" + *line +
            "' has not been registered.");

      result.push_back({.sample_name = name, .sample_id = it->second});
    };
    utils::read_file_lines(*vfs_, params_.samples_file_uri, per_line);
  }

  // No specified samples means all samples.
  if (result.empty()) {
    const auto& md = dataset_->metadata();
    for (const auto& s : md.sample_names_) {
      auto it = md.sample_ids.find(s);
      if (it == md.sample_ids.end())
        throw std::runtime_error(
            "Error preparing sample list for export; sample '" + s +
            "' has not been registered.");
      result.push_back({.sample_name = s, .sample_id = it->second});
    }
  }

  return result;
}

std::vector<SampleAndId> Reader::prepare_sample_names_v4(
    bool* all_samples) const {
  assert(all_samples);
  std::vector<SampleAndId> result;
  // Start assuming the user specified a list of samples
  *all_samples = false;

  for (const std::string& s : params_.sample_names) {
    std::string name;
    if (!VCFUtils::normalize_sample_name(s, &name))
      throw std::runtime_error(
          "Error preparing sample list for export; sample name '" + s +
          "' is invalid.");

    result.push_back({.sample_name = name, .sample_id = 0});
  }

  if (!params_.samples_file_uri.empty()) {
    const auto& metadata = dataset_->metadata();
    const bool check_samples_exist = params_.check_samples_exist;
    if (check_samples_exist)
      dataset_->load_sample_names_v4();
    auto per_line =
        [&metadata, &result, &check_samples_exist](std::string* line) {
          std::string name;
          if (!VCFUtils::normalize_sample_name(*line, &name))
            throw std::runtime_error(
                "Error preparing sample list for export; sample name '" +
                *line + "' is invalid.");

          if (check_samples_exist) {
            const auto& sample_ids = metadata.sample_ids;
            auto it = sample_ids.find(name);
            if (it == sample_ids.end())
              throw std::runtime_error(
                  "Error preparing sample list for export; sample '" + *line +
                  "' has not been registered.");

            result.push_back(
                {.sample_name = name,
                 .sample_id = static_cast<uint32_t>(result.size())});
          }
        };
    utils::read_file_lines(*vfs_, params_.samples_file_uri, per_line);
  }

  // No specified samples means all samples.
  if (result.empty()) {
    // If the user requested sample partitioning we need to fetch the list of
    // samples
    if (params_.sample_partitioning.num_partitions > 1) {
      const auto& samples = dataset_->get_all_samples_from_vcf_headers();
      for (const auto& s : samples) {
        result.push_back({.sample_name = s, .sample_id = 0});
      }
    }
    *all_samples = true;
  }

  return result;
}

void Reader::prepare_regions_v4(
    std::vector<Region>* regions,
    std::unordered_map<std::string, std::vector<size_t>>*
        regions_index_per_contig,
    std::vector<std::pair<std::string, std::vector<QueryRegion>>>*
        query_regions) const {
  assert(dataset_->metadata().version == TileDBVCFDataset::Version::V4);
  const uint32_t g = dataset_->metadata().anchor_gap;
  // Use a linked list for pre-partition regions to allow for parallel parsing
  // of BED file
  std::list<Region> pre_partition_regions_list;

  // Manually-specified regions (-r) are 1-indexed and inclusive
  for (const std::string& r : params_.regions)
    pre_partition_regions_list.emplace_back(
        r, Region::Type::OneIndexedInclusive);

  // Add BED file regions, if specified.
  if (!params_.regions_file_uri.empty()) {
    auto start_bed_file_parse = std::chrono::steady_clock::now();
    Region::parse_bed_file_htslib(
        params_.regions_file_uri, &pre_partition_regions_list);
    if (params_.verbose) {
      auto old_locale = std::cout.getloc();
      utils::enable_pretty_print_numbers(std::cout);
      std::cout << "Parsed bed file into " << pre_partition_regions_list.size()
                << " regions in "
                << utils::chrono_duration(start_bed_file_parse) << " seconds."
                << std::endl;
      std::cout.imbue(old_locale);
    }
  }

  std::pair<uint32_t, uint32_t> region_non_empty_domain =
      read_state_.array->non_empty_domain<uint32_t>("start_pos");

  std::pair<std::string, std::string> contig_non_empty_domain =
      read_state_.array->non_empty_domain_var("contig");

  // No specified regions means all regions.
  if (pre_partition_regions_list.empty()) {
    pre_partition_regions_list = dataset_->all_contigs_list_v4();
  }

  std::vector<Region> filtered_regions;
  // Loop through all contigs to query and pre-filter to ones which fall
  // inside the nonEmptyDomain This will balance the partitioning better my
  // removing empty regions
  for (auto& r : pre_partition_regions_list) {
    r.seq_offset = 0;
    const uint32_t reg_min = r.min;
    const uint32_t reg_max = r.max;

    // Widen the query region by the anchor gap value, avoiding overflow.
    uint64_t widened_reg_min = g > reg_min ? 0 : reg_min - g;
    if (widened_reg_min <= region_non_empty_domain.second &&
        reg_max >= region_non_empty_domain.first) {
      filtered_regions.emplace_back(std::move(r));
    }
  }
  *regions = filtered_regions;

  // Sort all by contig.
  if (params_.sort_regions) {
    auto start_region_sort = std::chrono::steady_clock::now();
    std::sort(regions->begin(), regions->end());

    if (params_.verbose) {
      auto old_locale = std::cout.getloc();
      utils::enable_pretty_print_numbers(std::cout);
      std::cout << "Sorted " << regions->size() << " regions in "
                << utils::chrono_duration(start_region_sort) << " seconds."
                << std::endl;
      std::cout.imbue(old_locale);
    }
  }

  // Apply region partitioning before expanding.
  // If we have less regions than requested partitions, handle that by
  // allowing empty partitions
  if (regions->size() < params_.region_partitioning.num_partitions) {
    // Make sure that we are not trying to fetch a partition that is out of
    // bounds
    if (params_.region_partitioning.partition_index >=
        params_.region_partitioning.num_partitions)
      throw std::runtime_error(
          "Error partitioning vector; partition index " +
          std::to_string(params_.region_partitioning.partition_index) +
          " >= num partitions " +
          std::to_string(params_.region_partitioning.num_partitions) + ".");
    std::vector<Region> tmp;
    if (params_.region_partitioning.partition_index < regions->size())
      tmp.emplace_back((*regions)[params_.region_partitioning.partition_index]);
    *regions = tmp;
  } else {
    utils::partition_vector(
        params_.region_partitioning.partition_index,
        params_.region_partitioning.num_partitions,
        regions);
  }

  // Expand individual regions to a minimum width of the anchor gap.
  size_t region_index = 0;
  for (auto& r : *regions) {
    // Save mapping of contig to region indexing
    // Used in read to limit region intersection checking to only regions of
    // same contig
    auto regions_index = regions_index_per_contig->find(r.seq_name);
    if (regions_index == regions_index_per_contig->end())
      regions_index_per_contig->emplace(r.seq_name, std::vector<size_t>());

    regions_index_per_contig->find(r.seq_name)
        ->second.emplace_back(region_index);
    ++region_index;

    r.seq_offset = 0;
    const uint32_t reg_min = r.min;
    const uint32_t reg_max = r.max;

    // Widen the query region by the anchor gap value, avoiding overflow.
    uint64_t widened_reg_min = g > reg_min ? 0 : reg_min - g;

    bool new_region = true;
    std::vector<QueryRegion>* query_region_contig = nullptr;
    for (auto& query_region_pair : *query_regions) {
      // Only coalesce regions of same contig
      if (query_region_pair.first != r.seq_name)
        continue;

      query_region_contig = &query_region_pair.second;

      // Since the query regions are pre-sorted by start position we
      // only need to check the last region we inserted for comparison.
      // We know that the current region we are merging/inserting comes
      // after the previous. The only thing to check is to insert or to
      // merge
      auto& query_region = query_region_contig->back();
      if (widened_reg_min <= query_region.col_max &&
          reg_max >= query_region.col_min) {
        query_region.col_max = std::max(query_region.col_max, reg_max);
        query_region.col_min = std::min(
            static_cast<uint64_t>(query_region.col_min), widened_reg_min);
        query_region.contig = r.seq_name;
        new_region = false;
      }
      // If we are here this was the right query region contig so we can stop
      // looking for it
      break;
    }
    if (new_region) {
      if (query_region_contig == nullptr) {
        query_regions->emplace_back(r.seq_name, std::vector<QueryRegion>());
        query_region_contig = &query_regions->back().second;
      }
      // Start a new query region.
      query_region_contig->emplace_back();
      query_region_contig->back().col_min = widened_reg_min;
      query_region_contig->back().col_max = reg_max;
      query_region_contig->back().contig = r.seq_name;
    }
  }

  // After we built the ranges we need to loop one more time to coalescing any
  // final ranges. While we are looping through originally we might miss
  // coalescing ranges as we don't consider the n+1 range that hasn't been
  // created yet
  for (auto& query_region_pair : *query_regions) {
    size_t query_regions_size = query_region_pair.second.size();
    for (size_t i = 0; i < query_regions_size; i++) {
      auto& query_region = query_region_pair.second[i];
      for (size_t j = i + 1; j < query_region_pair.second.size(); j++) {
        auto& query_region_to_check = query_region_pair.second[j];
        if (query_region.col_min <= query_region_to_check.col_max &&
            query_region.col_max >= query_region_to_check.col_min) {
          query_region.col_max =
              std::max(query_region.col_max, query_region_to_check.col_max);
          query_region.col_min =
              std::min(query_region.col_min, query_region_to_check.col_min);
          // If we merge the ranges, let's remove it from the query regions
          // and reset to previous to re-evaluate
          --query_regions_size;
          query_regions->erase(query_regions->begin() + j);
          --i;
          break;
        } else if (query_region.col_max < query_region_to_check.col_min) {
          // If we region we are checking is beyond the where this one
          // ends, exit early in the comparisons
          break;
        }
      }
    }
  }
}  // namespace vcf

void Reader::prepare_regions_v3(
    std::vector<Region>* regions,
    std::vector<QueryRegion>* query_regions) const {
  const uint32_t g = dataset_->metadata().anchor_gap;
  // Use a linked list for pre-partition regions to allow for parallel parsing
  // of BED file
  std::list<Region> pre_partition_regions_list;

  // Manually-specified regions (-r) are 1-indexed and inclusive
  for (const std::string& r : params_.regions)
    pre_partition_regions_list.emplace_back(
        r, Region::Type::OneIndexedInclusive);

  // Add BED file regions, if specified.
  if (!params_.regions_file_uri.empty()) {
    auto start_bed_file_parse = std::chrono::steady_clock::now();
    Region::parse_bed_file_htslib(
        params_.regions_file_uri, &pre_partition_regions_list);
    if (params_.verbose) {
      auto old_locale = std::cout.getloc();
      utils::enable_pretty_print_numbers(std::cout);
      std::cout << "Parsed bed file into " << pre_partition_regions_list.size()
                << " regions in "
                << utils::chrono_duration(start_bed_file_parse) << " seconds."
                << std::endl;
      std::cout.imbue(old_locale);
    }
  }

  // No specified regions means all regions.
  if (pre_partition_regions_list.empty())
    pre_partition_regions_list = dataset_->all_contigs_list();

  std::pair<uint32_t, uint32_t> region_non_empty_domain;
  const auto& nonEmptyDomain = read_state_.array->non_empty_domain<uint32_t>();
  region_non_empty_domain = nonEmptyDomain[1].second;
  std::vector<Region> filtered_regions;
  // Loop through all contigs to query and pre-filter to ones which fall
  // inside the nonEmptyDomain This will balance the partitioning better my
  // removing empty regions
  for (auto& r : pre_partition_regions_list) {
    uint32_t contig_offset;
    try {
      contig_offset = dataset_->metadata().contig_offsets.at(r.seq_name);
    } catch (const std::out_of_range&) {
      throw std::runtime_error(
          "Error preparing regions for export; no contig named '" + r.seq_name +
          "' in dataset.");
    }

    r.seq_offset = contig_offset;
    const uint32_t reg_min = contig_offset + r.min;
    const uint32_t reg_max = contig_offset + r.max;

    // Widen the query region by the anchor gap value, avoiding overflow.
    uint64_t widened_reg_min = g > reg_min ? 0 : reg_min - g;
    if (widened_reg_min <= region_non_empty_domain.second &&
        reg_max >= region_non_empty_domain.first) {
      filtered_regions.emplace_back(std::move(r));
    }
  }
  *regions = filtered_regions;

  // Sort all by global column coord.
  if (params_.sort_regions) {
    auto start_region_sort = std::chrono::steady_clock::now();
    Region::sort(dataset_->metadata().contig_offsets, regions);
    if (params_.verbose) {
      auto old_locale = std::cout.getloc();
      utils::enable_pretty_print_numbers(std::cout);
      std::cout << "Sorted " << regions->size() << " regions in "
                << utils::chrono_duration(start_region_sort) << " seconds."
                << std::endl;
      std::cout.imbue(old_locale);
    }
  }

  // Apply region partitioning before expanding.
  // If we have less regions than requested partitions, handle that by
  // allowing empty partitions
  if (regions->size() < params_.region_partitioning.num_partitions) {
    // Make sure that we are not trying to fetch a partition that is out of
    // bounds
    if (params_.region_partitioning.partition_index >=
        params_.region_partitioning.num_partitions)
      throw std::runtime_error(
          "Error partitioning vector; partition index " +
          std::to_string(params_.region_partitioning.partition_index) +
          " >= num partitions " +
          std::to_string(params_.region_partitioning.num_partitions) + ".");
    std::vector<Region> tmp;
    if (params_.region_partitioning.partition_index < regions->size())
      tmp.emplace_back((*regions)[params_.region_partitioning.partition_index]);
    *regions = tmp;
  } else {
    utils::partition_vector(
        params_.region_partitioning.partition_index,
        params_.region_partitioning.num_partitions,
        regions);
  }

  // Expand individual regions to a minimum width of the anchor gap.
  uint32_t prev_reg_max = 0;
  for (auto& r : *regions) {
    uint32_t contig_offset;
    try {
      contig_offset = dataset_->metadata().contig_offsets.at(r.seq_name);
    } catch (const std::out_of_range&) {
      throw std::runtime_error(
          "Error preparing regions for export; no contig named '" + r.seq_name +
          "' in dataset.");
    }

    r.seq_offset = contig_offset;
    const uint32_t reg_min = contig_offset + r.min;
    const uint32_t reg_max = contig_offset + r.max;

    // Widen the query region by the anchor gap value, avoiding overflow.
    uint64_t widened_reg_min = g > reg_min ? 0 : reg_min - g;

    if (prev_reg_max + 1 >= widened_reg_min && !query_regions->empty()) {
      // Previous widened region overlaps this one; merge.
      query_regions->back().col_max = reg_max;
    } else {
      // Start a new query region.
      query_regions->push_back({});
      query_regions->back().col_min = widened_reg_min;
      query_regions->back().col_max = reg_max;
    }

    prev_reg_max = reg_max;
  }
}

void Reader::prepare_regions_v2(
    std::vector<Region>* regions,
    std::vector<QueryRegion>* query_regions) const {
  const uint32_t g = dataset_->metadata().anchor_gap;
  // Use a linked list for pre-partition regions to allow for parallel parsing
  // of BED file
  std::list<Region> pre_partition_regions_list;

  // Manually-specified regions (-r) are 1-indexed and inclusive
  for (const std::string& r : params_.regions)
    pre_partition_regions_list.emplace_back(
        r, Region::Type::OneIndexedInclusive);

  // Add BED file regions, if specified.
  if (!params_.regions_file_uri.empty()) {
    auto start_bed_file_parse = std::chrono::steady_clock::now();
    Region::parse_bed_file_htslib(
        params_.regions_file_uri, &pre_partition_regions_list);
    if (params_.verbose) {
      auto old_locale = std::cout.getloc();
      utils::enable_pretty_print_numbers(std::cout);
      std::cout << "Parsed bed file into " << pre_partition_regions_list.size()
                << " regions in "
                << utils::chrono_duration(start_bed_file_parse) << " seconds."
                << std::endl;
      std::cout.imbue(old_locale);
    }
  }

  // No specified regions means all regions.
  if (pre_partition_regions_list.empty())
    pre_partition_regions_list = dataset_->all_contigs_list();

  std::pair<uint32_t, uint32_t> region_non_empty_domain;
  const auto& nonEmptyDomain = read_state_.array->non_empty_domain<uint32_t>();
  region_non_empty_domain = nonEmptyDomain[1].second;
  std::vector<Region> filtered_regions;
  // Loop through all contigs to query and pre-filter to ones which fall
  // inside the nonEmptyDomain This will balance the partitioning better my
  // removing empty regions
  for (auto& r : pre_partition_regions_list) {
    uint32_t contig_offset;
    try {
      contig_offset = dataset_->metadata().contig_offsets.at(r.seq_name);
    } catch (const std::out_of_range&) {
      throw std::runtime_error(
          "Error preparing regions for export; no contig named '" + r.seq_name +
          "' in dataset.");
    }

    r.seq_offset = contig_offset;
    const uint32_t reg_min = contig_offset + r.min;
    const uint32_t reg_max = contig_offset + r.max;

    // Widen the query region by the anchor gap value, avoiding overflow.
    uint64_t widened_reg_max = reg_max + g;
    widened_reg_max = std::min<uint64_t>(
        widened_reg_max, std::numeric_limits<uint32_t>::max() - 1);
    if (reg_min <= region_non_empty_domain.second &&
        widened_reg_max >= region_non_empty_domain.first) {
      filtered_regions.emplace_back(std::move(r));
    }
  }
  *regions = filtered_regions;

  // Sort all by global column coord.
  if (params_.sort_regions) {
    auto start_region_sort = std::chrono::steady_clock::now();
    Region::sort(dataset_->metadata().contig_offsets, regions);
    if (params_.verbose) {
      auto old_locale = std::cout.getloc();
      utils::enable_pretty_print_numbers(std::cout);
      std::cout << "Sorted " << regions->size() << " regions in "
                << utils::chrono_duration(start_region_sort) << " seconds."
                << std::endl;
      std::cout.imbue(old_locale);
    }
  }

  // Apply region partitioning before expanding.
  // If we have less regions than requested partitions, handle that by
  // allowing empty partitions
  if (regions->size() < params_.region_partitioning.num_partitions) {
    // Make sure that we are not trying to fetch a partition that is out of
    // bounds
    if (params_.region_partitioning.partition_index >=
        params_.region_partitioning.num_partitions)
      throw std::runtime_error(
          "Error partitioning vector; partition index " +
          std::to_string(params_.region_partitioning.partition_index) +
          " >= num partitions " +
          std::to_string(params_.region_partitioning.num_partitions) + ".");
    std::vector<Region> tmp;
    if (params_.region_partitioning.partition_index < regions->size())
      tmp.emplace_back((*regions)[params_.region_partitioning.partition_index]);
    *regions = tmp;
  } else {
    utils::partition_vector(
        params_.region_partitioning.partition_index,
        params_.region_partitioning.num_partitions,
        regions);
  }

  // Expand individual regions to a minimum width of the anchor gap.
  uint32_t prev_reg_max = 0;
  for (auto& r : *regions) {
    uint32_t contig_offset;
    try {
      contig_offset = dataset_->metadata().contig_offsets.at(r.seq_name);
    } catch (const std::out_of_range&) {
      throw std::runtime_error(
          "Error preparing regions for export; no contig named '" + r.seq_name +
          "' in dataset.");
    }

    r.seq_offset = contig_offset;
    const uint32_t reg_min = contig_offset + r.min;
    const uint32_t reg_max = contig_offset + r.max;

    // Widen the query region by the anchor gap value, avoiding overflow.
    uint64_t widened_reg_max = reg_max + g;
    widened_reg_max = std::min<uint64_t>(
        widened_reg_max, std::numeric_limits<uint32_t>::max() - 1);

    if (prev_reg_max + 1 >= reg_min && !query_regions->empty()) {
      // Previous widened region overlaps this one; merge.
      query_regions->back().col_max = widened_reg_max;
    } else {
      // Start a new query region.
      query_regions->push_back({});
      query_regions->back().col_min = reg_min;
      query_regions->back().col_max = widened_reg_max;
    }

    prev_reg_max = widened_reg_max;
  }
}

void Reader::prepare_attribute_buffers() {
  // This base set of attributes is required for the read algorithm to run.
  std::unordered_set<std::string> attrs;
  if (dataset_->metadata().version == TileDBVCFDataset::Version::V4) {
    attrs = {
        TileDBVCFDataset::DimensionNames::V4::sample,
        TileDBVCFDataset::DimensionNames::V4::contig,
        TileDBVCFDataset::DimensionNames::V4::start_pos,
        TileDBVCFDataset::AttrNames::V4::real_start_pos,
        TileDBVCFDataset::AttrNames::V4::end_pos};
  } else if (dataset_->metadata().version == TileDBVCFDataset::Version::V3) {
    attrs = {
        TileDBVCFDataset::DimensionNames::V3::sample,
        TileDBVCFDataset::DimensionNames::V3::start_pos,
        TileDBVCFDataset::AttrNames::V3::real_start_pos,
        TileDBVCFDataset::AttrNames::V3::end_pos};
  } else {
    assert(dataset_->metadata().version == TileDBVCFDataset::Version::V2);
    attrs = {
        TileDBVCFDataset::DimensionNames::V2::sample,
        TileDBVCFDataset::DimensionNames::V2::end_pos,
        TileDBVCFDataset::AttrNames::V2::pos,
        TileDBVCFDataset::AttrNames::V2::real_end};
  }

  buffers_a.reset(new AttributeBufferSet(params_.verbose));
  buffers_b.reset(new AttributeBufferSet(params_.verbose));

  const auto* user_exp = dynamic_cast<const InMemoryExporter*>(exporter_.get());
  if (params_.cli_count_only || exporter_ == nullptr ||
      (user_exp != nullptr && user_exp->array_attributes_required().empty())) {
    // Count only: need only required attributes. Do nothing here.
  } else if (exporter_ != nullptr) {
    // Exporters require different attribute sets.
    auto required = exporter_->array_attributes_required();
    attrs.insert(required.begin(), required.end());
  } else {
    throw std::runtime_error(
        "Error preparing attribute buffers; unhandled attribute export "
        "requirements.");
  }

  // We get one-forth of the memory budget for the query buffers.
  // another one-forth goes to TileDB for `sm.memory_budget` and
  // `sm.memory_budget_var`
  uint64_t alloc_budget = params_.memory_budget_breakdown.buffers;

  // If the query buffers would be less than 100MB don't double buffer
  if (AttributeBufferSet::compute_buffer_size(attrs, alloc_budget) >
      params_.double_buffering_threshold) {
    // If we are double buffering allocate half to each set of buffers
    alloc_budget = params_.memory_budget_breakdown.buffers / 2;
    buffers_a->allocate_fixed(
        attrs, alloc_budget, dataset_->metadata().version);
    buffers_b->allocate_fixed(
        attrs, alloc_budget, dataset_->metadata().version);
    double_buffering_ = true;
    if (params_.verbose)
      std::cout << "double buffering enabled because buffers are above "
                   "threshold size of "
                << params_.double_buffering_threshold << std::endl;
  } else {
    buffers_a->allocate_fixed(
        attrs, alloc_budget, dataset_->metadata().version);
    buffers_b.reset(nullptr);
    double_buffering_ = false;
    if (params_.verbose)
      std::cout << "double buffering disabled because buffers are below "
                   "threshold size of "
                << params_.double_buffering_threshold << std::endl;
  }
}

void Reader::init_tiledb() {
  tiledb::Config cfg;

  // Default settings
  // Tile cache gets 10% of memory budget
  compute_memory_budget_details();
  cfg["sm.tile_cache_size"] = params_.memory_budget_breakdown.tiledb_tile_cache;

  cfg["sm.sm.compute_concurrency_level"] =
      uint64_t(std::thread::hardware_concurrency() * 1.5f);

  // User overrides. We set it on the map and actual config
  utils::set_tiledb_config_map(
      params_.tiledb_config, &params_.tiledb_config_map);
  utils::set_tiledb_config(params_.tiledb_config_map, &cfg);

  // Set the tile cache to what the config is, this updates the value if the
  // user provided an override Currently this isn't used anywhere else but for
  // good measure let's update to the users value
  params_.memory_budget_breakdown.tiledb_tile_cache =
      std::stoull(cfg.get("sm.tile_cache_size"));

  ctx_.reset(new tiledb::Context(cfg));
  vfs_.reset(new tiledb::VFS(*ctx_, cfg));

  // Set htslib global config and context based on user passed TileDB config
  // options
  utils::set_htslib_tiledb_context(params_.tiledb_config);
}

void Reader::check_partitioning(
    uint64_t partition_idx, uint64_t num_partitions) {
  if (num_partitions == 0)
    throw std::runtime_error(
        "Invalid partitioning; cannot partition into " +
        std::to_string(num_partitions) + " partitions.");
  if (partition_idx >= num_partitions)
    throw std::runtime_error(
        "Invalid partitioning; partition index " +
        std::to_string(partition_idx) + " >= num partitions " +
        std::to_string(num_partitions) + ".");
}

void Reader::queryable_attribute_count(int32_t* count) {
  if (count == nullptr)
    throw std::runtime_error("count must be non-null in attribute_count");

  *count = this->dataset_->queryable_attribute_count();
}

void Reader::queryable_attribute_name(int32_t index, char** name) {
  *name = const_cast<char*>(this->dataset_->queryable_attribute_name(index));
}

void Reader::materialized_attribute_count(int32_t* count) {
  if (count == nullptr)
    throw std::runtime_error("count must be non-null in attribute_count");

  *count = this->dataset_->materialized_attribute_count();
}

void Reader::materialized_attribute_name(int32_t index, char** name) {
  *name = const_cast<char*>(this->dataset_->materialized_attribute_name(index));
}

void Reader::fmt_attribute_count(int32_t* count) {
  if (count == nullptr)
    throw std::runtime_error("count must be non-null in attribute_count");

  *count = this->dataset_->fmt_field_types().size();
}

void Reader::fmt_attribute_name(int32_t index, char** name) {
  auto fmt_attributes = this->dataset_->fmt_field_types();
  auto iter = fmt_attributes.begin();
  std::advance(iter, index);
  std::string s = "fmt_" + iter->first;

  // Loop through queryable attributes to find the preallocated string to
  // return
  for (int32_t i = 0; i < this->dataset_->queryable_attribute_count(); i++) {
    this->queryable_attribute_name(i, name);
    if (s == *name) {
      return;
    }
  }
}

void Reader::info_attribute_count(int32_t* count) {
  if (count == nullptr)
    throw std::runtime_error("count must be non-null in attribute_count");

  *count = this->dataset_->info_field_types().size();
}

void Reader::sample_count(int32_t* count) {
  if (count == nullptr)
    throw std::runtime_error("Error getting sample count");
  *count = dataset_->sample_names().size();
}

void Reader::sample_name(int32_t index, const char** name) {
  *name = dataset_->sample_name(index);
}

void Reader::info_attribute_name(int32_t index, char** name) {
  auto info_attributes = this->dataset_->info_field_types();
  auto iter = info_attributes.begin();
  std::advance(iter, index);
  std::string s = "info_" + iter->first;

  // Loop through queryable attributes to find the preallocated string to
  // return
  for (int32_t i = 0; i < this->dataset_->queryable_attribute_count(); i++) {
    this->queryable_attribute_name(i, name);
    if (s == *name) {
      return;
    }
  }
}

void Reader::set_verbose(const bool& verbose) {
  params_.verbose = verbose;
}

void Reader::set_tiledb_query_config() {
  assert(read_state_.query != nullptr);
  assert(buffers_a != nullptr);

  tiledb::Config cfg;
  if (params_.tiledb_config_map.find("sm.memory_budget") ==
      params_.tiledb_config_map.end())
    cfg["sm.memory_budget"] =
        params_.memory_budget_breakdown.tiledb_memory_budget /
        buffers_a->nbuffers();

  if (params_.tiledb_config_map.find("sm.memory_budget_var") ==
      params_.tiledb_config_map.end())
    cfg["sm.memory_budget_var"] =
        params_.memory_budget_breakdown.tiledb_memory_budget /
        buffers_a->nbuffers();

  read_state_.query->set_config(cfg);
}

void Reader::compute_memory_budget_details() {
  uint64_t memory_budget = params_.memory_budget_mb * 1024 * 1024;

  // Set the tile cache to 10% of total budget
  params_.memory_budget_breakdown.tiledb_tile_cache =
      memory_budget * params_.memory_budget_breakdown.tile_cache_percentage /
      100;
  memory_budget -= params_.memory_budget_breakdown.tiledb_tile_cache;

  // Set the buffers to 25% of the remaining budget
  params_.memory_budget_breakdown.buffers =
      memory_budget * params_.memory_budget_breakdown.buffers_percentage / 100;
  memory_budget -= params_.memory_budget_breakdown.buffers;

  // Set the buffers to all of the remaining budget (3x the buffers for a
  // 25/75 split between buffers and memory budget
  params_.memory_budget_breakdown.tiledb_memory_budget = memory_budget;

  if (params_.verbose) {
    std::cout << "Set memory budgets as follows: "
              << "starting budget: " << params_.memory_budget_mb * 1024 * 1024
              << ", tile_cache: "
              << params_.memory_budget_breakdown.tiledb_tile_cache
              << ", per_buffer_size: "
              << params_.memory_budget_breakdown.buffers
              << ", tiledb_memory_budget: "
              << params_.memory_budget_breakdown.tiledb_memory_budget
              << std::endl;
  }
}

void Reader::set_buffer_percentage(const float& buffer_percentage) {
  params_.memory_budget_breakdown.buffers_percentage = buffer_percentage;
  // Always recompute memory budgets after update
  compute_memory_budget_details();
}

void Reader::set_tiledb_tile_cache_percentage(
    const float& tile_cache_percentage) {
  params_.memory_budget_breakdown.tile_cache_percentage = tile_cache_percentage;
  // Always recompute memory budgets after update
  compute_memory_budget_details();
}

void Reader::set_check_samples_exist(const bool check_samples_exist) {
  params_.check_samples_exist = check_samples_exist;
}

}  // namespace vcf
}  // namespace tiledb
