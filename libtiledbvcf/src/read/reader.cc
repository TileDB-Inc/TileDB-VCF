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
}

void Reader::open_dataset(const std::string& dataset_uri) {
  init_tiledb();

  dataset_.reset(new TileDBVCFDataset);
  dataset_->open(dataset_uri, params_.tiledb_config);
}

void Reader::reset() {
  read_state_ = ReadState();
  if (exporter_ != nullptr)
    exporter_->reset();
}

void Reader::set_all_params(const ExportParams& params) {
  params_ = params;
  init_tiledb();
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
}

void Reader::set_buffer_offsets(
    const std::string& attribute, int32_t* buff, int64_t buff_size) {
  auto exp = set_in_memory_exporter();
  exp->set_buffer_offsets(attribute, buff, buff_size);
}

void Reader::set_buffer_list_offsets(
    const std::string& attribute, int32_t* buff, int64_t buff_size) {
  auto exp = set_in_memory_exporter();
  exp->set_buffer_list_offsets(attribute, buff, buff_size);
}

void Reader::set_buffer_validity_bitmap(
    const std::string& attribute, uint8_t* buff, int64_t buff_size) {
  auto exp = set_in_memory_exporter();
  exp->set_buffer_validity_bitmap(attribute, buff, buff_size);
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
  init_tiledb();
}

void Reader::set_record_limit(uint64_t max_num_records) {
  params_.max_num_records = max_num_records;
}

void Reader::set_tiledb_config(const std::string& config_str) {
  params_.tiledb_config = utils::split(config_str, ',');
  init_tiledb();
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

void Reader::tiledb_stats_enabled(bool* enabled) {
  *enabled = params_.tiledb_stats_enabled;
}

void Reader::tiledb_stats(char** stats) {
  auto rc = tiledb_stats_dump_str(stats);
  if (rc != TILEDB_OK)
    throw std::runtime_error("Error dumping tiledb statistics");
}

void Reader::dataset_version(int32_t* version) const {
  if (dataset_ == nullptr)
    throw std::runtime_error(
        "Error getting dataset version; dataset is not open.");
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
      // Do nothing and return.
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
  read_state_.batch_idx = 0;
  read_state_.sample_batches = prepare_sample_batches();
  read_state_.last_intersecting_region_idx_ = 0;

  init_exporter();
  prepare_regions(&read_state_.regions, &read_state_.query_regions);
  prepare_attribute_buffers();
}

bool Reader::next_read_batch() {
  // Check if we're done.
  if (read_state_.batch_idx >= read_state_.sample_batches.size() ||
      read_state_.total_num_records_exported >= params_.max_num_records)
    return false;

  // Handle edge case of an empty region partition
  if (read_state_.query_regions.empty())
    return false;

  // Start the first batch, or advance to the next one if possible.
  if (read_state_.status == ReadStatus::UNINITIALIZED) {
    read_state_.batch_idx = 0;
  } else if (read_state_.batch_idx + 1 < read_state_.sample_batches.size()) {
    read_state_.batch_idx++;
  } else {
    return false;
  }

  // Sample row range
  const auto& samples = read_state_.sample_batches[read_state_.batch_idx];
  read_state_.sample_min = std::numeric_limits<uint32_t>::max();
  read_state_.sample_max = std::numeric_limits<uint32_t>::min();
  for (const auto& s : samples) {
    read_state_.sample_min = std::min(read_state_.sample_min, s.sample_id);
    read_state_.sample_max = std::max(read_state_.sample_min, s.sample_id);
  }

  // User query regions
  read_state_.region_idx = 0;

  // One element per sample (row) containing the real_end position of the last
  // record that was reported.
  read_state_.last_reported_end.clear();
  read_state_.last_reported_end.resize(
      read_state_.sample_max - read_state_.sample_min + 1,
      std::numeric_limits<uint32_t>::max());

  // Headers
  read_state_.current_hdrs.clear();
  read_state_.current_hdrs = dataset_->fetch_vcf_headers(
      *ctx_, read_state_.sample_min, read_state_.sample_max);

  // Sample handles
  read_state_.current_samples.clear();
  for (const auto& s : samples) {
    read_state_.current_samples[s.sample_id - read_state_.sample_min] = s;
  }

  // Reopen the array so that irrelevant fragment metadata is unloaded.
  read_state_.array.reset(nullptr);
  read_state_.array.reset(new Array(*ctx_, dataset_->data_uri(), TILEDB_READ));

  // Set up the TileDB query
  read_state_.query.reset(new Query(*ctx_, *read_state_.array));
  read_state_.query->add_range(
      0, read_state_.sample_min, read_state_.sample_max);
  for (const auto& query_region : read_state_.query_regions)
    read_state_.query->add_range(1, query_region.col_min, query_region.col_max);
  read_state_.query->set_layout(TILEDB_UNORDERED);
  if (params_.verbose)
    std::cout << "Initialized TileDB query with "
              << read_state_.query_regions.size() << " column ranges."
              << std::endl;

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

  // Note that exporter may be null if the user has specified no export to disk
  // but did not set any buffers for in-memory export. This reduces to a count
  // operation, and is supported.
  if (exporter_ != nullptr)
    exporter_->set_dataset(dataset_.get());
}

bool Reader::read_current_batch() {
  tiledb::Query* query = read_state_.query.get();

  if (read_state_.status == ReadStatus::INCOMPLETE) {
    auto exp = dynamic_cast<InMemoryExporter*>(exporter_.get());
    if (exp == nullptr)
      throw std::runtime_error(
          "Error reading batch; incomplete query without user buffer "
          "exporter should not be possible.");

    // If the read status was incomplete, pick up processing the previous TileDB
    // query results.
    if (!process_query_results())
      return false;  // Still incomplete.

    // If we finished processing previous results and the TileDB query is now
    // complete, we are done. We check both the query_results and the query
    // itself to capture the case of a new underlying tiledb query for the next
    // range/sample partitioning since we partition samples on tile_extent
    if (read_state_.query_results.query_status() !=
            tiledb::Query::Status::INCOMPLETE &&
        read_state_.query->query_status() !=
            tiledb::Query::Status::UNINITIALIZED) {
      return true;
    }
  }

  // If a past TileDB query was in-flight (from incomplete reads), it was using
  // the B buffers, so start off with that. Otherwise, submit a new async query.
  if (read_state_.async_query.valid()) {
    std::swap(buffers_a, buffers_b);
  } else {
    buffers_a->set_buffers(query);
    read_state_.async_query =
        std::async(std::launch::async, [&query]() { return query->submit(); });
  }

  do {
    // Block on query completion.
    auto query_status = read_state_.async_query.get();
    read_state_.query_results.set_results(*dataset_, buffers_a.get(), *query);
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
    if (query_status == tiledb::Query::Status::INCOMPLETE) {
      buffers_b->set_buffers(query);
      read_state_.async_query = std::async(
          std::launch::async, [&query]() { return query->submit(); });
    }

    // Process the query results.
    auto old_num_exported = read_state_.last_num_records_exported;
    auto t0 = std::chrono::steady_clock::now();
    bool complete = process_query_results();

    if (params_.verbose)
      std::cout << "Processed " << read_state_.query_results.num_cells()
                << " cells in " << utils::chrono_duration(t0)
                << " sec. Reported "
                << (read_state_.last_num_records_exported - old_num_exported)
                << " cells." << std::endl;

    // Return early if we couldn't process all the results.
    if (!complete)
      return false;

    // Swap the buffers.
    std::swap(buffers_a, buffers_b);
  } while (read_state_.query_results.query_status() ==
               tiledb::Query::Status::INCOMPLETE &&
           read_state_.total_num_records_exported < params_.max_num_records);

  // Batch complete; finalize the export (if applicable).
  if (exporter_ != nullptr) {
    for (const auto& s : read_state_.sample_batches[read_state_.batch_idx]) {
      SafeBCFHdr& hdr =
          read_state_.current_hdrs[s.sample_id - read_state_.sample_min];
      exporter_->finalize_export(s, hdr.get());
    }
  }

  return true;
}

bool Reader::process_query_results() {
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
    const uint32_t sample_id = results.buffers()->sample().value<uint32_t>(i);
    const uint32_t end = results.buffers()->end_pos().value<uint32_t>(i);
    const uint32_t start = results.buffers()->pos().value<uint32_t>(i);
    const uint32_t real_end = results.buffers()->real_end().value<uint32_t>(i);

    // Skip cell if we've already reported the gVCF record for it.
    if (real_end ==
        read_state_.last_reported_end[sample_id - read_state_.sample_min])
      continue;

    // If we've passed into a new contig, get the new info for it.
    if (end >= contig_info.first + contig_info.second)
      contig_info = dataset_->contig_from_column(end);
    const uint32_t contig_offset = contig_info.first;

    // Get original regions which intersect the cell's gVCF range (may be none).
    size_t new_region_idx;
    std::pair<size_t, size_t> intersecting = get_intersecting_regions(
        read_state_.regions,
        read_state_.region_idx,
        start,
        end,
        real_end,
        &new_region_idx);
    if (intersecting.first == std::numeric_limits<uint32_t>::max() ||
        intersecting.second == std::numeric_limits<uint32_t>::max())
      continue;

    // Report all intersections. If the previous read returned before
    // reporting all intersecting regions, 'last_intersecting_region_idx_'
    // will be non-zero. All regions with an index less-than
    // 'last_intersecting_region_idx_' have already been reported, so we
    // must avoid reporting them multiple times.
    size_t j = read_state_.last_intersecting_region_idx_ > 0 ?
                   read_state_.last_intersecting_region_idx_ :
                   intersecting.first;
    for (; j <= intersecting.second; j++) {
      const auto& reg = read_state_.regions[j];
      const uint32_t reg_min = reg.seq_offset + reg.min;
      const uint32_t reg_max = reg.seq_offset + reg.max;
      bool intersects = start <= reg_max && real_end >= reg_min;
      if (!intersects)
        throw std::runtime_error(
            "Error in query result processing; range unexpectedly does not "
            "intersect cell.");

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

    read_state_.last_reported_end[sample_id - read_state_.sample_min] =
        real_end;
    read_state_.region_idx = new_region_idx;
  }

  return true;
}

std::pair<size_t, size_t> Reader::get_intersecting_regions(
    const std::vector<Region>& regions,
    size_t region_idx,
    uint32_t start,
    uint32_t end,
    uint32_t real_end,
    size_t* new_region_idx) {
  const auto intersects_p = [](const Region& r, uint32_t s, uint32_t e) {
    return s <= (r.seq_offset + r.max) && e >= (r.seq_offset + r.min);
  };
  const auto nil = std::numeric_limits<uint32_t>::max();

  if (regions.empty())
    return {nil, nil};

  // Find the index of the last region that intersects the cell's END position.
  // This is stored in the output variable for the new region index.
  size_t last = nil;
  for (size_t i = region_idx; i < regions.size(); i++) {
    // Regions are sorted on END, so stop searching early if possible.
    if (end < regions[i].seq_offset + regions[i].min)
      break;

    bool intersects = intersects_p(regions[i], start, end);
    if (i < regions.size() - 1) {
      bool next_intersects = intersects_p(regions[i + 1], start, end);
      if (intersects && !next_intersects) {
        last = i;
        break;
      }
    } else if (intersects) {
      last = i;
      break;
    }
  }

  // Check if no regions intersect.
  if (last == nil)
    return {nil, nil};

  // Set the new region index to the last intersecting region index.
  *new_region_idx = last;

  // Next find the index of the last region that intersects the cell's REAL_END
  // position. This is used as the actual interval of intersection.
  for (size_t i = *new_region_idx; i < regions.size(); i++) {
    bool intersects = intersects_p(regions[i], start, real_end);
    if (i < regions.size() - 1) {
      bool next_intersects = intersects_p(regions[i + 1], start, real_end);
      if (intersects && !next_intersects) {
        last = i;
        break;
      }
    } else if (intersects) {
      last = i;
      break;
    }
  }

  // Search backwards to find the first region that intersects the cell.
  size_t first = nil;
  for (size_t i = *new_region_idx;; i--) {
    bool intersects = intersects_p(regions[i], start, real_end);
    if (i > 0) {
      bool prev_intersects = intersects_p(regions[i - 1], start, real_end);
      if (intersects && !prev_intersects) {
        first = i;
        break;
      }
    } else if (intersects) {
      first = i;
      break;
    }

    if (i == 0)
      break;
  }

  // If we're here then we must have a valid interval.
  if (first == nil || last == nil)
    throw std::runtime_error(
        "Error finding intersection region interval; invalid interval.");

  return {first, last};
}

bool Reader::report_cell(
    const Region& region, uint32_t contig_offset, uint64_t cell_idx) {
  if (exporter_ == nullptr) {
    read_state_.last_num_records_exported++;
    read_state_.total_num_records_exported++;
    return true;
  }

  const auto& results = read_state_.query_results;
  uint32_t samp_idx = results.buffers()->sample().value<uint32_t>(cell_idx) -
                      read_state_.sample_min;

  // Skip this cell if we are not reporting its sample.
  if (read_state_.current_samples.count(samp_idx) == 0) {
    return true;
  }

  const auto& sample = read_state_.current_samples[samp_idx];
  const auto& hdr = read_state_.current_hdrs[samp_idx];

  if (!exporter_->export_record(
          sample, hdr.get(), region, contig_offset, results, cell_idx))
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
  const uint32_t space_tile_extent = dataset_->metadata().row_tile_extent;
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

std::vector<SampleAndId> Reader::prepare_sample_names() const {
  std::vector<SampleAndId> result;

  for (const std::string& s : params_.sample_names) {
    std::string name;
    if (!VCF::normalize_sample_name(s, &name))
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
      if (!VCF::normalize_sample_name(*line, &name))
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
    for (const auto& s : md.sample_names) {
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

void Reader::prepare_regions(
    std::vector<Region>* regions,
    std::vector<QueryRegion>* query_regions) const {
  const uint32_t g = dataset_->metadata().anchor_gap;

  // Manually-specified regions (-r) are 1-indexed and inclusive
  for (const std::string& r : params_.regions)
    regions->emplace_back(r, Region::Type::OneIndexedInclusive);

  // Add BED file regions, if specified.
  if (!params_.regions_file_uri.empty())
    Region::parse_bed_file(*vfs_, params_.regions_file_uri, regions);

  // No specified regions means all regions.
  if (regions->empty())
    *regions = dataset_->all_contigs();

  Array array = Array(*ctx_, dataset_->data_uri(), TILEDB_READ);
  std::pair<uint32_t, uint32_t> regionNonEmptyDomain;
  const auto& nonEmptyDomain = array.non_empty_domain<uint32_t>();
  regionNonEmptyDomain = nonEmptyDomain[1].second;
  std::vector<Region> filtered_regions;
  // Loop through all contigs to query and pre-filter to ones which fall inside
  // the nonEmptyDomain This will balance the partitioning better my removing
  // empty regions
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
        widened_reg_max, std::numeric_limits<uint32_t>::max());
    if (reg_min <= regionNonEmptyDomain.second &&
        widened_reg_max >= regionNonEmptyDomain.first) {
      filtered_regions.emplace_back(r);
    }
  }
  *regions = filtered_regions;

  // Sort all by global column coord.
  if (params_.sort_regions)
    Region::sort(dataset_->metadata().contig_offsets, regions);

  // Apply region partitioning before expanding.
  // If we have less regions than requested partitions, handle that by allowing
  // empty partitions
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
        widened_reg_max, std::numeric_limits<uint32_t>::max());

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
  std::set<std::string> attrs = {
      TileDBVCFDataset::DimensionNames::sample,
      TileDBVCFDataset::DimensionNames::end_pos,
      TileDBVCFDataset::AttrNames::pos,
      TileDBVCFDataset::AttrNames::real_end};

  buffers_a.reset(new AttributeBufferSet);
  buffers_b.reset(new AttributeBufferSet);

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

  // We get half of the memory budget for the query buffers.
  const unsigned alloc_budget = params_.memory_budget_mb / 4;
  buffers_a->allocate_fixed(attrs, alloc_budget);
  buffers_b->allocate_fixed(attrs, alloc_budget);
}

void Reader::init_tiledb() {
  tiledb::Config cfg;

  // Default settings
  cfg["sm.tile_cache_size"] = uint64_t(1) * 1024 * 1024 * 1024;
  cfg["sm.num_reader_threads"] =
      uint64_t(std::thread::hardware_concurrency() * 1.5f);

  // TileDB gets half of our memory budget, and a minimum of 10MB.
  const uint64_t tiledb_mem_budget = std::max<uint64_t>(
      10 * 1024 * 1024, (uint64_t(params_.memory_budget_mb) * 1024 * 1024) / 2);
  cfg["sm.memory_budget"] = tiledb_mem_budget / 2;
  cfg["sm.memory_budget_var"] = tiledb_mem_budget / 2;

  // User overrides
  utils::set_tiledb_config(params_.tiledb_config, &cfg);

  ctx_.reset(new tiledb::Context(cfg));
  vfs_.reset(new tiledb::VFS(*ctx_, cfg));
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

}  // namespace vcf
}  // namespace tiledb
