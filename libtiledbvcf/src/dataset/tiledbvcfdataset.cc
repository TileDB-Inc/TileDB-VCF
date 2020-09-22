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
#include <map>
#include <string>
#include <vector>

#include "base64/base64.h"
#include "dataset/tiledbvcfdataset.h"
#include "utils/utils.h"
#include "vcf/vcf_utils.h"

namespace tiledb {
namespace vcf {

using dimNamesV3 = TileDBVCFDataset::DimensionNames::V3;
const std::string dimNamesV3::sample = "sample";
const std::string dimNamesV3::start_pos = "start_pos";

using dimNamesV2 = TileDBVCFDataset::DimensionNames::V2;
const std::string dimNamesV2::sample = "sample";
const std::string dimNamesV2::end_pos = "end_pos";

using attrNamesV3 = TileDBVCFDataset::AttrNames::V3;
const std::string attrNamesV3::real_start_pos = "real_start_pos";
const std::string attrNamesV3::end_pos = "end_pos";
const std::string attrNamesV3::qual = "qual";
const std::string attrNamesV3::alleles = "alleles";
const std::string attrNamesV3::id = "id";
const std::string attrNamesV3::filter_ids = "filter_ids";
const std::string attrNamesV3::info = "info";
const std::string attrNamesV3::fmt = "fmt";

using attrNamesV2 = TileDBVCFDataset::AttrNames::V2;
const std::string attrNamesV2::pos = "pos";
const std::string attrNamesV2::real_end = "real_end";
const std::string attrNamesV2::qual = "qual";
const std::string attrNamesV2::alleles = "alleles";
const std::string attrNamesV2::id = "id";
const std::string attrNamesV2::filter_ids = "filter_ids";
const std::string attrNamesV2::info = "info";
const std::string attrNamesV2::fmt = "fmt";

namespace {
FilterList default_attribute_filter_list(const Context& ctx) {
  FilterList attribute_filter_list(ctx);
  attribute_filter_list.add_filter({ctx, TILEDB_FILTER_ZSTD});
  return attribute_filter_list;
}

FilterList default_offsets_filter_list(const Context& ctx) {
  FilterList offsets_filters(ctx);
  offsets_filters.add_filter({ctx, TILEDB_FILTER_DOUBLE_DELTA})
      .add_filter({ctx, TILEDB_FILTER_ZSTD});
  return offsets_filters;
}
}  // namespace

TileDBVCFDataset::TileDBVCFDataset()
    : open_(false) {
  utils::init_htslib();
}

void TileDBVCFDataset::create(const CreationParams& params) {
  Config cfg;
  utils::set_tiledb_config(params.tiledb_config, &cfg);
  Context ctx(cfg);

  VFS vfs(ctx);

  check_attribute_names(params.extra_attributes);

  if (vfs.is_dir(params.uri)) {
    // If the directory exists, check if it's a dataset. If so, return with no
    // error (allows for multiple no-op create calls).
    if (vfs.is_dir(data_array_uri(params.uri)))
      return;

    throw std::runtime_error(
        "Cannot create TileDB-VCF dataset; directory exists.");
  }
  create_group(ctx, params.uri);

  Metadata metadata;
  metadata.row_tile_extent = params.row_tile_extent;
  metadata.tile_capacity = params.tile_capacity;
  metadata.anchor_gap = params.anchor_gap;
  metadata.extra_attributes = params.extra_attributes;
  metadata.free_sample_id = 0;

  create_empty_metadata(ctx, params.uri, metadata, params.checksum);
  create_empty_data_array(
      ctx, params.uri, metadata, params.checksum, params.allow_duplicates);
  write_metadata(ctx, params.uri, metadata);
}

void TileDBVCFDataset::check_attribute_names(
    const std::vector<std::string>& attribues) {
  const auto errmsg = [](const std::string& attr_name) {
    return "Invalid attribute name '" + attr_name +
           "'; extracted attributes must be format 'info_*' or 'fmt_*'.";
  };

  for (const auto& attr_name : attribues) {
    bool info_or_fmt = utils::starts_with(attr_name, "info") ||
                       utils::starts_with(attr_name, "fmt");
    if (!info_or_fmt)
      throw std::runtime_error(errmsg(attr_name));

    auto underscore = attr_name.find('_');
    if (underscore == std::string::npos || underscore == 0)
      throw std::runtime_error(errmsg(attr_name));

    std::string kind = attr_name.substr(0, underscore);
    if (kind != "info" && kind != "fmt")
      throw std::runtime_error(errmsg(attr_name));

    std::string name = attr_name.substr(kind.size() + 1);
    if (name.empty())
      throw std::runtime_error(errmsg(attr_name));
  }
}

void TileDBVCFDataset::create_empty_metadata(
    const Context& ctx,
    const std::string& root_uri,
    const Metadata& metadata,
    const tiledb_filter_type_t& checksum) {
  create_group(ctx, utils::uri_join(root_uri, "metadata"));
  create_sample_header_array(ctx, root_uri, checksum);
}

void TileDBVCFDataset::create_empty_data_array(
    const Context& ctx,
    const std::string& root_uri,
    const Metadata& metadata,
    const tiledb_filter_type_t& checksum,
    const bool allow_duplicates) {
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_capacity(metadata.tile_capacity);
  schema.set_order({{TILEDB_COL_MAJOR, TILEDB_COL_MAJOR}});
  schema.set_allows_dups(allow_duplicates);

  Domain domain(ctx);
  {
    const auto dom_min = 0;
    const auto dom_max = std::numeric_limits<uint32_t>::max() - 1;
    const auto sample_dom_max =
        dom_max - static_cast<uint32_t>(metadata.row_tile_extent);
    auto sample = Dimension::create<uint32_t>(
        ctx,
        DimensionNames::V3::sample,
        {{dom_min, sample_dom_max}},
        metadata.row_tile_extent);
    auto start_pos = Dimension::create<uint32_t>(
        ctx,
        DimensionNames::V3::start_pos,
        {{dom_min, dom_max}},
        dom_max - dom_min + 1);
    domain.add_dimensions(sample, start_pos);
  }
  schema.set_domain(domain);
  auto offsets_filter_list = default_offsets_filter_list(ctx);

  // Set coords filters
  FilterList coords_filter_list(ctx);
  coords_filter_list.add_filter({ctx, TILEDB_FILTER_DOUBLE_DELTA})
      .add_filter({ctx, TILEDB_FILTER_ZSTD});

  // Create a byteshuffle -> zstd filter list used by a few attributes
  FilterList byteshuffle_zstd_filters(ctx);
  byteshuffle_zstd_filters.add_filter({ctx, TILEDB_FILTER_BYTESHUFFLE})
      .add_filter({ctx, TILEDB_FILTER_ZSTD});

  auto attribute_filter_list = default_attribute_filter_list(ctx);
  if (checksum != TILEDB_FILTER_NONE) {
    Filter checksum_filter(ctx, checksum);

    attribute_filter_list.add_filter(checksum_filter);
    byteshuffle_zstd_filters.add_filter(checksum_filter);
    coords_filter_list.add_filter(checksum_filter);
    offsets_filter_list.add_filter(checksum_filter);
  }
  schema.set_coords_filter_list(coords_filter_list);
  schema.set_offsets_filter_list(offsets_filter_list);

  auto real_start_pos = Attribute::create<uint32_t>(
      ctx, AttrNames::V3::real_start_pos, byteshuffle_zstd_filters);
  auto end_pos = Attribute::create<uint32_t>(
      ctx, AttrNames::V3::end_pos, byteshuffle_zstd_filters);
  auto qual =
      Attribute::create<float>(ctx, AttrNames::V3::qual, attribute_filter_list);
  auto alleles = Attribute::create<std::vector<char>>(
      ctx, AttrNames::V3::alleles, attribute_filter_list);
  auto id = Attribute::create<std::vector<char>>(
      ctx, AttrNames::V3::id, attribute_filter_list);
  auto filters_ids = Attribute::create<std::vector<int32_t>>(
      ctx, AttrNames::V3::filter_ids, byteshuffle_zstd_filters);
  auto info = Attribute::create<std::vector<uint8_t>>(
      ctx, AttrNames::V3::info, attribute_filter_list);
  auto fmt = Attribute::create<std::vector<uint8_t>>(
      ctx, AttrNames::V3::fmt, attribute_filter_list);
  schema.add_attributes(
      real_start_pos, end_pos, qual, alleles, id, filters_ids, info, fmt);

  // Remaining INFO/FMT fields extracted as separate attributes:
  std::set<std::string> used;
  for (auto& attr : metadata.extra_attributes) {
    if (used.count(attr))
      continue;
    used.insert(attr);
    schema.add_attribute(Attribute::create<std::vector<uint8_t>>(
        ctx, attr, attribute_filter_list));
  }

  Array::create(data_array_uri(root_uri), schema);
}

void TileDBVCFDataset::create_sample_header_array(
    const Context& ctx,
    const std::string& root_uri,
    const tiledb_filter_type_t& checksum) {
  ArraySchema schema(ctx, TILEDB_DENSE);

  // Set domain
  Domain domain(ctx);
  const uint32_t dom_min = 0;
  const uint32_t dom_max = std::numeric_limits<uint32_t>::max() - 1;
  const uint32_t tile_ext = 10;
  auto sample = Dimension::create<uint32_t>(
      ctx, "sample", {{dom_min, dom_max - tile_ext}}, tile_ext);
  domain.add_dimensions(sample);
  schema.set_domain(domain);

  // Set offsets filters
  FilterList offsets_filter_list = default_offsets_filter_list(ctx);

  // Add a single 'header' string attribute.
  FilterList attribute_filter_list = default_attribute_filter_list(ctx);
  if (checksum != TILEDB_FILTER_NONE) {
    Filter checksum_filter(ctx, checksum);

    attribute_filter_list.add_filter(checksum_filter);
    offsets_filter_list.add_filter(checksum_filter);
    FilterList coords_filter_list(ctx);
    coords_filter_list.add_filter(checksum_filter);
    schema.set_coords_filter_list(coords_filter_list);
  }
  schema.set_offsets_filter_list(offsets_filter_list);
  auto attr_header = Attribute::create<std::vector<char>>(
      ctx, "header", attribute_filter_list);
  schema.add_attributes(attr_header);

  Array::create(vcf_headers_uri(root_uri), schema);
}

void TileDBVCFDataset::open(
    const std::string& uri, const std::vector<std::string>& tiledb_config) {
  if (open_)
    throw std::runtime_error(
        "Cannot open TileDB-VCF dataset; dataset already open.");
  root_uri_ = uri;

  Config cfg;
  utils::set_tiledb_config(tiledb_config, &cfg);
  Context ctx(cfg);

  metadata_ = read_metadata(ctx, root_uri_);

  // We support V2 and V3 (current) formats.
  if (metadata_.version != Version::V2 && metadata_.version != Version::V3)
    throw std::runtime_error(
        "Cannot open TileDB-VCF dataset; dataset is version " +
        std::to_string(metadata_.version) +
        " but only versions 2 and 3 are supported.");

  load_field_type_maps(ctx);

  open_ = true;

  // Build queryable attribute and sample lists
  std::set<std::string> unique_queryable_attributes{
      "sample_name", "query_bed_start", "query_bed_end", "contig"};
  for (auto s : this->all_attributes()) {
    if (s == "end_pos" || s == "real_end")
      s = "pos_end";
    else if (s == "start_pos" || s == "real_start_pos" || s == "pos")
      s = "pos_start";
    else if (s == "filter_ids")
      s = "filters";

    unique_queryable_attributes.emplace(s);
  }

  for (const auto& info : info_field_types_) {
    unique_queryable_attributes.emplace("info_" + info.first);
  }

  for (const auto& fmt : fmt_field_types_) {
    unique_queryable_attributes.emplace("fmt_" + fmt.first);
  }

  for (const auto& key : unique_queryable_attributes) {
    std::vector<char> name(key.begin(), key.end());
    name.emplace_back('\0');
    vcf_attributes_.push_back(name);
  }

  for (const auto& s : metadata_.sample_names) {
    std::vector<char> sample(s.begin(), s.end());
    sample.emplace_back('\0');
    sample_names_.push_back(sample);
  }
}

void TileDBVCFDataset::load_field_type_maps(const tiledb::Context& ctx) {
  // Empty array (no samples registered); do nothing.
  if (metadata_.sample_ids.empty())
    return;

  auto first_sample = metadata_.sample_ids.at(metadata_.sample_names.at(0));
  auto hdrs = fetch_vcf_headers(ctx, first_sample, first_sample);
  if (hdrs.size() != 1)
    throw std::runtime_error(
        "Error loading dataset field types; no headers fetched.");

  bcf_hdr_t* hdr = hdrs[0].get();
  for (int i = 0; i < hdr->n[BCF_DT_ID]; i++) {
    bcf_idpair_t* idpair = hdr->id[BCF_DT_ID] + i;
    if (idpair == nullptr)
      throw std::runtime_error(
          "Error loading dataset field types; null idpair.");
    if (idpair->key == nullptr || idpair->val == nullptr)
      throw std::runtime_error(
          "Error loading dataset field types; null idpair field.");

    const char* name = idpair->key;
    bool is_info = idpair->val->hrec[1] != nullptr;
    bool is_fmt = idpair->val->hrec[2] != nullptr;

    if (is_info) {
      int type = bcf_hdr_id2type(hdr, idpair->val->hrec[1]->type, i);
      info_field_types_[name] = type;
    }

    if (is_fmt) {
      int type = bcf_hdr_id2type(hdr, idpair->val->hrec[2]->type, i);
      fmt_field_types_[name] = type;
    }
  }
}

void TileDBVCFDataset::register_samples(const RegistrationParams& params) {
  if (!open_)
    throw std::invalid_argument(
        "Cannot register samples; dataset is not open.");

  Config cfg;
  utils::set_tiledb_config(params.tiledb_config, &cfg);
  Context ctx(cfg);
  VFS vfs(ctx, cfg);

  std::vector<SampleAndIndex> samples = SampleUtils::build_samples_uri_list(
      vfs, params.sample_uris_file, params.sample_uris);
  if (samples.empty())
    throw std::invalid_argument(
        "Cannot register samples; samples list is empty.");

  // Registration proceeds in batches, double-buffering the downloads.
  std::set<std::string> sample_set(
      metadata_.all_samples.begin(), metadata_.all_samples.end());
  std::map<uint32_t, std::string> sample_headers;
  std::vector<std::vector<SampleAndIndex>> batches =
      utils::batch_elements(samples, 100);
  std::future<std::vector<SafeBCFHdr>> future_headers = std::async(
      std::launch::async,
      SampleUtils::download_sample_headers,
      vfs,
      batches[0],
      params.scratch_space);
  for (unsigned i = 1; i < batches.size(); i++) {
    auto headers = future_headers.get();
    // Start the next batch downloading
    future_headers = std::async(
        std::launch::async,
        SampleUtils::download_sample_headers,
        vfs,
        batches[i],
        params.scratch_space);
    // Register the batch
    register_samples_helper(headers, &metadata_, &sample_set, &sample_headers);
    write_vcf_headers(ctx, root_uri_, sample_headers);
    sample_headers.clear();
  }

  // Register the final batch.
  register_samples_helper(
      future_headers.get(), &metadata_, &sample_set, &sample_headers);
  write_vcf_headers(ctx, root_uri_, sample_headers);

  // Write the updated metadata.
  write_metadata(ctx, root_uri_, metadata_);
}

void TileDBVCFDataset::print_samples_list() {
  if (!open_)
    throw std::invalid_argument("Cannot list samples; dataset is not open.");
  for (const auto& s : metadata_.sample_names)
    std::cout << s << "\n";
}

void TileDBVCFDataset::print_dataset_stats() {
  if (!open_)
    throw std::invalid_argument(
        "Cannot print dataset stats; dataset is not open.");

  utils::enable_pretty_print_numbers(std::cout);
  std::cout << "Statistics for dataset '" << root_uri_ << "':" << std::endl;
  std::cout << "- Version: " << metadata_.version << std::endl;
  std::cout << "- Row tile extent: " << metadata_.row_tile_extent << std::endl;
  std::cout << "- Tile capacity: " << metadata_.tile_capacity << std::endl;
  std::cout << "- Anchor gap: " << metadata_.anchor_gap << std::endl;
  std::cout << "- Number of registered samples: "
            << metadata_.sample_names.size() << std::endl;

  std::cout << "- Extracted attributes: ";
  if (metadata_.extra_attributes.empty()) {
    std::cout << "none" << std::endl;
  } else {
    for (size_t i = 0; i < metadata_.extra_attributes.size(); i++) {
      std::cout << metadata_.extra_attributes[i];
      if (i < metadata_.extra_attributes.size() - 1)
        std::cout << ", ";
    }
    std::cout << std::endl;
  }
}

const TileDBVCFDataset::Metadata& TileDBVCFDataset::metadata() const {
  return metadata_;
}

std::string TileDBVCFDataset::data_uri() const {
  return data_array_uri(root_uri_);
}

std::vector<SafeBCFHdr> TileDBVCFDataset::fetch_vcf_headers(
    const tiledb::Context& ctx,
    uint32_t sample_id_min,
    uint32_t sample_id_max) const {
  std::vector<SafeBCFHdr> result;
  std::unique_ptr<Array> array;
  try {
    // First let's try to open the metadata using proper cloud detection
    std::string array_uri = vcf_headers_uri(root_uri_, true);
    // Set up and submit query
    array = std::unique_ptr<Array>(new Array(ctx, array_uri, TILEDB_READ));
  } catch (const tiledb::TileDBError& ex) {
    try {
      // Fall back to use s3 style paths, this handle datasets that are
      // registered on the cloud but not with the proper naming scheme. Allows
      // tiledb://namespace/s3://bucket/tiledbvcf_array style access
      std::string array_uri = vcf_headers_uri(root_uri_, false);

      // Set up and submit query
      array = std::unique_ptr<Array>(new Array(ctx, array_uri, TILEDB_READ));
    } catch (const tiledb::TileDBError& ex) {
      throw std::runtime_error(
          "Cannot open TileDB-VCF vcf headers; dataset '" + root_uri_ +
          "' or its metadata does not exist. TileDB error message: " +
          std::string(ex.what()));
    }
  }

  Query query(ctx, *array);

  std::vector<uint32_t> subarray = {sample_id_min, sample_id_max};
  query.set_layout(TILEDB_ROW_MAJOR).set_subarray(subarray);

  std::pair<uint64_t, uint64_t> est_size = query.est_result_size_var("header");
  std::vector<uint64_t> offsets(est_size.first);
  std::vector<char> data(est_size.second);

  Query::Status status;
  uint32_t sample_idx = sample_id_min;

  query.set_buffer("header", offsets, data);

  do {
    status = query.submit();

    std::pair<uint64_t, uint64_t> result_el =
        query.result_buffer_elements()["header"];
    uint64_t num_offsets = result_el.first;
    uint64_t num_chars = result_el.second;

    bool has_results = num_chars != 0;

    if (status == Query::Status::INCOMPLETE && !has_results) {
      // If there are no results, double the size of the buffer and then
      // resubmit the query.

      if (num_chars == 0)
        data.resize(data.size() * 2);

      if (num_offsets == 0)
        offsets.resize(offsets.size() * 2);

      query.set_buffer("header", offsets, data);
    } else if (has_results) {
      // Parse the samples.

      for (size_t offset_idx = 0; offset_idx < num_offsets; ++offset_idx) {
        char* beg_hdr = data.data() + offsets[offset_idx];
        uint64_t hdr_size =
            offset_idx == num_offsets - 1 ? num_chars : offsets[offset_idx + 1];
        hdr_size = hdr_size - offsets[offset_idx];
        std::string hdr_str(beg_hdr, hdr_size);

        bcf_hdr_t* hdr = bcf_hdr_init("r");
        if (!hdr)
          throw std::runtime_error(
              "Error fetching VCF header data; error allocating VCF header.");

        char* hdr_raw_str = strndup(hdr_str.c_str(), hdr_size);
        if (NULL == hdr_raw_str) {
          throw std::runtime_error(
              "Error allocating space for the char* header string.");
        }
        if (0 != bcf_hdr_parse(hdr, hdr_raw_str)) {
          throw std::runtime_error("Error parsing the BCF header.");
        }
        free(hdr_raw_str);

        if (0 != bcf_hdr_add_sample(
                     hdr, metadata_.sample_names[sample_idx++].c_str())) {
          throw std::runtime_error("Error adding the sample.");
        }

        if (bcf_hdr_sync(hdr) < 0)
          throw std::runtime_error(
              "Error in bcftools: failed to update VCF header.");

        result.emplace_back(hdr, bcf_hdr_destroy);
      }
    }
  } while (status == Query::Status::INCOMPLETE);

  return result;
}  // namespace vcf

std::string TileDBVCFDataset::first_contig() const {
  for (const auto& pair : metadata_.contig_offsets) {
    if (pair.second == 0)
      return pair.first;
  }

  throw std::runtime_error(
      "Error getting first contig; no contig had offset 0");
}

std::vector<Region> TileDBVCFDataset::all_contigs() const {
  std::vector<Region> result;

  if (metadata_.contig_offsets.empty())
    return result;

  // Sort by offset
  std::map<uint32_t, std::string> sorted_contigs;
  for (const auto& c : metadata_.contig_offsets)
    sorted_contigs[c.second] = c.first;

  // Push one region per contig, sorted on global position.
  uint32_t prev_offset = std::numeric_limits<uint32_t>::max();
  std::string prev_contig;
  for (const auto& c : sorted_contigs) {
    if (prev_offset != std::numeric_limits<uint32_t>::max()) {
      uint32_t prev_len = c.first - prev_offset;
      result.emplace_back(prev_contig, 0, prev_len - 1);
    }
    prev_offset = c.first;
    prev_contig = c.second;
  }

  // Final region
  uint32_t last_len = metadata_.total_contig_length - prev_offset;
  result.emplace_back(prev_contig, 0, last_len - 1);

  return result;
}

std::list<Region> TileDBVCFDataset::all_contigs_list() const {
  std::list<Region> result;

  if (metadata_.contig_offsets.empty())
    return result;

  // Sort by offset
  std::map<uint32_t, std::string> sorted_contigs;
  for (const auto& c : metadata_.contig_offsets)
    sorted_contigs[c.second] = c.first;

  // Push one region per contig, sorted on global position.
  uint32_t prev_offset = std::numeric_limits<uint32_t>::max();
  std::string prev_contig;
  for (const auto& c : sorted_contigs) {
    if (prev_offset != std::numeric_limits<uint32_t>::max()) {
      uint32_t prev_len = c.first - prev_offset;
      result.emplace_back(prev_contig, 0, prev_len - 1);
    }
    prev_offset = c.first;
    prev_contig = c.second;
  }

  // Final region
  uint32_t last_len = metadata_.total_contig_length - prev_offset;
  result.emplace_back(prev_contig, 0, last_len - 1);

  return result;
}

std::pair<uint32_t, uint32_t> TileDBVCFDataset::contig_from_column(
    uint32_t col) const {
  bool found = false;
  uint32_t contig_offset = 0, contig_length = 0;
  for (const auto& it : metadata_.contig_offsets) {
    uint32_t offset = it.second;
    if (col >= offset) {
      uint32_t length = metadata_.contig_lengths.at(it.first);
      if (col < offset + length) {
        contig_offset = offset;
        contig_length = length;
        found = true;
        break;
      }
    }
  }

  if (!found)
    throw std::runtime_error(
        "Error finding contig containing column " + std::to_string(col));

  return {contig_offset, contig_length};
}

TileDBVCFDataset::Metadata TileDBVCFDataset::read_metadata(
    const Context& ctx, const std::string& root_uri) {
  std::unique_ptr<Array> data_array;
  try {
    // First let's try to open the metadata using proper cloud detection
    data_array.reset(
        new Array(ctx, data_array_uri(root_uri, true), TILEDB_READ));
  } catch (const tiledb::TileDBError& ex) {
    try {
      // Fall back to use s3 style paths, this handle datasets that are
      // registered on the cloud but not with the proper naming scheme. Allows
      // tiledb://namespace/s3://bucket/tiledbvcf_array style access
      data_array.reset(
          new Array(ctx, data_array_uri(root_uri, false), TILEDB_READ));
    } catch (const tiledb::TileDBError& ex) {
      throw std::runtime_error(
          "Cannot open TileDB-VCF dataset; dataset '" + root_uri +
          "' or its metadata does not exist. TileDB error message: " +
          std::string(ex.what()));
    }
  }

  Metadata metadata;

  /** Helper function to get a scalar metadata value. */
  const auto get_md_value = [&data_array](
                                const std::string& name,
                                tiledb_datatype_t expected_dtype,
                                void* dest) {
    const void* ptr = nullptr;
    tiledb_datatype_t dtype;
    uint32_t value_num = 0;
    data_array->get_metadata(name, &dtype, &value_num, &ptr);
    if (dtype != expected_dtype || ptr == nullptr)
      throw std::runtime_error(
          "Error loading metadata; '" + name + "' field has invalid value.");
    std::memcpy(dest, ptr, tiledb_datatype_size(dtype) * value_num);
  };

  /** Helper function to read a CSV string metadata value. */
  const auto get_csv_md_value = [&data_array](
                                    const std::string& name,
                                    std::vector<std::string>* result) {
    const void* ptr = nullptr;
    tiledb_datatype_t dtype;
    uint32_t value_num = 0;
    data_array->get_metadata(name, &dtype, &value_num, &ptr);
    if (ptr != nullptr) {
      if (dtype != TILEDB_CHAR)
        throw std::runtime_error(
            "Error loading metadata; '" + name + "' field has invalid value.");
      std::string b64_str(static_cast<const char*>(ptr), value_num);
      *result = utils::split(base64_decode(b64_str), ',');
    }
  };

  /** Helper function to read a CSV list of pairs metadata value. */
  const auto get_csv_pairs_md_value =
      [&data_array](
          const std::string& name, std::map<std::string, uint32_t>* result) {
        const void* ptr = nullptr;
        tiledb_datatype_t dtype;
        uint32_t value_num = 0;
        data_array->get_metadata(name, &dtype, &value_num, &ptr);
        if (ptr != nullptr) {
          if (dtype != TILEDB_CHAR)
            throw std::runtime_error(
                "Error loading metadata; '" + name +
                "' field has invalid value.");
          std::string b64_str(static_cast<const char*>(ptr), value_num);
          auto pairs = utils::split(base64_decode(b64_str), ',');
          for (const auto& p : pairs) {
            auto pair = utils::split(p, '\t');
            (*result)[pair[0]] = (uint32_t)std::stoul(pair[1]);
          }
        }
      };

  get_md_value("version", TILEDB_UINT32, &metadata.version);
  get_md_value("tile_capacity", TILEDB_UINT64, &metadata.tile_capacity);
  get_md_value("row_tile_extent", TILEDB_UINT32, &metadata.row_tile_extent);
  get_md_value("anchor_gap", TILEDB_UINT32, &metadata.anchor_gap);
  get_md_value("free_sample_id", TILEDB_UINT32, &metadata.free_sample_id);
  get_md_value(
      "total_contig_length", TILEDB_UINT32, &metadata.total_contig_length);

  get_csv_md_value("extra_attributes", &metadata.extra_attributes);
  get_csv_md_value("all_samples", &metadata.all_samples);

  get_csv_pairs_md_value("sample_ids", &metadata.sample_ids);
  get_csv_pairs_md_value("contig_offsets", &metadata.contig_offsets);
  get_csv_pairs_md_value("contig_lengths", &metadata.contig_lengths);

  // Derive the sample id -> name map.
  metadata.sample_names.resize(metadata.sample_ids.size());
  for (const auto& pair : metadata.sample_ids)
    metadata.sample_names[pair.second] = pair.first;

  return metadata;
}

void TileDBVCFDataset::write_metadata(
    const Context& ctx, const std::string& root_uri, const Metadata& metadata) {
  Array data_array(ctx, data_array_uri(root_uri), TILEDB_WRITE);

  /**
   * Helper function to CSV-join a list of values and store the base64 encoded
   * result as an array metadata item.
   */
  const auto put_csv_metadata = [&data_array](
                                    const std::string& name,
                                    const std::vector<std::string>& values) {
    std::string val_str;
    for (unsigned i = 0; i < values.size(); i++) {
      val_str += values[i];
      if (i < values.size() - 1)
        val_str.push_back(',');
    }
    std::string b64_str = base64_encode(val_str.c_str(), val_str.size());
    data_array.put_metadata(name, TILEDB_CHAR, b64_str.size(), b64_str.data());
  };

  /**
   * Helper function to CSV-join a list of pairs of values and store the
   * base64 encoded result as an array metadata item.
   */
  const auto put_csv_pairs_metadata =
      [&data_array](
          const std::string& name,
          const std::map<std::string, uint32_t>& values) {
        std::string val_str;
        for (const auto& s : values) {
          val_str += s.first;
          val_str.push_back('\t');
          val_str += std::to_string(s.second);
          val_str.push_back(',');
        }
        if (!val_str.empty())
          val_str.pop_back();
        std::string b64_str = base64_encode(val_str.c_str(), val_str.size());
        data_array.put_metadata(
            name, TILEDB_CHAR, b64_str.size(), b64_str.data());
      };

  // Scalar values
  data_array.put_metadata("version", TILEDB_UINT32, 1, &metadata.version);
  data_array.put_metadata(
      "tile_capacity", TILEDB_UINT64, 1, &metadata.tile_capacity);
  data_array.put_metadata(
      "row_tile_extent", TILEDB_UINT32, 1, &metadata.row_tile_extent);
  data_array.put_metadata("anchor_gap", TILEDB_UINT32, 1, &metadata.anchor_gap);
  data_array.put_metadata(
      "free_sample_id", TILEDB_UINT32, 1, &metadata.free_sample_id);
  data_array.put_metadata(
      "total_contig_length", TILEDB_UINT32, 1, &metadata.total_contig_length);

  // Base64 encoded CSV strings
  put_csv_metadata("extra_attributes", metadata.extra_attributes);
  put_csv_metadata("all_samples", metadata.all_samples);

  // Base64 encoded TSV+CSV strings
  put_csv_pairs_metadata("sample_ids", metadata.sample_ids);
  put_csv_pairs_metadata("contig_offsets", metadata.contig_offsets);
  put_csv_pairs_metadata("contig_lengths", metadata.contig_lengths);
}

void TileDBVCFDataset::write_vcf_headers(
    const Context& ctx,
    const std::string& root_uri,
    const std::map<uint32_t, std::string>& vcf_headers) {
  if (vcf_headers.empty())
    throw std::runtime_error("Error writing VCF headers; empty headers list.");

  std::string array_uri = vcf_headers_uri(root_uri);
  Array array(ctx, array_uri, TILEDB_WRITE);
  Query query(ctx, array);

  // Build data vector and subarray
  std::vector<std::string> headers;
  std::vector<uint32_t> subarray = {
      std::numeric_limits<uint32_t>::max(),
      std::numeric_limits<uint32_t>::min()};
  for (const auto& pair : vcf_headers) {
    subarray[0] = std::min(subarray[0], pair.first);
    subarray[1] = std::max(subarray[1], pair.first);
    headers.push_back(pair.second);
  }

  auto offsets_and_data = ungroup_var_buffer(headers);
  query.set_layout(TILEDB_ROW_MAJOR)
      .set_subarray(subarray)
      .set_buffer("header", offsets_and_data);
  auto st = query.submit();
  if (st != Query::Status::COMPLETE)
    throw std::runtime_error(
        "Error writing VCF header data; unexpected TileDB query status.");
}

void TileDBVCFDataset::register_samples_helper(
    const std::vector<SafeBCFHdr>& headers,
    Metadata* metadata,
    std::set<std::string>* sample_set,
    std::map<uint32_t, std::string>* sample_headers) {
  for (size_t i = 0; i < headers.size(); i++) {
    const auto& hdr = headers[i];
    auto hdr_samples = VCFUtils::hdr_get_samples(hdr.get());
    if (hdr_samples.size() > 1)
      throw std::invalid_argument(
          "Error registering samples; a file has more than 1 sample. "
          "Ingestion "
          "from cVCF is not supported.");

    const auto& s = hdr_samples[0];
    if (sample_set->count(s))
      throw std::invalid_argument(
          "Error registering samples; sample " + s + " already exists.");
    sample_set->insert(s);

    (*sample_headers)[metadata->free_sample_id] =
        VCFUtils::hdr_to_string(hdr.get());
    metadata->all_samples.push_back(s);
    metadata->sample_ids[s] = metadata->free_sample_id++;
    if (metadata->contig_offsets.empty()) {
      metadata->contig_offsets = VCFUtils::hdr_get_contig_offsets(
          hdr.get(), &metadata->contig_lengths);
      metadata->total_contig_length = 0;
      for (const auto& it : metadata->contig_lengths)
        metadata->total_contig_length += it.second;
    }
  }
}

std::pair<std::string, std::string> TileDBVCFDataset::split_info_fmt_attr_name(
    const std::string& attr_name) {
  auto underscore = attr_name.find('_');
  if (underscore == std::string::npos || underscore == 0)
    throw std::runtime_error(
        "Error splitting info/fmt attribute name '" + attr_name +
        "'; invalid format.");

  std::string kind = attr_name.substr(0, underscore);
  if (kind != "info" && kind != "fmt")
    throw std::runtime_error(
        "Error splitting info/fmt attribute name '" + attr_name +
        "'; invalid format.");

  std::string name = attr_name.substr(kind.size() + 1);
  if (name.empty())
    throw std::runtime_error(
        "Error splitting info/fmt attribute name '" + attr_name +
        "'; invalid format.");

  return {kind, name};
}

std::set<std::string> TileDBVCFDataset::builtin_attributes_v3() {
  return {
      AttrNames::V3::real_start_pos,
      AttrNames::V3::end_pos,
      AttrNames::V3::qual,
      AttrNames::V3::alleles,
      AttrNames::V3::id,
      AttrNames::V3::filter_ids,
      AttrNames::V3::info,
      AttrNames::V3::fmt};
}

std::set<std::string> TileDBVCFDataset::builtin_attributes_v2() {
  return {
      AttrNames::V2::pos,
      AttrNames::V2::real_end,
      AttrNames::V2::qual,
      AttrNames::V2::alleles,
      AttrNames::V2::id,
      AttrNames::V2::filter_ids,
      AttrNames::V2::info,
      AttrNames::V2::fmt};
}

bool TileDBVCFDataset::attribute_is_fixed_len(const std::string& attr) {
  return attr == DimensionNames::V3::sample ||
         attr == DimensionNames::V3::start_pos ||
         attr == DimensionNames::V2::sample ||
         attr == DimensionNames::V2::end_pos ||
         attr == AttrNames::V3::real_start_pos ||
         attr == AttrNames::V3::end_pos || attr == AttrNames::V3::qual ||
         attr == AttrNames::V2::pos || attr == AttrNames::V2::real_end ||
         attr == AttrNames::V2::qual;
}

std::set<std::string> TileDBVCFDataset::all_attributes() const {
  if (!open_)
    throw std::invalid_argument(
        "Cannot get attributes from dataset; dataset is not open.");

  std::set<std::string> result;
  if (metadata_.version == Version::V2) {
    result = builtin_attributes_v2();
  } else {
    assert(metadata_.version == Version::V3);
    result = builtin_attributes_v3();
  }

  for (const auto& s : metadata_.extra_attributes)
    result.insert(s);
  return result;
}

int TileDBVCFDataset::info_field_type(const std::string& name) const {
  auto it = info_field_types_.find(name);
  if (it == info_field_types_.end())
    throw std::invalid_argument("Error getting INFO type for '" + name + "'");
  return it->second;
}

int TileDBVCFDataset::fmt_field_type(const std::string& name) const {
  auto it = fmt_field_types_.find(name);
  if (it == fmt_field_types_.end())
    throw std::invalid_argument("Error getting FMT type for '" + name + "'");
  return it->second;
}

int32_t TileDBVCFDataset::queryable_attribute_count() const {
  return this->vcf_attributes_.size();
}

const char* TileDBVCFDataset::queryable_attribute_name(
    const int32_t index) const {
  return this->vcf_attributes_[index].data();
}

const char* TileDBVCFDataset::sample_name(const int32_t index) const {
  return this->sample_names_[index].data();
}

std::string TileDBVCFDataset::data_array_uri(
    const std::string& root_uri, bool check_for_cloud) {
  char delimiter = '/';
  // Check if we want to use the cloud array naming format which does not
  // support slashes This will be replaced in the future with more proper
  // group support in the cloud
  if (check_for_cloud && cloud_dataset(root_uri))
    delimiter = '-';

  return utils::uri_join(root_uri, "data", delimiter);
}

std::string TileDBVCFDataset::vcf_headers_uri(
    const std::string& root_uri, bool check_for_cloud) {
  char delimiter = '/';
  // Check if we want to use the cloud array naming format which does not
  // support slashes This will be replaced in the future with more proper
  // group support in the cloud
  if (check_for_cloud && cloud_dataset(root_uri))
    delimiter = '-';

  auto grp = utils::uri_join(root_uri, "metadata", delimiter);
  return utils::uri_join(grp, "vcf_headers", delimiter);
}

bool TileDBVCFDataset::cloud_dataset(std::string root_uri) {
  return utils::starts_with(root_uri, "tiledb://");
}

std::map<std::string, int> TileDBVCFDataset::info_field_types() {
  return info_field_types_;
}

std::map<std::string, int> TileDBVCFDataset::fmt_field_types() {
  return fmt_field_types_;
}

}  // namespace vcf
}  // namespace tiledb
