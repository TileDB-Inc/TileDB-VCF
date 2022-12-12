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

#include <algorithm>
#include <future>
#include <map>
#include <string>
#include <vector>

#include "base64/base64.h"
#include "dataset/tiledbvcfdataset.h"
#include "stats/allele_count.h"
#include "stats/variant_stats.h"
#include "utils/logger_public.h"
#include "utils/sample_utils.h"
#include "utils/unique_rwlock.h"
#include "utils/utils.h"
#include "vcf/vcf_utils.h"

namespace tiledb {
namespace vcf {

using dimNamesV4 = TileDBVCFDataset::DimensionNames::V4;
const std::string dimNamesV4::sample = "sample";
const std::string dimNamesV4::contig = "contig";
const std::string dimNamesV4::start_pos = "start_pos";

using dimNamesV3 = TileDBVCFDataset::DimensionNames::V3;
const std::string dimNamesV3::sample = "sample";
const std::string dimNamesV3::start_pos = "start_pos";

using dimNamesV2 = TileDBVCFDataset::DimensionNames::V2;
const std::string dimNamesV2::sample = "sample";
const std::string dimNamesV2::end_pos = "end_pos";

using attrNamesV4 = TileDBVCFDataset::AttrNames::V4;
const std::string attrNamesV4::real_start_pos = "real_start_pos";
const std::string attrNamesV4::end_pos = "end_pos";
const std::string attrNamesV4::qual = "qual";
const std::string attrNamesV4::alleles = "alleles";
const std::string attrNamesV4::id = "id";
const std::string attrNamesV4::filter_ids = "filter_ids";
const std::string attrNamesV4::info = "info";
const std::string attrNamesV4::fmt = "fmt";

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

TileDBVCFDataset::TileDBVCFDataset(std::shared_ptr<Context> ctx)
    : open_(false)
    , data_array_fragment_info_loaded_(false)
    , ctx_(ctx)
    , tiledb_stats_enabled_(true)
    , tiledb_stats_enabled_vcf_header_(true)
    , sample_names_loaded_(false)
    , info_fmt_field_types_loaded_(false)
    , info_iaf_field_type_added_(false)
    , queryable_attribute_loaded_(false)
    , materialized_attribute_loaded_(false) {
  utils::init_htslib();
}

TileDBVCFDataset::TileDBVCFDataset(const tiledb::Config& config)
    : TileDBVCFDataset(std::make_shared<Context>(config)) {
}

TileDBVCFDataset::~TileDBVCFDataset() {
  // block until it's safe to delete the arrays
  lock_and_join_data_array();
  lock_and_join_vcf_header_array();

  data_array_fragment_info_ = nullptr;
  data_array_ = nullptr;
  vcf_header_array_ = nullptr;
}

void TileDBVCFDataset::lock_and_join_data_array() {
  // Grabbing a lock makes sure we don't delete or close the array in the middle
  // of any usage (i.e. preloading)
  utils::UniqueWriteLock data_array_lck_(
      const_cast<utils::RWLock*>(&data_array_lock_));

  // Block if the non empty domain threads have not completed.
  if (data_array_preload_non_empty_domain_thread_.valid()) {
    TRY_CATCH_THROW(data_array_preload_non_empty_domain_thread_.get());
  }
  if (data_array_preload_fragment_info_thread_.valid()) {
    TRY_CATCH_THROW(data_array_preload_fragment_info_thread_.get());
  }
}

void TileDBVCFDataset::lock_and_join_vcf_header_array() {
  // Grabbing a lock makes sure we don't delete or close the array in the middle
  // of any usage (i.e. preloading)
  utils::UniqueWriteLock vcf_header_array_lck_(
      const_cast<utils::RWLock*>(&vcf_header_array_lock_));

  // Block if the non empty domain threads have not completed.
  if (vcf_header_array_preload_non_empty_domain_thread_.valid()) {
    TRY_CATCH_THROW(vcf_header_array_preload_non_empty_domain_thread_.get());
  }
}

std::vector<std::string> TileDBVCFDataset::get_vcf_attributes(std::string uri) {
  LOG_INFO("Reading attributes from: {}", uri);
  std::vector<std::string> attributes;

  // Read VCF header into string for parsing
  bcf_hdr_t* hdr = VCFUtils::hdr_read_header(uri);
  std::string hdrstr = VCFUtils::hdr_to_string(hdr);
  const char* p = hdrstr.c_str();

  // Parse header and find FORMAT/INFO records
  bcf_hrec_t* hrec;
  int len;
  while (NULL != (hrec = bcf_hdr_parse_line(hdr, p, &len))) {
    int i = bcf_hrec_find_key(hrec, "ID");
    if (i >= 0) {
      if (strcmp("FORMAT", hrec->key) == 0) {
        LOG_INFO("Adding attribute: fmt_{}", hrec->vals[i]);
        attributes.emplace_back(fmt::format("fmt_{}", hrec->vals[i]));
      } else if (strcmp("INFO", hrec->key) == 0) {
        LOG_INFO("Adding attribute: info_{}", hrec->vals[i]);
        attributes.emplace_back(fmt::format("info_{}", hrec->vals[i]));
      }
    }

    bcf_hrec_destroy(hrec);
    p += len;
  }

  return attributes;
}

void TileDBVCFDataset::create(const CreationParams& params) {
  LOG_TRACE("Create dataset: {}", params.uri);

  // Add VFS plugin to htslib, so we can read VCF attributes through VFS
  // (for example, when reading a VCF file from s3 with an ARN)
  utils::init_htslib();

  // Set htslib global config and context based on user passed TileDB config
  // options
  utils::set_htslib_tiledb_context(params.tiledb_config);

  Config cfg;
  utils::set_tiledb_config(params.tiledb_config, &cfg);
  Context ctx(cfg);

  check_attribute_names(params.extra_attributes);

  // Create root group, which creates the root directory
  create_group(ctx, params.uri);

  Metadata metadata;
  metadata.tile_capacity = params.tile_capacity;
  metadata.anchor_gap = params.anchor_gap;
  metadata.extra_attributes = params.extra_attributes;
  metadata.free_sample_id = 0;

  // Materialize fmt_GT if variant stats is enabled for AF filtering
  if (params.enable_variant_stats) {
    bool found_gt = false;
    for (auto& attr : metadata.extra_attributes) {
      found_gt |= attr.compare("fmt_GT");
    }
    if (!found_gt) {
      metadata.extra_attributes.push_back("fmt_GT");
    }
  }

  // Materialize all attributes in the provided VCF file
  if (!params.vcf_uri.empty()) {
    metadata.extra_attributes =
        get_vcf_attributes(SampleUtils::build_vfs_plugin_uri(params.vcf_uri));
    check_attribute_names(metadata.extra_attributes);
  }

  // Create arrays and subgroups and add them to the root group
  create_empty_metadata(ctx, params.uri, metadata, params.checksum);
  create_empty_data_array(
      ctx,
      params.uri,
      metadata,
      params.checksum,
      params.allow_duplicates,
      params.compress_sample_dim);

  if (params.enable_allele_count) {
    AlleleCount::create(ctx, params.uri, params.checksum);
  }
  if (params.enable_variant_stats) {
    VariantStats::create(ctx, params.uri, params.checksum);
  }

  write_metadata_v4(ctx, params.uri, metadata);

  // Log the group structure
  tiledb::Group group(ctx, params.uri, TILEDB_READ);
  LOG_DEBUG("TileDB Groups: \n{}", group.dump(true));

  for (uint64_t i = 0; i < group.member_count(); i++) {
    auto member = group.member(i);
    LOG_DEBUG(
        "Group member name='{}' uri='{}'", member.name().value(), member.uri());
  }
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
  create_group(ctx, metadata_group_uri(root_uri));
  create_sample_header_array(ctx, root_uri, checksum);

  // Group assests use full paths for tiledb cloud, relative paths otherwise
  bool relative = !cloud_dataset(root_uri);

  // Add arrays to the root group
  // We create the metadata group to maintain the existing directory structure,
  // but add the vcf_header array to the root group to simplify array opening.
  Group root_group(ctx, root_uri, TILEDB_WRITE);
  auto array_uri = vcf_headers_uri(root_uri, relative);
  LOG_DEBUG(
      "Adding array name='{}' uri='{}' to group uri='{}'",
      VCF_HEADER_ARRAY,
      array_uri,
      root_uri);
  root_group.add_member(array_uri, relative, VCF_HEADER_ARRAY);

  // Add the metadata group to the root group
  auto group_uri = metadata_group_uri(root_uri, relative);
  LOG_DEBUG(
      "Adding group name='{}' uri='{}' to group uri='{}'",
      METADATA_GROUP,
      group_uri,
      root_uri);
  root_group.add_member(group_uri, relative, METADATA_GROUP);
}

void TileDBVCFDataset::create_empty_data_array(
    const Context& ctx,
    const std::string& root_uri,
    const Metadata& metadata,
    const tiledb_filter_type_t& checksum,
    const bool allow_duplicates,
    const bool compress_sample_dim) {
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_capacity(metadata.tile_capacity);
  schema.set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
  schema.set_allows_dups(allow_duplicates);

  // Create filter lists
  FilterList contig_coord_filters(ctx);
  FilterList pos_coord_filters(ctx);
  FilterList sample_coord_filters(ctx);
  FilterList str_attr_filters(ctx);
  FilterList int_attr_filters(ctx);
  FilterList float_attr_filters(ctx);
  FilterList byte_attr_filters(ctx);

  Filter compression(ctx, TILEDB_FILTER_ZSTD);
  compression.set_option(TILEDB_COMPRESSION_LEVEL, 4);

  contig_coord_filters.add_filter({ctx, TILEDB_FILTER_RLE});
  pos_coord_filters.add_filter({ctx, TILEDB_FILTER_DOUBLE_DELTA})
      .add_filter(compression);
  sample_coord_filters.add_filter({ctx, TILEDB_FILTER_DICTIONARY});
  if (compress_sample_dim) {
    sample_coord_filters.add_filter(compression);
  }

  str_attr_filters.add_filter(compression);
  int_attr_filters.add_filter({ctx, TILEDB_FILTER_BYTESHUFFLE})
      .add_filter(compression);
  float_attr_filters.add_filter(compression);
  byte_attr_filters.add_filter(compression);

  auto offsets_filters = pos_coord_filters;

  if (checksum != TILEDB_FILTER_NONE) {
    Filter checksum_filter(ctx, checksum);

    // contig_coord_filters.add_filter(checksum_filter);
    pos_coord_filters.add_filter(checksum_filter);
    // sample_coord_filters.add_filter(checksum_filter);
    str_attr_filters.add_filter(checksum_filter);
    int_attr_filters.add_filter(checksum_filter);
    float_attr_filters.add_filter(checksum_filter);
    byte_attr_filters.add_filter(checksum_filter);
  }
  schema.set_offsets_filter_list(offsets_filters);

  // Create domain and add dimensions
  Domain domain(ctx);
  {
    auto contig = Dimension::create(
        ctx, DimensionNames::V4::contig, TILEDB_STRING_ASCII, nullptr, nullptr);
    contig.set_filter_list(contig_coord_filters);  // d0

    const uint32_t start_pos_min = 0;
    const uint32_t start_pos_max = std::numeric_limits<uint32_t>::max() - 1;
    const uint32_t start_pos_extent = start_pos_max - start_pos_min + 1;
    auto start_pos = Dimension::create<uint32_t>(
        ctx,
        DimensionNames::V4::start_pos,
        {{start_pos_min, start_pos_max}},
        start_pos_extent);
    start_pos.set_filter_list(pos_coord_filters);  // d1

    auto sample = Dimension::create(
        ctx, DimensionNames::V4::sample, TILEDB_STRING_ASCII, nullptr, nullptr);
    sample.set_filter_list(sample_coord_filters);  // d2

    domain.add_dimensions(contig, start_pos, sample);
  }
  schema.set_domain(domain);

  auto real_start_pos = Attribute::create<uint32_t>(
      ctx, AttrNames::V4::real_start_pos, int_attr_filters);  // a0
  auto end_pos = Attribute::create<uint32_t>(
      ctx, AttrNames::V4::end_pos, int_attr_filters);  // a1
  auto qual = Attribute::create<float>(
      ctx, AttrNames::V4::qual, float_attr_filters);  // a2
  auto alleles = Attribute::create<std::vector<char>>(
      ctx, AttrNames::V4::alleles, byte_attr_filters);  // a3
  auto id = Attribute::create<std::vector<char>>(
      ctx, AttrNames::V4::id, byte_attr_filters);  // a4
  auto filters_ids = Attribute::create<std::vector<int32_t>>(
      ctx, AttrNames::V4::filter_ids, int_attr_filters);  // a5
  auto info = Attribute::create<std::vector<uint8_t>>(
      ctx, AttrNames::V4::info, byte_attr_filters);  // a6
  auto fmt = Attribute::create<std::vector<uint8_t>>(
      ctx, AttrNames::V4::fmt, byte_attr_filters);  // a7
  schema.add_attributes(
      real_start_pos, end_pos, qual, alleles, id, filters_ids, info, fmt);

  // Remaining INFO/FMT fields extracted as separate attributes:
  std::set<std::string> used;
  for (auto& attr : metadata.extra_attributes) {
    if (used.count(attr))
      continue;
    used.insert(attr);
    schema.add_attribute(
        Attribute::create<std::vector<uint8_t>>(ctx, attr, byte_attr_filters));
  }

  Array::create(data_array_uri(root_uri), schema);

  // Add the array to the root group
  // Group assests use full paths for tiledb cloud, relative paths otherwise
  bool relative = !cloud_dataset(root_uri);
  auto array_uri = data_array_uri(root_uri, relative);
  LOG_DEBUG(
      "Adding array name='{}' uri='{}' to group uri='{}'",
      DATA_ARRAY,
      array_uri,
      root_uri);
  Group root_group(ctx, root_uri, TILEDB_WRITE);
  root_group.add_member(array_uri, relative, DATA_ARRAY);
}

void TileDBVCFDataset::create_sample_header_array(
    const Context& ctx,
    const std::string& root_uri,
    const tiledb_filter_type_t& checksum) {
  ArraySchema schema(ctx, TILEDB_SPARSE);

  // Set domain
  Domain domain(ctx);
  auto sample =
      Dimension::create(ctx, "sample", TILEDB_STRING_ASCII, nullptr, nullptr);
  sample.set_filter_list(default_attribute_filter_list(ctx));
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
    const std::string& uri,
    const std::vector<std::string>& tiledb_config,
    const bool prefetch_data_array_fragment_info) {
  utils::UniqueWriteLock lck_(const_cast<utils::RWLock*>(&dataset_lock_));
  if (open_)
    throw std::runtime_error(
        "Cannot open TileDB-VCF dataset; dataset already open.");
  root_uri_ = uri;

  // Always enable stats when opening the dataset. If the user doesn't want
  // stats it'll be disabled in the reader or writer
  tiledb::Stats::enable();

  // Disable estimated partition result size
  cfg_.set("sm.skip_est_size_partitioning", "true");

  utils::set_tiledb_config(tiledb_config, &cfg_);

  if (prefetch_data_array_fragment_info)
    preload_data_array_fragment_info();

  data_array_ = open_data_array(TILEDB_READ);
  vcf_header_array_ = open_vcf_array(TILEDB_READ);
  read_metadata();

  // We support V2, V3 and V4 (current) formats.
  if (metadata_.version != Version::V2 && metadata_.version != Version::V3 &&
      metadata_.version != Version::V4)
    throw std::runtime_error(
        "Cannot open TileDB-VCF dataset; dataset is version " +
        std::to_string(metadata_.version) +
        " but only versions 2, 3 and 4 are supported.");

  // Handle time traveling by looking for 'vcf.start_timestamp' and
  // 'vcf.end_timestamp' in tiledb_config. If either timestamp is provided,
  // reopen the arrays.
  bool reopen = false;

  try {
    auto start_timestamp = std::stoull(cfg_.get("vcf.start_timestamp"));
    data_array_->set_open_timestamp_start(start_timestamp);
    vcf_header_array_->set_open_timestamp_start(start_timestamp);
    LOG_INFO("Using vcf.start_timestamp from config: {}", start_timestamp);
    reopen = true;
  } catch (const tiledb::TileDBError& ex) {
    LOG_TRACE("'vcf.start_timestamp' not specified in config, using default");
  } catch (...) {
    LOG_WARN(
        "Invalid vcf.start_timestamp '{}', using default",
        cfg_.get("vcf.start_timestamp"));
  }

  try {
    auto end_timestamp = std::stoull(cfg_.get("vcf.end_timestamp"));
    data_array_->set_open_timestamp_end(end_timestamp);
    vcf_header_array_->set_open_timestamp_end(end_timestamp);
    LOG_INFO("Using vcf.end_timestamp from config: {}", end_timestamp);
    reopen = true;
  } catch (const tiledb::TileDBError& ex) {
    LOG_TRACE("'vcf.end_timestamp' not specified in config, using default");
  } catch (...) {
    LOG_WARN(
        "Invalid vcf.end_timestamp '{}', using default",
        cfg_.get("vcf.end_timestamp"));
  }

  if (reopen) {
    data_array_->reopen();
    vcf_header_array_->reopen();
  }

  auto start_ms = data_array_->open_timestamp_start();
  auto end_ms = data_array_->open_timestamp_end();

  if (reopen) {
    LOG_INFO(
        "start_timestamp = {:013d} = {}", start_ms, asc_timestamp(start_ms));
    LOG_INFO("end_timestamp   = {:013d} = {}", end_ms, asc_timestamp(end_ms));
  } else {
    LOG_TRACE(
        "start_timestamp = {:013d} = {}", start_ms, asc_timestamp(start_ms));
    LOG_TRACE("end_timestamp   = {:013d} = {}", end_ms, asc_timestamp(end_ms));
  }

  open_ = true;

  // Preloading runs on a background stl thread
  // We can ignore the returns because the core TileDB library will cache the
  // non-empty-domain after its loaded the first time
  // TODO: revisit preloading after sc-17632 is addressed
  // preload_data_array_non_empty_domain();
  // preload_vcf_header_array_non_empty_domain();

  // only v2/v3 arrays preload sample list
  if (metadata_.version == Version::V2 || metadata_.version == Version::V3) {
    for (const auto& s : metadata_.sample_names_) {
      std::vector<char> sample(s.begin(), s.end());
      sample.emplace_back('\0');
      sample_names_.push_back(sample);
    }
  }
}

void TileDBVCFDataset::build_materialized_attributes() const {
  utils::UniqueWriteLock lck_(
      const_cast<utils::RWLock*>(&materialized_attribute_lock_));
  // After the write lock is acquired check to make sure a different thread
  // hasn't loaded the attribute list
  if (!materialized_vcf_attributes_.empty())
    return;
  // Build queryable attribute and sample lists
  std::set<std::string> unique_queryable_attributes{
      "sample_name",
      "query_bed_start",
      "query_bed_end",
      "contig",
      "query_bed_line"};
  for (auto s : this->all_attributes()) {
    if (s == "end_pos" || s == "real_end")
      s = "pos_end";
    else if (s == "start_pos" || s == "real_start_pos" || s == "pos")
      s = "pos_start";
    else if (s == "filter_ids")
      s = "filters";

    unique_queryable_attributes.emplace(s);
  }

  // Set materialized attributes
  for (const auto& key : unique_queryable_attributes) {
    std::vector<char> name(key.begin(), key.end());
    name.emplace_back('\0');
    materialized_vcf_attributes_.push_back(name);
  }
  materialized_attribute_loaded_ = true;
}

void TileDBVCFDataset::build_queryable_attributes() const {
  utils::UniqueWriteLock lck_(
      const_cast<utils::RWLock*>(&queryable_attribute_lock_));
  // After the write lock is acquired check to make sure a different thread
  // hasn't loaded the attribute list
  if (!vcf_attributes_.empty())
    return;
  // Build queryable attribute and sample lists
  std::set<std::string> unique_queryable_attributes{
      "sample_name",
      "query_bed_start",
      "query_bed_end",
      "contig",
      "query_bed_line"};
  for (auto s : this->all_attributes()) {
    if (s == "end_pos" || s == "real_end")
      s = "pos_end";
    else if (s == "start_pos" || s == "real_start_pos" || s == "pos")
      s = "pos_start";
    else if (s == "filter_ids")
      s = "filters";

    unique_queryable_attributes.emplace(s);
  }

  for (const auto& info : info_field_types()) {
    unique_queryable_attributes.emplace("info_" + info.first);
  }

  for (const auto& fmt : fmt_field_types()) {
    unique_queryable_attributes.emplace("fmt_" + fmt.first);
  }

  for (const auto& key : unique_queryable_attributes) {
    std::vector<char> name(key.begin(), key.end());
    name.emplace_back('\0');
    vcf_attributes_.push_back(name);
  }
  queryable_attribute_loaded_ = true;
}

void TileDBVCFDataset::load_field_type_maps() const {
  utils::UniqueWriteLock lck_(const_cast<utils::RWLock*>(&type_field_rw_lock_));
  // After we acquire the write lock we need to check if another thread has
  // loaded the field types
  if (info_fmt_field_types_loaded_)
    return;

  // Empty array (no samples registered); do nothing.
  if (metadata_.sample_ids.empty())
    return;

  std::string first_sample_name = metadata_.sample_names_.at(0);
  uint32_t first_sample_id = metadata_.sample_ids.at(first_sample_name);
  SampleAndId first_sample = {first_sample_name, first_sample_id};

  std::unordered_map<uint32_t, SafeBCFHdr> hdrs;
  hdrs = fetch_vcf_headers({first_sample});
  if (hdrs.size() != 1)
    throw std::runtime_error(
        "Error loading dataset field types; no headers fetched.");

  const auto& hdr_ptr = hdrs.at(first_sample_id);
  bcf_hdr_t* hdr = hdr_ptr.get();
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

  info_fmt_field_types_loaded_ = true;
}

void TileDBVCFDataset::load_field_type_maps_v4(
    const bcf_hdr_t* hdr, bool add_iaf) const {
  utils::UniqueWriteLock lck_(const_cast<utils::RWLock*>(&type_field_rw_lock_));
  // After we acquire the write lock we need to check if another thread has
  // loaded the field types
  if (info_fmt_field_types_loaded_ && (!add_iaf || info_iaf_field_type_added_))
    return;

  std::unordered_map<uint32_t, SafeBCFHdr> hdrs;
  if (hdr == nullptr) {
    hdrs = fetch_vcf_headers_v4({}, nullptr, false, true);

    if (hdrs.empty())
      return;
    else if (hdrs.size() != 1)
      throw std::runtime_error(
          "Error loading dataset field types; no headers fetched.");

    hdr = hdrs.begin()->second.get();
  }

  if (add_iaf) {
    if (bcf_hdr_append(
            // TODO: do something better than promoting this pointer type;
            // perhaps the header should be duplicated and later modified
            const_cast<bcf_hdr_t*>(hdr),
            "##INFO=<ID=TILEDB_IAF,Number=R,Type=Float,Description=\"Internal "
            "Allele Frequency, computed over dataset by TileDB\">") < 0) {
      throw std::runtime_error(
          "Error appending to header for internal allele frequency.");
    }
    info_iaf_field_type_added_ = true;
    if (bcf_hdr_sync(const_cast<bcf_hdr_t*>(hdr)) < 0) {
      throw std::runtime_error("Error syncing header after adding IAF record.");
    }
  }
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

  info_fmt_field_types_loaded_ = true;
}

void TileDBVCFDataset::register_samples(const RegistrationParams& params) {
  if (!open_)
    throw std::invalid_argument(
        "Cannot register samples; dataset is not open.");

  assert(metadata_.version == Version::V2 || metadata_.version == Version::V3);

  if (!tiledb_stats_enabled_vcf_header_)
    tiledb::Stats::disable();

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
  std::future<std::vector<SafeBCFHdr>> future_headers;
  TRY_CATCH_THROW(
      future_headers = std::async(
          std::launch::async,
          SampleUtils::get_sample_headers,
          vfs,
          batches[0],
          params.scratch_space));
  for (unsigned i = 1; i < batches.size(); i++) {
    std::vector<SafeBCFHdr> headers;
    TRY_CATCH_THROW(headers = future_headers.get());
    // Start the next batch downloading
    TRY_CATCH_THROW(
        future_headers = std::async(
            std::launch::async,
            SampleUtils::get_sample_headers,
            vfs,
            batches[i],
            params.scratch_space));
    // Register the batch
    register_samples_helper(headers, &metadata_, &sample_set, &sample_headers);
    write_vcf_headers_v2(ctx, root_uri_, sample_headers);
    sample_headers.clear();
  }

  // Register the final batch.
  TRY_CATCH_THROW(register_samples_helper(
      future_headers.get(), &metadata_, &sample_set, &sample_headers));
  write_vcf_headers_v2(ctx, root_uri_, sample_headers);

  if (tiledb_stats_enabled_)
    tiledb::Stats::enable();

  // Write the updated metadata.
  write_metadata(ctx, root_uri_, metadata_);
}

void TileDBVCFDataset::print_samples_list() {
  if (!open_)
    throw std::invalid_argument("Cannot list samples; dataset is not open.");
  for (const auto& s : sample_names())
    std::cout << s.data() << "\n";
}

void TileDBVCFDataset::print_dataset_stats() {
  if (!open_)
    throw std::invalid_argument(
        "Cannot print dataset stats; dataset is not open.");

  utils::enable_pretty_print_numbers(std::cout);
  std::cout << "Statistics for dataset '" << root_uri_ << "':" << std::endl;
  std::cout << "- Version: " << metadata_.version << std::endl;
  if (metadata_.version == Version::V2 || metadata_.version == Version::V3)
    std::cout << "- Row tile extent: " << metadata_.ingestion_sample_batch_size
              << std::endl;
  std::cout << "- Tile capacity: " << metadata_.tile_capacity << std::endl;
  std::cout << "- Anchor gap: " << metadata_.anchor_gap << std::endl;
  std::cout << "- Number of samples: " << sample_names().size() << std::endl;

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

std::string TileDBVCFDataset::root_uri() const {
  return root_uri_;
}

std::unique_ptr<tiledb::Array> TileDBVCFDataset::open_array(
    tiledb_query_type_t query_type,
    const std::string& member_name,
    const std::string& uri_path,
    const std::string& legacy_uri) {
  std::unique_ptr<Array> array = nullptr;
  std::string array_uri = uri_path;

  LOG_DEBUG(
      "Lookup member name='{}' in group uri='{}'", member_name, root_uri_);

  try {
    tiledb::Group group(*ctx_, root_uri_, TILEDB_READ);
    auto member = group.member(member_name);
    array_uri = member.uri();
    LOG_DEBUG("Found group member name='{}' uri='{}'", member_name, array_uri);

    // If the group member uri is a cloud uri and the root uri is not,
    // use the uri_path instead of the group member uri
    if (cloud_dataset(array_uri) && !cloud_dataset(root_uri_)) {
      array_uri = uri_path;
      LOG_DEBUG(
          "Override group uri '{}'. Open '{}' using uri path '{}'",
          member.uri(),
          member_name,
          array_uri);
    } else {
      LOG_DEBUG("Open '{}' using group uri '{}'", member_name, array_uri);
    }
  } catch (const tiledb::TileDBError& ex) {
    LOG_DEBUG("Open '{}' using uri path '{}'", member_name, array_uri);
  }

  try {
    // Open the array with the uri detemined above
    array = std::unique_ptr<Array>(new Array(*ctx_, array_uri, query_type));
  } catch (const tiledb::TileDBError& ex) {
    try {
      // Last chance, try the legacy uri to support legacy cloud arrays
      array = std::unique_ptr<Array>(new Array(*ctx_, legacy_uri, query_type));
    } catch (const tiledb::TileDBError& ex) {
      throw std::runtime_error(
          "Cannot open TileDB-VCF dataset; dataset '" + root_uri_ +
          "' or its metadata does not exist. TileDB error message: " +
          std::string(ex.what()));
    }
  }

  return array;
}

std::unique_ptr<tiledb::Array> TileDBVCFDataset::open_vcf_array(
    tiledb_query_type_t query_type) {
  return open_array(
      query_type,
      VCF_HEADER_ARRAY,
      vcf_headers_uri(root_uri_),
      vcf_headers_uri(root_uri_, false, true));
}

std::shared_ptr<tiledb::Array> TileDBVCFDataset::open_data_array(
    tiledb_query_type_t query_type) {
  return open_array(
      query_type,
      DATA_ARRAY,
      data_array_uri(root_uri_),
      data_array_uri(root_uri_, false, true));
}

std::unordered_map<uint32_t, SafeBCFHdr> TileDBVCFDataset::fetch_vcf_headers_v4(
    const std::vector<SampleAndId>& samples,
    std::unordered_map<std::string, size_t>* lookup_map,
    const bool all_samples,
    const bool first_sample) const {
  // Grab a read lock of concurrency so we don't destroy the vcf_header_array
  // during fetching
  utils::UniqueReadLock lck_(
      const_cast<utils::RWLock*>(&vcf_header_array_lock_));

  if (!tiledb_stats_enabled_vcf_header_)
    tiledb::Stats::disable();

  if (all_samples && first_sample)
    throw std::runtime_error(
        "Cannot set all_samples and first_sample in same fetch vcf headers "
        "request");

  if (first_sample && !samples.empty())
    throw std::runtime_error(
        "Cannot set first_sample and samples list in same fetch vcf headers "
        "request");

  std::unordered_map<uint32_t, SafeBCFHdr> result;

  if (vcf_header_array_ == nullptr)
    throw std::runtime_error(
        "Cannot fetch TileDB-VCF vcf headers; Array object unexpectedly null");

  Query query(*ctx_, *vcf_header_array_);

  if (!samples.empty()) {
    // If all samples but we have a sample list we know its sorted and can use
    // the min/max
    if (all_samples) {
      query.add_range(
          0, samples[0].sample_name, samples[samples.size() - 1].sample_name);
    } else {
      for (const auto& sample : samples) {
        query.add_range(0, sample.sample_name, sample.sample_name);
      }
    }
  } else if (all_samples) {
    // When no samples are passed grab the first one
    auto non_empty_domain = vcf_header_array_->non_empty_domain_var(0);
    if (!non_empty_domain.first.empty())
      query.add_range(0, non_empty_domain.first, non_empty_domain.second);
  } else if (first_sample) {
    // When no samples are passed grab the first one
    auto non_empty_domain = vcf_header_array_->non_empty_domain_var(0);
    if (!non_empty_domain.first.empty())
      query.add_range(0, non_empty_domain.first, non_empty_domain.first);
  }
  query.set_layout(TILEDB_ROW_MAJOR);

  uint64_t header_offset_element = 0;
  uint64_t header_data_element = 0;
  uint64_t sample_offset_element = 0;
  uint64_t sample_data_element = 0;
#if TILEDB_VERSION_MAJOR == 2 and TILEDB_VERSION_MINOR < 2
  std::pair<uint64_t, uint64_t> header_est_size =
      query.est_result_size_var("header");
  header_offset_element =
      std::max(header_est_size.first, static_cast<uint64_t>(1));
  header_data_element =
      std::max(header_est_size.second / sizeof(char), static_cast<uint64_t>(1));

  // Sample estimate
  std::pair<uint64_t, uint64_t> sample_est_size =
      query.est_result_size_var("sample");
  sample_offset_element =
      std::max(sample_est_size.first, static_cast<uint64_t>(1));
  sample_data_element =
      std::max(sample_est_size.second / sizeof(char), static_cast<uint64_t>(1));
#else
  std::array<uint64_t, 2> header_est_size = query.est_result_size_var("header");
  header_offset_element =
      std::max(header_est_size[0] / sizeof(uint64_t), static_cast<uint64_t>(1));
  header_data_element =
      std::max(header_est_size[1] / sizeof(char), static_cast<uint64_t>(1));

  // Sample estimate
  std::array<uint64_t, 2> sample_est_size = query.est_result_size_var("sample");
  sample_offset_element =
      std::max(sample_est_size[0] / sizeof(uint64_t), static_cast<uint64_t>(1));
  sample_data_element =
      std::max(sample_est_size[1] / sizeof(char), static_cast<uint64_t>(1));
#endif

  std::vector<uint64_t> offsets(header_offset_element);
  std::vector<char> data(header_data_element);
  std::vector<uint64_t> sample_offsets(sample_offset_element);
  std::vector<char> sample_data(sample_data_element);

  Query::Status status;
  uint32_t sample_idx = 0;

  do {
    // Always reset buffer to avoid issue with core library and REST not using
    // original buffer sizes
    query.set_buffer("header", offsets, data);
    query.set_buffer("sample", sample_offsets, sample_data);

    status = query.submit();

    auto result_el = query.result_buffer_elements();
    uint64_t num_offsets = result_el["header"].first;
    uint64_t num_chars = result_el["header"].second;
    uint64_t num_samples_offsets = result_el["sample"].first;
    uint64_t num_samples_chars = result_el["sample"].second;

    bool has_results = num_chars != 0;

    if (status == Query::Status::INCOMPLETE && !has_results) {
      // If there are no results, double the size of the buffer and then
      // resubmit the query.

      if (num_chars == 0)
        data.resize(data.size() * 2);

      if (num_offsets == 0)
        offsets.resize(offsets.size() * 2);

      if (num_samples_chars == 0)
        sample_data.resize(sample_data.size() * 2);

      if (num_samples_offsets == 0)
        sample_offsets.resize(sample_offsets.size() * 2);

    } else if (has_results) {
      // Parse the samples.

      for (size_t offset_idx = 0; offset_idx < num_offsets; ++offset_idx) {
        // Get sample
        char* sample_beg = sample_data.data() + sample_offsets[offset_idx];
        uint64_t sample_end = offset_idx == num_samples_offsets - 1 ?
                                  num_samples_chars :
                                  sample_offsets[offset_idx + 1];
        uint64_t sample_size = sample_end - sample_offsets[offset_idx];
        std::string sample(sample_beg, sample_size);

        char* beg_hdr = data.data() + offsets[offset_idx];
        uint64_t end =
            offset_idx == num_offsets - 1 ? num_chars : offsets[offset_idx + 1];
        uint64_t start = offsets[offset_idx];
        uint64_t hdr_size = end - start;

        std::string hdr_str(beg_hdr, hdr_size);

        bcf_hdr_t* hdr = bcf_hdr_init("r");
        if (!hdr)
          throw std::runtime_error(
              "Error fetching VCF header data; error allocating VCF header.");

        if (0 != bcf_hdr_parse(hdr, const_cast<char*>(hdr_str.c_str()))) {
          throw std::runtime_error(
              "TileDBVCFDataset::fetch_vcf_headers_v4: Error parsing the BCF "
              "header for sample " +
              sample + ".");
        }

        if (!sample.empty()) {
          if (0 != bcf_hdr_add_sample(hdr, sample.c_str())) {
            throw std::runtime_error(
                "TileDBVCFDataset::fetch_vcf_headers_v4: Error adding sample "
                "to "
                "BCF header for sample " +
                sample + ".");
          }
        }

        if (bcf_hdr_sync(hdr) < 0)
          throw std::runtime_error(
              "Error in bcftools: failed to update VCF header.");

        result.emplace(
            std::make_pair(sample_idx, SafeBCFHdr(hdr, bcf_hdr_destroy)));
        if (lookup_map != nullptr)
          (*lookup_map)[sample] = sample_idx;

        ++sample_idx;
      }
    }
  } while (status == Query::Status::INCOMPLETE);

  if (tiledb_stats_enabled_)
    tiledb::Stats::enable();
  return result;
}  // namespace vcf

std::unordered_map<uint32_t, SafeBCFHdr> TileDBVCFDataset::fetch_vcf_headers(
    const std::vector<SampleAndId>& samples) const {
  // Grab a read lock of concurrency so we don't destroy the vcf_header_array
  // during fetching
  utils::UniqueReadLock lck_(
      const_cast<utils::RWLock*>(&vcf_header_array_lock_));

  if (!tiledb_stats_enabled_vcf_header_)
    tiledb::Stats::disable();

  std::unordered_map<uint32_t, SafeBCFHdr> result;

  if (vcf_header_array_ == nullptr)
    throw std::runtime_error(
        "Cannot fetch TileDB-VCF vcf headers; Array object unexpectedly null");

  Query query(*ctx_, *vcf_header_array_);

  std::unordered_map<uint32_t, std::string> sample_name_mapping;
  for (const auto& sample : samples) {
    sample_name_mapping[sample.sample_id] = sample.sample_name;
    query.add_range(0, sample.sample_id, sample.sample_id);
  }
  query.set_layout(TILEDB_ROW_MAJOR);

  uint64_t header_offset_element = 0;
  uint64_t header_data_element = 0;
#if TILEDB_VERSION_MAJOR == 2 and TILEDB_VERSION_MINOR < 2
  std::pair<uint64_t, uint64_t> header_est_size =
      query.est_result_size_var("header");
  header_offset_element =
      std::max(header_est_size.first, static_cast<uint64_t>(1));
  header_data_element =
      std::max(header_est_size.second / sizeof(char), static_cast<uint64_t>(1));
#else
  std::array<uint64_t, 2> header_est_size = query.est_result_size_var("header");
  header_offset_element =
      std::max(header_est_size[0] / sizeof(uint64_t), static_cast<uint64_t>(1));
  header_data_element =
      std::max(header_est_size[1] / sizeof(char), static_cast<uint64_t>(1));
#endif

  std::vector<uint64_t> offsets(header_offset_element);
  std::vector<char> data(header_data_element);

  uint64_t sample_est_size = query.est_result_size("sample");
  std::vector<uint32_t> sample_data(sample_est_size);

  Query::Status status;

  do {
    // Always reset buffer to avoid issue with core library and REST not using
    // original buffer sizes
    query.set_buffer("header", offsets, data);
    query.set_buffer("sample", sample_data);

    status = query.submit();

    auto result_el = query.result_buffer_elements();
    uint64_t num_offsets = result_el["header"].first;
    uint64_t num_chars = result_el["header"].second;
    uint64_t num_samples = result_el["samples"].second;

    bool has_results = num_chars != 0;

    if (status == Query::Status::INCOMPLETE && !has_results) {
      // If there are no results, double the size of the buffer and then
      // resubmit the query.

      if (num_chars == 0)
        data.resize(data.size() * 2);

      if (num_offsets == 0)
        offsets.resize(offsets.size() * 2);

      if (num_samples == 0)
        sample_data.resize(sample_data.size() * 2);

    } else if (has_results) {
      // Parse the samples.

      for (size_t offset_idx = 0; offset_idx < num_offsets; ++offset_idx) {
        // Get sample
        uint32_t sample = sample_data[offset_idx];

        char* beg_hdr = data.data() + offsets[offset_idx];
        uint64_t hdr_size =
            offset_idx == num_offsets - 1 ? num_chars : offsets[offset_idx + 1];
        hdr_size = hdr_size - offsets[offset_idx];
        char* hdr_str =
            static_cast<char*>(std::malloc(sizeof(char) * (hdr_size + 1)));
        memcpy(hdr_str, beg_hdr, hdr_size);
        hdr_str[hdr_size] = '\0';

        bcf_hdr_t* hdr = bcf_hdr_init("r");
        if (!hdr)
          throw std::runtime_error(
              "Error fetching VCF header data; error allocating VCF header.");

        if (0 != bcf_hdr_parse(hdr, hdr_str)) {
          throw std::runtime_error(
              "TileDBVCFDataset::fetch_vcf_headers: Error parsing the BCF "
              "header for sample " +
              std::to_string(sample) + ".");
        }
        std::free(hdr_str);

        if (0 !=
            bcf_hdr_add_sample(hdr, metadata_.sample_names_[sample].c_str())) {
          throw std::runtime_error("Error adding the sample.");
        }

        if (bcf_hdr_sync(hdr) < 0)
          throw std::runtime_error(
              "Error in bcftools: failed to update VCF header.");

        result.emplace(
            std::make_pair(sample, SafeBCFHdr(hdr, bcf_hdr_destroy)));
      }
    }
  } while (status == Query::Status::INCOMPLETE);

  if (tiledb_stats_enabled_)
    tiledb::Stats::enable();
  return result;
}  // namespace vcf

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

std::vector<Region> TileDBVCFDataset::all_contigs_v4() const {
  std::unordered_map<uint32_t, SafeBCFHdr> hdrs =
      fetch_vcf_headers_v4({}, nullptr, false, true);

  if (hdrs.empty())
    return {};
  else if (hdrs.size() != 1)
    throw std::runtime_error(
        "Error loading dataset field types; no headers fetched.");

  const auto& hdr = hdrs.begin()->second;

  return VCFUtils::hdr_get_contigs_regions(hdr.get());
}

std::list<Region> TileDBVCFDataset::all_contigs_list_v4() const {
  std::list<Region> result;
  for (const auto& region : all_contigs_v4())
    result.emplace_back(region);

  return result;
}

std::tuple<uint32_t, uint32_t, std::string>
TileDBVCFDataset::contig_from_column(uint32_t col) const {
  if (metadata_.version != TileDBVCFDataset::Version::V2 &&
      metadata_.version != TileDBVCFDataset::Version::V3)
    throw std::runtime_error(
        "Error trying to lookup contig from metadata for 3D array");

  bool found = false;
  uint32_t contig_offset = 0, contig_length = 0;
  std::string contig;
  for (const auto& it : metadata_.contig_offsets) {
    uint32_t offset = it.second;
    if (col >= offset) {
      uint32_t length = metadata_.contig_lengths.at(it.first);
      if (col < offset + length) {
        contig_offset = offset;
        contig_length = length;
        contig = it.first;
        found = true;
        break;
      }
    }
  }

  if (!found)
    throw std::runtime_error(
        "Error finding contig containing column " + std::to_string(col));

  return std::tuple<uint32_t, uint32_t, std::string>{
      contig_offset, contig_length, contig};
}

void TileDBVCFDataset::read_metadata_v4() {
  if (data_array_ == nullptr)
    throw std::runtime_error(
        "Cannot open TileDB-VCF dataset; dataset '" + root_uri_ +
        "' or its metadata does not exist.");

  // Grab a read lock of concurrency so we don't destroy the data_array during
  // fetching
  utils::UniqueReadLock lck_(const_cast<utils::RWLock*>(&data_array_lock_));

  Metadata metadata;
  std::shared_ptr<tiledb::Array>& data_array = data_array_;

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

  get_md_value("version", TILEDB_UINT32, &metadata.version);
  get_md_value("tile_capacity", TILEDB_UINT64, &metadata.tile_capacity);
  get_md_value("anchor_gap", TILEDB_UINT32, &metadata.anchor_gap);

  get_csv_md_value("extra_attributes", &metadata.extra_attributes);

  // Set ingestion_sample_batch_size default to 10
  metadata.ingestion_sample_batch_size = 10;

  metadata_ = metadata;
}

void TileDBVCFDataset::read_metadata() {
  if (data_array_ == nullptr)
    throw std::runtime_error(
        "Cannot open TileDB-VCF dataset; dataset '" + root_uri_ +
        "' or its metadata does not exist.");

  // Grab a read lock of concurrency so we don't destroy the data_array during
  // fetching
  utils::UniqueReadLock lck_(const_cast<utils::RWLock*>(&data_array_lock_));

  Metadata metadata;
  std::shared_ptr<tiledb::Array>& data_array = data_array_;

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
  // If v4 stop and fetch for v4
  if (metadata.version == TileDBVCFDataset::Version::V4)
    return read_metadata_v4();

  get_md_value("tile_capacity", TILEDB_UINT64, &metadata.tile_capacity);
  get_md_value(
      "row_tile_extent", TILEDB_UINT32, &metadata.ingestion_sample_batch_size);
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
  metadata.sample_names_.resize(metadata.sample_ids.size());
  for (const auto& pair : metadata.sample_ids)
    metadata.sample_names_[pair.second] = pair.first;

  metadata_ = metadata;
  sample_names_loaded_ = true;
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
    std::stringstream val_strstr;
    for (unsigned i = 0; i < values.size(); i++) {
      val_strstr << values[i];
      if (i < values.size() - 1)
        val_strstr << ',';
    }
    std::string val_str = val_strstr.str();
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
        std::stringstream val_strstr;
        for (const auto& s : values) {
          val_strstr << s.first;
          val_strstr << '\t';
          val_strstr << std::to_string(s.second);
          val_strstr << ',';
        }
        std::string val_str = val_strstr.str();
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
      "row_tile_extent",
      TILEDB_UINT32,
      1,
      &metadata.ingestion_sample_batch_size);
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

void TileDBVCFDataset::write_metadata_v4(
    const Context& ctx, const std::string& root_uri, const Metadata& metadata) {
  Array data_array(ctx, data_array_uri(root_uri), TILEDB_WRITE);

  /**
   * Helper function to CSV-join a list of values and store the base64 encoded
   * result as an array metadata item.
   */
  const auto put_csv_metadata = [&data_array](
                                    const std::string& name,
                                    const std::vector<std::string>& values) {
    std::stringstream val_strstr;
    for (unsigned i = 0; i < values.size(); i++) {
      val_strstr << values[i];
      if (i < values.size() - 1)
        val_strstr << ',';
    }
    std::string val_str = val_strstr.str();
    std::string b64_str = base64_encode(val_str.c_str(), val_str.size());
    data_array.put_metadata(name, TILEDB_CHAR, b64_str.size(), b64_str.data());
  };

  // Scalar values
  data_array.put_metadata("version", TILEDB_UINT32, 1, &metadata.version);
  data_array.put_metadata(
      "tile_capacity", TILEDB_UINT64, 1, &metadata.tile_capacity);
  data_array.put_metadata("anchor_gap", TILEDB_UINT32, 1, &metadata.anchor_gap);

  // Base64 encoded CSV strings
  put_csv_metadata("extra_attributes", metadata.extra_attributes);
}

void TileDBVCFDataset::write_vcf_headers_v4(
    const Context& ctx,
    const std::map<std::string, std::string>& vcf_headers) const {
  if (!tiledb_stats_enabled_vcf_header_)
    tiledb::Stats::disable();

  write_vcf_headers_v4(ctx, root_uri_, vcf_headers);

  if (tiledb_stats_enabled_)
    tiledb::Stats::enable();
}

void TileDBVCFDataset::write_vcf_headers_v4(
    const Context& ctx,
    const std::string& root_uri,
    const std::map<std::string, std::string>& vcf_headers) {
  if (vcf_headers.empty())
    throw std::runtime_error("Error writing VCF headers; empty headers list.");

  std::string array_uri = vcf_headers_uri(root_uri);
  Array array(ctx, array_uri, TILEDB_WRITE);
  Query query(ctx, array);

  // Build data vector and subarray
  std::vector<std::string> headers, samples;
  for (const auto& pair : vcf_headers) {
    samples.push_back(pair.first);
    headers.push_back(pair.second);
  }

  query.set_layout(TILEDB_UNORDERED);
  auto offsets_and_data = ungroup_var_buffer(headers);
  query.set_buffer("header", offsets_and_data);

  auto sample_offsets_and_data = ungroup_var_buffer(samples);

  query.set_buffer("sample", sample_offsets_and_data);
  auto st = query.submit();
  if (st != Query::Status::COMPLETE)
    throw std::runtime_error(
        "Error writing VCF header data; unexpected TileDB query status.");
}

void TileDBVCFDataset::write_vcf_headers_v2(
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
    sample_set->emplace(s);

    (*sample_headers)[metadata->free_sample_id] =
        VCFUtils::hdr_to_string(hdr.get());
    metadata->all_samples.emplace_back(s);
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

std::set<std::string> TileDBVCFDataset::builtin_attributes_v4() {
  return {
      AttrNames::V4::real_start_pos,
      AttrNames::V4::end_pos,
      AttrNames::V4::qual,
      AttrNames::V4::alleles,
      AttrNames::V4::id,
      AttrNames::V4::filter_ids,
      AttrNames::V4::info,
      AttrNames::V4::fmt};
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
  if (metadata_.version == Version::V2 || metadata_.version == Version::V3) {
    if (attr == DimensionNames::V3::sample ||
        attr == DimensionNames::V2::sample) {
      return true;
    }
  }

  return attr == DimensionNames::V4::start_pos ||
         attr == DimensionNames::V3::start_pos ||
         attr == DimensionNames::V2::end_pos ||
         attr == AttrNames::V4::real_start_pos ||
         attr == AttrNames::V4::end_pos || attr == AttrNames::V4::qual ||
         attr == AttrNames::V3::real_start_pos ||
         attr == AttrNames::V3::end_pos || attr == AttrNames::V3::qual ||
         attr == AttrNames::V2::pos || attr == AttrNames::V2::real_end ||
         attr == AttrNames::V2::qual;
}

bool TileDBVCFDataset::attribute_is_nullable(const std::string& attr) {
  // Nulls are not included in schema yet
  return false;
}

bool TileDBVCFDataset::attribute_is_list(const std::string& attr) {
  // Lists are not included in schema yet
  return false;
}

std::set<std::string> TileDBVCFDataset::all_attributes() const {
  if (!open_)
    throw std::invalid_argument(
        "Cannot get attributes from dataset; dataset is not open.");

  std::set<std::string> result;
  if (metadata_.version == Version::V2) {
    result = builtin_attributes_v2();
  } else if (metadata_.version == Version::V3) {
    result = builtin_attributes_v3();
  } else {
    assert(metadata_.version == Version::V4);
    result = builtin_attributes_v4();
  }

  for (const auto& s : metadata_.extra_attributes)
    result.insert(s);
  return result;
}

int TileDBVCFDataset::info_field_type(
    const std::string& name, const bcf_hdr_t* hdr, bool add_iaf) const {
  utils::UniqueReadLock lck_(const_cast<utils::RWLock*>(&type_field_rw_lock_));
  if (!info_fmt_field_types_loaded_ ||
      (add_iaf && !info_iaf_field_type_added_)) {
    lck_.unlock();
    if (metadata_.version == Version::V2 || metadata_.version == Version::V3)
      load_field_type_maps();
    else {
      assert(metadata_.version == Version::V4);
      load_field_type_maps_v4(hdr, add_iaf);
    }
    lck_.lock();
  }

  auto it = info_field_types_.find(name);
  if (it == info_field_types_.end())
    throw std::invalid_argument("Error getting INFO type for '" + name + "'");
  return it->second;
}

int TileDBVCFDataset::fmt_field_type(
    const std::string& name, const bcf_hdr_t* hdr) const {
  utils::UniqueReadLock lck_(const_cast<utils::RWLock*>(&type_field_rw_lock_));
  if (!info_fmt_field_types_loaded_) {
    lck_.unlock();
    if (metadata_.version == Version::V2 || metadata_.version == Version::V3)
      load_field_type_maps();
    else {
      assert(metadata_.version == Version::V4);
      load_field_type_maps_v4(hdr);
    }
    lck_.lock();
  }

  auto it = fmt_field_types_.find(name);
  if (it == fmt_field_types_.end())
    throw std::invalid_argument("Error getting FMT type for '" + name + "'");
  return it->second;
}

int32_t TileDBVCFDataset::queryable_attribute_count() const {
  utils::UniqueReadLock lck_(
      const_cast<utils::RWLock*>(&queryable_attribute_lock_));
  if (!queryable_attribute_loaded_) {
    lck_.unlock();
    build_queryable_attributes();
    lck_.lock();
  }

  return this->vcf_attributes_.size();
}

const char* TileDBVCFDataset::queryable_attribute_name(
    const int32_t index) const {
  utils::UniqueReadLock lck_(
      const_cast<utils::RWLock*>(&queryable_attribute_lock_));
  if (!queryable_attribute_loaded_) {
    lck_.unlock();
    build_queryable_attributes();
    lck_.lock();
  }

  return this->vcf_attributes_[index].data();
}

int32_t TileDBVCFDataset::materialized_attribute_count() const {
  utils::UniqueReadLock lck_(
      const_cast<utils::RWLock*>(&materialized_attribute_lock_));
  if (!materialized_attribute_loaded_) {
    lck_.unlock();
    build_materialized_attributes();
    lck_.lock();
  }

  return this->materialized_vcf_attributes_.size();
}

const char* TileDBVCFDataset::materialized_attribute_name(
    const int32_t index) const {
  utils::UniqueReadLock lck_(
      const_cast<utils::RWLock*>(&materialized_attribute_lock_));
  if (!materialized_attribute_loaded_) {
    lck_.unlock();
    build_materialized_attributes();
    lck_.lock();
  }

  return this->materialized_vcf_attributes_[index].data();
}

bool TileDBVCFDataset::is_attribute_materialized(
    const std::string& attr) const {
  utils::UniqueReadLock lck_(
      const_cast<utils::RWLock*>(&materialized_attribute_lock_));
  if (!materialized_attribute_loaded_) {
    lck_.unlock();
    build_materialized_attributes();
    lck_.lock();
  }

  for (const auto& materialized_attr_name :
       this->materialized_vcf_attributes_) {
    if (std::string(materialized_attr_name.data()) == attr)
      return true;
  }

  return false;
}

const char* TileDBVCFDataset::sample_name(const int32_t index) const {
  if (!sample_names_loaded_ && metadata_.version == Version::V4)
    load_sample_names_v4();
  return this->sample_names_[index].data();
}

std::string TileDBVCFDataset::data_array_uri(
    const std::string& root_uri, bool relative, bool legacy) {
  char delimiter = legacy ? '-' : '/';
  auto root = relative ? "" : root_uri;
  return utils::uri_join(root, DATA_ARRAY, delimiter);
}

std::string TileDBVCFDataset::metadata_group_uri(
    const std::string& root_uri, bool relative, bool legacy) {
  char delimiter = legacy ? '-' : '/';
  auto root = relative ? "" : root_uri;
  return utils::uri_join(root, METADATA_GROUP, delimiter);
}

std::string TileDBVCFDataset::vcf_headers_uri(
    const std::string& root_uri, bool relative, bool legacy) {
  char delimiter = legacy ? '-' : '/';
  auto root = relative ? "" : root_uri;
  auto group = metadata_group_uri(root, relative, legacy);
  return utils::uri_join(group, VCF_HEADER_ARRAY, delimiter);
}

bool TileDBVCFDataset::cloud_dataset(const std::string& root_uri) {
  return utils::starts_with(root_uri, "tiledb://");
}

bool TileDBVCFDataset::tiledb_cloud_dataset() const {
  return cloud_dataset(root_uri_);
}

std::map<std::string, int> TileDBVCFDataset::info_field_types() const {
  utils::UniqueReadLock lck_(const_cast<utils::RWLock*>(&type_field_rw_lock_));
  if (!info_fmt_field_types_loaded_) {
    lck_.unlock();
    if (metadata_.version == Version::V2 || metadata_.version == Version::V3)
      load_field_type_maps();
    else {
      assert(metadata_.version == Version::V4);
      load_field_type_maps_v4(nullptr);
    }
    lck_.lock();
  }
  return info_field_types_;
}

std::map<std::string, int> TileDBVCFDataset::fmt_field_types() const {
  utils::UniqueReadLock lck_(const_cast<utils::RWLock*>(&type_field_rw_lock_));
  if (!info_fmt_field_types_loaded_) {
    lck_.unlock();
    if (metadata_.version == Version::V2 || metadata_.version == Version::V3)
      load_field_type_maps();
    else {
      assert(metadata_.version == Version::V4);
      load_field_type_maps_v4(nullptr);
    }
    lck_.lock();
  }

  return fmt_field_types_;
}

std::vector<std::string> TileDBVCFDataset::get_all_samples_from_vcf_headers()
    const {
  // Grab a read lock of concurrency so we don't destroy the vcf_header_array
  // during fetching
  utils::UniqueReadLock lck_(
      const_cast<utils::RWLock*>(&vcf_header_array_lock_));

  if (!tiledb_stats_enabled_vcf_header_)
    tiledb::Stats::disable();

  std::vector<std::string> result;

  if (vcf_header_array_ == nullptr)
    throw std::runtime_error(
        "Cannot fetch TileDB-VCF samples from vcf header array; Array object "
        "unexpectedly null");

  Query query(*ctx_, *vcf_header_array_);

  auto non_empty_domain = vcf_header_array_->non_empty_domain_var(0);
  if (non_empty_domain.first.empty() && non_empty_domain.second.empty())
    return {};
  query.add_range(0, non_empty_domain.first, non_empty_domain.second);
  query.set_layout(TILEDB_ROW_MAJOR);

  uint64_t sample_offset_element = 0;
  uint64_t sample_data_element = 0;
#if TILEDB_VERSION_MAJOR == 2 and TILEDB_VERSION_MINOR < 2
  // Sample estimate
  std::pair<uint64_t, uint64_t> sample_est_size =
      query.est_result_size_var("sample");
  sample_offset_element =
      std::max(sample_est_size.first, static_cast<uint64_t>(1));
  sample_data_element =
      std::max(sample_est_size.second / sizeof(char), static_cast<uint64_t>(1));
#else
  // Sample estimate
  std::array<uint64_t, 2> sample_est_size = query.est_result_size_var("sample");
  sample_offset_element =
      std::max(sample_est_size[0] / sizeof(uint64_t), static_cast<uint64_t>(1));
  sample_data_element =
      std::max(sample_est_size[1] / sizeof(char), static_cast<uint64_t>(1));
#endif

  std::vector<uint64_t> sample_offsets(sample_offset_element);
  std::vector<char> sample_data(sample_data_element);

  Query::Status status;

  do {
    // Always reset buffer to avoid issue with core library and REST not using
    // original buffer sizes
    query.set_buffer("sample", sample_offsets, sample_data);

    status = query.submit();

    auto result_el = query.result_buffer_elements();
    uint64_t num_offsets = result_el["sample"].first;
    uint64_t num_chars = result_el["sample"].second;

    bool has_results = num_chars != 0;

    if (status == Query::Status::INCOMPLETE && !has_results) {
      // If there are no results, double the size of the buffer and then
      // resubmit the query.

      if (num_chars == 0)
        sample_data.resize(sample_data.size() * 2);

      if (num_offsets == 0)
        sample_offsets.resize(sample_offsets.size() * 2);

    } else if (has_results) {
      // Parse the samples.

      for (size_t offset_idx = 0; offset_idx < num_offsets; ++offset_idx) {
        // Get sample
        char* sample_chr = sample_data.data() + sample_offsets[offset_idx];
        uint64_t end = offset_idx == num_offsets - 1 ?
                           num_chars :
                           sample_offsets[offset_idx + 1];
        uint64_t start = sample_offsets[offset_idx];
        uint64_t size = end - start;

        result.emplace_back(sample_chr, size);
      }
    }
  } while (status == Query::Status::INCOMPLETE);

  if (tiledb_stats_enabled_)
    tiledb::Stats::enable();
  return result;
}

std::shared_ptr<tiledb::Array> TileDBVCFDataset::data_array() const {
  return data_array_;
}

const bool TileDBVCFDataset::tiledb_stats_enabled() const {
  return tiledb_stats_enabled_;
}
void TileDBVCFDataset::set_tiledb_stats_enabled(const bool stats_enabled) {
  tiledb_stats_enabled_ = stats_enabled;
}

/** Should tiledb stats be collected for vcf header arrays */
const bool TileDBVCFDataset::tiledb_stats_enabled_vcf_header() const {
  return tiledb_stats_enabled_vcf_header_;
}
void TileDBVCFDataset::set_tiledb_stats_enabled_vcf_header(
    const bool stats_enabled) {
  tiledb_stats_enabled_vcf_header_ = stats_enabled;
}

void TileDBVCFDataset::consolidate_vcf_header_array_commits(
    const UtilsParams& params) {
  Config cfg;
  utils::set_tiledb_config(params.tiledb_config, &cfg);
  cfg["sm.consolidation.mode"] = "commits";
  tiledb::Array::consolidate(*ctx_, vcf_headers_uri(params.uri), &cfg);
}

void TileDBVCFDataset::consolidate_data_array_commits(
    const UtilsParams& params) {
  Config cfg;
  utils::set_tiledb_config(params.tiledb_config, &cfg);
  cfg["sm.consolidation.mode"] = "commits";
  tiledb::Array::consolidate(*ctx_, data_array_uri(params.uri), &cfg);
}

void TileDBVCFDataset::consolidate_commits(const UtilsParams& params) {
  LOG_DEBUG("Consolidate data array commits.");
  consolidate_data_array_commits(params);
  LOG_DEBUG("Consolidate vcf_header array commits.");
  consolidate_vcf_header_array_commits(params);
  LOG_DEBUG("Consolidate allele_count array commits.");
  AlleleCount::consolidate_commits(ctx_, params.tiledb_config, params.uri);
  LOG_DEBUG("Consolidate variant_stats array commits.");
  VariantStats::consolidate_commits(ctx_, params.tiledb_config, params.uri);
}

void TileDBVCFDataset::consolidate_vcf_header_array_fragment_metadata(
    const UtilsParams& params) {
  Config cfg;
  utils::set_tiledb_config(params.tiledb_config, &cfg);
  cfg["sm.consolidation.mode"] = "fragment_meta";
  tiledb::Array::consolidate(*ctx_, vcf_headers_uri(params.uri), &cfg);
}

void TileDBVCFDataset::consolidate_data_array_fragment_metadata(
    const UtilsParams& params) {
  Config cfg;
  utils::set_tiledb_config(params.tiledb_config, &cfg);
  cfg["sm.consolidation.mode"] = "fragment_meta";
  tiledb::Array::consolidate(*ctx_, data_array_uri(params.uri), &cfg);
}

void TileDBVCFDataset::consolidate_fragment_metadata(
    const UtilsParams& params) {
  LOG_DEBUG("Consolidate data array fragment metadata.");
  consolidate_data_array_fragment_metadata(params);
  LOG_DEBUG("Consolidate vcf_header array fragment metadata.");
  consolidate_vcf_header_array_fragment_metadata(params);
  LOG_DEBUG("Consolidate allele_count array fragment metadata.");
  AlleleCount::consolidate_fragment_metadata(
      ctx_, params.tiledb_config, params.uri);
  LOG_DEBUG("Consolidate variant_stats array fragment metadata.");
  VariantStats::consolidate_fragment_metadata(
      ctx_, params.tiledb_config, params.uri);
}

void TileDBVCFDataset::consolidate_vcf_header_array_fragments(
    const UtilsParams& params) {
  Config cfg;
  utils::set_tiledb_config(params.tiledb_config, &cfg);
  cfg["sm.consolidation.mode"] = "fragments";
  tiledb::Array::consolidate(*ctx_, vcf_headers_uri(params.uri), &cfg);
}

void TileDBVCFDataset::consolidate_data_array_fragments(
    const UtilsParams& params) {
  Config cfg;
  utils::set_tiledb_config(params.tiledb_config, &cfg);
  cfg["sm.consolidation.mode"] = "fragments";
  tiledb::Array::consolidate(*ctx_, data_array_uri(params.uri), &cfg);
}

void TileDBVCFDataset::consolidate_fragments(const UtilsParams& params) {
  consolidate_data_array_fragments(params);
  consolidate_vcf_header_array_fragments(params);
}

void TileDBVCFDataset::vacuum_vcf_header_array_commits(
    const UtilsParams& params) {
  Config cfg;
  utils::set_tiledb_config(params.tiledb_config, &cfg);
  cfg["sm.vacuum.mode"] = "commits";
  tiledb::Array::vacuum(*ctx_, vcf_headers_uri(params.uri), &cfg);
}

void TileDBVCFDataset::vacuum_data_array_commits(const UtilsParams& params) {
  Config cfg;
  utils::set_tiledb_config(params.tiledb_config, &cfg);
  cfg["sm.vacuum.mode"] = "commits";
  tiledb::Array::vacuum(*ctx_, data_array_uri(params.uri), &cfg);
}

void TileDBVCFDataset::vacuum_commits(const UtilsParams& params) {
  LOG_DEBUG("Vacuum data array commits.");
  vacuum_data_array_commits(params);
  LOG_DEBUG("Vacuum vcf_header array commits.");
  vacuum_vcf_header_array_commits(params);
  LOG_DEBUG("Vacuum allele_count array commits.");
  AlleleCount::vacuum_commits(ctx_, params.tiledb_config, params.uri);
  LOG_DEBUG("Vacuum variant_stats array commits.");
  VariantStats::vacuum_commits(ctx_, params.tiledb_config, params.uri);
}

void TileDBVCFDataset::vacuum_vcf_header_array_fragment_metadata(
    const UtilsParams& params) {
  Config cfg;
  utils::set_tiledb_config(params.tiledb_config, &cfg);
  cfg["sm.vacuum.mode"] = "fragment_meta";
  tiledb::Array::vacuum(*ctx_, vcf_headers_uri(params.uri), &cfg);
}

void TileDBVCFDataset::vacuum_data_array_fragment_metadata(
    const UtilsParams& params) {
  Config cfg;
  utils::set_tiledb_config(params.tiledb_config, &cfg);
  cfg["sm.vacuum.mode"] = "fragment_meta";
  tiledb::Array::vacuum(*ctx_, data_array_uri(params.uri), &cfg);
}

void TileDBVCFDataset::vacuum_fragment_metadata(const UtilsParams& params) {
  LOG_DEBUG("Vacuum data array fragment metadata.");
  vacuum_data_array_fragment_metadata(params);
  LOG_DEBUG("Vacuum vcf_header array fragment metadata.");
  vacuum_vcf_header_array_fragment_metadata(params);
  LOG_DEBUG("Vacuum allele_count array fragment metadata.");
  AlleleCount::vacuum_fragment_metadata(ctx_, params.tiledb_config, params.uri);
  LOG_DEBUG("Vacuum variant_stats array fragment metadata.");
  VariantStats::vacuum_fragment_metadata(
      ctx_, params.tiledb_config, params.uri);
}

void TileDBVCFDataset::vacuum_vcf_header_array_fragments(
    const UtilsParams& params) {
  Config cfg;
  utils::set_tiledb_config(params.tiledb_config, &cfg);
  cfg["sm.vacuum.mode"] = "fragments";
  tiledb::Array::vacuum(*ctx_, vcf_headers_uri(params.uri), &cfg);
}

void TileDBVCFDataset::vacuum_data_array_fragments(const UtilsParams& params) {
  Config cfg;
  utils::set_tiledb_config(params.tiledb_config, &cfg);
  cfg["sm.vacuum.mode"] = "fragments";
  tiledb::Array::vacuum(*ctx_, data_array_uri(params.uri), &cfg);
}

void TileDBVCFDataset::vacuum_fragments(const UtilsParams& params) {
  vacuum_data_array_fragments(params);
  vacuum_vcf_header_array_fragments(params);
}

void TileDBVCFDataset::load_sample_names_v4() const {
  utils::UniqueWriteLock lck_(&metadata_.sample_names_rw_lock_);
  // After the lock is acquired we need to make sure a different thread hasn't
  // loaded it
  if (sample_names_loaded_)
    return;

  metadata_.all_samples = get_all_samples_from_vcf_headers();

  // Derive the sample id -> name map.
  metadata_.sample_names_.resize(metadata_.all_samples.size());
  for (size_t i = 0; i < metadata_.all_samples.size(); i++) {
    const std::string& sample_name = metadata_.all_samples[i];
    metadata_.sample_names_[i] = sample_name;
    metadata_.sample_ids[sample_name] = i;

    std::vector<char> sample(sample_name.begin(), sample_name.end());
    sample.emplace_back('\0');
    sample_names_.push_back(sample);
  }

  sample_names_loaded_ = true;
}

std::vector<std::vector<char>> TileDBVCFDataset::sample_names() const {
  utils::UniqueReadLock lck_(&metadata_.sample_names_rw_lock_);
  if (!sample_names_loaded_) {
    // Unlock the read if we are going to load it
    lck_.unlock();
    // Only v4 needs to load sample names
    if (metadata_.version == Version::V4)
      load_sample_names_v4();
    // Reacquire the read lock to return a copy
    lck_.lock();
  }

  return sample_names_;
}

bool TileDBVCFDataset::is_info_field(const std::string& attr) const {
  return attr.substr(0, 5) == "info_";
}

bool TileDBVCFDataset::is_fmt_field(const std::string& attr) const {
  return attr.substr(0, 4) == "fmt_";
}

void TileDBVCFDataset::attribute_datatype_non_fmt_info(
    const std::string& attribute,
    tiledb_datatype_t* datatype,
    bool* var_len,
    bool* nullable,
    bool* list) {
  bool fixed_len = attribute_is_fixed_len(attribute);
  *var_len = !fixed_len;
  *var_len = !fixed_len;

  *nullable = attribute_is_nullable(attribute);
  *list = attribute_is_list(attribute);
  *var_len = !fixed_len;

  auto schema = data_array_->schema();
  if (schema.has_attribute(attribute)) {
    auto attr = schema.attribute(attribute);
    *datatype = attr.type();
  } else {
    auto dimension = schema.domain().dimension(attribute);
    *datatype = dimension.type();
  }
}

void TileDBVCFDataset::preload_data_array_non_empty_domain() {
  if (metadata_.version == Version::V2 || metadata_.version == Version::V3)
    return preload_data_array_non_empty_domain_v2_v3();

  assert(metadata_.version == Version::V4);
  return preload_data_array_non_empty_domain_v4();
}

void TileDBVCFDataset::preload_data_array_non_empty_domain_v2_v3() {
  TRY_CATCH_THROW(
      data_array_preload_non_empty_domain_thread_ = std::async(
          std::launch::async,
          [this]() { data_array_->non_empty_domain<uint32_t>(0); }));
}

void TileDBVCFDataset::preload_data_array_non_empty_domain_v4() {
  TRY_CATCH_THROW(
      data_array_preload_non_empty_domain_thread_ = std::async(
          std::launch::async,
          [this]() { data_array_->non_empty_domain_var(0); }));
}

void TileDBVCFDataset::preload_vcf_header_array_non_empty_domain() {
  if (metadata_.version == Version::V2 || metadata_.version == Version::V3)
    return preload_vcf_header_array_non_empty_domain_v2_v3();

  assert(metadata_.version == Version::V4);
  return preload_vcf_header_array_non_empty_domain_v4();
}

void TileDBVCFDataset::preload_vcf_header_array_non_empty_domain_v2_v3() {
  TRY_CATCH_THROW(
      vcf_header_array_preload_non_empty_domain_thread_ = std::async(
          std::launch::async,
          [this]() { vcf_header_array_->non_empty_domain<uint32_t>(0); }));
}

void TileDBVCFDataset::preload_vcf_header_array_non_empty_domain_v4() {
  TRY_CATCH_THROW(
      vcf_header_array_preload_non_empty_domain_thread_ = std::async(
          std::launch::async,
          [this]() { vcf_header_array_->non_empty_domain_var(0); }));
}

void TileDBVCFDataset::data_array_fragment_info_load() {
  std::unique_lock<std::mutex> lck(data_array_fragment_info_mtx_);
  if (data_array_fragment_info_loaded_)
    return;

  data_array_fragment_info_ =
      std::make_shared<tiledb::FragmentInfo>(*ctx_, data_uri());
  data_array_fragment_info_->load();
  data_array_fragment_info_loaded_ = true;
}

void TileDBVCFDataset::preload_data_array_fragment_info() {
  data_array_fragment_info_load();
  // TODO: revisit preloading in parallel
  /*
  TRY_CATCH_THROW(
      data_array_preload_fragment_info_thread_ = std::async(
          std::launch::async, [this]() { data_array_fragment_info_load(); }));
  */
}

std::shared_ptr<tiledb::FragmentInfo>
TileDBVCFDataset::data_array_fragment_info() {
  std::unique_lock<std::mutex> lck(data_array_fragment_info_mtx_);
  if (!data_array_fragment_info_loaded_) {
    lck.unlock();
    data_array_fragment_info_load();
    lck.lock();
  }

  return data_array_fragment_info_;
}

std::unordered_map<
    std::pair<std::string, std::string>,
    std::vector<std::pair<std::string, std::string>>,
    tiledb::vcf::pair_hash>
TileDBVCFDataset::fragment_sample_contig_list() {
  if (metadata_.version == Version::V2 || metadata_.version == Version::V3)
    throw std::runtime_error(
        "Fragment contig sample listing not supported for v2/v3 datasets");

  assert(metadata_.version == Version::V4);
  return fragment_sample_contig_list_v4();
}

std::unordered_map<
    std::pair<std::string, std::string>,
    std::vector<std::pair<std::string, std::string>>,
    tiledb::vcf::pair_hash>
TileDBVCFDataset::fragment_sample_contig_list_v4() {
  const auto fragment_info = data_array_fragment_info();
  std::unordered_map<
      std::pair<std::string, std::string>,
      std::vector<std::pair<std::string, std::string>>,
      tiledb::vcf::pair_hash>
      results;
  for (uint64_t i = 0; i < fragment_info->fragment_num(); i++) {
    auto fragment_contig_range = fragment_info->non_empty_domain_var(i, 0);
    auto fragment_sample_range = fragment_info->non_empty_domain_var(i, 2);
    auto samples = std::make_pair(
        fragment_sample_range.first, fragment_sample_range.second);
    auto contigs = std::make_pair(
        fragment_contig_range.first, fragment_contig_range.second);
    if (results.find(samples) == results.end()) {
      results.emplace(
          samples, std::vector<std::pair<std::string, std::string>>{contigs});
    } else {
      auto& vec = results.at(samples);
      vec.emplace_back(contigs);
    }
  }

  return results;
}
}  // namespace vcf
}  // namespace tiledb
