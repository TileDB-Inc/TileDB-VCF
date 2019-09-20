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
#include "vcf/vcf.h"

namespace tiledb {
namespace vcf {

const std::string TileDBVCFDataset::AttrNames::pos = "pos";
const std::string TileDBVCFDataset::AttrNames::real_end = "real_end";
const std::string TileDBVCFDataset::AttrNames::qual = "qual";
const std::string TileDBVCFDataset::AttrNames::alleles = "alleles";
const std::string TileDBVCFDataset::AttrNames::id = "id";
const std::string TileDBVCFDataset::AttrNames::filter_ids = "filter_ids";
const std::string TileDBVCFDataset::AttrNames::info = "info";
const std::string TileDBVCFDataset::AttrNames::fmt = "fmt";

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
}

void TileDBVCFDataset::create(const CreationParams& params) {
  Context ctx;
  VFS vfs(ctx);

  check_attribute_names(params.extra_attributes);

  if (vfs.is_dir(params.uri)) {
    // If the directory exists, check if it's a dataset. If so, return with no
    // error (allows for multiple no-op create calls).
    if (vfs.is_file(general_metadata_uri(params.uri)))
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

  create_empty_metadata(ctx, params.uri, metadata);
  create_empty_data_array(ctx, params.uri, metadata);
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
    const Context& ctx, const std::string& root_uri, const Metadata& metadata) {
  create_group(ctx, utils::uri_join(root_uri, "metadata"));
  create_sample_header_array(ctx, root_uri);
  write_metadata(ctx, root_uri, metadata);
}

void TileDBVCFDataset::create_empty_data_array(
    const Context& ctx, const std::string& root_uri, const Metadata& metadata) {
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_capacity(metadata.tile_capacity);
  schema.set_order({{TILEDB_COL_MAJOR, TILEDB_COL_MAJOR}});

  Domain domain(ctx);
  {
    const auto dom_min = 0;
    const auto dom_max = std::numeric_limits<uint32_t>::max() - 1;
    const auto sample_dom_max =
        dom_max - static_cast<uint32_t>(metadata.row_tile_extent);
    auto sample = Dimension::create<uint32_t>(
        ctx, "sample", {{dom_min, sample_dom_max}}, metadata.row_tile_extent);
    auto end_pos = Dimension::create<uint32_t>(
        ctx, "end_pos", {{dom_min, dom_max}}, dom_max - dom_min + 1);
    domain.add_dimensions(sample, end_pos);
  }
  schema.set_domain(domain);
  schema.set_offsets_filter_list(default_offsets_filter_list(ctx));

  // Set coords filters
  FilterList coords_filter_list(ctx);
  coords_filter_list.add_filter({ctx, TILEDB_FILTER_DOUBLE_DELTA})
      .add_filter({ctx, TILEDB_FILTER_ZSTD});
  schema.set_coords_filter_list(coords_filter_list);

  // Create a byteshuffle -> zstd filter list used by a few attributes
  FilterList byteshuffle_zstd_filters(ctx);
  byteshuffle_zstd_filters.add_filter({ctx, TILEDB_FILTER_BYTESHUFFLE})
      .add_filter({ctx, TILEDB_FILTER_ZSTD});

  auto pos = Attribute::create<uint32_t>(
      ctx, AttrNames::pos, byteshuffle_zstd_filters);
  auto real_end = Attribute::create<uint32_t>(
      ctx, AttrNames::real_end, byteshuffle_zstd_filters);
  auto qual = Attribute::create<float>(
      ctx, AttrNames::qual, default_attribute_filter_list(ctx));
  auto alleles = Attribute::create<std::vector<char>>(
      ctx, AttrNames::alleles, default_attribute_filter_list(ctx));
  auto id = Attribute::create<std::vector<char>>(
      ctx, AttrNames::id, default_attribute_filter_list(ctx));
  auto filters_ids = Attribute::create<std::vector<int32_t>>(
      ctx, AttrNames::filter_ids, byteshuffle_zstd_filters);
  auto info = Attribute::create<std::vector<uint8_t>>(
      ctx, AttrNames::info, default_attribute_filter_list(ctx));
  auto fmt = Attribute::create<std::vector<uint8_t>>(
      ctx, AttrNames::fmt, default_attribute_filter_list(ctx));
  schema.add_attributes(
      pos, real_end, qual, alleles, id, filters_ids, info, fmt);

  // Remaining INFO/FMT fields extracted as separate attributes:
  std::set<std::string> used;
  for (auto& attr : metadata.extra_attributes) {
    if (used.count(attr))
      continue;
    used.insert(attr);
    schema.add_attribute(Attribute::create<std::vector<uint8_t>>(
        ctx, attr, default_attribute_filter_list(ctx)));
  }

  Array::create(data_array_uri(root_uri), schema);
}

void TileDBVCFDataset::create_sample_header_array(
    const Context& ctx, const std::string& root_uri) {
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
  schema.set_offsets_filter_list(offsets_filter_list);

  // Add a single 'header' string attribute.
  FilterList attribute_filter_list = default_attribute_filter_list(ctx);
  auto attr_header = Attribute::create<std::vector<char>>(
      ctx, "header", attribute_filter_list);
  schema.add_attributes(attr_header);

  Array::create(vcf_headers_uri(root_uri), schema);
}

void TileDBVCFDataset::open(const std::string& uri) {
  if (open_)
    throw std::runtime_error(
        "Cannot open TileDB-VCF dataset; dataset already open.");
  root_uri_ = uri;

  Context ctx;
  VFS vfs(ctx);
  if (!vfs.is_dir(root_uri_))
    throw std::runtime_error(
        "Cannot open TileDB-VCF dataset; dataset '" + root_uri_ +
        "' does not exist.");

  metadata_ = read_metadata(ctx, root_uri_);
  if (metadata_.version != TILEVCF_ARRAY_VERSION)
    throw std::runtime_error(
        "Cannot open TileDB-VCF dataset; dataset version mismatch.");

  load_field_type_maps();

  open_ = true;
}

void TileDBVCFDataset::load_field_type_maps() {
  // Empty array (no samples registered); do nothing.
  if (metadata_.sample_ids.empty())
    return;

  auto first_sample = metadata_.sample_ids.at(metadata_.sample_names.at(0));
  auto hdrs = fetch_vcf_headers(first_sample, first_sample);
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

  std::vector<SampleAndIndex> samples = SampleUtils::build_samples_uri_list(
      params.sample_uris_file, params.sample_uris);
  if (samples.empty())
    throw std::invalid_argument(
        "Cannot register samples; samples list is empty.");

  // Registration proceeds in batches, double-buffering the downloads.
  Context ctx;
  VFS vfs(ctx);
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
    uint32_t sample_id_min, uint32_t sample_id_max) const {
  Context ctx;
  std::vector<SafeBCFHdr> result;
  std::string array_uri = vcf_headers_uri(root_uri_);
  std::vector<uint32_t> subarray = {sample_id_min, sample_id_max};

  // Set up and submit query
  Array array(ctx, array_uri, TILEDB_READ);
  auto max_el = array.max_buffer_elements(subarray);
  std::vector<uint64_t> offsets(max_el["header"].first);
  std::vector<char> data(max_el["header"].second);
  Query query(ctx, array);
  query.set_layout(TILEDB_ROW_MAJOR)
      .set_subarray(subarray)
      .set_buffer("header", offsets, data);
  query.submit();
  if (query.query_status() != Query::Status::COMPLETE)
    throw std::runtime_error(
        "Error fetching VCF header data; unexpected TileDB query status.");

  // Sanity check result size
  auto result_el = query.result_buffer_elements();
  auto num_offsets = result_el["header"].first;
  auto num_chars = result_el["header"].second;
  if (num_chars == 0 || subarray[1] - subarray[0] + 1 != num_offsets)
    throw std::runtime_error(
        "Error fetching VCF header data; unexpected query result size.");

  // Parse headers from text
  for (uint32_t i = subarray[0]; i <= subarray[1]; i++) {
    // Make a copy into a std::string to ensure null termination.
    uint32_t ibase = i - subarray[0];
    uint64_t offset = offsets[ibase];
    uint64_t next_offset =
        ibase < num_offsets - 1 ? offsets[ibase + 1] : num_chars;
    auto header_len = next_offset - offset;
    std::string header_str(data.data() + offset, header_len);

    bcf_hdr_t* hdr = bcf_hdr_init("r");
    if (!hdr)
      throw std::runtime_error(
          "Error fetching VCF header data; error allocating VCF header.");
    bcf_hdr_parse(hdr, const_cast<char*>(header_str.c_str()));
    bcf_hdr_add_sample(hdr, metadata_.sample_names.at(i).c_str());
    bcf_hdr_sync(hdr);
    result.emplace_back(hdr, bcf_hdr_destroy);
  }

  return result;
}

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
  std::string uri = general_metadata_uri(root_uri);
  VFS vfs(ctx);

  Metadata metadata;
  auto per_line = [&metadata](std::string* line) {
    auto kv_pair = utils::split(*line, '\t');
    if (kv_pair.size() < 2)
      return;
    std::string key = kv_pair[0];
    if (key == "version") {
      metadata.version = (unsigned)std::stoul(kv_pair[1]);
    } else if (key == "tile_capacity") {
      metadata.tile_capacity = std::stoul(kv_pair[1]);
    } else if (key == "row_tile_extent") {
      metadata.row_tile_extent = (uint32_t)std::stoul(kv_pair[1]);
    } else if (key == "anchor_gap") {
      metadata.anchor_gap = (uint32_t)std::stoul(kv_pair[1]);
    } else if (key == "extra_attributes") {
      metadata.extra_attributes = utils::split(base64_decode(kv_pair[1]), ',');
    } else if (key == "free_sample_id") {
      metadata.free_sample_id = (uint32_t)std::stoul(kv_pair[1]);
    } else if (key == "all_samples") {
      metadata.all_samples = utils::split(base64_decode(kv_pair[1]), ',');
    } else if (key == "sample_ids") {
      auto pairs = utils::split(base64_decode(kv_pair[1]), ',');
      for (const auto& p : pairs) {
        auto pair = utils::split(p, '\t');
        metadata.sample_ids[pair[0]] = (uint32_t)std::stoul(pair[1]);
      }
    } else if (key == "contig_offsets") {
      auto pairs = utils::split(base64_decode(kv_pair[1]), ',');
      for (const auto& p : pairs) {
        auto pair = utils::split(p, '\t');
        metadata.contig_offsets[pair[0]] = (uint32_t)std::stoul(pair[1]);
      }
    } else if (key == "contig_lengths") {
      auto pairs = utils::split(base64_decode(kv_pair[1]), ',');
      for (const auto& p : pairs) {
        auto pair = utils::split(p, '\t');
        metadata.contig_lengths[pair[0]] = (uint32_t)std::stoul(pair[1]);
      }
    } else if (key == "total_contig_length") {
      metadata.total_contig_length = (uint32_t)std::stoul(kv_pair[1]);
    } else {
      throw std::runtime_error(
          "Error reading metadata; unknown key '" + key + "'");
    }
  };

  // Populate the metadata struct
  utils::read_file_lines(vfs, uri, per_line);

  // Derive the sample id -> name map.
  metadata.sample_names.resize(metadata.sample_ids.size());
  for (const auto& pair : metadata.sample_ids)
    metadata.sample_names[pair.second] = pair.first;

  return metadata;
}

void TileDBVCFDataset::write_metadata(
    const Context& ctx, const std::string& root_uri, const Metadata& metadata) {
  // Serialize the metadata
  std::stringstream ss;
  ss << "version\t" << metadata.version << "\n";
  ss << "tile_capacity\t" << metadata.tile_capacity << "\n";
  ss << "row_tile_extent\t" << metadata.row_tile_extent << "\n";
  ss << "anchor_gap\t" << metadata.anchor_gap << "\n";
  ss << "free_sample_id\t" << metadata.free_sample_id << "\n";

  std::string attrs;
  for (unsigned i = 0; i < metadata.extra_attributes.size(); i++) {
    attrs += metadata.extra_attributes[i];
    if (i < metadata.extra_attributes.size() - 1)
      attrs.push_back(',');
  }
  ss << "extra_attributes\t" << base64_encode(attrs.c_str(), attrs.size())
     << "\n";

  std::string all_samples;
  for (unsigned i = 0; i < metadata.all_samples.size(); i++) {
    all_samples += metadata.all_samples[i];
    if (i < metadata.all_samples.size() - 1)
      all_samples.push_back(',');
  }
  ss << "all_samples\t"
     << base64_encode(all_samples.c_str(), all_samples.size()) << "\n";

  std::string sample_ids;
  for (const auto& s : metadata.sample_ids) {
    sample_ids += s.first;
    sample_ids.push_back('\t');
    sample_ids += std::to_string(s.second);
    sample_ids.push_back(',');
  }
  if (!sample_ids.empty())
    sample_ids.pop_back();
  ss << "sample_ids\t" << base64_encode(sample_ids.c_str(), sample_ids.size())
     << "\n";

  std::string contig_offsets;
  for (const auto& s : metadata.contig_offsets) {
    contig_offsets += s.first;
    contig_offsets.push_back('\t');
    contig_offsets += std::to_string(s.second);
    contig_offsets.push_back(',');
  }
  if (!contig_offsets.empty())
    contig_offsets.pop_back();
  ss << "contig_offsets\t"
     << base64_encode(contig_offsets.c_str(), contig_offsets.size()) << "\n";

  std::string contig_lengths;
  for (const auto& s : metadata.contig_lengths) {
    contig_lengths += s.first;
    contig_lengths.push_back('\t');
    contig_lengths += std::to_string(s.second);
    contig_lengths.push_back(',');
  }
  if (!contig_lengths.empty())
    contig_lengths.pop_back();
  ss << "contig_lengths\t"
     << base64_encode(contig_lengths.c_str(), contig_lengths.size()) << "\n";

  ss << "total_contig_length\t" << metadata.total_contig_length << "\n";

  // Write serialized metadata
  VFS vfs(ctx);
  VFS::filebuf buff(vfs);
  buff.open(general_metadata_uri(root_uri), std::ios::out);
  std::ostream os(&buff);
  if (!os.good() || os.fail() || os.bad()) {
    const char* err_c_str = strerror(errno);
    throw std::runtime_error(
        "Error opening metadata file '" + general_metadata_uri(root_uri) +
        "' for writing: " + std::string(err_c_str));
  }

  auto str = ss.str();
  os.write(str.data(), str.size());
  os.flush();
  buff.close();
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
  std::vector<uint32_t> subarray = {std::numeric_limits<uint32_t>::max(),
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
    auto hdr_samples = VCF::hdr_get_samples(hdr.get());
    if (hdr_samples.size() > 1)
      throw std::invalid_argument(
          "Error registering samples; a file has more than 1 sample. Ingestion "
          "from cVCF is not supported.");

    const auto& s = hdr_samples[0];
    if (sample_set->count(s))
      throw std::invalid_argument(
          "Error registering samples; sample " + s + " already exists.");
    sample_set->insert(s);

    (*sample_headers)[metadata->free_sample_id] = VCF::hdr_to_string(hdr.get());
    metadata->all_samples.push_back(s);
    metadata->sample_ids[s] = metadata->free_sample_id++;
    if (metadata->contig_offsets.empty()) {
      metadata->contig_offsets =
          VCF::hdr_get_contig_offsets(hdr.get(), &metadata->contig_lengths);
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

std::set<std::string> TileDBVCFDataset::builtin_attributes() {
  return {AttrNames::pos,
          AttrNames::real_end,
          AttrNames::qual,
          AttrNames::alleles,
          AttrNames::id,
          AttrNames::filter_ids,
          AttrNames::info,
          AttrNames::fmt};
}

std::set<std::string> TileDBVCFDataset::all_attributes() const {
  if (!open_)
    throw std::invalid_argument(
        "Cannot get attributes from dataset; dataset is not open.");
  std::set<std::string> result = builtin_attributes();
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

std::string TileDBVCFDataset::data_array_uri(const std::string& root_uri) {
  return utils::uri_join(root_uri, "data");
}

std::string TileDBVCFDataset::general_metadata_uri(
    const std::string& root_uri) {
  auto grp = utils::uri_join(root_uri, "metadata");
  return utils::uri_join(grp, "general.txt");
}

std::string TileDBVCFDataset::vcf_headers_uri(const std::string& root_uri) {
  auto grp = utils::uri_join(root_uri, "metadata");
  return utils::uri_join(grp, "vcf_headers");
}

}  // namespace vcf
}  // namespace tiledb
