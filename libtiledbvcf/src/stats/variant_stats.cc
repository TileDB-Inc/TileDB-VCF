/**
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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

#include "variant_stats.h"
#include "utils/logger_public.h"
#include "utils/utils.h"

namespace tiledb::vcf {

//===================================================================
//= public static functions
//===================================================================

void VariantStats::create(
    Context& ctx, const std::string& root_uri, tiledb_filter_type_t checksum) {
  LOG_DEBUG("VariantStats: Create array");

  // Create filter lists
  FilterList rle_coord_filters(ctx);
  FilterList int_coord_filters(ctx);
  FilterList str_filters(ctx);
  FilterList offset_filters(ctx);
  FilterList int_attr_filters(ctx);

  rle_coord_filters.add_filter({ctx, TILEDB_FILTER_RLE});
  int_coord_filters.add_filter({ctx, TILEDB_FILTER_DOUBLE_DELTA})
      .add_filter({ctx, TILEDB_FILTER_BIT_WIDTH_REDUCTION})
      .add_filter({ctx, TILEDB_FILTER_ZSTD});
  str_filters.add_filter({ctx, TILEDB_FILTER_ZSTD});
  offset_filters.add_filter({ctx, TILEDB_FILTER_DOUBLE_DELTA})
      .add_filter({ctx, TILEDB_FILTER_BIT_WIDTH_REDUCTION})
      .add_filter({ctx, TILEDB_FILTER_ZSTD});
  int_attr_filters.add_filter({ctx, TILEDB_FILTER_BIT_WIDTH_REDUCTION})
      .add_filter({ctx, TILEDB_FILTER_ZSTD});

  if (checksum) {
    // rle_coord_filters.add_filter({ctx, checksum});
    int_coord_filters.add_filter({ctx, checksum});
    str_filters.add_filter({ctx, checksum});
    offset_filters.add_filter({ctx, checksum});
    int_attr_filters.add_filter({ctx, checksum});
  }

  // Create schema and domain
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
  schema.set_allows_dups(true);
  schema.set_offsets_filter_list(offset_filters);

  Domain domain(ctx);
  const uint32_t pos_min = 0;
  const uint32_t pos_max = std::numeric_limits<uint32_t>::max() - 1;
  const uint32_t pos_extent = pos_max - pos_min + 1;

  auto contig = Dimension::create(
      ctx, COLUMN_STR[CONTIG], TILEDB_STRING_ASCII, nullptr, nullptr);
  contig.set_filter_list(rle_coord_filters);  // d0

  auto pos = Dimension::create<uint32_t>(
      ctx, COLUMN_STR[POS], {{pos_min, pos_max}}, pos_extent);
  pos.set_filter_list(int_coord_filters);  // d1

  domain.add_dimensions(contig, pos);
  schema.set_domain(domain);

  auto allele =
      Attribute::create<std::string>(ctx, COLUMN_STR[ALLELE], str_filters);
  schema.add_attributes(allele);

  // Create attributes
  for (int i = 0; i < LAST_; i++) {
    auto attr = Attribute::create<int32_t>(ctx, ATTR_STR[i], int_attr_filters);
    schema.add_attributes(attr);
  }

  // Create array
  auto uri = get_uri(root_uri);
  Array::create(uri, schema);

  // Write metadata
  Array array(ctx, uri, TILEDB_WRITE);
  array.put_metadata("version", TILEDB_UINT32, 1, &VARIANT_STATS_VERSION);

  // Add array to root group
  // Group assests use full paths for tiledb cloud, relative paths otherwise
  auto relative = !utils::starts_with(root_uri, "tiledb://");
  auto array_uri = get_uri(root_uri, relative);
  LOG_DEBUG("Adding array '{}' to group '{}'", array_uri, root_uri);
  Group root_group(ctx, root_uri, TILEDB_WRITE);
  root_group.add_member(array_uri, relative, VARIANT_STATS_ARRAY);
}

void VariantStats::init(
    std::shared_ptr<Context> ctx, const std::string& root_uri) {
  std::lock_guard<std::mutex> lock(query_lock_);
  LOG_DEBUG("VariantStats: Open array");

  // Open array
  auto uri = get_uri(root_uri);
  try {
    array_ = std::make_unique<Array>(*ctx, uri, TILEDB_WRITE);
    enabled_ = true;
  } catch (const tiledb::TileDBError& ex) {
    LOG_DEBUG("VariantStats: Ingestion task disabled");
    enabled_ = false;
    return;
  }

  query_ = std::make_unique<Query>(*ctx, *array_);
  query_->set_layout(TILEDB_GLOBAL_ORDER);
  ctx_ = ctx;
}

void VariantStats::finalize() {
  if (!enabled_) {
    return;
  }

  std::lock_guard<std::mutex> lock(query_lock_);
  LOG_DEBUG("VariantStats: Finalize query with {} records", contig_records_);
  contig_records_ = 0;
  query_->finalize();

  // Write fragment uri -> sample names to array metadata
  auto frag_num = query_->fragment_num();
  if (frag_num > 0) {
    auto uri = query_->fragment_uri(frag_num - 1);
    std::string samples;
    for (auto& sample : fragment_sample_names_) {
      samples += sample + ",";
    }
    if (!samples.empty()) {
      samples.pop_back();
    }
    LOG_DEBUG(
        "VariantStats: fragment_num = {} uri = {} samples = {}",
        frag_num,
        uri,
        samples);
    array_->put_metadata(
        uri, TILEDB_STRING_ASCII, samples.size(), samples.c_str());

    fragment_sample_names_.clear();
  }

  query_ = std::make_unique<Query>(*ctx_, *array_);
  query_->set_layout(TILEDB_GLOBAL_ORDER);
}

void VariantStats::close() {
  if (!enabled_) {
    return;
  }

  std::lock_guard<std::mutex> lock(query_lock_);
  LOG_DEBUG("VariantStats: Close array");

  if (query_ != nullptr) {
    query_->finalize();
    query_ = nullptr;
  }

  if (array_ != nullptr) {
    array_->close();
    array_ = nullptr;
  }

  // Release the context shared_ptr
  ctx_ = nullptr;
  enabled_ = false;
}

std::string VariantStats::get_uri(std::string_view root_uri, bool relative) {
  auto root = relative ? "" : root_uri;
  return utils::uri_join(std::string(root), VARIANT_STATS_ARRAY);
}

void VariantStats::consolidate_commits(
    std::shared_ptr<Context> ctx,
    const std::vector<std::string>& tiledb_config,
    const std::string& root_uri) {
  // Return if the array does not exist
  tiledb::VFS vfs(*ctx);
  if (!vfs.is_dir(get_uri(root_uri))) {
    return;
  }

  Config cfg;
  utils::set_tiledb_config(tiledb_config, &cfg);
  cfg["sm.consolidation.mode"] = "commits";
  tiledb::Array::consolidate(*ctx, get_uri(root_uri), &cfg);
}

void VariantStats::consolidate_fragment_metadata(
    std::shared_ptr<Context> ctx,
    const std::vector<std::string>& tiledb_config,
    const std::string& root_uri) {
  // Return if the array does not exist
  tiledb::VFS vfs(*ctx);
  if (!vfs.is_dir(get_uri(root_uri))) {
    return;
  }

  Config cfg;
  utils::set_tiledb_config(tiledb_config, &cfg);
  cfg["sm.consolidation.mode"] = "fragment_meta";
  tiledb::Array::consolidate(*ctx, get_uri(root_uri), &cfg);
}

void VariantStats::vacuum_commits(
    std::shared_ptr<Context> ctx,
    const std::vector<std::string>& tiledb_config,
    const std::string& root_uri) {
  // Return if the array does not exist
  tiledb::VFS vfs(*ctx);
  if (!vfs.is_dir(get_uri(root_uri))) {
    return;
  }

  Config cfg;
  utils::set_tiledb_config(tiledb_config, &cfg);
  cfg["sm.vacuum.mode"] = "commits";
  tiledb::Array::vacuum(*ctx, get_uri(root_uri), &cfg);
}

void VariantStats::vacuum_fragment_metadata(
    std::shared_ptr<Context> ctx,
    const std::vector<std::string>& tiledb_config,
    const std::string& root_uri) {
  // Return if the array does not exist
  tiledb::VFS vfs(*ctx);
  if (!vfs.is_dir(get_uri(root_uri))) {
    return;
  }

  Config cfg;
  utils::set_tiledb_config(tiledb_config, &cfg);
  cfg["sm.vacuum.mode"] = "fragment_meta";
  tiledb::Array::vacuum(*ctx, get_uri(root_uri), &cfg);
}

//===================================================================
//= public functions
//===================================================================

VariantStats::VariantStats() {
}

VariantStats::~VariantStats() {
  if (dst_ != nullptr) {
    free(dst_);
  }
}

void VariantStats::flush() {
  if (!enabled_) {
    return;
  }

  // Update results for the last locus before flushing
  update_results();

  int buffered_records = attr_buffers_[AC].size();

  if (buffered_records == 0) {
    LOG_DEBUG("VariantStats: flush called with 0 records ");
    return;
  }

  {
    std::lock_guard<std::mutex> lock(query_lock_);
    contig_records_ += buffered_records;

    LOG_DEBUG(
        "VariantStats: flushing {} records from {}:{}-{}",
        buffered_records,
        contig_buffer_.substr(0, contig_offsets_[1]),
        pos_buffer_.front(),
        pos_buffer_.back());

    query_->set_data_buffer(COLUMN_STR[CONTIG], contig_buffer_)
        .set_offsets_buffer(COLUMN_STR[CONTIG], contig_offsets_)
        .set_data_buffer(COLUMN_STR[POS], pos_buffer_)
        .set_data_buffer(COLUMN_STR[ALLELE], allele_buffer_)
        .set_offsets_buffer(COLUMN_STR[ALLELE], allele_offsets_);

    for (int i = 0; i < LAST_; i++) {
      query_->set_data_buffer(ATTR_STR[i], attr_buffers_[i]);
    }

    auto st = query_->submit();

    if (st != Query::Status::COMPLETE) {
      LOG_FATAL("VariantStats: error submitting TileDB write query");
    }

    // Insert sample names from this query into the set of fragment sample names
    fragment_sample_names_.insert(sample_names_.begin(), sample_names_.end());
    sample_names_.clear();
  }

  // Clear buffers
  contig_buffer_.clear();
  contig_offsets_.clear();
  pos_buffer_.clear();
  allele_buffer_.clear();
  allele_offsets_.clear();
  for (int i = 0; i < LAST_; i++) {
    attr_buffers_[i].clear();
  }
}

void VariantStats::process(
    bcf_hdr_t* hdr,
    const std::string& sample_name,
    const std::string& contig,
    uint32_t pos,
    bcf1_t* rec) {
  if (!enabled_) {
    return;
  }

  // Check if locus has changed
  if (contig != contig_ || pos != pos_) {
    if (contig != contig_) {
      LOG_DEBUG("VariantStats: new contig = {}", contig);
    } else if (pos < pos_) {
      LOG_ERROR(
          "VariantStats: contig {} pos out of order {} < {}",
          contig,
          pos,
          pos_);
    }
    update_results();
    contig_ = contig;
    pos_ = pos;
  }

  // Read GT data from record
  int ngt = bcf_get_genotypes(hdr, rec, &dst_, &ndst_);

  // Skip missing GT
  if (ngt <= 0 || bcf_gt_is_missing(dst_[0])) {
    return;
  }

  // Add sample name to the set of sample name in this query
  sample_names_.insert(sample_name);

  // Update called for the REF allele
  auto ref = rec->d.allele[0];
  values_[ref][N_CALLED]++;

  int gt0 = bcf_gt_allele(dst_[0]);
  std::string allele0 = rec->d.allele[gt0];

  // Increment allele count for GT[0]
  values_[allele0][AC]++;

  if (ngt == 2) {
    int gt1 = bcf_gt_allele(dst_[1]);
    std::string allele1 = rec->d.allele[gt1];

    // Increment allele count for GT[1]
    values_[allele1][AC]++;

    // Update homozygote count, only diploid genotype calls are counted
    if (gt0 == gt1) {
      values_[allele1][N_HOM]++;
    }
  } else if (ngt > 2) {
    LOG_FATAL(
        "Ploidy > 2 not supported: sample={} locus={}:{} ploidy={}",
        sample_name,
        contig,
        pos,
        ngt);
  }
}

//===================================================================
//= private functions
//===================================================================

void VariantStats::update_results() {
  if (values_.size() > 0) {
    for (auto& [allele, value] : values_) {
      contig_offsets_.push_back(contig_buffer_.size());
      contig_buffer_ += contig_;
      pos_buffer_.push_back(pos_);
      allele_offsets_.push_back(allele_buffer_.size());
      allele_buffer_ += allele;
      for (int i = 0; i < LAST_; i++) {
        attr_buffers_[i].push_back(value[i]);
      }
    }
    values_.clear();
  }
}

}  // namespace tiledb::vcf
