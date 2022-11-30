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

#include "allele_count.h"
#include "utils/logger_public.h"
#include "utils/utils.h"

namespace tiledb::vcf {

//===================================================================
//= public static functions
//===================================================================

void AlleleCount::create(
    Context& ctx, const std::string& root_uri, tiledb_filter_type_t checksum) {
  LOG_DEBUG("AlleleCount: Create array");

  // Create filter lists
  FilterList rle_coord_filters(ctx);
  FilterList int_coord_filters(ctx);
  FilterList offset_filters(ctx);
  FilterList int_filters(ctx);
  FilterList str_filters(ctx);

  rle_coord_filters.add_filter({ctx, TILEDB_FILTER_RLE});
  int_coord_filters.add_filter({ctx, TILEDB_FILTER_DOUBLE_DELTA})
      .add_filter({ctx, TILEDB_FILTER_BIT_WIDTH_REDUCTION})
      .add_filter({ctx, TILEDB_FILTER_ZSTD});
  offset_filters.add_filter({ctx, TILEDB_FILTER_DOUBLE_DELTA})
      .add_filter({ctx, TILEDB_FILTER_BIT_WIDTH_REDUCTION})
      .add_filter({ctx, TILEDB_FILTER_ZSTD});
  int_filters.add_filter({ctx, TILEDB_FILTER_BIT_WIDTH_REDUCTION})
      .add_filter({ctx, TILEDB_FILTER_ZSTD});
  str_filters.add_filter({ctx, TILEDB_FILTER_ZSTD});

  if (checksum) {
    // rle_coord_filters.add_filter({ctx, checksum});
    int_coord_filters.add_filter({ctx, checksum});
    offset_filters.add_filter({ctx, checksum});
    int_filters.add_filter({ctx, checksum});
    str_filters.add_filter({ctx, checksum});
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

  // Create dimensions
  auto contig = Dimension::create(
      ctx, COLUMN_NAME[CONTIG], TILEDB_STRING_ASCII, nullptr, nullptr);
  contig.set_filter_list(rle_coord_filters);  // d0

  auto pos = Dimension::create<uint32_t>(
      ctx, COLUMN_NAME[POS], {{pos_min, pos_max}}, pos_extent);
  pos.set_filter_list(int_coord_filters);  // d1

  domain.add_dimensions(contig, pos);
  schema.set_domain(domain);

  // Create attributes
  for (int i = REF; i < COUNT; i++) {
    auto attr =
        Attribute::create<std::string>(ctx, COLUMN_NAME[i], str_filters);
    schema.add_attributes(attr);
  }

  auto count = Attribute::create<int32_t>(ctx, COLUMN_NAME[COUNT], int_filters);
  schema.add_attributes(count);

  // Create array
  auto uri = get_uri(root_uri);
  Array::create(uri, schema);

  // Write metadata
  Array array(ctx, uri, TILEDB_WRITE);
  array.put_metadata("version", TILEDB_UINT32, 1, &ALLELE_COUNT_VERSION);

  // Add array to root group
  // Group assets use full paths for tiledb cloud, relative paths otherwise
  auto relative = !utils::starts_with(root_uri, "tiledb://");
  auto array_uri = get_uri(root_uri, relative);
  LOG_DEBUG("Adding array '{}' to group '{}'", array_uri, root_uri);
  Group root_group(ctx, root_uri, TILEDB_WRITE);
  root_group.add_member(array_uri, relative, ALLELE_COUNT_ARRAY);
}

void AlleleCount::init(
    std::shared_ptr<Context> ctx, const std::string& root_uri) {
  std::lock_guard<std::mutex> lock(query_lock_);
  LOG_DEBUG("AlleleCount: Open array");

  // Open array
  auto uri = get_uri(root_uri);
  try {
    array_ = std::make_unique<Array>(*ctx, uri, TILEDB_WRITE);
    enabled_ = true;
  } catch (const tiledb::TileDBError& ex) {
    LOG_DEBUG("AlleleCount: Ingestion task disabled");
    enabled_ = false;
    return;
  }

  query_ = std::make_unique<Query>(*ctx, *array_);
  query_->set_layout(TILEDB_GLOBAL_ORDER);
  ctx_ = ctx;
}

void AlleleCount::finalize() {
  if (!enabled_) {
    return;
  }

  std::lock_guard<std::mutex> lock(query_lock_);
  LOG_DEBUG("AlleleCount: Finalize query with {} records", contig_records_);
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
        "AlleleCount: fragment_num = {} uri = {} samples = {}",
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

void AlleleCount::close() {
  if (!enabled_) {
    return;
  }

  std::lock_guard<std::mutex> lock(query_lock_);
  LOG_DEBUG("AlleleCount: Close array");

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

std::string AlleleCount::get_uri(const std::string& root_uri, bool relative) {
  auto root = relative ? "" : root_uri;
  return utils::uri_join(root, ALLELE_COUNT_ARRAY);
}

void AlleleCount::consolidate_commits(
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

void AlleleCount::consolidate_fragment_metadata(
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

void AlleleCount::vacuum_commits(
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

void AlleleCount::vacuum_fragment_metadata(
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

AlleleCount::AlleleCount() {
}

AlleleCount::~AlleleCount() {
  if (dst_ != nullptr) {
    free(dst_);
  }
}

void AlleleCount::flush() {
  if (!enabled_) {
    return;
  }

  // Update results for the last locus before flushing
  update_results();

  int buffered_records = count_buffer_.size();

  if (buffered_records == 0) {
    return;
  }

  {
    std::lock_guard<std::mutex> lock(query_lock_);
    contig_records_ += buffered_records;

    LOG_DEBUG(
        "AlleleCount: flushing {} records from {}:{}-{}",
        buffered_records,
        contig_buffer_.substr(0, contig_offsets_[1]),
        pos_buffer_.front(),
        pos_buffer_.back());

    query_->set_data_buffer(COLUMN_NAME[CONTIG], contig_buffer_)
        .set_offsets_buffer(COLUMN_NAME[CONTIG], contig_offsets_)
        .set_data_buffer(COLUMN_NAME[POS], pos_buffer_)
        .set_data_buffer(COLUMN_NAME[REF], ref_buffer_)
        .set_offsets_buffer(COLUMN_NAME[REF], ref_offsets_)
        .set_data_buffer(COLUMN_NAME[ALT], alt_buffer_)
        .set_offsets_buffer(COLUMN_NAME[ALT], alt_offsets_)
        .set_data_buffer(COLUMN_NAME[FILTER], filter_buffer_)
        .set_offsets_buffer(COLUMN_NAME[FILTER], filter_offsets_)
        .set_data_buffer(COLUMN_NAME[GT], gt_buffer_)
        .set_offsets_buffer(COLUMN_NAME[GT], gt_offsets_)
        .set_data_buffer(COLUMN_NAME[COUNT], count_buffer_);

    auto st = query_->submit();

    if (st != Query::Status::COMPLETE) {
      LOG_FATAL("AlleleCount: error submitting TileDB write query");
    }

    // Insert sample names from this query into the set of fragment sample names
    fragment_sample_names_.insert(sample_names_.begin(), sample_names_.end());
    sample_names_.clear();
  }

  // Clear buffers
  contig_buffer_.clear();
  contig_offsets_.clear();
  pos_buffer_.clear();
  ref_buffer_.clear();
  ref_offsets_.clear();
  alt_buffer_.clear();
  alt_offsets_.clear();
  filter_buffer_.clear();
  filter_offsets_.clear();
  gt_buffer_.clear();
  gt_offsets_.clear();
  count_buffer_.clear();
}

void AlleleCount::process(
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
      LOG_DEBUG("AlleleCount: new contig = {}", contig);
    } else if (pos < pos_) {
      LOG_ERROR(
          "AlleleCount: contig {} pos out of order {} < {}", contig, pos, pos_);
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

  // Only haploid and diploid are supported
  if (ngt > 2) {
    LOG_FATAL(
        "Ploidy > 2 not supported: sample={} locus={}:{} ploidy={}",
        sample_name,
        contig,
        pos,
        ngt);
  }

  // Skip if homozygous ref or missing allele
  if (bcf_gt_allele(dst_[0]) == 0 || bcf_gt_is_missing(dst_[0])) {
    if (ngt == 1) {
      return;  // haploid
    } else if (bcf_gt_allele(dst_[1]) == 0 || bcf_gt_is_missing(dst_[1])) {
      return;  // diploid
    }
  }

  // Build FILTER value string
  std::string filter;
  for (int i = 0; i < rec->d.n_flt; i++) {
    filter.append(bcf_hdr_int2id(hdr, BCF_DT_ID, rec->d.flt[i]));
    if (i < rec->d.n_flt - 1) {
      filter.append(";");
    }
  }
  if (filter.empty()) {
    filter = ".";
  }

  // Build ALT and GT strings
  // Sort ALT alleles and normalize GT
  //  - haploid GT = 1
  //  - diploid GT = 0,1 or 1,1 or 1,2
  std::string alt;
  std::string gt;
  if (ngt == 1) {
    // haploid
    int gt0 = bcf_gt_allele(dst_[0]);
    alt = rec->d.allele[gt0];
    gt = "1";
  } else {
    // diploid
    int gt0 = bcf_gt_allele(dst_[0]);
    int gt1 = bcf_gt_allele(dst_[1]);
    std::string_view alt0{rec->d.allele[gt0]};
    std::string_view alt1{rec->d.allele[gt1]};

    if (!gt0 || !gt1) {
      gt = "0,1";
      alt = gt0 ? alt0 : alt1;
    } else if (gt0 == gt1) {
      gt = "1,1";
      alt = alt0;
    } else {
      gt = "1,2";
      if (alt0 < alt1) {
        alt.append(alt0).append(",").append(alt1);
      } else {
        alt.append(alt1).append(",").append(alt0);
      }
    }
  }

  // Build key = "ref:alt:filter:gt"
  std::string key(rec->d.allele[0]);
  key.append(":").append(alt).append(":").append(filter).append(":").append(gt);

  // Increment count
  count_[key]++;

  // Add sample name to the set of sample name in this query
  sample_names_.insert(sample_name);
}

//===================================================================
//= private functions
//===================================================================

void AlleleCount::update_results() {
  if (count_.size() > 0) {
    for (auto& [key, count] : count_) {
      contig_offsets_.push_back(contig_buffer_.size());
      contig_buffer_ += contig_;
      pos_buffer_.push_back(pos_);

      auto tokens = utils::split(key, ":");
      ref_offsets_.push_back(ref_buffer_.size());
      ref_buffer_ += tokens[0];
      alt_offsets_.push_back(alt_buffer_.size());
      alt_buffer_ += tokens[1];
      filter_offsets_.push_back(filter_buffer_.size());
      filter_buffer_ += tokens[2];
      gt_offsets_.push_back(gt_buffer_.size());
      gt_buffer_ += tokens[3];

      count_buffer_.push_back(count);
    }
    count_.clear();
  }
}

}  // namespace tiledb::vcf
