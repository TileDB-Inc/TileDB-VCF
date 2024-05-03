/**
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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

#include "sample_stats.h"
#include "array_buffers.h"
#include "managed_query.h"
#include "utils/utils.h"

namespace tiledb::vcf {

std::string SampleStats::get_uri_(const Group& group) {
  try {
    auto member = group.member(SAMPLE_STATS_ARRAY);
    return member.uri();
  } catch (const tiledb::TileDBError& ex) {
    return "";
  }
}

std::string SampleStats::get_uri_(const std::string& root_uri, bool relative) {
  auto root = relative ? "" : root_uri;
  return utils::uri_join(root, SAMPLE_STATS_ARRAY);
}

void SampleStats::create(
    Context& ctx, const std::string& root_uri, int compression_level) {
  // Create filter lists
  FilterList int_fl(ctx);
  FilterList float_fl(ctx);
  FilterList ascii_fl(ctx);
  FilterList dict_fl(ctx);
  FilterList rle_fl(ctx);

  // Use tile level filtering
  int_fl.set_max_chunk_size(0);
  float_fl.set_max_chunk_size(0);
  ascii_fl.set_max_chunk_size(0);
  dict_fl.set_max_chunk_size(0);
  rle_fl.set_max_chunk_size(0);

  // Create checksum and compression filters
  Filter checksum(ctx, TILEDB_FILTER_CHECKSUM_SHA256);
  Filter compression(ctx, TILEDB_FILTER_ZSTD);
  compression.set_option(TILEDB_COMPRESSION_LEVEL, compression_level);

  int_fl.add_filter(compression).add_filter(checksum);
  float_fl.add_filter(compression).add_filter(checksum);
  ascii_fl.add_filter(compression).add_filter(checksum);
  dict_fl.add_filter({ctx, TILEDB_FILTER_DICTIONARY})
      .add_filter(compression)
      .add_filter(checksum);
  rle_fl.add_filter({ctx, TILEDB_FILTER_RLE})
      .add_filter(compression)
      .add_filter(checksum);

  // Create schema
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
  schema.set_allows_dups(true);
  schema.set_offsets_filter_list(int_fl);

  // Create dimensions
  Domain domain(ctx);

  auto d0 =
      Dimension::create(ctx, "sample", TILEDB_STRING_ASCII, nullptr, nullptr);
  d0.set_filter_list(dict_fl);

  domain.add_dimensions(d0);
  schema.set_domain(domain);

  // Create attributes
  auto add_attribute = [&](const std::string& name,
                           tiledb_datatype_t type,
                           FilterList& filter_list,
                           bool is_var = false,
                           bool is_nullable = false) {
    auto attr = Attribute::create(ctx, name, type);
    attr.set_cell_val_num(is_var ? TILEDB_VAR_NUM : 1);
    attr.set_nullable(is_nullable);
    attr.set_filter_list(filter_list);
    schema.add_attribute(attr);
  };

  std::vector<std::string> columns = {
      "dp_sum",      "dp_sum2",  "dp_count",     "dp_min",    "dp_max",
      "gq_sum",      "gq_sum2",  "gq_count",     "gq_min",    "gq_max",
      "n_records",   "n_called", "n_not_called", "n_hom_ref", "n_het",
      "n_singleton", "n_snp",    "n_indel",      "n_ins",     "n_del",
      "n_ti",        "n_tv",     "n_overlap",    "n_multi"};

  for (const auto& name : columns) {
    if (name.starts_with("dp_") || name.starts_with("gq_")) {
      // Add nullable attributes for DP and GQ fields, since they may be missing
      add_attribute(name, TILEDB_UINT64, int_fl, false, true);
    } else {
      add_attribute(name, TILEDB_UINT64, int_fl);
    }
  }

  // Check schema
  try {
    schema.check();
  } catch (const tiledb::TileDBError& e) {
    throw std::runtime_error(e.what());
  }

  // Create array
  LOG_DEBUG("[SampleStats] create array");
  auto uri = get_uri_(root_uri);
  tiledb::Array::create(uri, schema);

  // Add array to root group
  // Group assets use full paths for tiledb cloud, relative paths otherwise
  auto relative = !utils::starts_with(root_uri, "tiledb://");
  auto array_uri = get_uri_(root_uri, relative);
  LOG_DEBUG("Adding array '{}' to group '{}'", array_uri, root_uri);
  Group root_group(ctx, root_uri, TILEDB_WRITE);
  root_group.add_member(array_uri, relative, SAMPLE_STATS_ARRAY);
}

bool SampleStats::exists(const Group& group) {
  auto uri = get_uri_(group);
  return !uri.empty();
}

void SampleStats::init(
    std::shared_ptr<Context> ctx, const Group& group, bool delete_mode) {
  auto uri = get_uri_(group);

  if (uri.empty()) {
    LOG_DEBUG("[SampleStats] Ingestion task disabled");
    enabled_ = false;
    return;
  }

  LOG_DEBUG("[SampleStats] Open array '{}'", uri);

  // Open array
  auto mode = delete_mode ? TILEDB_DELETE : TILEDB_WRITE;
  array_ = std::make_shared<Array>(*ctx, uri, mode);
  enabled_ = true;
}

void SampleStats::process(
    const bcf_hdr_t* hdr,
    const std::string& sample,
    const std::string& contig,
    uint32_t pos,
    bcf1_t* rec) {
  if (bcf_get_format_int32(hdr, rec, "DP", &dst_, &ndst_) > 0) {
    if (dst_[0] != bcf_int32_missing) {
      auto dp = static_cast<uint64_t>(dst_[0]);
      stats_[sample]["dp_sum"] += dp;
      stats_[sample]["dp_sum2"] += dp * dp;
      stats_[sample]["dp_count"] += 1;
      if (!stats_[sample].contains("dp_min")) {
        stats_[sample]["dp_min"] = std::numeric_limits<uint64_t>::max();
      }
      stats_[sample]["dp_min"] = std::min(stats_[sample]["dp_min"], dp);
      stats_[sample]["dp_max"] = std::max(stats_[sample]["dp_max"], dp);
    }
  }

  if (bcf_get_format_int32(hdr, rec, "GQ", &dst_, &ndst_) > 0) {
    if (dst_[0] != bcf_int32_missing) {
      auto gq = static_cast<uint64_t>(dst_[0]);
      stats_[sample]["gq_sum"] += gq;
      stats_[sample]["gq_sum2"] += gq * gq;
      stats_[sample]["gq_count"] += 1;
      if (!stats_[sample].contains("gq_min")) {
        stats_[sample]["gq_min"] = std::numeric_limits<uint64_t>::max();
      }
      stats_[sample]["gq_min"] = std::min(stats_[sample]["gq_min"], gq);
      stats_[sample]["gq_max"] = std::max(stats_[sample]["gq_max"], gq);
    }
  }

  auto is_transition = [](char a, char b) {
    return (a == 'A' && b == 'G') || (a == 'G' && b == 'A') ||
           (a == 'C' && b == 'T') || (a == 'T' && b == 'C');
  };

  // Compute stats based on the genotypes
  bool is_ref = true;
  bool is_hom = true;
  bool is_missing = true;
  std::unordered_map<int, int> ac;

  int ngt = bcf_get_genotypes(hdr, rec, &dst_, &ndst_);
  if (ngt > 0) {
    int first_allele = bcf_gt_allele(dst_[0]);

    for (int i = 0; i < ngt; i++) {
      int allele = bcf_gt_allele(dst_[i]);
      is_ref &= allele == 0;
      is_hom &= allele == first_allele;
      is_missing &= bcf_gt_is_missing(dst_[i]);
      ac[allele] += allele > 0;
    }
  }

  bool is_hom_ref = !is_missing && is_hom && is_ref;
  bool is_het = !is_missing && !is_hom;
  bool is_multi = rec->n_allele > 2;

  // Count singletons, multi-allelic records can have multiple singletons
  int n_singleton = 0;
  for (const auto& [allele, count] : ac) {
    n_singleton += count == 1;
  }

  // Get types of variants in the record
  auto var_types = bcf_has_variant_types(
      rec,
      VCF_SNP | VCF_INDEL | VCF_INS | VCF_DEL | VCF_OVERLAP,
      bcf_match_overlap);

  // Compute stats based on the alleles
  int n_snp = 0;
  int n_ti = 0;
  int n_tv = 0;

  if (var_types & VCF_SNP) {
    n_snp++;

    // Count tis and tvs for each SNP, possibly multi-allelic
    for (int i = 1; i < rec->n_allele; i++) {
      if (bcf_get_variant_type(rec, i) & VCF_SNP) {
        bool is_ti = is_transition(rec->d.allele[0][0], rec->d.allele[i][0]);
        n_ti += is_ti;
        n_tv += !is_ti;
      }
    }
  }

  // For multi-allelic records, n_ins + n_del can be > n_indel
  int n_indel = (var_types & VCF_INDEL) > 0;
  int n_ins = (var_types & VCF_INS) > 0;
  int n_del = (var_types & VCF_DEL) > 0;
  int n_overlap = (var_types & VCF_OVERLAP) > 0;

  stats_[sample]["n_records"] += 1;
  stats_[sample]["n_called"] += !is_missing;
  stats_[sample]["n_not_called"] += is_missing;
  stats_[sample]["n_hom_ref"] += is_hom_ref;
  stats_[sample]["n_het"] += is_het;
  stats_[sample]["n_singleton"] += n_singleton;
  stats_[sample]["n_snp"] += n_snp;
  stats_[sample]["n_ti"] += n_ti;
  stats_[sample]["n_tv"] += n_tv;
  stats_[sample]["n_indel"] += n_indel;
  stats_[sample]["n_ins"] += n_ins;
  stats_[sample]["n_del"] += n_del;
  stats_[sample]["n_overlap"] += n_overlap;
  stats_[sample]["n_multi"] += is_multi;
}

void SampleStats::flush(bool finalize) {
  if (!enabled_ || !finalize) {
    return;
  }

  if (stats_.empty()) {
    LOG_DEBUG("[SampleStats] No stats to flush");
    return;
  }

  LOG_DEBUG("[SampleStats] Finalize stats");

  ArrayBuffers buffers;

  auto add_buffer_ = [&](const std::string& name) {
    buffers.emplace(name, ColumnBuffer::create(array_, name));
  };

  // Add buffers for all dimensions
  auto schema = array_->schema();
  for (const auto& dim : schema.domain().dimensions()) {
    add_buffer_(dim.name());
  }

  // Add buffers for all attributes
  for (const auto& [name, attr] : schema.attributes()) {
    add_buffer_(name);
  }

  bool found_dp = false;
  bool found_gq = false;

  for (const auto& [sample, stat] : stats_) {
    buffers["sample"]->push_back(sample);

    // Add stats to buffers
    for (const auto& [field, value] : stat) {
      buffers[field]->push_back(value);
      found_dp |= field == "dp_sum";
      found_gq |= field == "gq_sum";
    }

    // Add nulls for missing DP fields
    if (!found_dp) {
      buffers["dp_sum"]->push_null();
      buffers["dp_sum2"]->push_null();
      buffers["dp_count"]->push_null();
      buffers["dp_min"]->push_null();
      buffers["dp_max"]->push_null();
    }

    // Add nulls for missing GQ fields
    if (!found_gq) {
      buffers["gq_sum"]->push_null();
      buffers["gq_sum2"]->push_null();
      buffers["gq_count"]->push_null();
      buffers["gq_min"]->push_null();
      buffers["gq_max"]->push_null();
    }
  }

  // Create managed query
  ManagedQuery mq(array_, "sample_stats");

  // Attach buffers to the query
  for (const auto& name : buffers.names()) {
    mq.set_column_data(name, buffers[name]);
  }

  // Submit the write query and finalize
  mq.submit_write();
  mq.finalize();

  // Clear the stats
  stats_.clear();
}

void SampleStats::delete_sample(const std::string& sample) {
  if (!enabled_ || sample.empty()) {
    return;
  }

  LOG_DEBUG("[SampleStats] Delete samples {}", sample);

  if (array_ == nullptr) {
    LOG_FATAL("[SampleStats] Array not initialized for deletion");
  }

  auto ctx = array_->schema().context();
  Query delete_query(ctx, *array_, TILEDB_DELETE);
  QueryCondition qc(ctx);
  qc.init("sample", sample, TILEDB_EQ);
  delete_query.set_condition(qc);
  delete_query.submit();
}

void SampleStats::close() {
  if (!enabled_) {
    return;
  }

  LOG_DEBUG("[SampleStats] Close array");

  if (array_ != nullptr) {
    array_->close();
    array_ = nullptr;
  }

  enabled_ = false;
}

}  // namespace tiledb::vcf
