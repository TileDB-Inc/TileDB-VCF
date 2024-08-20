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
#include <htslib/vcf.h>
#include <htslib/vcfutils.h>
#include "array_buffers.h"
#include "managed_query.h"
#include "utils/utils.h"

namespace tiledb::vcf {

SampleStats::~SampleStats() {
  if (!enabled_) {
    return;
  }

  // Flush any remaining stats
  flush(true);

  if (dst_ != nullptr) {
    hts_free(dst_);
  }
}

std::string SampleStats::get_uri_(const Group& group) {
  try {
    auto member = group.member(SAMPLE_STATS_ARRAY);
    return member.uri();
  } catch (const TileDBError& ex) {
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
      "dp_sum",         "dp_sum2",     "dp_count",      "dp_min",
      "dp_max",         "gq_sum",      "gq_sum2",       "gq_count",
      "gq_min",         "gq_max",      "n_records",     "n_called",
      "n_not_called",   "n_hom_ref",   "n_het",         "n_singleton",
      "n_snp",          "n_insertion", "n_deletion",    "n_transition",
      "n_transversion", "n_star",      "n_multiallelic"};

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
  } catch (const TileDBError& e) {
    throw std::runtime_error(e.what());
  }

  // Create array
  LOG_DEBUG("[SampleStats] create array");
  auto uri = get_uri_(root_uri);
  Array::create(uri, schema);

  // Write metadata
  Array array(ctx, uri, TILEDB_WRITE);
  array.put_metadata("version", TILEDB_INT32, 1, &SAMPLE_STATS_VERSION);

  // Add array to root group
  // Group assets use full paths for tiledb cloud, relative paths otherwise
  auto relative = !utils::starts_with(root_uri, "tiledb://");
  auto array_uri = get_uri_(root_uri, relative);
  LOG_DEBUG(
      "[SampleStats] Adding array '{}' to group '{}'", array_uri, root_uri);
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

  // Check array version
  {
    auto array = Array(*ctx, uri, TILEDB_READ);

    auto get_version = [&]() -> std::optional<int> {
      tiledb_datatype_t value_type;
      uint32_t value_num;
      const void* value;
      array.get_metadata("version", &value_type, &value_num, &value);
      if (value_type == TILEDB_INT32 && value_num == 1) {
        return *static_cast<const int*>(value);
      }
      return std::nullopt;
    };

    auto version = get_version();
    if (version == std::nullopt) {
      LOG_WARN(
          "[SampleStats] Sample stats are deprecated for this array version.");
      enabled_ = false;
      return;
    }
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

  // Compute stats based on the genotypes
  bool is_ref = true;
  bool is_hom = true;
  bool is_missing = true;
  std::unordered_map<int, int> ac;
  int n_ti = 0;
  int n_tv = 0;
  int n_ins = 0;
  int n_del = 0;
  int n_star = 0;

  int ngt = bcf_get_genotypes(hdr, rec, &dst_, &ndst_);
  if (ngt > 0) {
    int first_allele = bcf_gt_allele(dst_[0]);

    for (int i = 0; i < ngt; i++) {
      int allele = bcf_gt_allele(dst_[i]);
      is_ref &= allele == 0;
      is_hom &= allele == first_allele;
      is_missing &= bcf_gt_is_missing(dst_[i]);
      ac[allele] += allele > 0;

      // Skip invalid GT values
      if (allele >= rec->n_allele) {
        LOG_WARN(
            "[SampleStats] Skipping invalid GT: sample={} locus={}:{} "
            "gt={} n_allele={}",
            sample,
            contig,
            pos,
            allele,
            rec->n_allele);
        continue;
      }

      // Update counts for each called, non-REF allele
      if (!bcf_gt_is_missing(dst_[i]) && allele != 0) {
        if (bcf_has_variant_type(rec, allele, VCF_SNP)) {
          // Find REF and ALT for the SNP, which may not be the first base for
          // multi-allelic MNP.
          for (size_t j = 0; j < strlen(rec->d.allele[0]); j++) {
            int ref = bcf_acgt2int(rec->d.allele[0][j]);
            int alt = bcf_acgt2int(rec->d.allele[allele][j]);
            if (ref != alt) {
              if (abs(ref - alt) == 2) {
                n_ti++;
              } else {
                n_tv++;
              }
              break;
            }
          }
        } else if (bcf_has_variant_type(rec, allele, VCF_INS)) {
          n_ins++;
        } else if (bcf_has_variant_type(rec, allele, VCF_DEL)) {
          n_del++;
        } else if (bcf_has_variant_type(rec, allele, VCF_OVERLAP)) {
          n_star++;
        }
      }
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

  stats_[sample]["n_records"] += 1;
  stats_[sample]["n_called"] += !is_missing;
  stats_[sample]["n_not_called"] += is_missing;
  stats_[sample]["n_hom_ref"] += is_hom_ref;
  stats_[sample]["n_het"] += is_het;
  stats_[sample]["n_singleton"] += n_singleton;
  stats_[sample]["n_snp"] += n_ti + n_tv;
  stats_[sample]["n_transition"] += n_ti;
  stats_[sample]["n_transversion"] += n_tv;
  stats_[sample]["n_insertion"] += n_ins;
  stats_[sample]["n_deletion"] += n_del;
  stats_[sample]["n_star"] += n_star;
  stats_[sample]["n_multiallelic"] += is_multi;
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

void SampleStats::consolidate_commits(
    std::shared_ptr<Context> ctx, const Group& group) {
  auto uri = get_uri_(group);

  // Return if the array does not exist
  if (uri.empty()) {
    return;
  }

  Config cfg = ctx->config();
  cfg["sm.consolidation.mode"] = "commits";
  Array::consolidate(*ctx, uri, &cfg);
}

void SampleStats::consolidate_fragment_metadata(
    std::shared_ptr<Context> ctx, const Group& group) {
  auto uri = get_uri_(group);

  // Return if the array does not exist
  if (uri.empty()) {
    return;
  }

  Config cfg = ctx->config();
  cfg["sm.consolidation.mode"] = "fragment_meta";
  Array::consolidate(*ctx, uri, &cfg);
}

void SampleStats::vacuum_commits(
    std::shared_ptr<Context> ctx, const Group& group) {
  auto uri = get_uri_(group);

  // Return if the array does not exist
  if (uri.empty()) {
    return;
  }

  Config cfg = ctx->config();
  cfg["sm.vacuum.mode"] = "commits";
  Array::vacuum(*ctx, uri, &cfg);
}

void SampleStats::vacuum_fragment_metadata(
    std::shared_ptr<Context> ctx, const Group& group) {
  auto uri = get_uri_(group);

  // Return if the array does not exist
  if (uri.empty()) {
    return;
  }

  Config cfg = ctx->config();
  cfg["sm.vacuum.mode"] = "fragment_meta";
  Array::vacuum(*ctx, uri, &cfg);
}

std::shared_ptr<ArrayBuffers> SampleStats::sample_qc(
    std::string dataset_uri,
    std::vector<std::string> samples,
    std::map<std::string, std::string> config) {
  auto context = Context(Config(config));
  auto group = Group(context, dataset_uri, TILEDB_READ);
  auto ss_uri = group.member(SAMPLE_STATS_ARRAY).uri();
  auto array = std::make_shared<Array>(context, ss_uri, TILEDB_READ);

  auto get_version = [&]() -> std::optional<int> {
    tiledb_datatype_t value_type;
    uint32_t value_num;
    const void* value;
    array->get_metadata("version", &value_type, &value_num, &value);
    if (value_type == TILEDB_INT32 && value_num == 1) {
      return *static_cast<const int*>(value);
    }
    return std::nullopt;
  };

  // Check version
  auto version = get_version();
  if (version != SAMPLE_STATS_VERSION) {
    throw std::runtime_error(
        "The sample_stats array is the wrong version. Please re-ingest.");
  }

  // Setup the query
  ManagedQuery mq(array, "samples_stats", TILEDB_UNORDERED);
  mq.select_points("sample", samples);

  // Read the results, group by sample and aggregate
  std::unordered_map<std::string, std::unordered_map<std::string, uint64_t>>
      stats;

  while (!mq.is_complete()) {
    mq.submit();
    auto results = mq.results();

    for (unsigned int i = 0; i < results->num_rows(); i++) {
      auto sample = std::string(mq.string_view("sample", i));
      for (auto& name : results->names()) {
        if (name.ends_with("_max")) {
          // Max aggregation
          stats[sample][name] =
              stats[sample].contains(name) ?
                  std::max(stats[sample][name], mq.data<uint64_t>(name)[i]) :
                  mq.data<uint64_t>(name)[i];
        } else if (name.ends_with("_min")) {
          // Min aggregation
          stats[sample][name] =
              stats[sample].contains(name) ?
                  std::min(stats[sample][name], mq.data<uint64_t>(name)[i]) :
                  mq.data<uint64_t>(name)[i];
        } else if (name != "sample") {
          // Sum aggregation
          stats[sample][name] += mq.data<uint64_t>(name)[i];
        }
      }
    }
  }

  static const std::vector<std::string> column_names{
      "sample",        "dp_mean",
      "dp_stddev",     "dp_min",
      "dp_max",        "gq_mean",
      "gq_stddev",     "gq_min",
      "gq_max",        "call_rate",
      "n_called",      "n_not_called",
      "n_hom_ref",     "n_het",
      "n_hom_var",     "n_non_ref",
      "n_singleton",   "n_snp",
      "n_insertion",   "n_deletion",
      "n_transition",  "n_transversion",
      "n_star",        "r_ti_tv",
      "r_het_hom_var", "r_insertion_deletion",
      "n_records",     "n_multiallelic",
  };

  static const std::set<std::string> fp_column_names{
      "dp_mean",
      "dp_stddev",
      "gq_mean",
      "gq_stddev",
      "call_rate",
      "r_ti_tv",
      "r_het_hom_var",
      "r_insertion_deletion",
  };

  // Create buffers to store the results
  auto buffers = std::make_shared<ArrayBuffers>();
  for (const auto& name : column_names) {
    if (name == "sample") {
      auto buffer =
          ColumnBuffer::create(name, TILEDB_STRING_ASCII, true, false);
      buffers->emplace(name, buffer);
    } else if (fp_column_names.contains(name)) {
      auto buffer = ColumnBuffer::create(name, TILEDB_FLOAT32, false, true);
      buffers->emplace(name, buffer);
    } else {
      auto buffer = ColumnBuffer::create(name, TILEDB_UINT64, false, false);
      buffers->emplace(name, buffer);
    }
  }

  // Sort sample names
  std::set<std::string> sorted_samples;
  for (const auto& pair : stats) {
    sorted_samples.insert(pair.first);
  }

  // Compute results and add them to the buffers
  for (const auto& sample : sorted_samples) {
    auto& ss = stats[sample];

    LOG_DEBUG("[SampleStats] Aggregating sample '{}'", sample);
    for (const auto& name : column_names) {
      LOG_DEBUG("[SampleStats]   '{}'", name);

      // Helper function to get a value from the stats map
      auto get = [&](const std::string& key, uint64_t missing_value = 0) {
        return ss.contains(key) ? ss.at(key) : missing_value;
      };

      // Helper functions to calculate and push the mean
      auto push_mean = [&](const std::string& prefix) {
        auto count = get(prefix + "_count");
        auto sum = get(prefix + "_sum");
        if (count > 0) {
          auto mean = static_cast<float>(sum) / count;
          buffers->at(prefix + "_mean")->push_back(mean);
        } else {
          buffers->at(prefix + "_mean")->push_null();
        }
      };

      // Helper functions to calculate and push the stddev
      auto push_stddev = [&](const std::string& prefix) {
        auto count = static_cast<float>(get(prefix + "_count"));
        auto sum = static_cast<float>(get(prefix + "_sum"));
        auto sum2 = static_cast<float>(get(prefix + "_sum2"));
        if (count > 1) {
          auto mean = sum / count;
          // Note: Hail uses population variance to compute stddev, while pandas
          // uses sample variance. We choose population variance here to match
          // the hail results. For reference, sample variance:
          //   variance = (sum2 - count * mean * mean) / (count - 1)
          auto variance = sum2 / count - mean * mean;
          auto stddev = std::sqrt(variance);
          buffers->at(prefix + "_stddev")->push_back(stddev);
        } else {
          buffers->at(prefix + "_stddev")->push_null();
        }
      };

      if (name == "sample") {
        buffers->at("sample")->push_back(sample);
      } else if (name == "dp_mean") {
        push_mean("dp");
      } else if (name == "dp_stddev") {
        push_stddev("dp");
      } else if (name == "gq_mean") {
        push_mean("gq");
      } else if (name == "gq_stddev") {
        push_stddev("gq");
      } else if (name == "n_hom_var") {
        auto n_hom_var = get("n_called") - get("n_hom_ref") - get("n_het");
        buffers->at(name)->push_back(n_hom_var);

        if (n_hom_var > 0) {
          auto value = static_cast<float>(get("n_het")) / n_hom_var;
          buffers->at("r_het_hom_var")->push_back(value);
        } else {
          buffers->at("r_het_hom_var")->push_null();
        }
      } else if (name == "n_non_ref") {
        auto value = get("n_called") - get("n_hom_ref");
        buffers->at(name)->push_back(value);
      } else if (name == "r_ti_tv") {
        if (get("n_transversion") > 0) {
          auto value =
              static_cast<float>(get("n_transition")) / get("n_transversion");
          buffers->at(name)->push_back(value);
        } else {
          buffers->at(name)->push_null();
        }
      } else if (name == "r_insertion_deletion") {
        if (get("n_deletion") > 0) {
          auto value =
              static_cast<float>(get("n_insertion")) / get("n_deletion");
          buffers->at(name)->push_back(value);
        } else {
          buffers->at(name)->push_null();
        }
      } else if (name == "r_het_hom_var") {
        // Skip, handled in "n_hom_var"
      } else if (name == "call_rate") {
        auto total = get("n_called") + get("n_not_called");
        if (total > 0) {
          auto value = static_cast<float>(get("n_called")) / total;
          buffers->at(name)->push_back(value);
        } else {
          buffers->at(name)->push_null();
        }
      } else {
        buffers->at(name)->push_back(get(name));
      }
    }
  }

  return buffers;
}
}  // namespace tiledb::vcf
