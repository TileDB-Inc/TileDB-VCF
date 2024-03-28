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
#include "managed_query.h"
#include "utils/logger_public.h"
#include "utils/utils.h"

namespace tiledb::vcf {

//===================================================================
//= public static functions
//===================================================================

std::string AlleleCount::get_uri(const Group& group) {
  try {
    auto member = group.member(ALLELE_COUNT_ARRAY);
    return member.uri();
  } catch (const tiledb::TileDBError& ex) {
    return "";
  }
}

void AlleleCount::create(
    Context& ctx, const std::string& root_uri, tiledb_filter_type_t checksum) {
  LOG_DEBUG("[AlleleCount] Create array");

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

bool AlleleCount::exists(const Group& group) {
  auto uri = get_uri(group);
  return !uri.empty();
}

void AlleleCount::init(std::shared_ptr<Context> ctx, const Group& group) {
  auto uri = get_uri(group);

  if (uri.empty()) {
    LOG_DEBUG("[AlleleCount] Ingestion task disabled");
    enabled_ = false;
    return;
  }

  std::lock_guard<std::mutex> lock(query_lock_);
  LOG_DEBUG("[AlleleCount] Open array '{}'", uri);

  // Open array
  array_ = std::make_unique<Array>(*ctx, uri, TILEDB_WRITE);
  enabled_ = true;

  // Create query
  query_ = std::make_unique<Query>(*ctx, *array_);
  query_->set_layout(TILEDB_GLOBAL_ORDER);
  ctx_ = ctx;
}

void AlleleCount::finalize_query() {
  if (!enabled_) {
    return;
  }

  LOG_DEBUG(
      "[AlleleCount] Finalize query with {} records", contig_records_.load());
  if (contig_records_ > 0) {
    if (utils::query_buffers_set(query_.get())) {
      LOG_FATAL("Cannot submit_and_finalize query with buffers set.");
    }
    query_->submit_and_finalize();
    if (query_->query_status() == Query::Status::FAILED) {
      LOG_FATAL("Error submitting TileDB write query: status = FAILED");
    }
  }
  contig_records_ = 0;

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
        "[AlleleCount] fragment_num = {} uri = {} samples = {}",
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
  LOG_DEBUG("[AlleleCount] Close array");

  if (query_ != nullptr) {
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

void AlleleCount::consolidate_commits(
    std::shared_ptr<Context> ctx, const Group& group) {
  auto uri = get_uri(group);

  // Return if the array does not exist
  if (uri.empty()) {
    return;
  }

  Config cfg = ctx->config();
  cfg["sm.consolidation.mode"] = "commits";
  tiledb::Array::consolidate(*ctx, uri, &cfg);
}

void AlleleCount::consolidate_fragment_metadata(
    std::shared_ptr<Context> ctx, const Group& group) {
  auto uri = get_uri(group);

  // Return if the array does not exist
  if (uri.empty()) {
    return;
  }

  Config cfg = ctx->config();
  cfg["sm.consolidation.mode"] = "fragment_meta";
  tiledb::Array::consolidate(*ctx, uri, &cfg);
}

void AlleleCount::vacuum_commits(
    std::shared_ptr<Context> ctx, const Group& group) {
  auto uri = get_uri(group);

  // Return if the array does not exist
  if (uri.empty()) {
    return;
  }

  Config cfg = ctx->config();
  cfg["sm.vacuum.mode"] = "commits";
  tiledb::Array::vacuum(*ctx, uri, &cfg);
}

void AlleleCount::vacuum_fragment_metadata(
    std::shared_ptr<Context> ctx, const Group& group) {
  auto uri = get_uri(group);

  // Return if the array does not exist
  if (uri.empty()) {
    return;
  }

  Config cfg = ctx->config();
  cfg["sm.vacuum.mode"] = "fragment_meta";
  tiledb::Array::vacuum(*ctx, uri, &cfg);
}

//===================================================================
//= public functions
//===================================================================

AlleleCount::AlleleCount(bool delete_mode) {
  count_delta_ = delete_mode ? -1 : 1;
}

AlleleCount::~AlleleCount() {
  if (dst_ != nullptr) {
    hts_free(dst_);
  }
}

void AlleleCount::flush(bool finalize) {
  if (!enabled_) {
    return;
  }

  // Update results for the last locus before flushing
  update_results();

  int buffered_records = count_buffer_.size();

  if (contig_offsets_.data() == nullptr) {
    LOG_DEBUG("[AlleleCount] flush called with no records written");
    return;
  }

  if (buffered_records == 0 && !finalize) {
    LOG_DEBUG("[AlleleCount] flush called with 0 records");
    return;
  }

  std::lock_guard<std::mutex> lock(query_lock_);
  contig_records_ += buffered_records;

  if (buffered_records) {
    LOG_DEBUG(
        "[AlleleCount] flushing {} records from {}:{}-{}",
        buffered_records,
        contig_offsets_.size() > 1 ?
            contig_buffer_.substr(0, contig_offsets_[1]) :
            contig_buffer_,
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
    if (st == Query::Status::FAILED) {
      LOG_FATAL("[AlleleCount] error submitting TileDB write query");
    }

    // Insert sample names from this query into the set of fragment sample names
    fragment_sample_names_.insert(sample_names_.begin(), sample_names_.end());
    sample_names_.clear();

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

  if (finalize) {
    // For remote global order writes, zero query buffers prior to
    // submit_and_finalize.
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

    finalize_query();
  }
}

void AlleleCount::process(
    const bcf_hdr_t* hdr,
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
      LOG_DEBUG("[AlleleCount] new contig = {}", contig);
    } else if (pos < pos_) {
      LOG_ERROR(
          "[AlleleCount] contig {} pos out of order {} < {}",
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

  // Skip if no GT data
  if (ngt < 0) {
    return;
  }

  int gt0 = bcf_gt_allele(dst_[0]);
  int gt1 = ngt == 2 ? bcf_gt_allele(dst_[1]) : 0;
  int gt0_missing = bcf_gt_is_missing(dst_[0]);
  int gt1_missing = ngt == 2 ? bcf_gt_is_missing(dst_[1]) : 1;
  int n_allele = rec->n_allele;

  // Skip if GT value is not a valid allele
  if (gt0 >= n_allele || gt1 >= n_allele) {
    LOG_WARN(
        "[AlleleCount] skipping invalid GT value: sample={} locus={}:{} "
        "gt={}/{} n_allele={}",
        sample_name,
        contig,
        pos + 1,
        gt0,
        gt1,
        n_allele);
    return;
  }

  // Skip if homozygous ref or alleles are missing
  if (ngt == 1) {
    if (gt0 == 0 || gt0_missing) {
      return;
    }
  } else if (ngt == 2) {
    if ((gt0 == 0 && gt1 == 0) || (gt0_missing && gt1_missing)) {
      return;
    }
  } else {
    return;  // ngt <= 0
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
    alt = rec->d.allele[gt0];
    gt = "1";
  } else {
    // diploid
    std::string_view alt0{gt0_missing ? "." : rec->d.allele[gt0]};
    std::string_view alt1{gt1_missing ? "." : rec->d.allele[gt1]};

    if (gt0_missing || gt1_missing) {
      gt = ".,1";
      alt = gt0_missing ? alt1 : alt0;
    } else if (!gt0 || !gt1) {
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

  // Update count
  count_[key] += count_delta_;

  // Add sample name to the set of sample name in this query
  sample_names_.insert(sample_name);
}

//===================================================================
//= private functions
//===================================================================

std::string AlleleCount::get_uri(const std::string& root_uri, bool relative) {
  auto root = relative ? "" : root_uri;
  return utils::uri_join(root, ALLELE_COUNT_ARRAY);
}

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

std::tuple<size_t, size_t, size_t, size_t, size_t>
AlleleCountReader::allele_count_buffer_sizes() {
  size_t ref = 0, alt = 0, filter = 0, gt = 0;
  for (std::pair<AlleleCountKey, int32_t> ac : AlleleCountGroupBy) {
    ref += ac.first.ref.size();
    alt += ac.first.alt.size();
    filter += ac.first.filter.size();
    gt += ac.first.gt.size();
  }
  return {AlleleCountGroupBy.size(), ref, alt, filter, gt};
}

void AlleleCountReader::prepare_allele_count(Region region) {
  // TODO: fix this ALLELE_COUNT_ARRAY so it links against the static singleton
  // defined in allele_count.h
  const std::string ALLELE_COUNT_ARRAY = "allele_count";

  // TODO: fix this COLUMN_NAME so it links against the static singleton defined
  // in allele_count.h
  std::vector<std::string> COLUMN_NAME = {
      "pos", "ref", "alt", "filter", "gt", "count"};
  // TODO: likewise with this enum
  enum Columns { POS, REF, ALT, FILTER, GT, COUNT };

  ManagedQuery mq(array_, ALLELE_COUNT_ARRAY, TILEDB_UNORDERED);
  mq.select_columns(COLUMN_NAME);
  mq.select_point<std::string>("contig", region.seq_name);
  mq.select_ranges<uint32_t>("pos", {{region.min, region.max}});
  // TODO: use regions set in reader to assign subarray

  while (!mq.is_complete()) {
    mq.submit();
    size_t num_rows = mq.results()->num_rows();

    for (size_t i = 0; i < num_rows; i++) {
      uint32_t pos = mq.data<uint32_t>("pos")[i];
      std::string ref = std::string(mq.string_view("ref", i));
      std::string alt = std::string(mq.string_view("alt", i));
      std::string filter = std::string(mq.string_view("filter", i));
      std::string gt = std::string(mq.string_view("gt", i));
      // TODO: add function to generate key by concatenating pos, ref, alt,
      // filter, gt
      AlleleCountKey key(pos, ref, alt, filter, gt);
      AlleleCountGroupBy[key] += mq.data<uint32_t>("count")[i];
    }
  }
}

// TODO: move this utils and unite with implementation in variant_stats
std::string AlleleCountReader::get_uri(
    const Group& group, std::string array_name) {
  try {
    auto member = group.member(array_name);
    return member.uri();
  } catch (const tiledb::TileDBError& ex) {
    return "";
  }
}

AlleleCountReader::AlleleCountReader(
    std::shared_ptr<Context> ctx, const Group& group) {
  auto uri = AlleleCountReader::get_uri(group, "allele_count");
  array_ = std::make_shared<Array>(*ctx, uri, TILEDB_READ);
}

void AlleleCountReader::read_from_allele_count(
    uint32_t* pos,
    char* ref,
    uint32_t* ref_offsets,
    char* alt,
    uint32_t* alt_offsets,
    char* filter,
    uint32_t* filter_offsets,
    char* gt,
    uint32_t* gt_offsets,
    int32_t* count) {
  ref_offsets[0] = 0;
  alt_offsets[0] = 0;
  filter_offsets[0] = 0;
  gt_offsets[0] = 0;
  size_t i = 0;
  for (std::pair<AlleleCountKey, int32_t> ac : AlleleCountGroupBy) {
    pos[i] = ac.first.pos;
    std::strncpy(
        ref + ref_offsets[i], ac.first.ref.data(), ac.first.ref.size());
    ref_offsets[i + 1] = ref_offsets[i] + ac.first.ref.size();
    std::strncpy(
        alt + alt_offsets[i], ac.first.alt.data(), ac.first.alt.size());
    alt_offsets[i + 1] = alt_offsets[i] + ac.first.alt.size();
    std::strncpy(
        filter + filter_offsets[i],
        ac.first.filter.data(),
        ac.first.filter.size());
    filter_offsets[i + 1] = filter_offsets[i] + ac.first.filter.size();
    std::strncpy(gt + gt_offsets[i], ac.first.gt.data(), ac.first.gt.size());
    gt_offsets[i + 1] = gt_offsets[i] + ac.first.gt.size();
    count[i] = ac.second;
    i++;
  }
}

AlleleCountKey::AlleleCountKey(
    uint32_t pos,
    std::string ref,
    std::string alt,
    std::string filter,
    std::string gt)
    : pos(pos)
    , ref(ref)
    , alt(alt)
    , filter(filter)
    , gt(gt) {
}

AlleleCountKey::AlleleCountKey(AlleleCountKey&& toMove) {
  pos = toMove.pos;
  ref = std::move(toMove.ref);
  alt = std::move(toMove.alt);
  filter = std::move(toMove.filter);
  gt = std::move(toMove.gt);
}

AlleleCountKey::AlleleCountKey(const AlleleCountKey& toCopy) {
  pos = toCopy.pos;
  ref = toCopy.ref;
  alt = toCopy.alt;
  filter = toCopy.filter;
  gt = toCopy.gt;
}

bool AlleleCountKey::operator<(const tiledb::vcf::AlleleCountKey& b) const {
  if (pos != b.pos) {
    return pos < b.pos;
  } else {
    if (ref != b.ref) {
      return ref < b.ref;
    } else {
      if (alt != b.alt) {
        return alt < b.alt;
      } else {
        if (filter != b.filter) {
          return filter < b.filter;
        } else {
          if (gt != b.gt) {
            return gt < b.gt;
          }
        }
      }
    }
  }
  return false;
}

bool AlleleCountKey::operator>(const AlleleCountKey& b) const {
  return !(*this < b) && !(*this == b);
}

bool AlleleCountKey::operator==(const AlleleCountKey& b) const {
  return pos == b.pos && ref == b.ref && alt == b.alt && filter == b.filter &&
         gt == b.gt;
}

}  // namespace tiledb::vcf
