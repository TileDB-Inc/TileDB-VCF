#include "write/allele_counter.h"
#include "utils/logger_public.h"
#include "utils/utils.h"

namespace tiledb::vcf {

// Define static variables
std::unique_ptr<Array> AlleleCounter::array_ = nullptr;
std::shared_ptr<Query> AlleleCounter::query_ = nullptr;
std::mutex AlleleCounter::query_lock_;
std::atomic_int AlleleCounter::contig_records_ = 0;

AlleleCounter::AlleleCounter() {
}

AlleleCounter::~AlleleCounter() {
  if (dst_ != nullptr) {
    free(dst_);
  }
}

void AlleleCounter::create(
    Context& ctx, std::string root_uri, tiledb_filter_type_t checksum) {
  LOG_DEBUG("AlleleCounter: Create array");

  // Create filters
  Filter zstd(ctx, TILEDB_FILTER_ZSTD);
  //  zstd.set_option(TILEDB_COMPRESSION_LEVEL, 22);

  FilterList filters_1(ctx);
  FilterList filters_2(ctx);
  filters_1.add_filter(zstd);
  filters_2.add_filter({ctx, TILEDB_FILTER_DOUBLE_DELTA});
  filters_2.add_filter(zstd);
  if (checksum) {
    filters_1.add_filter({ctx, checksum});
    filters_2.add_filter({ctx, checksum});
  }

  // Create schema and domain
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
  schema.set_allows_dups(true);

  Domain domain(ctx);
  const uint32_t pos_min = 0;
  const uint32_t pos_max = std::numeric_limits<uint32_t>::max() - 1;
  const uint32_t pos_extent = pos_max - pos_min + 1;
  auto contig =
      Dimension::create(ctx, AC_CONTIG, TILEDB_STRING_ASCII, nullptr, nullptr);
  auto pos = Dimension::create<uint32_t>(
      ctx, AC_POS, {{pos_min, pos_max}}, pos_extent);
  auto allele =
      Dimension::create(ctx, AC_ALLELE, TILEDB_STRING_ASCII, nullptr, nullptr);
  domain.add_dimensions(contig, pos, allele);
  schema.set_domain(domain);
  schema.set_coords_filter_list(filters_2);
  schema.set_offsets_filter_list(filters_2);

  // Create attributes
  auto count = Attribute::create<int32_t>(ctx, AC_COUNT, filters_2);
  schema.add_attributes(count);

  // Create array
  // TODO: create utils function
  auto delim = utils::starts_with(root_uri, "tiledb://") ? '-' : '/';
  auto uri = utils::uri_join(root_uri, AC_URI, delim);
  Array::create(uri, schema);
}

void AlleleCounter::init(std::shared_ptr<Context> ctx, std::string root_uri) {
  std::lock_guard<std::mutex> lock(query_lock_);
  LOG_DEBUG("AlleleCounter: Open array");

  auto uri = utils::uri_join(root_uri, AC_URI, '/');
  array_.reset(new Array(*ctx, uri, TILEDB_WRITE));
  if (array_ == nullptr) {
    LOG_FATAL("AlleleCounter: error opening array '{}'", uri);
  }

  if (query_ != nullptr) {
    LOG_FATAL("AlleleCounter::init called when query still open.");
  }

  query_.reset(new Query(*ctx, *array_));
  query_->set_layout(TILEDB_GLOBAL_ORDER);
}

void AlleleCounter::finalize() {
  std::lock_guard<std::mutex> lock(query_lock_);
  LOG_DEBUG("AlleleCounter: Finalize query with {} records", contig_records_);
  contig_records_ = 0;
  query_->finalize();
}

void AlleleCounter::close() {
  std::lock_guard<std::mutex> lock(query_lock_);
  LOG_DEBUG("AlleleCounter: Close array");

  query_->finalize();
  array_->close();

  query_ = nullptr;
  array_ = nullptr;
}

void AlleleCounter::flush() {
  // Update results for the last locus before flushing
  update_results();

  if (ac_count_.size() == 0) {
    LOG_DEBUG("AlleleCounter: flush called with 0 records ");
    return;
  }

  LOG_DEBUG("AlleleCounter: flush {} records", ac_count_.size());
  contig_records_ += ac_count_.size();

  {
    std::lock_guard<std::mutex> lock(query_lock_);
    auto st = query_->set_data_buffer(AC_CONTIG, ac_contig_)
                  .set_offsets_buffer(AC_CONTIG, ac_contig_offsets_)
                  .set_data_buffer(AC_POS, ac_pos_)
                  .set_data_buffer(AC_ALLELE, ac_allele_)
                  .set_offsets_buffer(AC_ALLELE, ac_allele_offsets_)
                  .set_data_buffer(AC_COUNT, ac_count_)
                  .submit();

    if (st != Query::Status::COMPLETE) {
      LOG_FATAL("AlleleCounter: error submitting TileDB write query");
    }
  }

  // Clear buffers
  ac_contig_.clear();
  ac_contig_offsets_.clear();
  ac_pos_.clear();
  ac_allele_.clear();
  ac_allele_offsets_.clear();
  ac_count_.clear();
}

void AlleleCounter::update_results() {
  if (allele_count_.size() > 0) {
    for (auto& [allele, count] : allele_count_) {
      ac_contig_offsets_.push_back(ac_contig_.size());
      ac_contig_ += contig_;
      ac_pos_.push_back(pos_);
      ac_allele_offsets_.push_back(ac_allele_.size());
      ac_allele_ += allele;
      ac_count_.push_back(count);
    }
    allele_count_.clear();
  }
}

void AlleleCounter::process(
    bcf_hdr_t* hdr,
    const std::string& sample_name,
    const std::string& contig,
    uint32_t pos,
    bcf1_t* rec) {
  // Check if locus has changed
  if (contig != contig_ || pos != pos_) {
    if (contig != contig_) {
      LOG_DEBUG(
          "AlleleCounter: process old contig = {} new contig = {}",
          contig_,
          contig);
    }
    update_results();
    contig_ = contig;
    pos_ = pos;
  }

  // TODO: should we normalize REF, ALT alleles?
  // ngt = 2 for diploid, 1 for haploid
  int ngt = bcf_get_genotypes(hdr, rec, &dst_, &ndst_);
  for (int i = 0; i < ngt; i++) {
    // Skip missing and REF alleles
    if (bcf_gt_is_missing(dst_[i]) || bcf_gt_allele(dst_[i]) == 0) {
      continue;
    }
    std::string allele = rec->d.allele[bcf_gt_allele(dst_[i])];
    allele_count_[allele]++;
  }
}

}  // namespace tiledb::vcf
