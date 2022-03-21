#include "write/qc_arrays.h"
#include "utils/logger_public.h"
#include "utils/utils.h"

namespace tiledb::vcf {

// Define static variables
std::unique_ptr<Array> QCArrays::array_ = nullptr;
std::shared_ptr<Query> QCArrays::query_ = nullptr;
std::mutex QCArrays::query_lock_;
std::atomic_int QCArrays::contig_records_ = 0;

QCArrays::QCArrays() {
}

QCArrays::~QCArrays() {
  if (dst_ != nullptr) {
    free(dst_);
  }
}

void QCArrays::create(
    Context& ctx, std::string root_uri, tiledb_filter_type_t checksum) {
  LOG_DEBUG("QCArrays: Create array");

  // Create filters
  Filter zstd(ctx, TILEDB_FILTER_ZSTD);
  zstd.set_option(TILEDB_COMPRESSION_LEVEL, 22);

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
  auto contig = Dimension::create(
      ctx, DIM_STR[CONTIG], TILEDB_STRING_ASCII, nullptr, nullptr);
  auto pos = Dimension::create<uint32_t>(
      ctx, DIM_STR[POS], {{pos_min, pos_max}}, pos_extent);
  auto allele = Dimension::create(
      ctx, DIM_STR[ALLELE], TILEDB_STRING_ASCII, nullptr, nullptr);
  domain.add_dimensions(contig, pos, allele);
  schema.set_domain(domain);
  schema.set_coords_filter_list(filters_2);
  schema.set_offsets_filter_list(filters_2);

  // Create attributes
  for (int i = 0; i < LAST_; i++) {
    auto attr = Attribute::create<int32_t>(ctx, ATTR_STR[i], filters_2);
    schema.add_attributes(attr);
  }

  // Create array
  // TODO: create utils function
  auto delim = utils::starts_with(root_uri, "tiledb://") ? '-' : '/';
  auto uri = utils::uri_join(root_uri, VARIANT_QC_URI, delim);
  Array::create(uri, schema);
}

void QCArrays::init(std::shared_ptr<Context> ctx, std::string root_uri) {
  std::lock_guard<std::mutex> lock(query_lock_);
  LOG_DEBUG("QCArrays: Open array");

  auto uri = utils::uri_join(root_uri, VARIANT_QC_URI, '/');
  array_.reset(new Array(*ctx, uri, TILEDB_WRITE));
  if (array_ == nullptr) {
    LOG_FATAL("QCArrays: error opening array '{}'", uri);
  }

  if (query_ != nullptr) {
    LOG_FATAL("QCArrays::init called when query still open.");
  }

  query_.reset(new Query(*ctx, *array_));
  query_->set_layout(TILEDB_GLOBAL_ORDER);
}

void QCArrays::finalize() {
  std::lock_guard<std::mutex> lock(query_lock_);
  LOG_DEBUG("QCArrays: Finalize query with {} records", contig_records_);
  contig_records_ = 0;
  query_->finalize();
}

void QCArrays::close() {
  std::lock_guard<std::mutex> lock(query_lock_);
  LOG_DEBUG("QCArrays: Close array");

  query_->finalize();
  array_->close();

  query_ = nullptr;
  array_ = nullptr;
}

void QCArrays::flush() {
  // Update results for the last locus before flushing
  update_results();

  int buffered_records = attr_buffers_[AC].size();

  if (buffered_records == 0) {
    LOG_DEBUG("QCArrays: flush called with 0 records ");
    return;
  }

  LOG_DEBUG("QCArrays: flush {} records", buffered_records);
  contig_records_ += buffered_records;

  {
    std::lock_guard<std::mutex> lock(query_lock_);
    query_->set_data_buffer(DIM_STR[CONTIG], contig_buffer_)
        .set_offsets_buffer(DIM_STR[CONTIG], contig_offsets_)
        .set_data_buffer(DIM_STR[POS], pos_buffer_)
        .set_data_buffer(DIM_STR[ALLELE], allele_buffer_)
        .set_offsets_buffer(DIM_STR[ALLELE], allele_offsets_);

    for (int i = 0; i < LAST_; i++) {
      query_->set_data_buffer(ATTR_STR[i], attr_buffers_[i]);
    }

    auto st = query_->submit();

    if (st != Query::Status::COMPLETE) {
      LOG_FATAL("QCArrays: error submitting TileDB write query");
    }
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

void QCArrays::update_results() {
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

void QCArrays::process(
    bcf_hdr_t* hdr,
    const std::string& sample_name,
    const std::string& contig,
    uint32_t pos,
    bcf1_t* rec) {
  // Check if locus has changed
  if (contig != contig_ || pos != pos_) {
    if (contig != contig_) {
      LOG_DEBUG(
          "QCArrays: process old contig = {} new contig = {}", contig_, contig);
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
    values_[allele][AC]++;

    // TODO: update all attr values
    values_[allele][N_HOM] = -1;
    values_[allele][N_CALLED] = -2;
    values_[allele][N_NOT_CALLED] = -3;
    values_[allele][N_PASS] = -4;
  }
}

}  // namespace tiledb::vcf
