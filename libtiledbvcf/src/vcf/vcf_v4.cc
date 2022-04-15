/**
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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

#include "vcf/vcf_v4.h"
#include "utils/logger_public.h"

namespace tiledb {
namespace vcf {

VCFV4::VCFV4()
    : open_(false)
    , inited_(false)
    , max_record_buffer_size_(10000)
    , hdr_(nullptr)
    , index_tbx_(nullptr)
    , index_hts_(nullptr) {
}

VCFV4::~VCFV4() {
  close();
}

void VCFV4::open(const std::string& file, const std::string& index_file) {
  if (open_)
    close();
  if (file.empty())
    throw std::invalid_argument("Cannot open VCF file; path is empty");

  path_ = file;
  index_path_ = index_file;

  SafeBCFFh fh(bcf_open(path_.c_str(), "r"), hts_close);
  if (fh == nullptr)
    throw std::runtime_error("Cannot open VCF file; bcf_open failed");
  if (fh->format.compression != bgzf) {
    close();
    throw std::runtime_error(
        "Cannot open VCF file; must be compressed with BGZF and indexed.");
  }

  LOG_DEBUG("Reading VCF header {}", file);
  hdr_ = bcf_hdr_read(fh.get());
  if (hdr_ == nullptr) {
    close();
    throw std::runtime_error("Cannot open VCF file; bcf_hdr_read failed.");
  }

  switch (fh->format.format) {
    case bcf:
      index_hts_ = index_path_.empty() ?
                       bcf_index_load(path_.c_str()) :
                       bcf_index_load2(path_.c_str(), index_path_.c_str());
      if (index_hts_ == nullptr) {
        close();
        throw std::runtime_error(
            "Cannot open VCF file; failed to load BCF index.");
      }
      break;
    case ::vcf:
      index_tbx_ = index_path_.empty() ?
                       tbx_index_load(path_.c_str()) :
                       tbx_index_load2(path_.c_str(), index_path_.c_str());
      if (index_tbx_ == nullptr) {
        close();
        throw std::runtime_error(
            "Cannot open VCF file; failed to load TBX index.");
      }
      break;
    default:
      close();
      throw std::runtime_error(
          "Cannot open VCF file; must be VCF or BCF format.");
      break;
  }

  open_ = true;
}

void VCFV4::close() {
  // Clear the record queue and associated allocation pool.
  std::queue<SafeSharedBCFRec>().swap(record_queue_);
  std::queue<SafeSharedBCFRec>().swap(record_queue_pool_);

  if (index_hts_ != nullptr) {
    hts_idx_destroy(index_hts_);
    index_hts_ = nullptr;
  }

  if (index_tbx_ != nullptr) {
    tbx_destroy(index_tbx_);
    index_tbx_ = nullptr;
  }

  if (hdr_ != nullptr) {
    bcf_hdr_destroy(hdr_);
    hdr_ = nullptr;
  }

  open_ = false;
  inited_ = false;
  path_.clear();
  index_path_.clear();
}

bool VCFV4::is_open() const {
  return open_;
}

SafeSharedBCFRec VCFV4::front_record() {
  if (!open_)
    return nullptr;

  if (record_queue_.empty())
    read_records();

  if (!record_queue_.empty())
    return record_queue_.front();

  return nullptr;
}

void VCFV4::pop_record() {
  if (!open_)
    return;

  if (record_queue_.empty())
    read_records();

  if (!record_queue_.empty())
    record_queue_.pop();

  return;
}

void VCFV4::return_record(SafeSharedBCFRec& record) {
  record_queue_pool_.emplace(std::move(record));
}

std::string VCFV4::contig_name(bcf1_t* const r) const {
  if (!open_)
    throw std::runtime_error(
        "Error getting contig name from VCF; file not open.");
  if (hdr_ == nullptr)
    throw std::runtime_error(
        "Error getting contig name from VCF; header is null.");
  if (r == nullptr)
    throw std::runtime_error(
        "Error getting contig name from VCF; record is null.");
  return std::string(bcf_seqname(hdr_, r));
}

std::string VCFV4::sample_name() const {
  if (!open_)
    throw std::runtime_error(
        "Error getting sample name from VCF; file not open.");

  // If there are no samples set the name to an empty string
  if (hdr_ == nullptr || bcf_hdr_nsamples(hdr_) == 0)
    return std::string();

  std::string unnormalized_name(hdr_->samples[0]);
  std::string name;
  if (!VCFUtils::normalize_sample_name(unnormalized_name, &name))
    throw std::runtime_error(
        "Error getting sample name from VCF; sample name has invalid "
        "characters: '" +
        std::string(unnormalized_name) + "'");
  return name;
}

bool VCFV4::contig_has_records(const std::string& contig_name) const {
  return record_count(contig_name) > 0;
}

size_t VCFV4::record_count(const std::string& contig_name) const {
  if (!open_)
    throw std::runtime_error(
        "Error checking empty contig in VCF; file not open.");

  hts_idx_t* idx = index_tbx_ != nullptr ? index_tbx_->idx : index_hts_;
  if (idx == nullptr)
    throw std::runtime_error(
        "Error checking empty contig in VCF; no index instance.");

  int region_id = index_tbx_ != nullptr ?
                      tbx_name2id(index_tbx_, contig_name.c_str()) :
                      bcf_hdr_name2id(hdr_, contig_name.c_str());
  if (region_id == -1)
    return false;

  uint64_t records, unused;
  hts_idx_get_stat(idx, region_id, &records, &unused);
  return records;
}

void VCFV4::set_max_record_buff_size(uint64_t max_record_buffer_size) {
  max_record_buffer_size_ = max_record_buffer_size;
}

bcf_hdr_t* VCFV4::hdr() const {
  return hdr_;
}

bool VCFV4::init(const std::string& contig_name, uint32_t pos) {
  // Reset the record queue on all seeks
  if (!record_queue_.empty())
    std::queue<SafeSharedBCFRec>().swap(record_queue_);

  SafeBCFFh fh(bcf_open(path_.c_str(), "r"), hts_close);
  if (fh == nullptr)
    throw std::runtime_error("Error seeking in VCF; bcf_open failed");

  record_iter_.reset();
  if (fh->format.format == bcf) {
    if (!record_iter_.init_bcf(
            std::move(fh), hdr_, index_hts_, contig_name, pos))
      return false;

  } else {
    if (fh->format.format != ::vcf)
      throw std::runtime_error("Error seeking in VCF; unknown format.");

    if (!record_iter_.init_tbx(
            std::move(fh), hdr_, index_tbx_, contig_name, pos))
      return false;
  }

  inited_ = true;
  return true;
}

bool VCFV4::seek(const std::string& contig_name, uint32_t pos) {
  if (!open_)
    return false;

  // Reset the record queue on all seeks
  if (!record_queue_.empty())
    std::queue<SafeSharedBCFRec>().swap(record_queue_);

  // On first contig we need to open the VCF file
  if (!inited_) {
    if (!init(contig_name, pos))
      return false;
  } else {
    if (!record_iter_.seek(contig_name, pos))
      return false;
  }
  seeked_contig_name_ = contig_name;

  // Buffer the next records into `record_queue_`.
  read_records();

  // HTSlib seek finds interescting records, which can include records
  // with rec.pos < pos && rec.end > pos.
  // We want to seek to the first record with rec.pos >= pos, so we
  // pop records where rec.pos < pos.
  while (front_record() != nullptr && front_record()->pos < pos) {
    pop_record();
  }

  // Return true if any records were buffered.
  return !record_queue_.empty();
}

void VCFV4::read_records() {
  if (!record_queue_.empty())
    std::queue<SafeSharedBCFRec>().swap(record_queue_);

  SafeBCFRec tmp_r(bcf_init1(), bcf_destroy);
  size_t record_buffer_size = 0;
  while (record_buffer_size < max_record_buffer_size_) {
    if (!record_iter_.next(tmp_r.get()))
      break;

    // Iteration does not cross contigs.
    std::string contig_name = bcf_seqname(hdr_, tmp_r.get());
    if (seeked_contig_name_ != contig_name) {
      break;
    }

    if (!record_queue_pool_.empty()) {
      // Pop a stale record for re-use. Note that `bcf_copy`
      // destroys (frees) the stale data to prevent a memory
      // leak.
      SafeSharedBCFRec r = record_queue_pool_.front();
      record_queue_pool_.pop();
      bcf_copy(r.get(), tmp_r.get());
      bcf_unpack(r.get(), BCF_UN_ALL);
      record_queue_.emplace(std::move(r));
    } else {
      SafeSharedBCFRec r(bcf_dup(tmp_r.get()), bcf_destroy);
      bcf_unpack(r.get(), BCF_UN_ALL);
      record_queue_.emplace(std::move(r));
    }
    record_buffer_size += sizeof(bcf1_t) + record_queue_.front()->shared.m +
                          record_queue_.front()->indiv.m;
  }
  if (record_buffer_size) {
    LOG_TRACE(
        "Filled VCF record queue: bytes={} records={} avg record bytes={}",
        record_buffer_size,
        record_queue_.size(),
        record_buffer_size / record_queue_.size());
  }
}

void VCFV4::swap(VCFV4& other) {
  std::swap(open_, other.open_);
  std::swap(path_, other.path_);
  std::swap(index_path_, other.index_path_);
  std::swap(record_queue_, other.record_queue_);
  std::swap(record_queue_pool_, other.record_queue_pool_);
  record_iter_.swap(other.record_iter_);
  std::swap(hdr_, other.hdr_);
  std::swap(index_tbx_, other.index_tbx_);
  std::swap(index_hts_, other.index_hts_);
}

/* ****************************** */
/*              Iter              */
/* ****************************** */

VCFV4::Iter::Iter()
    : fh_(nullptr, hts_close)
    , hdr_(nullptr)
    , hts_iter_(nullptr)
    , tbx_(nullptr) {
}

VCFV4::Iter::~Iter() {
  reset();
}

bool VCFV4::Iter::init_bcf(
    SafeBCFFh&& fh,
    bcf_hdr_t* hdr,
    hts_idx_t* index,
    const std::string& contig_name,
    uint32_t pos) {
  fh_ = std::move(fh);
  hdr_ = hdr;

  if (contig_name.empty())
    throw std::runtime_error(
        "Failed to init BCF iterator; contig name cannot be empty.");

  int region_id = bcf_hdr_name2id(hdr, contig_name.c_str());
  if (region_id == -1)
    return false;

  int region_min = pos;
  int region_max = std::numeric_limits<int>::max();

  hts_idx_ = index;
  hts_iter_ = bcf_itr_queryi(index, region_id, region_min, region_max);
  if (hts_iter_ == nullptr)
    return false;

  return true;
}

bool VCFV4::Iter::init_tbx(
    SafeBCFFh&& fh,
    bcf_hdr_t* hdr,
    tbx_t* index,
    const std::string& contig_name,
    uint32_t pos) {
  fh_ = std::move(fh);
  hdr_ = hdr;

  if (contig_name.empty())
    throw std::runtime_error(
        "Failed to init TBX iterator; contig name cannot be empty.");

  int region_id = tbx_name2id(index, contig_name.c_str());
  if (region_id == -1)
    return false;

  int region_min = pos;
  int region_max = std::numeric_limits<int>::max();

  tbx_ = index;
  hts_iter_ = tbx_itr_queryi(index, region_id, region_min, region_max);
  if (hts_iter_ == nullptr)
    return false;

  return true;
}

bool VCFV4::Iter::seek(const std::string& contig_name, uint32_t pos) {
  if (contig_name.empty())
    throw std::runtime_error(
        "Failed to init TBX iterator; contig name cannot be empty.");

  int64_t region_min = pos;
  int64_t region_max = std::numeric_limits<int64_t>::max();

  // If we have a tbx index
  if (tbx_ != nullptr) {
    // First free existing
    if (hts_iter_ != nullptr && hts_iter_->bins.a != nullptr) {
      hts_itr_destroy(hts_iter_);
      hts_iter_ = nullptr;
    }

    int region_id = tbx_name2id(tbx_, contig_name.c_str());
    if (region_id == -1)
      return false;

    hts_iter_ = tbx_itr_queryi(tbx_, region_id, region_min, region_max);
    if (hts_iter_ == nullptr)
      return false;
  } else {
    // First free existing
    if (hts_iter_ != nullptr && hts_iter_->bins.a != nullptr) {
      hts_itr_destroy(hts_iter_);
      hts_iter_ = nullptr;
    }

    int region_id = bcf_hdr_name2id(hdr_, contig_name.c_str());
    if (region_id == -1)
      return false;

    hts_iter_ = bcf_itr_queryi(hts_idx_, region_id, region_min, region_max);
    if (hts_iter_ == nullptr)
      return false;
  }

  return true;
}

void VCFV4::Iter::reset() {
  fh_.reset();
  hdr_ = nullptr;

  if (hts_iter_ != nullptr && hts_iter_->bins.a != nullptr) {
    hts_itr_destroy(hts_iter_);
    hts_iter_ = nullptr;
  }

  tbx_ = nullptr;

  if (tmps_.m) {
    free(tmps_.s);
    tmps_ = {0, 0, nullptr};
  }
}

void VCFV4::Iter::swap(VCFV4::Iter& other) {
  std::swap(fh_, other.fh_);
  std::swap(hdr_, other.hdr_);
  std::swap(hts_iter_, other.hts_iter_);
  std::swap(tbx_, other.tbx_);
  std::swap(tmps_, other.tmps_);
}

bool VCFV4::Iter::next(bcf1_t* rec) {
  int ret;

  if (tbx_ == nullptr) {
    ret = bcf_itr_next(fh_.get(), hts_iter_, rec);
  } else {
    ret = tbx_itr_next(fh_.get(), tbx_, hts_iter_, &tmps_);
    vcf_parse1(&tmps_, hdr_, rec);
  }

  return ret >= 0;
}

}  // namespace vcf
}  // namespace tiledb
