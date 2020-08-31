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

#include "vcf/vcf_v2.h"

namespace tiledb {
namespace vcf {

VCFV2::VCFV2()
    : open_(false)
    , path_("")
    , index_path_("")
    , buffer_offset_(0)
    , max_record_buffer_size_(10000)
    , hdr_(nullptr)
    , index_tbx_(nullptr)
    , index_hts_(nullptr) {
}

VCFV2::~VCFV2() {
  close();
}

void VCFV2::open(const std::string& file, const std::string& index_file) {
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

void VCFV2::close() {
  destroy_buffer();

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
  path_.clear();
  index_path_.clear();
}

bool VCFV2::is_open() const {
  return open_;
}

bool VCFV2::next() {
  if (!open_)
    return false;

  buffer_offset_ += sizeof(bcf1_t*);

  if (buffer_offset_ < buffer_.size()) {
    return true;
  } else {
    // Buffer the next records into `buffer_`.
    read_records();

    // Return true if any records were buffered.
    return buffer_.size() > 0;
  }
}

std::string VCFV2::contig_name() const {
  if (!open_)
    throw std::runtime_error(
        "Error getting contig name from VCF; file not open.");
  if (hdr_ == nullptr)
    throw std::runtime_error(
        "Error getting contig name from VCF; header is null.");
  if (curr_rec() == nullptr)
    throw std::runtime_error(
        "Error getting contig name from VCF; current record is null.");
  return std::string(bcf_seqname(hdr_, curr_rec()));
}

std::string VCFV2::sample_name() const {
  if (!open_)
    throw std::runtime_error(
        "Error getting sample name from VCF; file not open.");
  if (hdr_ == nullptr || bcf_hdr_nsamples(hdr_) == 0)
    throw std::runtime_error(
        "Error getting sample name from VCF; header has no samples.");
  std::string unnormalized_name(hdr_->samples[0]);
  std::string name;
  if (!VCFUtils::normalize_sample_name(unnormalized_name, &name))
    throw std::runtime_error(
        "Error getting sample name from VCF; sample name has invalid "
        "characters: '" +
        std::string(unnormalized_name) + "'");
  return name;
}

bool VCFV2::contig_has_records(const std::string& contig_name) const {
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
  return records > 0;
}

void VCFV2::set_max_record_buff_size(uint64_t max_record_buffer_size) {
  max_record_buffer_size_ = max_record_buffer_size;
}

bcf1_t* VCFV2::curr_rec() const {
  if (buffer_.size() == 0)
    return nullptr;
  return buffer_.value<bcf1_t*>(buffer_offset_ / sizeof(bcf1_t*));
}

bcf_hdr_t* VCFV2::hdr() const {
  return hdr_;
}

bool VCFV2::seek(const std::string& contig_name, uint32_t pos) {
  if (!open_)
    return false;

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

  // Buffer the next records into `buffer_`.
  read_records();

  // Return true if any records were buffered.
  return buffer_.size() > 0;
}

void VCFV2::read_records() {
  // Reset the buffer but don't destroy it, so we can reuse any allocated
  // record structs.
  reset_buffer();

  std::unique_ptr<bcf1_t, decltype(&bcf_destroy)> curr_rec(
      bcf_init1(), bcf_destroy);
  uint64_t num_records = 0;
  std::string first_contig_name;
  while (num_records < max_record_buffer_size_) {
    if (!record_iter_.next(curr_rec.get()))
      break;

    // Iteration does not cross contigs.
    std::string contig_name = bcf_seqname(hdr_, curr_rec.get());
    if (first_contig_name.empty()) {
      first_contig_name = contig_name;
    } else if (first_contig_name != contig_name) {
      break;
    }

    // Check to see if an old record was allocated that we can reuse.
    bcf1_t** record_ptrs = buffer_.data<bcf1_t*>();
    if ((num_records + 1) * sizeof(bcf1_t*) <= buffer_.alloced_size() &&
        record_ptrs[num_records] != nullptr) {
      auto* dst = record_ptrs[num_records];
      bcf_copy(dst, curr_rec.get());
      bcf_unpack(dst, BCF_UN_ALL);
      // Update the size (zeroing out extra space).
      buffer_.resize(buffer_.size() + sizeof(bcf1_t*), true);
    } else {
      auto* dup = bcf_dup(curr_rec.get());
      bcf_unpack(dup, BCF_UN_ALL);
      // Note: appending pointers here, not structs. Reserve space first so
      // any extra alloced space gets zeroed.
      buffer_.reserve((num_records + 1) * sizeof(bcf1_t*), true);
      buffer_.append(&dup, sizeof(bcf1_t*));
    }

    num_records++;
  }

  buffer_offset_ = 0;
}

void VCFV2::destroy_buffer() {
  const size_t num_possible_records = buffer_.alloced_size() / sizeof(bcf1_t*);
  bcf1_t** record_ptrs = buffer_.data<bcf1_t*>();
  for (size_t i = 0; i < num_possible_records; i++) {
    auto* r = record_ptrs[i];
    if (r != nullptr)
      bcf_destroy(r);
  }
  std::memset(record_ptrs, 0, buffer_.alloced_size());
  reset_buffer();
}

void VCFV2::reset_buffer() {
  buffer_.clear();
  buffer_offset_ = 0;
}

void VCFV2::swap(VCFV2& other) {
  std::swap(open_, other.open_);
  std::swap(path_, other.path_);
  std::swap(index_path_, other.index_path_);
  buffer_.swap(other.buffer_);
  std::swap(buffer_offset_, other.buffer_offset_);
  record_iter_.swap(other.record_iter_);
  std::swap(hdr_, other.hdr_);
  std::swap(index_tbx_, other.index_tbx_);
  std::swap(index_hts_, other.index_hts_);
}

/* ****************************** */
/*              Iter              */
/* ****************************** */

VCFV2::Iter::Iter()
    : fh_(nullptr, hts_close)
    , hts_iter_(nullptr)
    , tbx_(nullptr) {
}

VCFV2::Iter::~Iter() {
  reset();
}

bool VCFV2::Iter::init_bcf(
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

  hts_iter_ = bcf_itr_queryi(index, region_id, region_min, region_max);
  if (hts_iter_ == nullptr)
    return false;

  return true;
}

bool VCFV2::Iter::init_tbx(
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

void VCFV2::Iter::reset() {
  fh_.reset();
  hdr_ = nullptr;

  if (hts_iter_ != nullptr) {
    hts_itr_destroy(hts_iter_);
    hts_iter_ = nullptr;
  }

  tbx_ = nullptr;

  if (tmps_.m) {
    free(tmps_.s);
    tmps_ = {0, 0, nullptr};
  }
}

void VCFV2::Iter::swap(VCFV2::Iter& other) {
  std::swap(fh_, other.fh_);
  std::swap(hdr_, other.hdr_);
  std::swap(hts_iter_, other.hts_iter_);
  std::swap(tbx_, other.tbx_);
  std::swap(tmps_, other.tmps_);
}

bool VCFV2::Iter::next(bcf1_t* rec) {
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
