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

#include "vcf/vcf.h"
#include "utils/sample_utils.h"

namespace tiledb {
namespace vcf {

VCF::VCF()
    : open_(false)
    , path_("")
    , index_path_("")
    , buffer_offset_(0)
    , max_record_buffer_size_(10000)
    , hdr_(nullptr)
    , index_tbx_(nullptr)
    , index_hts_(nullptr) {
}

VCF::~VCF() {
  close();
}

void VCF::open(const std::string& file, const std::string& index_file) {
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

void VCF::close() {
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

bool VCF::is_open() const {
  return open_;
}

bool VCF::next() {
  if (!open_)
    return false;

  buffer_offset_ += sizeof(bcf1_t*);

  if (buffer_offset_ < buffer_.size()) {
    return true;
  } else if (
      static_cast<uint64_t>(buffered_region_.max) + 1 >=
      std::numeric_limits<int>::max()) {
    // False on overflow
    return false;
  } else {
    // Seek to the first record after the old max
    return seek(buffered_region_.seq_name, buffered_region_.max + 1);
  }
}

std::string VCF::contig_name() const {
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

std::string VCF::sample_name() const {
  if (!open_)
    throw std::runtime_error(
        "Error getting sample name from VCF; file not open.");
  if (hdr_ == nullptr || bcf_hdr_nsamples(hdr_) == 0)
    throw std::runtime_error(
        "Error getting sample name from VCF; header has no samples.");
  std::string unnormalized_name(hdr_->samples[0]);
  std::string name;
  if (!normalize_sample_name(unnormalized_name, &name))
    throw std::runtime_error(
        "Error getting sample name from VCF; sample name has invalid "
        "characters: '" +
        std::string(unnormalized_name) + "'");
  return name;
}

bool VCF::contig_has_records(const std::string& contig_name) const {
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

void VCF::set_max_record_buff_size(uint64_t max_record_buffer_size) {
  max_record_buffer_size_ = max_record_buffer_size;
}

bcf1_t* VCF::curr_rec() const {
  if (buffer_.size() == 0)
    return nullptr;
  return buffer_.value<bcf1_t*>(buffer_offset_ / sizeof(bcf1_t*));
}

bcf_hdr_t* VCF::hdr() const {
  return hdr_;
}

bool VCF::seek(const std::string& contig_name, uint32_t pos) {
  if (!open_)
    return false;

  SafeBCFFh fh(bcf_open(path_.c_str(), "r"), hts_close);
  if (fh == nullptr)
    throw std::runtime_error("Error seeking in VCF; bcf_open failed");

  Iter iter(fh.get(), hdr_);
  if (fh->format.format == bcf) {
    if (!iter.init(hdr_, index_hts_, contig_name, pos))
      return false;

  } else {
    if (fh->format.format != ::vcf)
      throw std::runtime_error("Error seeking in VCF; unknown format.");

    if (!iter.init(index_tbx_, contig_name, pos))
      return false;
  }

  // Buffer records in the given region into memory.
  read_records(&iter);

  // Update the buffered region info.
  if (buffer_.size() > 0) {
    HtslibValueMem val;
    bcf1_t* first = buffer_.value<bcf1_t*>(0);
    bcf1_t* last =
        buffer_.value<bcf1_t*>((buffer_.size() / sizeof(bcf1_t*)) - 1);
    buffered_region_.seq_name = bcf_seqname(hdr_, first);
    buffered_region_.min = first->pos;
    buffered_region_.max = get_end_pos(hdr_, last, &val);
    // Sanity check contigs are the same
    if (buffered_region_.seq_name != bcf_seqname(hdr_, last))
      throw std::runtime_error(
          "Cannot seek iterator to '" + contig_name + ":" +
          std::to_string(pos) + "'; unexpected contig name mismatch.");
  }

  // Return true if any records were buffered.
  return buffer_.size() > 0;
}

void VCF::read_records(Iter* iter) {
  // Reset the buffer but don't destroy it, so we can reuse any allocated
  // record structs.
  reset_buffer();

  std::unique_ptr<bcf1_t, decltype(&bcf_destroy)> curr_rec(
      bcf_init1(), bcf_destroy);
  uint64_t num_records = 0;
  std::string first_contig_name;
  while (num_records < max_record_buffer_size_) {
    if (!iter->next(curr_rec.get()))
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

void VCF::destroy_buffer() {
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

void VCF::reset_buffer() {
  buffer_.clear();
  buffer_offset_ = 0;
}

uint32_t VCF::get_end_pos(bcf_hdr_t* hdr, bcf1_t* rec, HtslibValueMem* val) {
  val->ndst = HtslibValueMem::convert_ndst_for_type(
      val->ndst, BCF_HT_INT, &val->type_for_ndst);
  int rc =
      bcf_get_info_values(hdr, rec, "END", &val->dst, &val->ndst, BCF_HT_INT);
  if (rc > 0) {
    assert(val->dst);
    assert(val->ndst >= 1);
    uint32_t copy = ((uint32_t*)val->dst)[0];
    assert(copy > 0);
    copy -= 1;
    return copy;
  } else {
    return (uint32_t)rec->pos + rec->rlen - 1;
  }
}

bcf_hdr_t* VCF::hdr_read_header(const std::string& path) {
  auto fh = vcf_open(path.c_str(), "r");
  if (!fh)
    return nullptr;
  auto hdr = bcf_hdr_read(fh);
  vcf_close(fh);
  return hdr;
}

std::vector<std::string> VCF::hdr_get_samples(bcf_hdr_t* hdr) {
  if (!hdr)
    throw std::invalid_argument(
        "Cannot get samples from header; bad VCF header.");
  std::vector<std::string> ret;
  const auto nsamp = bcf_hdr_nsamples(hdr);
  for (int i = 0; i < nsamp; ++i) {
    std::string normalized;
    if (!VCF::normalize_sample_name(hdr->samples[i], &normalized))
      throw std::runtime_error(
          "Cannot get samples from header; VCF header contains sample name "
          "with invalid characters: '" +
          std::string(hdr->samples[i]) + "'");
    ret.push_back(normalized);
  }
  return ret;
}

std::string VCF::hdr_to_string(bcf_hdr_t* hdr) {
  if (!hdr)
    throw std::invalid_argument(
        "Cannot convert header to string; bad VCF header.");
  kstring_t t = {0, 0, 0};
  auto tmp = bcf_hdr_dup(hdr);
  bcf_hdr_set_samples(tmp, 0, 0);
  bcf_hdr_format(tmp, 0, &t);
  std::string ret(t.s, t.l);
  bcf_hdr_destroy(tmp);
  free(t.s);
  return ret;
}

std::map<std::string, uint32_t> VCF::hdr_get_contig_offsets(
    bcf_hdr_t* hdr, std::map<std::string, uint32_t>* contig_lengths) {
  std::map<std::string, uint32_t> offsets;
  if (!hdr)
    throw std::invalid_argument(
        "Cannot get contig offsets from header; bad VCF header.");
  int nseq;
  const char** seqnames = bcf_hdr_seqnames(hdr, &nseq);
  uint32_t curr = 0;
  for (int i = 0; i < nseq; ++i) {
    std::string seqname(seqnames[i]);
    bcf_hrec_t* hrec =
        bcf_hdr_get_hrec(hdr, BCF_HL_CTG, "ID", seqname.c_str(), 0);
    if (!hrec)
      throw std::invalid_argument(
          "Cannot get contig offsets from header; error reading contig header "
          "line " +
          std::to_string(i));
    int j = bcf_hrec_find_key(hrec, "length");
    if (j < 0)
      throw std::invalid_argument(
          "Cannot get contig offsets from header; contig def does not have "
          "length");
    auto length = strtol(hrec->vals[j], nullptr, 10);
    offsets[seqname] = curr;
    (*contig_lengths)[seqname] = length;
    curr += length;
  }
  free(seqnames);
  return offsets;
}

bool VCF::normalize_sample_name(
    const std::string& sample, std::string* normalized) {
  if (sample.empty())
    return false;

  // Check for invalid chars
  const size_t num_invalid = 3;
  const char invalid_char_list[num_invalid] = {',', '\t', '\0'};
  if (sample.find_first_of(invalid_char_list, 0, num_invalid) !=
      std::string::npos)
    return false;

  // Trim leading/trailing whitespace
  const std::string whitespace_chars = " \t\n\r\v\f";
  auto first_non_wsp = sample.find_first_not_of(whitespace_chars);
  auto last_non_wsp = sample.find_last_not_of(whitespace_chars);
  if (first_non_wsp == std::string::npos)
    return false;

  if (normalized != nullptr) {
    *normalized =
        sample.substr(first_non_wsp, last_non_wsp - first_non_wsp + 1);
  }

  return true;
}

void VCF::swap(VCF& other) {
  std::swap(open_, other.open_);
  std::swap(path_, other.path_);
  std::swap(index_path_, other.index_path_);
  buffer_.swap(other.buffer_);
  std::swap(buffer_offset_, other.buffer_offset_);
  std::swap(buffered_region_, other.buffered_region_);
  std::swap(hdr_, other.hdr_);
  std::swap(index_tbx_, other.index_tbx_);
  std::swap(index_hts_, other.index_hts_);
}

/* ****************************** */
/*              Iter              */
/* ****************************** */

VCF::Iter::Iter(htsFile* fh, bcf_hdr_t* hdr)
    : fh_(fh)
    , hdr_(hdr)
    , iter_(nullptr)
    , tbx_(nullptr) {
}

VCF::Iter::~Iter() {
  if (iter_ != nullptr)
    hts_itr_destroy(iter_);
  if (tmps_.m) {
    free(tmps_.s);
    tmps_ = {0, 0, nullptr};
  }
}

bool VCF::Iter::init(
    bcf_hdr_t* hdr,
    hts_idx_t* index,
    const std::string& contig_name,
    uint32_t pos) {
  if (contig_name.empty())
    throw std::runtime_error(
        "Failed to init BCF iterator; contig name cannot be empty.");

  int region_id = bcf_hdr_name2id(hdr, contig_name.c_str());
  if (region_id == -1)
    return false;

  int region_min = pos;
  int region_max = std::numeric_limits<int>::max();

  iter_ = bcf_itr_queryi(index, region_id, region_min, region_max);
  if (iter_ == nullptr)
    return false;

  return true;
}

bool VCF::Iter::init(
    tbx_t* index, const std::string& contig_name, uint32_t pos) {
  if (contig_name.empty())
    throw std::runtime_error(
        "Failed to init TBX iterator; contig name cannot be empty.");

  int region_id = tbx_name2id(index, contig_name.c_str());
  if (region_id == -1)
    return false;

  int region_min = pos;
  int region_max = std::numeric_limits<int>::max();

  tbx_ = index;
  iter_ = tbx_itr_queryi(index, region_id, region_min, region_max);
  if (iter_ == nullptr)
    return false;

  return true;
}

bool VCF::Iter::next(bcf1_t* rec) {
  int ret;

  if (tbx_ == nullptr) {
    ret = bcf_itr_next(fh_, iter_, rec);
  } else {
    ret = tbx_itr_next(fh_, tbx_, iter_, &tmps_);
    vcf_parse1(&tmps_, hdr_, rec);
  }

  return ret >= 0;
}

}  // namespace vcf
}  // namespace tiledb