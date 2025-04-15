/**
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020 TileDB, Inc.
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

#include "vcf/vcf_utils.h"

namespace tiledb {
namespace vcf {

uint32_t VCFUtils::get_end_pos(
    const bcf_hdr_t* hdr, bcf1_t* rec, HtslibValueMem* val) {
  // Set the default value of END, in case it is not present in the VCF
  uint32_t end = rec->pos + rec->rlen - 1;

  int* dst = nullptr;
  int ndst = 0;

  // Check if END is present as an integer
  int ret = bcf_get_info_int32(hdr, rec, "END", &dst, &ndst);
  if (ret >= 0) {
    end = *dst - 1;
  } else {
    // Check if END is present as a string
    ret = bcf_get_info_string(hdr, rec, "END", &dst, &ndst);
    if (ret >= 0) {
      std::string end_str = reinterpret_cast<char*>(dst);
      try {
        end = std::stoi(end_str) - 1;
      } catch (std::invalid_argument const& e) {
        throw std::runtime_error(
            "Error parsing END field as an integer: " + end_str);
      }
    }
  }

  hts_free(dst);

  return end;
}

bcf_hdr_t* VCFUtils::hdr_read_header(const std::string& path) {
  auto fh = vcf_open(path.c_str(), "r");

  int retries = 3;
  while (!fh && retries--) {
    fh = vcf_open(path.c_str(), "r");
  }

  if (!fh)
    return nullptr;
  auto hdr = bcf_hdr_read(fh);
  vcf_close(fh);
  return hdr;
}

std::vector<std::string> VCFUtils::get_sample_name_from_vcf(
    const std::string& path) {
  SafeBCFHdr hdr(hdr_read_header(path), bcf_hdr_destroy);
  auto samples = hdr_get_samples(hdr.get());
  // If there are no samples, add an empty string to the list
  // to indicate this is a sampleless VCF.
  if (samples.empty()) {
    samples.push_back("");
  }
  return samples;
}

std::vector<std::string> VCFUtils::hdr_get_samples(bcf_hdr_t* hdr) {
  if (!hdr)
    throw std::invalid_argument(
        "Cannot get samples from header; bad VCF header.");
  std::vector<std::string> ret;
  const auto nsamp = bcf_hdr_nsamples(hdr);
  for (int i = 0; i < nsamp; ++i) {
    std::string normalized;
    if (!VCFUtils::normalize_sample_name(hdr->samples[i], &normalized))
      throw std::runtime_error(
          "Cannot get samples from header; VCF header contains sample name "
          "with invalid characters: '" +
          std::string(hdr->samples[i]) + "'");
    ret.push_back(normalized);
  }
  return ret;
}

std::string VCFUtils::hdr_to_string(bcf_hdr_t* hdr) {
  if (!hdr)
    throw std::invalid_argument(
        "Cannot convert header to string; bad VCF header.");
  kstring_t t = {0, 0, 0};
  auto tmp = bcf_hdr_dup(hdr);

  int res = 0;
  res = bcf_hdr_set_samples(tmp, 0, 0);
  if (res != 0) {
    if (res == -1) {
      throw std::invalid_argument(
          "Cannot set VCF samples; possibly bad VCF header.");
    } else if (res > 0) {
      throw std::runtime_error(
          std::string("Cannot set VCF samples: list contains samples not "
                      "present in VCF header, sample #:") +
          std::to_string(res));
    }
  }
  bcf_hdr_format(tmp, 0, &t);
  std::string ret(t.s, t.l);
  bcf_hdr_destroy(tmp);
  hts_free(t.s);
  return ret;
}

std::map<std::string, uint32_t> VCFUtils::hdr_get_contig_offsets(
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

std::vector<Region> VCFUtils::hdr_get_contigs_regions(bcf_hdr_t* hdr) {
  std::vector<Region> contigs;
  if (!hdr)
    throw std::invalid_argument(
        "Cannot get contig offsets from header; bad VCF header.");
  int nseq;
  const char** seqnames = bcf_hdr_seqnames(hdr, &nseq);
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
    uint32_t length = std::numeric_limits<uint32_t>::max() - 1;
    if (j >= 0) {
      length = strtol(hrec->vals[j], nullptr, 10);
    }

    Region region;
    region.seq_name = seqname;
    region.min = 0;
    region.max = length;

    contigs.push_back(region);
  }
  hts_free(seqnames);
  return contigs;
}

bool VCFUtils::normalize_sample_name(
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

}  // namespace vcf
}  // namespace tiledb
