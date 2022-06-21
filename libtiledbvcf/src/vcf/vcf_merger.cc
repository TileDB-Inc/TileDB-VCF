#include "vcf_merger.h"

namespace tiledb {
namespace vcf {

//===================================================================
//= public functions
//===================================================================

VCFMerger::VCFMerger()
    : hdr_(nullptr, bcf_hdr_destroy)
    , rec_(bcf_init(), bcf_destroy) {
}

VCFMerger::~VCFMerger() {
}

void VCFMerger::init(
    const std::vector<std::pair<std::string, size_t>>& sorted_hdrs,
    const std::unordered_map<uint32_t, SafeBCFHdr>& hdr_map) {
  hdr_.reset(bcf_hdr_init("w"));
  hdrs_.clear();

  for (const auto& [name, hdr_key] : sorted_hdrs) {
    LOG_DEBUG("Adding sample_num {}: {}", hdrs_.size(), name);
    sample_map_[name] = hdrs_.size();
    auto hdr = hdr_map.at(hdr_key).get();
    hdrs_.push_back(hdr);
    if (bcf_hdr_merge(hdr_.get(), hdr) == NULL) {
      LOG_FATAL("Error merging header from sample: {}", name);
    }
    if (bcf_hdr_add_sample(hdr_.get(), name.c_str()) < 0) {
      LOG_FATAL("Error adding sample to merged header: {}", name);
    }
  }

  if (bcf_hdr_sync(hdr_.get()) < 0) {
    LOG_FATAL("Error merging headers.");
  }

  num_samples_ = bcf_hdr_nsamples(hdr_.get());
  pass_filter_id_ = bcf_hdr_id2int(hdr_.get(), BCF_DT_ID, "PASS");
  LOG_INFO("VCFMerger: Number of samples = {}", num_samples_);
}

void VCFMerger::reset() {
  sample_map_.clear();
  num_samples_ = -1;
  contig_ = -1;
}

void VCFMerger::finish() {
  try_merge(true);
}

void VCFMerger::close() {
  if (merge_buffer_.size()) {
    LOG_ERROR("VCFMerger closed but merge buffer is not empty.");
  }
  if (output_buffer_.size()) {
    LOG_ERROR("VCFMerger closed but output buffer is not empty.");
  }
  LOG_INFO(
      "VCFMerger closed: {} records in {} records out",
      write_count_,
      read_count_);

  if (dst_) {
    free(dst_);
  }
}

void VCFMerger::write(const std::string& sample_name, SafeBCFRec rec) {
  write_count_++;

  auto contig = rec->rid;

  // If record is from a new contig, merge and flush all data from prev contig
  if (contig != contig_) {
    try_merge(true);
    contig_ = contig;
  }

  auto sample_num = sample_map_[sample_name];
  // Write new record to merge buffer
  merge_buffer_.push_back({std::move(rec), sample_num});

  // Merge without flushing
  try_merge(false);
}

SafeBCFRec VCFMerger::read() {
  read_count_++;
  auto result = std::move(output_buffer_.front());
  output_buffer_.pop_front();
  return result;
}

bool VCFMerger::is_empty() {
  return output_buffer_.empty();
}

//===================================================================
//= private functions
//===================================================================

// Remove matching bases from the end of all alleles
// based on https://github.com/samtools/bcftools/blob/develop/vcfmerge.c
static void normalize_alleles(char** als, int nals) {
  if (strlen(als[0]) == 1) {
    return;
  }

  int min_length = INT_MAX;
  std::vector<int> lens(nals);
  for (int i = 0; i < nals; i++) {
    lens[i] = strlen(als[i]);
    min_length = std::min(min_length, lens[i]);
  }

  // find number of matching bases at the end of all alleles
  bool done = false;
  int i = 1;
  while (!done && i < min_length) {
    for (int j = 1; j < nals; j++) {
      if (als[j][lens[j] - i] != als[0][lens[0] - i]) {
        done = true;
        break;
      }
    }
    if (!done) {
      i++;
    }
  }
  i--;

  // trim i bases from the end of each allele
  if (i > 0) {
    als[0][lens[0] - i] = 0;
    for (int j = 1; j < nals; j++) {
      als[j][lens[j] - i] = 0;
    }
  }
}

// based on https://github.com/samtools/bcftools/blob/develop/vcfmerge.c
void VCFMerger::merge_alleles(int sample_num, bcf1_t* input) {
  normalize_alleles(input->d.allele, input->n_allele);
  md_.suffix = "";
  if (md_.ref == "") {
    md_.ref = input->d.allele[0];
  } else if (md_.ref.compare(input->d.allele[0]) != 0) {
    std::string new_ref = input->d.allele[0];

    if (new_ref.length() > md_.ref.length()) {
      // new REF is longer, extend existing merged alleles
      md_.suffix = new_ref.substr(md_.ref.length());
      md_.ref = new_ref;
      for (auto& allele : md_.alleles) {
        // do not extend <NON_REF> alleles
        if (allele[0] != '<') {
          allele += md_.suffix;
        }
      }
      md_.suffix = "";
    } else if (new_ref.length() < md_.ref.length()) {
      // old REF is longer, extend new alleles when added below
      md_.suffix = md_.ref.substr(new_ref.length());
    } else {
      LOG_WARN(
          "REF mismatch at {}:{}, {} != {}",
          input->rid,
          input->pos,
          input->d.allele[0],
          md_.ref);
    }
  }

  // update allele map: sample allele index -> merged allele index
  // ref (index 0) always maps to 0
  md_.allele_maps[sample_num][0] = 0;
  for (int i = 1; i < input->n_allele; i++) {
    std::string allele = input->d.allele[i];
    // extend allele and add to allele vector if unique
    int index;
    if (allele[0] == '<') {
      // do not extend <NON_REF> alleles
      index = utils::push_unique(md_.alleles, allele);
    } else {
      index = utils::push_unique(md_.alleles, allele + md_.suffix);
    }

    // update allele map
    // need index + 1 because ref is index 0
    md_.allele_maps[sample_num][i] = index + 1;
  }
}

std::tuple<int, int, int> VCFMerger::get_number_type_values(
    int id, int hdr_type, int sample_num) {
  auto hdr = sample_num > 0 ? hdrs_[sample_num] : hdr_.get();
  int number = bcf_hdr_id2length(hdr, hdr_type, id);
  int type = bcf_hdr_id2type(hdr, hdr_type, id);

  int num_alleles = md_.alleles.size();
  int values = -1;

  switch (number) {
    case BCF_VL_FIXED:
      values = bcf_hdr_id2number(hdr_.get(), hdr_type, id);
      break;
    case BCF_VL_VAR:
      values = 0;
      break;
    case BCF_VL_A:
      values = num_alleles;
      break;
    case BCF_VL_G:
      values = (num_alleles + 1) * (num_alleles + 2) / 2;
      break;
    case BCF_VL_R:
      values = num_alleles + 1;
      break;
    default:
      LOG_FATAL("Unknown NUMBER code = {}", number);
  }

  return {number, type, values};
}

std::tuple<int, int> VCFMerger::get_missing_vector_end(int type) {
  int missing = bcf_int32_missing;
  int vector_end = bcf_int32_vector_end;

  if (type == BCF_HT_REAL) {
    missing = bcf_float_missing;
    vector_end = bcf_float_vector_end;
  } else if (type == BCF_HT_STR) {
    missing = bcf_str_missing;
    vector_end = bcf_str_vector_end;
  }

  return {missing, vector_end};
}

bool VCFMerger::can_merge_record(SafeBCFRec& record) {
  // Add more complex merging logic here
  return true;
}

void VCFMerger::merge_record(int sample_num, SafeBCFRec input) {
  md_.merged_samples.insert(sample_num);

  // CHROM and POS
  md_.rid = input->rid;
  md_.pos = input->pos;

  // ID: add non-empty ID to set of merged IDs
  std::string id = input->d.id;
  if (id.compare(".") != 0) {
    utils::push_unique(md_.ids, id);
  }

  // REF, ALT
  merge_alleles(sample_num, input.get());

  // QUAL
  if (!bcf_float_is_missing(input->qual)) {
    // max of all QUALs
    if (bcf_float_is_missing(md_.qual) || md_.qual < input->qual) {
      md_.qual = input->qual;
    }
  }

  // FILTER
  for (int i = 0; i < input->d.n_flt; i++) {
    utils::push_unique(md_.filters, input->d.flt[i]);
  }

  // INFO and FORMAT handled in finish_merge

  // mark format id as present in the formats
  // TODO: Switch to string version of format id, since samples may use
  // different VCF headers with different id numbers
  for (int i = 0; i < input->n_fmt; i++) {
    utils::push_unique(md_.format_keys, input->d.fmt[i].id);
  }

  // mark sample_num as present in the merged data
  md_.samples.push_back({sample_num, std::move(input)});
}

void VCFMerger::finish_info(SafeBCFRec& rec) {
  // merge FORMAT:GT and INFO:AC,AN,DP
  for (const auto& [sample_num, rec] : md_.samples) {
    int values_read =
        bcf_get_genotypes(hdrs_[sample_num], rec.get(), &dst_, &ndst_);

    if (values_read == 1) {
      md_.gts[2 * sample_num + 1] = bcf_int32_vector_end;
    }

    // update gt
    for (int i = 0; i < values_read; i++) {
      if (bcf_gt_is_missing(dst_[i])) {
        continue;
      }

      int update = dst_[i];

      // no update required for 1 alt allele
      if (md_.alleles.size() > 1) {
        int index = md_.allele_maps[sample_num][bcf_gt_allele(dst_[i])];
        update = bcf_gt_is_phased(dst_[i]) ? bcf_gt_phased(index) :
                                             bcf_gt_unphased(index);
      }

      md_.gts[2 * sample_num + i] = update;

      // update allele count for ALT alleles
      unsigned int allele = bcf_gt_allele(update);
      if (allele > 0) {
        if (md_.allele_count.size() < allele) {
          md_.allele_count.resize(allele);
        }
        md_.allele_count[allele - 1]++;
      }
      md_.allele_total++;
    }

    // add sample read depth to total depth
    if (bcf_get_info_int32(hdrs_[sample_num], rec.get(), "DP", &dst_, &ndst_) >
        0) {
      md_.depth_total += *dst_;
    }
  }

  for (const auto& [sample_num, rec] : md_.samples) {
    for (int i = 0; i < rec->n_info; i++) {
      const bcf_info_t* info = &rec->d.info[i];
      int key = info->key;
      const char* key_str = hdr_->id[BCF_DT_ID][key].key;

      auto [number, type, values] = get_number_type_values(key, BCF_HL_INFO);
      auto [missing, vector_end] = get_missing_vector_end(type);
      (void)vector_end;  // unused

      int values_read = bcf_get_info_values(
          hdrs_[sample_num], rec.get(), key_str, (void**)(&dst_), &ndst_, type);
      int* data = (int*)(dst_);

      // set expected values to number of values read
      if (number == BCF_VL_VAR || type == BCF_HT_FLAG) {
        values = values_read;

        // data contains 4 char values per int
        if (type == BCF_HT_STR) {
          values = ceil(values / 4.0);
        }
      }

      if (number == BCF_VL_FIXED || number == BCF_VL_VAR) {
        // merge first value seen in sample order
        if (md_.info[key].size() == 0) {
          for (int j = 0; j < values; j++) {
            md_.info[key].push_back(*data++);
          }
        }
      } else if (number == BCF_VL_A || number == BCF_VL_R) {
        // if type=string, merge first value seen in sample order
        if (type == BCF_HT_STR && md_.info[key].size()) {
          continue;
        }
        // merge last value seen in sample order
        md_.info[key].resize(values, missing);
        int from = number == BCF_VL_A ? 1 : 0;
        for (int ai = from; ai < rec->n_allele; ai++) {
          int new_ai = md_.allele_maps[sample_num][ai] - from;
          md_.info[key][new_ai] = *data++;
        }
      } else if (number == BCF_VL_G) {
        if (type == BCF_HT_STR) {
          // TODO: support INFO Number=G Type=String|Character
          LOG_WARN(
              "INFO {} Number=G and Type=String|Charater not supported yet",
              key_str);
          // auto strings = utils::split(std::string((char*)(dst_)), ',');
          continue;
        }
        // merge last value seen in sample order
        md_.info[key].resize(values, missing);
        for (int ai = 0; ai < values_read; ai++) {
          int a, b;
          bcf_gt2alleles(ai, &a, &b);
          a = md_.allele_maps[sample_num][a];
          b = md_.allele_maps[sample_num][b];
          md_.info[key][bcf_alleles2gt(a, b)] = *data++;
        }
      }
    }
  }

  for (const auto& [key, info_data] : md_.info) {
    const char* key_str = hdr_->id[BCF_DT_ID][key].key;

    // update END if less than POS + len(REF)
    if (!strcmp(key_str, "END")) {
      md_.info[key][0] = std::max(
          md_.info[key][0], static_cast<uint32_t>(md_.pos + md_.ref.size()));
    }

    auto [number, type, values] = get_number_type_values(key, BCF_HL_INFO);
    auto data = info_data.data();
    if (type == BCF_HT_FLAG) {
      values = 1;
      data = nullptr;
    }

    // for variable length field, set expected values to number of values read
    if (number == BCF_VL_VAR) {
      values = info_data.size();
    }

    bcf_update_info(
        hdr_.get(), rec.get(), key_str, (void*)(data), values, type);
  }

  if (md_.depth_total) {
    bcf_update_info_int32(hdr_.get(), rec.get(), "DP", &md_.depth_total, 1);
  }

  bcf_update_info_int32(hdr_.get(), rec.get(), "AN", &md_.allele_total, 1);

  if (md_.allele_count.size() < md_.alleles.size()) {
    md_.allele_count.resize(md_.alleles.size(), 0);
  }
  bcf_update_info_int32(
      hdr_.get(),
      rec.get(),
      "AC",
      md_.allele_count.data(),
      md_.allele_count.size());
}

void VCFMerger::finish_format(SafeBCFRec& rec) {
  bcf_update_genotypes(hdr_.get(), rec.get(), md_.gts.data(), md_.gts.size());

  std::vector<std::string> sample_strings;
  int max_string_len = 0;

  // loop through all FORMAT fields in merged records
  for (const auto key : md_.format_keys) {
    const char* key_str = hdr_->id[BCF_DT_ID][key].key;

    // GT is handled separately in the code above
    if (!strcmp(key_str, "GT")) {
      continue;
    }

    auto [number, type, values] = get_number_type_values(key, BCF_HL_FMT);
    auto [missing, vector_end] = get_missing_vector_end(type);

    // init buffer with the expected number of missing/vector end values.
    // for variable length fields, init buffer after reading the first field
    if (number != BCF_VL_VAR) {
      buffer_.clear();

      // fill buffer with empty values
      for (int i = 0; i < num_samples_; i++) {
        // add missing values
        buffer_.append(&missing, utils::bcf_type_size(type));
        for (int j = 1; j < values; j++) {
          buffer_.append(&vector_end, utils::bcf_type_size(type));
        }
      }
    }

    if (type == BCF_HT_STR) {
      sample_strings = std::vector<std::string>(num_samples_, ".");
      max_string_len = 1;
    }

    bool first_variable_length = true;
    for (const auto& [sample_num, rec_in] : md_.samples) {
      int values_read = bcf_get_format_values(
          hdrs_[sample_num],
          rec_in.get(),
          key_str,
          (void**)(&dst_),
          &ndst_,
          type);

      if (values_read <= 0) {
        continue;
      }

      // for variable length field, set expected values to number of values
      // read
      if (number == BCF_VL_VAR) {
        values = values_read;

        // init buffer with the expected number of missing/vector end values
        if (first_variable_length) {
          buffer_.clear();
          // fill buffer with empty values
          for (int i = 0; i < num_samples_; i++) {
            // add missing values
            buffer_.append(&missing, utils::bcf_type_size(type));
            for (int j = 1; j < values; j++) {
              buffer_.append(&vector_end, utils::bcf_type_size(type));
            }
          }
          first_variable_length = false;
        }
      }

      int* src = dst_;
      auto dst = buffer_.data<int>();

      // merge string
      if (type == BCF_HT_STR) {
        values_read -= 1;
        sample_strings[sample_num] = std::string((char*)(dst_), values_read);
        max_string_len = std::max(max_string_len, values_read);
      } else if (
          number == BCF_VL_FIXED || number == BCF_VL_VAR ||
          md_.alleles.size() == 1) {
        // allele indexes unchanged, copy values
        for (int j = 0; j < values_read; j++) {
          dst[sample_num * values + j] = *src++;
        }
      } else {
        // set all expected values as missing
        for (int j = 0; j < values; j++) {
          dst[sample_num * values + j] = missing;
        }

        // update allele indexes with allele_map
        if (number == BCF_VL_A || number == BCF_VL_R) {
          int from = number == BCF_VL_A ? 1 : 0;
          for (int ai = 0; ai < values_read; ai++) {
            int new_ai = md_.allele_maps[sample_num][ai + from] - from;
            dst[sample_num * values + new_ai] = *src++;
          }
        } else if (number == BCF_VL_G) {
          for (int ai = 0; ai < values_read; ai++) {
            int a, b;
            bcf_gt2alleles(ai, &a, &b);
            a = md_.allele_maps[sample_num][a];
            b = md_.allele_maps[sample_num][b];
            dst[sample_num * values + bcf_alleles2gt(a, b)] = *src++;
          }
        }
      }
    }

    if (type == BCF_HT_STR) {
      // create array of strings with same length for each sample
      std::string buffer;
      buffer.reserve(num_samples_ * max_string_len);
      for (auto& str : sample_strings) {
        // pad with 0s
        str.append(max_string_len - str.size(), '\0');
        buffer.append(str.data(), str.size());
      }
      bcf_update_format(
          hdr_.get(), rec.get(), key_str, buffer.c_str(), buffer.size(), type);
    } else {
      bcf_update_format(
          hdr_.get(),
          rec.get(),
          key_str,
          buffer_.data<void>(),
          values * num_samples_,
          type);
    }
  }
}

void VCFMerger::finish_merge() {
  SafeBCFRec rec(bcf_init(), bcf_destroy);

  // CHROM, POS
  rec->rid = md_.rid;
  rec->pos = md_.pos;

  // ID
  std::string ids =
      md_.ids.size() ? fmt::format("{}", fmt::join(md_.ids, ";")) : ".";
  bcf_update_id(hdr_.get(), rec.get(), ids.c_str());

  // REF, ALT
  std::string alleles = md_.ref;
  for (auto& allele : md_.alleles) {
    alleles += "," + allele;
  }
  bcf_update_alleles_str(hdr_.get(), rec.get(), alleles.c_str());

  // QUAL
  rec->qual = md_.qual;

  // FILTER
  // if there are multiple filters, remove the PASS filter
  if (md_.filters.size() > 1) {
    md_.filters.erase(
        std::remove(md_.filters.begin(), md_.filters.end(), pass_filter_id_),
        md_.filters.end());
  }
  bcf_update_filter(
      hdr_.get(), rec.get(), md_.filters.data(), md_.filters.size());

  // INFO, FORMAT
  finish_info(rec);
  finish_format(rec);

  // move merged record to output buffer
  output_buffer_.push_back(std::move(rec));
}

void VCFMerger::merge_records() {
  auto start = merge_buffer_.front().record->pos;

  // While more records to be merged
  while (!merge_buffer_.empty() && merge_buffer_.front().record->pos == start) {
    md_.reset(num_samples_);

    for (auto it = merge_buffer_.begin();
         it != merge_buffer_.end() && it->record->pos == start;) {
      // If not the first record being merged
      if (md_.merged_samples.size()) {
        // Skip record if merged data already includes a record from this sample
        if (md_.merged_samples.count(it->sample_num)) {
          it++;
          continue;
        }

        // Placeholder for more complex merging logic
        /*
        if (!can_merge_record(it->record)) {
          it++;
          continue;
        }
        */
      }

      merge_record(it->sample_num, std::move(it->record));
      it = merge_buffer_.erase(it);
    }
    finish_merge();
  }
}

void VCFMerger::try_merge(bool flush) {
  // Return if the merge buffer is empty (nothing to do) OR
  // if we are not flushing AND all records in the merge buffer have the same
  // start position (waiting for more records at the same position)
  if (merge_buffer_.empty() ||
      (!flush &&
       merge_buffer_.front().record->pos == merge_buffer_.back().record->pos)) {
    return;
  }

  merge_records();

  // Flush remaining records in the merge buffer
  if (flush && !merge_buffer_.empty()) {
    merge_records();
    if (merge_buffer_.size()) {
      LOG_ERROR("VCFMerger merge buffer not empty after flush.");
    }
  }
}

}  // namespace vcf
}  // namespace tiledb
