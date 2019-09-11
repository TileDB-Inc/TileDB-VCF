/**
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2019 TileDB, Inc.
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

#include "read/in_memory_exporter.h"
#include "enums/attr_datatype.h"
#include "utils/constants.h"

namespace tiledb {
namespace vcf {

void InMemoryExporter::set_buffer(
    const std::string& attribute,
    int64_t* offsets,
    int64_t max_num_offsets,
    void* data,
    int64_t max_data_bytes) {
  if (fixed_len_attr(attribute) && offsets != nullptr) {
    throw std::runtime_error(
        "Error setting buffer; attribute '" + attribute +
        "' is fixed-length but offset buffer was provided.");
  } else if (!fixed_len_attr(attribute) && offsets == nullptr) {
    throw std::runtime_error(
        "Error setting buffer; attribute '" + attribute +
        "' is variable-length but no offset buffer was provided.");
  } else if (data == nullptr) {
    throw std::runtime_error(
        "Error setting buffer; no data buffer provided for attribute '" +
        attribute + "'.");
  }

  auto it = user_buffers_.find(attribute);
  UserBuffer* buff = nullptr;
  if (it == user_buffers_.end()) {
    buff = &user_buffers_[attribute];
    user_buffers_by_idx_.push_back(buff);
  } else {
    buff = &it->second;
  }

  buff->attr = attr_name_to_enum(attribute);
  buff->attr_name = attribute;
  buff->data = data;
  buff->max_data_bytes = max_data_bytes;
  buff->curr_data_bytes = 0;
  buff->offsets = offsets;
  buff->max_num_offsets = max_num_offsets;
  buff->curr_num_offsets = 0;
}

std::set<std::string> InMemoryExporter::array_attributes_required() const {
  if (dataset_ == nullptr)
    throw std::runtime_error(
        "Error getting required attributes; no dataset is initialized.");

  const std::set<std::string> extracted(
      dataset_->metadata().extra_attributes.begin(),
      dataset_->metadata().extra_attributes.end());

  std::set<std::string> result;
  for (const auto& it : user_buffers_) {
    switch (it.second.attr) {
      case ExportableAttribute::SampleName:
      case ExportableAttribute::PosEnd:
        result.insert(TILEDB_COORDS);
        break;
      case ExportableAttribute::PosStart:
        result.insert(TileDBVCFDataset::AttrNames::pos);
        break;
      case ExportableAttribute::Alleles:
        result.insert(TileDBVCFDataset::AttrNames::alleles);
        break;
      case ExportableAttribute::Id:
        result.insert(TileDBVCFDataset::AttrNames::id);
        break;
      case ExportableAttribute::Filters:
        result.insert(TileDBVCFDataset::AttrNames::filter_ids);
        break;
      case ExportableAttribute::Qual:
        result.insert(TileDBVCFDataset::AttrNames::qual);
        break;
      case ExportableAttribute::Fmt:
        result.insert(TileDBVCFDataset::AttrNames::fmt);
        break;
      case ExportableAttribute::Info:
        result.insert(TileDBVCFDataset::AttrNames::info);
        break;
      case ExportableAttribute::InfoOrFmt:
        if (extracted.count(it.first)) {
          result.insert(it.first);
        } else {
          auto p = TileDBVCFDataset::split_info_fmt_attr_name(it.first);
          if (p.first == "info")
            result.insert(TileDBVCFDataset::AttrNames::info);
          else
            result.insert(TileDBVCFDataset::AttrNames::fmt);
        }
        break;
      case ExportableAttribute::Contig:
      case ExportableAttribute::QueryBedStart:
      case ExportableAttribute::QueryBedEnd:
        // No attribute required
        break;
      default:
        throw std::runtime_error(
            "Error getting required attributes; unimplemented '" + it.first +
            "'");
        break;
    }
  }
  return result;
}

void InMemoryExporter::reset() {
  Exporter::reset();
  reset_current_sizes();
}

void InMemoryExporter::result_size(
    const std::string& attribute,
    uint64_t* num_offsets,
    uint64_t* nbytes) const {
  auto it = user_buffers_.find(attribute);
  if (it == user_buffers_.end())
    throw std::runtime_error(
        "Error getting result size; attribute '" + attribute +
        "' had no buffer set.");
  const UserBuffer& buff = it->second;
  if (num_offsets)
    *num_offsets = buff.curr_num_offsets;
  if (nbytes)
    *nbytes = buff.curr_data_bytes;
}

void InMemoryExporter::num_buffers(int32_t* num_buffers) const {
  *num_buffers = user_buffers_.size();
}

void InMemoryExporter::get_buffer(
    int32_t buffer_idx,
    const char** name,
    int64_t** offset_buff,
    int64_t* offset_buff_size,
    void** data_buff,
    int64_t* data_buff_size) const {
  if (buffer_idx < 0 || (size_t)buffer_idx >= user_buffers_by_idx_.size())
    throw std::runtime_error(
        "Error getting buffer information; index out of bounds.");
  UserBuffer* buff = user_buffers_by_idx_[buffer_idx];
  *name = buff->attr_name.c_str();
  *offset_buff = buff->offsets;
  *offset_buff_size = buff->max_num_offsets;
  *data_buff = buff->data;
  *data_buff_size = buff->max_data_bytes;
}

void InMemoryExporter::reset_current_sizes() {
  for (auto& it : user_buffers_) {
    it.second.curr_data_bytes = 0;
    it.second.curr_num_offsets = 0;
  }
}

bool InMemoryExporter::export_record(
    const SampleAndId& sample,
    const bcf_hdr_t* hdr,
    const Region& query_region,
    uint32_t contig_offset,
    const ReadQueryResults& query_results,
    uint64_t cell_idx) {
  curr_query_results_ = &query_results;
  return copy_cell(hdr, query_region, contig_offset, cell_idx);
}

InMemoryExporter::ExportableAttribute InMemoryExporter::attr_name_to_enum(
    const std::string& name) {
  std::string lname = name;
  std::transform(lname.begin(), lname.end(), lname.begin(), ::tolower);
  std::map<std::string, ExportableAttribute> mapping = {
      {"sample_name", ExportableAttribute::SampleName},
      {"contig", ExportableAttribute::Contig},
      {"pos_start", ExportableAttribute::PosStart},
      {"pos_end", ExportableAttribute::PosEnd},
      {"query_bed_start", ExportableAttribute::QueryBedStart},
      {"query_bed_end", ExportableAttribute::QueryBedEnd},
      {"alleles", ExportableAttribute::Alleles},
      {"id", ExportableAttribute::Id},
      {"filters", ExportableAttribute::Filters},
      {"qual", ExportableAttribute::Qual},
      {"fmt", ExportableAttribute::Fmt},
      {"info", ExportableAttribute::Info},
  };

  auto it = mapping.find(lname);
  if (it != mapping.end())
    return it->second;
  else
    return ExportableAttribute::InfoOrFmt;
}

bool InMemoryExporter::fixed_len_attr(const std::string& attr) {
  std::set<std::string> fixed_len = {
      "pos_start", "pos_end", "query_bed_start", "query_bed_end", "qual"};
  return fixed_len.count(attr) > 0;
}

void InMemoryExporter::attribute_datatype(
    const TileDBVCFDataset* dataset,
    const std::string& attribute,
    AttrDatatype* datatype,
    bool* var_len) {
  ExportableAttribute attr = attr_name_to_enum(attribute);
  switch (attr) {
    case ExportableAttribute::SampleName:
      *datatype = AttrDatatype::CHAR;
      break;
    case ExportableAttribute::PosEnd:
      *datatype = AttrDatatype::INT32;
      break;
    case ExportableAttribute::PosStart:
      *datatype = AttrDatatype::INT32;
      break;
    case ExportableAttribute::Alleles:
      *datatype = AttrDatatype::CHAR;
      break;
    case ExportableAttribute::Id:
      *datatype = AttrDatatype::CHAR;
      break;
    case ExportableAttribute::Filters:
      *datatype = AttrDatatype::CHAR;
      break;
    case ExportableAttribute::Qual:
      *datatype = AttrDatatype::FLOAT32;
      break;
    case ExportableAttribute::Fmt:
      *datatype = AttrDatatype::UINT8;
      break;
    case ExportableAttribute::Info:
      *datatype = AttrDatatype::UINT8;
      break;
    case ExportableAttribute::Contig:
      *datatype = AttrDatatype::CHAR;
      break;
    case ExportableAttribute::QueryBedStart:
      *datatype = AttrDatatype::INT32;
      break;
    case ExportableAttribute::QueryBedEnd:
      *datatype = AttrDatatype::INT32;
      break;
    case ExportableAttribute::InfoOrFmt:
      *datatype = get_info_fmt_datatype(dataset, attribute);
      break;
    default:
      throw std::runtime_error(
          "Error getting attribute '" + attribute +
          "' datatype; unhandled attribute.");
      break;
  }

  *var_len = !fixed_len_attr(attribute);
}

AttrDatatype InMemoryExporter::get_info_fmt_datatype(
    const TileDBVCFDataset* dataset, const std::string& attr) {
  // Special-case genotype, since the header thinks it's a string.
  if (attr == "fmt_GT")
    return AttrDatatype::INT32;

  auto parts = TileDBVCFDataset::split_info_fmt_attr_name(attr);
  bool is_info = parts.first == "info";
  const auto& field_name = parts.second;

  int htslib_type = is_info ? dataset->info_field_type(field_name) :
                              dataset->fmt_field_type(field_name);
  switch (htslib_type) {
    case BCF_HT_FLAG:
      return AttrDatatype::INT32;
    case BCF_HT_STR:
      return AttrDatatype::CHAR;
    case BCF_HT_INT:
      return AttrDatatype::INT32;
    case BCF_HT_REAL:
      return AttrDatatype::FLOAT32;
    default:
      throw std::runtime_error(
          "Error getting attribute datatype for '" + field_name +
          "'; unhandled htslib type.");
  }
}

bool InMemoryExporter::copy_cell(
    const bcf_hdr_t* hdr,
    const Region& region,
    uint32_t contig_offset,
    uint64_t cell_idx) {
  if (user_buffers_.empty()) {
    // With no user buffers to receive data, just degenerate to a count.
    return true;
  }

  // TODO: this is probably too expensive.
  // Record current buffer sizes in case of overflow on some attribute.
  std::map<std::string, std::pair<uint64_t, uint64_t>> curr_user_buff_sizes;
  for (auto& it : user_buffers_) {
    curr_user_buff_sizes[it.first].first = it.second.curr_num_offsets;
    curr_user_buff_sizes[it.first].second = it.second.curr_data_bytes;
  }

  const auto* buffers = curr_query_results_->buffers();
  bool overflow = false;
  for (auto& it : user_buffers_) {
    if (overflow)
      break;

    UserBuffer& user_buff = it.second;
    switch (user_buff.attr) {
      case ExportableAttribute::SampleName: {
        const uint32_t sample_id =
            buffers->coords().value<uint32_t>(2 * cell_idx + 0);
        const std::string& sample_name =
            dataset_->metadata().sample_names[sample_id];
        overflow = !copy_to_user_buff(
            &user_buff, sample_name.c_str(), sample_name.size());
        break;
      }
      case ExportableAttribute::Contig: {
        overflow = !copy_to_user_buff(
            &user_buff, region.seq_name.c_str(), region.seq_name.size());
        break;
      }
      case ExportableAttribute::PosStart: {
        const uint32_t pos =
            (buffers->pos().value<uint32_t>(cell_idx) - contig_offset) + 1;
        overflow = !copy_to_user_buff(&user_buff, &pos, sizeof(pos));
        break;
      }
      case ExportableAttribute::PosEnd: {
        const uint32_t real_end =
            (buffers->real_end().value<uint32_t>(cell_idx) - contig_offset) + 1;
        overflow = !copy_to_user_buff(&user_buff, &real_end, sizeof(real_end));
        break;
      }
      case ExportableAttribute::QueryBedStart: {
        overflow =
            !copy_to_user_buff(&user_buff, &region.min, sizeof(region.min));
        break;
      }
      case ExportableAttribute::QueryBedEnd: {
        uint32_t end = region.max + 1;
        overflow = !copy_to_user_buff(&user_buff, &end, sizeof(end));
        break;
      }
      case ExportableAttribute::Alleles: {
        void* data;
        uint64_t nbytes;
        get_var_attr_value(
            buffers->alleles(),
            cell_idx,
            curr_query_results_->alleles_size().second,
            &data,
            &nbytes);
        overflow = !copy_to_user_buff(&user_buff, data, nbytes);
        break;
      }
      case ExportableAttribute::Id: {
        void* data;
        uint64_t nbytes;
        get_var_attr_value(
            buffers->id(),
            cell_idx,
            curr_query_results_->id_size().second,
            &data,
            &nbytes);
        overflow = !copy_to_user_buff(&user_buff, data, nbytes);
        break;
      }
      case ExportableAttribute::Filters: {
        void* data;
        uint64_t nbytes;
        get_var_attr_value(
            buffers->filter_ids(),
            cell_idx,
            curr_query_results_->filter_ids_size().second,
            &data,
            &nbytes);
        make_csv_filter_list(hdr, data, nbytes, &str_buff_);
        overflow = !copy_to_user_buff(
            &user_buff, str_buff_.data(), str_buff_.size() + 1);
        break;
      }
      case ExportableAttribute::Qual: {
        const auto qual = buffers->qual().value<float>(cell_idx);
        overflow = !copy_to_user_buff(&user_buff, &qual, sizeof(qual));
        break;
      }
      case ExportableAttribute::Fmt: {
        void* data;
        uint64_t nbytes;
        get_var_attr_value(
            buffers->fmt(),
            cell_idx,
            curr_query_results_->fmt_size().second,
            &data,
            &nbytes);
        overflow = !copy_to_user_buff(&user_buff, data, nbytes);
        break;
      }
      case ExportableAttribute::Info: {
        void* data;
        uint64_t nbytes;
        get_var_attr_value(
            buffers->info(),
            cell_idx,
            curr_query_results_->info_size().second,
            &data,
            &nbytes);
        overflow = !copy_to_user_buff(&user_buff, data, nbytes);
        break;
      }
      case ExportableAttribute::InfoOrFmt: {
        overflow = !copy_info_fmt_value(cell_idx, &user_buff);
        break;
      }
      default:
        throw std::runtime_error(
            "Error copying cell; unimplemented attribute '" + it.first + "'");
        break;
    }
  }

  if (overflow) {
    // Restore old buffer sizes so the user can process the incomplete results.
    for (auto& it : user_buffers_) {
      it.second.curr_num_offsets = curr_user_buff_sizes[it.first].first;
      it.second.curr_data_bytes = curr_user_buff_sizes[it.first].second;
    }
    return false;
  }

  return true;
}

void InMemoryExporter::get_var_attr_value(
    const Buffer& src,
    uint64_t cell_idx,
    uint64_t buff_var_size,
    void** data,
    uint64_t* nbytes) const {
  const uint64_t num_cells = curr_query_results_->num_cells();
  const auto& offsets = src.offsets();
  uint64_t offset = offsets[cell_idx];
  uint64_t next_offset =
      cell_idx == num_cells - 1 ? buff_var_size : offsets[cell_idx + 1];
  *nbytes = next_offset - offset;
  *data = src.data<char>() + offset;
}

bool InMemoryExporter::copy_to_user_buff(
    UserBuffer* dest, const void* data, uint64_t nbytes) const {
  const bool var_len = dest->offsets != nullptr;

  // Check for overflow
  if (dest->curr_data_bytes + nbytes > dest->max_data_bytes)
    return false;
  if (var_len && dest->curr_num_offsets + 1 > dest->max_num_offsets)
    return false;

  // Copy data
  std::memcpy(
      static_cast<char*>(dest->data) + dest->curr_data_bytes, data, nbytes);
  if (var_len)
    dest->offsets[dest->curr_num_offsets++] = dest->curr_data_bytes;

  dest->curr_data_bytes += nbytes;
  return true;
}

bool InMemoryExporter::copy_info_fmt_value(
    uint64_t cell_idx, UserBuffer* dest) const {
  auto parts = TileDBVCFDataset::split_info_fmt_attr_name(dest->attr_name);
  if (parts.first != "fmt" && parts.first != "info")
    throw std::runtime_error(
        "Error copying unknown attribute cell value; unhandled attribute '" +
        dest->attr_name + "'");

  const std::string& field_name = parts.second;
  const bool is_gt = field_name == "GT";
  const void* src = nullptr;
  uint64_t nbytes = 0;
  get_info_fmt_value(dest->attr_name, field_name, cell_idx, &src, &nbytes);

  if (is_gt) {
    // Genotype needs special handling to be decoded.
    const int* genotype = reinterpret_cast<const int*>(src);
    unsigned num_values = nbytes / sizeof(int);
    int decoded[num_values];
    for (unsigned i = 0; i < num_values; i++)
      decoded[i] = bcf_gt_allele(genotype[i]);
    return copy_to_user_buff(dest, decoded, num_values * sizeof(int));
  } else {
    return copy_to_user_buff(dest, src, nbytes);
  }
}

void InMemoryExporter::get_info_fmt_value(
    const std::string& attr_name,
    const std::string& field_name,
    uint64_t cell_idx,
    const void** data,
    uint64_t* nbytes) const {
  // Get either the extracted attribute buffer, or the info/fmt blob attribute.
  const bool is_info = utils::starts_with(attr_name, "info_");
  const Buffer* src = nullptr;
  std::pair<uint64_t, uint64_t> src_size;
  bool is_extracted_attr = false;
  if (curr_query_results_->buffers()->extra_attr(attr_name, &src)) {
    is_extracted_attr = true;
    src_size = curr_query_results_->extra_attrs_size().at(attr_name);
  } else if (is_info) {
    src = &curr_query_results_->buffers()->info();
    src_size = curr_query_results_->info_size();
  } else {
    src = &curr_query_results_->buffers()->fmt();
    src_size = curr_query_results_->fmt_size();
  }

  if (src == nullptr)
    throw std::runtime_error(
        "Error copying attribute '" + attr_name + "'; no source buffer.");

  const uint64_t num_cells = curr_query_results_->num_cells();
  const auto& offsets = src->offsets();
  uint64_t offset = offsets[cell_idx];
  uint64_t next_offset =
      cell_idx == num_cells - 1 ? src_size.second : offsets[cell_idx + 1];
  uint64_t tot_nbytes = next_offset - offset;
  const char* ptr = src->data<char>() + offset;

  // Check for null (dummy byte).
  if (tot_nbytes == 1 && *ptr == '\0') {
    get_null_value(field_name, is_info, data, nbytes);
    return;
  }

  if (is_extracted_attr) {
    int type = *reinterpret_cast<const int*>(ptr);
    ptr += sizeof(int);
    int num_values = *reinterpret_cast<const int*>(ptr);
    ptr += sizeof(int);
    *data = ptr;
    *nbytes = utils::bcf_type_size(type) * num_values;
    return;
  } else {
    // Skip initial 'nfmt'/'ninfo' field.
    tot_nbytes -= sizeof(uint32_t);
    ptr += sizeof(uint32_t);

    const char* end = ptr + tot_nbytes + 1;
    while (ptr < end) {
      size_t keylen = strlen(ptr);
      bool match = strcmp(field_name.c_str(), ptr) == 0;
      ptr += keylen + 1;
      int type = *reinterpret_cast<const int*>(ptr);
      int type_size = utils::bcf_type_size(type);
      ptr += sizeof(int);
      int num_values = *reinterpret_cast<const int*>(ptr);
      ptr += sizeof(int);

      if (match) {
        *data = ptr;
        *nbytes = type_size * num_values;
        return;
      }

      ptr += num_values * type_size;
    }
  }

  // If we get here, no value for this field; return null value.
  get_null_value(field_name, is_info, data, nbytes);
}

void InMemoryExporter::get_null_value(
    const std::string& field_name,
    bool is_info,
    const void** data,
    uint64_t* nbytes) const {
  int type = is_info ? dataset_->info_field_type(field_name) :
                       dataset_->fmt_field_type(field_name);
  switch (type) {
    case BCF_HT_INT:
      *data = &Constants::values().null_int32();
      *nbytes = sizeof(int32_t);
      break;
    case BCF_HT_REAL:
      *data = &Constants::values().null_float32();
      *nbytes = sizeof(float);
      break;
    default:
      throw std::runtime_error(
          "Error loading null value for field '" + field_name +
          "'; unhandled type.");
  }
}

void InMemoryExporter::make_csv_filter_list(
    const bcf_hdr_t* hdr,
    const void* data,
    uint64_t nbytes,
    std::string* dest) const {
  const int* int_data = reinterpret_cast<const int*>(data);
  const int* filter_ids = int_data + 1;
  int num_filters = *int_data;
  dest->clear();
  for (int i = 0; i < num_filters; i++) {
    (*dest) += std::string(bcf_hdr_int2id(hdr, BCF_DT_ID, filter_ids[i]));
    if (i < num_filters - 1)
      (*dest) += ",";
  }
}

}  // namespace vcf
}  // namespace tiledb
