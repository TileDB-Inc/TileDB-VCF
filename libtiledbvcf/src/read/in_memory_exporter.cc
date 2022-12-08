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

namespace tiledb {
namespace vcf {

void InMemoryExporter::set_buffer_values(
    const std::string& attribute, void* buff, int64_t buff_size) {
  if (buff == nullptr) {
    throw std::runtime_error(
        "Error setting buffer; null values buffer provided for attribute '" +
        attribute + "'.");
  }

  auto user_buff = get_buffer(attribute);
  user_buff->data = buff;
  user_buff->max_data_bytes = buff_size;
}

void InMemoryExporter::set_buffer_offsets(
    const std::string& attribute, int32_t* buff, int64_t buff_size) {
  if (fixed_len_attr(attribute) && buff != nullptr) {
    throw std::runtime_error(
        "Error setting buffer; attribute '" + attribute +
        "' is fixed-length but offset buffer was provided.");
  } else if (!fixed_len_attr(attribute) && buff == nullptr) {
    throw std::runtime_error(
        "Error setting buffer; attribute '" + attribute +
        "' is variable-length but null offset buffer was provided.");
  }

  auto user_buff = get_buffer(attribute);
  user_buff->offsets = buff;
  user_buff->max_num_offsets = buff_size / sizeof(int32_t);
}

void InMemoryExporter::set_buffer_list_offsets(
    const std::string& attribute, int32_t* buff, int64_t buff_size) {
  if (!var_len_list_attr(attribute) && buff != nullptr) {
    throw std::runtime_error(
        "Error setting buffer; attribute '" + attribute +
        "' is not a var-len list attribute but list offset buffer was "
        "provided.");
  } else if (buff == nullptr) {
    throw std::runtime_error(
        "Error setting buffer; attribute '" + attribute +
        "' is a var-len list attribute but null list offset buffer was "
        "provided.");
  }

  auto user_buff = get_buffer(attribute);
  user_buff->list_offsets = buff;
  user_buff->max_num_list_offsets = buff_size / sizeof(int32_t);
}

void InMemoryExporter::set_buffer_validity_bitmap(
    const std::string& attribute, uint8_t* buff, int64_t buff_size) {
  if (!nullable_attr(attribute))
    throw std::runtime_error(
        "Error setting buffer; attribute '" + attribute +
        "' is not nullable but bitmap buffer was provided.");
  auto user_buff = get_buffer(attribute);
  user_buff->bitmap_buff = buff;
  user_buff->max_bitmap_bytes = buff_size;
  user_buff->bitmap.reset(new Bitmap(buff, buff_size));
}

InMemoryExporter::UserBuffer* InMemoryExporter::get_buffer(
    const std::string& attribute) {
  auto it = user_buffers_.find(attribute);
  UserBuffer* buff = nullptr;
  if (it == user_buffers_.end()) {
    buff = &user_buffers_[attribute];
    buff->attr = attr_name_to_enum(attribute);
    buff->attr_name = attribute;
    if (buff->attr == ExportableAttribute::InfoOrFmt) {
      auto p = TileDBVCFDataset::split_info_fmt_attr_name(buff->attr_name);
      buff->is_info = p.first == "info";
      buff->info_fmt_field_name = p.second;
    }
    user_buffers_by_idx_.push_back(buff);
  } else {
    buff = &it->second;
  }
  return buff;
}

std::set<std::string> InMemoryExporter::array_attributes_required() const {
  if (dataset_ == nullptr)
    throw std::runtime_error(
        "Error getting required attributes; no dataset is initialized.");

  const std::set<std::string> extracted(
      dataset_->metadata().extra_attributes.begin(),
      dataset_->metadata().extra_attributes.end());

  const unsigned version = dataset_->metadata().version;

  std::set<std::string> result;
  for (const auto& it : user_buffers_) {
    switch (it.second.attr) {
      case ExportableAttribute::SampleName:
        if (version == TileDBVCFDataset::Version::V4) {
          result.insert(TileDBVCFDataset::DimensionNames::V4::sample);
        } else if (version == TileDBVCFDataset::Version::V3) {
          result.insert(TileDBVCFDataset::DimensionNames::V3::sample);
        } else {
          assert(version == TileDBVCFDataset::Version::V2);
          result.insert(TileDBVCFDataset::DimensionNames::V2::sample);
        }
        break;
      case ExportableAttribute::PosStart:
        if (version == TileDBVCFDataset::Version::V4) {
          result.insert(TileDBVCFDataset::DimensionNames::V4::start_pos);
        } else if (version == TileDBVCFDataset::Version::V3) {
          result.insert(TileDBVCFDataset::DimensionNames::V3::start_pos);
        } else {
          assert(version == TileDBVCFDataset::Version::V2);
          result.insert(TileDBVCFDataset::AttrNames::V2::pos);
        }
        break;
      case ExportableAttribute::PosEnd:
        if (version == TileDBVCFDataset::Version::V4) {
          result.insert(TileDBVCFDataset::AttrNames::V4::end_pos);
        } else if (version == TileDBVCFDataset::Version::V3) {
          result.insert(TileDBVCFDataset::AttrNames::V3::end_pos);
        } else {
          assert(version == TileDBVCFDataset::Version::V2);
          result.insert(TileDBVCFDataset::DimensionNames::V2::end_pos);
        }
        break;
      case ExportableAttribute::Alleles:
        if (version == TileDBVCFDataset::Version::V4) {
          result.insert(TileDBVCFDataset::AttrNames::V4::alleles);
        } else if (version == TileDBVCFDataset::Version::V3) {
          result.insert(TileDBVCFDataset::AttrNames::V3::alleles);
        } else {
          assert(version == TileDBVCFDataset::Version::V2);
          result.insert(TileDBVCFDataset::AttrNames::V2::alleles);
        }
        break;
      case ExportableAttribute::Id:
        if (version == TileDBVCFDataset::Version::V4) {
          result.insert(TileDBVCFDataset::AttrNames::V4::id);
        } else if (version == TileDBVCFDataset::Version::V3) {
          result.insert(TileDBVCFDataset::AttrNames::V3::id);
        } else {
          assert(version == TileDBVCFDataset::Version::V2);
          result.insert(TileDBVCFDataset::AttrNames::V2::id);
        }
        break;
      case ExportableAttribute::Filters:
        if (version == TileDBVCFDataset::Version::V4) {
          result.insert(TileDBVCFDataset::AttrNames::V4::filter_ids);
        } else if (version == TileDBVCFDataset::Version::V3) {
          result.insert(TileDBVCFDataset::AttrNames::V3::filter_ids);
        } else {
          assert(version == TileDBVCFDataset::Version::V2);
          result.insert(TileDBVCFDataset::AttrNames::V2::filter_ids);
        }
        break;
      case ExportableAttribute::Qual:
        if (version == TileDBVCFDataset::Version::V4) {
          result.insert(TileDBVCFDataset::AttrNames::V4::qual);
        } else if (version == TileDBVCFDataset::Version::V3) {
          result.insert(TileDBVCFDataset::AttrNames::V3::qual);
        } else {
          assert(version == TileDBVCFDataset::Version::V2);
          result.insert(TileDBVCFDataset::AttrNames::V2::qual);
        }
        break;
      case ExportableAttribute::Fmt:
        if (version == TileDBVCFDataset::Version::V4) {
          result.insert(TileDBVCFDataset::AttrNames::V4::fmt);
        } else if (version == TileDBVCFDataset::Version::V3) {
          result.insert(TileDBVCFDataset::AttrNames::V3::fmt);
        } else {
          assert(version == TileDBVCFDataset::Version::V2);
          result.insert(TileDBVCFDataset::AttrNames::V2::fmt);
        }
        break;
      case ExportableAttribute::Info:
        if (version == TileDBVCFDataset::Version::V4) {
          result.insert(TileDBVCFDataset::AttrNames::V4::info);
        } else if (version == TileDBVCFDataset::Version::V3) {
          result.insert(TileDBVCFDataset::AttrNames::V3::info);
        } else {
          assert(version == TileDBVCFDataset::Version::V2);
          result.insert(TileDBVCFDataset::AttrNames::V2::info);
        }
        break;
      case ExportableAttribute::InfoOrFmt:
        if (extracted.count(it.first)) {
          result.insert(it.first);
        } else {
          auto p = TileDBVCFDataset::split_info_fmt_attr_name(it.first);
          if (p.first == "info") {
            if (version == TileDBVCFDataset::Version::V4) {
              result.insert(TileDBVCFDataset::AttrNames::V4::info);
            } else if (version == TileDBVCFDataset::Version::V3) {
              result.insert(TileDBVCFDataset::AttrNames::V3::info);
            } else {
              assert(version == TileDBVCFDataset::Version::V2);
              result.insert(TileDBVCFDataset::AttrNames::V2::info);
            }
          } else {
            if (version == TileDBVCFDataset::Version::V4) {
              result.insert(TileDBVCFDataset::AttrNames::V4::fmt);
            } else if (version == TileDBVCFDataset::Version::V3) {
              result.insert(TileDBVCFDataset::AttrNames::V3::fmt);
            } else {
              assert(version == TileDBVCFDataset::Version::V2);
              result.insert(TileDBVCFDataset::AttrNames::V2::fmt);
            }
          }
        }
        break;
      case ExportableAttribute::Contig:
        if (version == TileDBVCFDataset::Version::V4) {
          result.insert(TileDBVCFDataset::DimensionNames::V4::contig);
          break;
        }
      case ExportableAttribute::QueryBedStart:
      case ExportableAttribute::QueryBedEnd:
      case ExportableAttribute::QueryBedLine:
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

void InMemoryExporter::reset_buffers() {
  user_buffers_.clear();
  user_buffers_by_idx_.clear();
}

void InMemoryExporter::result_size(
    const std::string& attribute,
    int64_t* num_offsets,
    int64_t* num_data_elements,
    int64_t* num_data_bytes) const {
  auto it = user_buffers_.find(attribute);
  if (it == user_buffers_.end())
    throw std::runtime_error(
        "Error getting result size; attribute '" + attribute +
        "' had no buffer set.");
  const UserBuffer& buff = it->second;
  if (num_offsets) {
    // If there was any data written, there is an extra "offset" stored at the
    // end of the offsets buffer (storing the total data size, c.f. Arrow).
    *num_offsets =
        buff.curr_sizes.num_offsets == 0 ? 0 : buff.curr_sizes.num_offsets + 1;
  }
  if (num_data_elements)
    *num_data_elements = buff.curr_sizes.data_nelts;
  if (num_data_bytes)
    *num_data_bytes = buff.curr_sizes.data_bytes;
}

void InMemoryExporter::num_buffers(int32_t* num_buffers) const {
  *num_buffers = user_buffers_.size();
}

void InMemoryExporter::get_buffer_values(
    int32_t buffer_idx, const char** name, void** buff) const {
  if (buffer_idx < 0 || (size_t)buffer_idx >= user_buffers_by_idx_.size())
    throw std::runtime_error(
        "Error getting buffer information; index out of bounds.");
  UserBuffer* user_buff = user_buffers_by_idx_[buffer_idx];
  *name = user_buff->attr_name.c_str();
  *buff = user_buff->data;
}

void InMemoryExporter::get_buffer_offsets(
    int32_t buffer_idx, const char** name, int32_t** buff) const {
  if (buffer_idx < 0 || (size_t)buffer_idx >= user_buffers_by_idx_.size())
    throw std::runtime_error(
        "Error getting buffer information; index out of bounds.");
  UserBuffer* user_buff = user_buffers_by_idx_[buffer_idx];
  *name = user_buff->attr_name.c_str();
  *buff = user_buff->offsets;
}

void InMemoryExporter::get_buffer_list_offsets(
    int32_t buffer_idx, const char** name, int32_t** buff) const {
  if (buffer_idx < 0 || (size_t)buffer_idx >= user_buffers_by_idx_.size())
    throw std::runtime_error(
        "Error getting buffer information; index out of bounds.");
  UserBuffer* user_buff = user_buffers_by_idx_[buffer_idx];
  *name = user_buff->attr_name.c_str();
  *buff = user_buff->list_offsets;
}

void InMemoryExporter::get_buffer_validity_bitmap(
    int32_t buffer_idx, const char** name, uint8_t** buff) const {
  if (buffer_idx < 0 || (size_t)buffer_idx >= user_buffers_by_idx_.size())
    throw std::runtime_error(
        "Error getting buffer information; index out of bounds.");
  UserBuffer* user_buff = user_buffers_by_idx_[buffer_idx];
  *name = user_buff->attr_name.c_str();
  *buff = user_buff->bitmap_buff;
}

void InMemoryExporter::reset_current_sizes() {
  for (auto& it : user_buffers_)
    it.second.curr_sizes = UserBufferSizes();
}

bool InMemoryExporter::export_record(
    const SampleAndId& sample,
    const bcf_hdr_t* hdr,
    const Region& query_region,
    uint32_t contig_offset,
    const ReadQueryResults& query_results,
    uint64_t cell_idx) {
  const unsigned version = dataset_->metadata().version;

  // Keep a convenience reference to the current query results.
  curr_query_results_ = &query_results;

  if (user_buffers_.empty()) {
    // With no user buffers to receive data, just degenerate to a count.
    return true;
  }

  // Record current buffer sizes in case of overflow on some attribute.
  std::vector<UserBufferSizes> saved_sizes(user_buffers_.size());
  for (size_t i = 0; i < user_buffers_.size(); i++)
    saved_sizes[i] = user_buffers_by_idx_[i]->curr_sizes;

  // For all user buffers, copy the appropriate data.
  const auto* buffers = curr_query_results_->buffers();
  bool overflow = false;
  for (auto& it : user_buffers_) {
    if (overflow)
      break;

    UserBuffer& user_buff = it.second;
    switch (user_buff.attr) {
      case ExportableAttribute::SampleName: {
        const std::string& sample_name = sample.sample_name;
        overflow = !copy_cell(
            &user_buff,
            sample_name.c_str(),
            sample_name.size(),
            sample_name.size(),
            hdr);
        break;
      }
      case ExportableAttribute::Contig: {
        if (version == TileDBVCFDataset::Version::V4) {
          uint64_t size = 0;
          const char* contig = buffers->contig().value<char>(cell_idx, &size);
          overflow = !copy_cell(&user_buff, contig, size, size, hdr);
        } else {
          overflow = !copy_cell(
              &user_buff,
              query_region.seq_name.c_str(),
              query_region.seq_name.size(),
              query_region.seq_name.size(),
              hdr);
        }
        break;
      }
      case ExportableAttribute::PosStart: {
        if (version == TileDBVCFDataset::Version::V4) {
          const uint32_t real_start_pos =
              buffers->real_start_pos().value<uint32_t>(cell_idx) + 1;
          overflow = !copy_cell(
              &user_buff, &real_start_pos, sizeof(real_start_pos), 1, hdr);

        } else if (version == TileDBVCFDataset::Version::V3) {
          const uint32_t real_start_pos =
              (buffers->real_start_pos().value<uint32_t>(cell_idx) -
               contig_offset) +
              1;
          overflow = !copy_cell(
              &user_buff, &real_start_pos, sizeof(real_start_pos), 1, hdr);
        } else {
          assert(version == TileDBVCFDataset::Version::V2);
          const uint32_t pos =
              (buffers->pos().value<uint32_t>(cell_idx) - contig_offset) + 1;
          overflow = !copy_cell(&user_buff, &pos, sizeof(pos), 1, hdr);
        }
        break;
      }
      case ExportableAttribute::PosEnd: {
        if (version == TileDBVCFDataset::Version::V4) {
          const uint32_t end_pos =
              buffers->end_pos().value<uint32_t>(cell_idx) + 1;
          overflow = !copy_cell(&user_buff, &end_pos, sizeof(end_pos), 1, hdr);
        } else if (version == TileDBVCFDataset::Version::V3) {
          const uint32_t end_pos =
              (buffers->end_pos().value<uint32_t>(cell_idx) - contig_offset) +
              1;
          overflow = !copy_cell(&user_buff, &end_pos, sizeof(end_pos), 1, hdr);
        } else {
          assert(version == TileDBVCFDataset::Version::V2);
          const uint32_t real_end =
              (buffers->real_end().value<uint32_t>(cell_idx) - contig_offset) +
              1;
          overflow =
              !copy_cell(&user_buff, &real_end, sizeof(real_end), 1, hdr);
        }
        break;
      }
      case ExportableAttribute::QueryBedStart: {
        overflow = !copy_cell(
            &user_buff, &query_region.min, sizeof(query_region.min), 1, hdr);
        break;
      }
      case ExportableAttribute::QueryBedEnd: {
        // converting 0-indexed, inclusive end position to 0-indexed, half-open
        // end position to match the BED file
        uint32_t end = query_region.max + 1;
        overflow = !copy_cell(&user_buff, &end, sizeof(end), 1, hdr);
        break;
      }
      case ExportableAttribute::QueryBedLine: {
        overflow = !copy_cell(
            &user_buff, &query_region.line, sizeof(query_region.line), 1, hdr);
        break;
      }
      case ExportableAttribute::Alleles: {
        overflow = !copy_alleles_list(cell_idx, &user_buff);
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
        // Don't copy terminating null byte
        if (nbytes > 0)
          nbytes -= 1;
        overflow = !copy_cell(&user_buff, data, nbytes, nbytes, hdr);
        break;
      }
      case ExportableAttribute::Filters: {
        overflow = !copy_filters_list(hdr, cell_idx, &user_buff);
        break;
      }
      case ExportableAttribute::Qual: {
        const auto qual = buffers->qual().value<float>(cell_idx);
        overflow = !copy_cell(&user_buff, &qual, sizeof(qual), 1, hdr);
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
        overflow = !copy_cell(&user_buff, data, nbytes, nbytes, hdr);
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
        overflow = !copy_cell(&user_buff, data, nbytes, nbytes, hdr);
        break;
      }
      case ExportableAttribute::InfoOrFmt: {
        overflow = !copy_info_fmt_value(cell_idx, &user_buff, hdr);
        break;
      }
      default:
        throw std::runtime_error(
            "Error copying cell; unimplemented attribute '" + it.first + "'");
        break;
    }
  }

  // Overflow can occur if a user buffer was too small to receive the copied
  // data. Restore old buffer sizes so the user can process the incomplete
  // results.
  if (overflow) {
    for (size_t i = 0; i < user_buffers_.size(); i++)
      user_buffers_by_idx_[i]->curr_sizes = saved_sizes[i];
    return false;
  }

  return true;
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
      {"query_bed_line", ExportableAttribute::QueryBedLine},
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
      "pos_start",
      "pos_end",
      "query_bed_start",
      "query_bed_end",
      "query_bed_line",
      "qual",
      "fmt_DP",
      "fmt_GQ",
      "fmt_PS",
      "fmt_PQ",
      "fmt_MQ",
      "fmt_MIN_DP"};
  return fixed_len.count(attr) > 0;
}

bool InMemoryExporter::var_len_list_attr(const std::string& attr) {
  ExportableAttribute attribute = attr_name_to_enum(attr);
  return attribute == ExportableAttribute::Filters ||
         attribute == ExportableAttribute::Alleles;
}

bool InMemoryExporter::nullable_attr(const std::string& attr) {
  ExportableAttribute attribute = attr_name_to_enum(attr);
  return attribute == ExportableAttribute::Filters ||
         attribute == ExportableAttribute::Info ||
         attribute == ExportableAttribute::Fmt ||
         attribute == ExportableAttribute::InfoOrFmt;
}

void InMemoryExporter::attribute_datatype(
    const TileDBVCFDataset* dataset,
    const std::string& attribute,
    AttrDatatype* datatype,
    bool* var_len,
    bool* nullable,
    bool* list,
    bool add_iaf) {
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
    case ExportableAttribute::Info:
      *datatype = AttrDatatype::UINT8;
      break;
    case ExportableAttribute::Contig:
      *datatype = AttrDatatype::CHAR;
      break;
    case ExportableAttribute::QueryBedStart:
    case ExportableAttribute::QueryBedEnd:
    case ExportableAttribute::QueryBedLine:
      *datatype = AttrDatatype::INT32;
      break;
    case ExportableAttribute::InfoOrFmt:
      *datatype = get_info_fmt_datatype(dataset, attribute, nullptr, add_iaf);
      break;
    default:
      throw std::runtime_error(
          "Error getting attribute '" + attribute +
          "' datatype; unhandled attribute.");
      break;
  }

  *var_len = !fixed_len_attr(attribute);
  *nullable = nullable_attr(attribute);
  *list = var_len_list_attr(attribute);
}

AttrDatatype InMemoryExporter::get_info_fmt_datatype(
    const TileDBVCFDataset* dataset,
    const std::string& attr,
    const bcf_hdr_t* hdr,
    bool add_iaf) {
  // Special-case genotype, since the header thinks it's a string.
  if (attr == "fmt_GT")
    return AttrDatatype::INT32;

  auto parts = TileDBVCFDataset::split_info_fmt_attr_name(attr);
  bool is_info = parts.first == "info";
  const auto& field_name = parts.second;

  int htslib_type = is_info ?
                        dataset->info_field_type(field_name, hdr, add_iaf) :
                        dataset->fmt_field_type(field_name, hdr);
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

bool InMemoryExporter::copy_cell(
    UserBuffer* dest,
    const void* data,
    uint64_t nbytes,
    uint64_t nelts,
    const bcf_hdr_t* hdr) const {
  const bool var_len = dest->offsets != nullptr;
  int64_t index = dest->curr_sizes.num_offsets;
  // If its not var length get the index from data element count
  if (!var_len)
    index = dest->curr_sizes.data_nelts;

  if (!copy_cell_data(dest, data, nbytes, nelts, hdr))
    return false;
  bool is_null = data == nullptr;
  if (!update_cell_list_and_bitmap(dest, index, is_null, 1, index))
    return false;
  return true;
}

bool InMemoryExporter::copy_cell_data(
    UserBuffer* dest,
    const void* data,
    uint64_t nbytes,
    uint64_t nelts,
    const bcf_hdr_t* hdr) const {
  const bool var_len = dest->offsets != nullptr;

  // Check for data buffer overflow
  if (dest->curr_sizes.data_bytes + nbytes > (uint64_t)dest->max_data_bytes)
    return false;
  // Check for offsets overflow (var-len only)
  if (var_len && (dest->curr_sizes.num_offsets + 2 > dest->max_num_offsets ||
                  dest->curr_sizes.data_nelts + nelts >
                      (uint64_t)std::numeric_limits<int32_t>::max()))
    return false;

  // Copy data
  if (data != nullptr) {
    std::memcpy(
        static_cast<char*>(dest->data) + dest->curr_sizes.data_bytes,
        data,
        nbytes);
  } else if (dest->bitmap_buff != nullptr && !var_len) {
    // Handle null by setting the bytes and elements to 1
    // Arrow expects null values to have 1 value which is ignored for fixed
    // length fields
    nbytes = attr_datatype_size(
        get_info_fmt_datatype(this->dataset_, dest->attr_name, hdr, add_iaf));
    nelts = 1;
  }

  // Update offsets
  if (var_len) {
    // Set offset of current cell
    dest->offsets[dest->curr_sizes.num_offsets++] =
        static_cast<int32_t>(dest->curr_sizes.data_nelts);
    // Always keep the final offset set to the current data buffer size.
    // (This is why we have the +2 overflow check above).
    dest->offsets[dest->curr_sizes.num_offsets] =
        static_cast<int32_t>(dest->curr_sizes.data_nelts + nelts);
  }

  dest->curr_sizes.data_bytes += nbytes;
  dest->curr_sizes.data_nelts += nelts;
  return true;
}

bool InMemoryExporter::update_cell_list_and_bitmap(
    UserBuffer* dest,
    int64_t index,
    bool is_null,
    int32_t num_list_values,
    int64_t list_index) const {
  const bool nullable = dest->bitmap_buff != nullptr;
  const bool list = dest->list_offsets != nullptr;

  // If there's no nullable bitmap or list offsets, nothing to do here.
  if (!nullable && !list)
    return true;

  // Check for bitmap overflow (nullable only)
  if (nullable && (list_index / 8 >= dest->max_bitmap_bytes))
    return false;

  // Check for list offsets overflow (lists only)
  if (list &&
      (dest->curr_sizes.num_list_offsets + 2 > dest->max_num_list_offsets ||
       dest->curr_sizes.num_list_offsets + (uint64_t)num_list_values >
           (uint64_t)std::numeric_limits<int32_t>::max()))
    return false;

  // Update validity bitmap
  if (nullable) {
    if (is_null)
      dest->bitmap->clear(list_index);
    else
      dest->bitmap->set(list_index);
  }

  // Update offsets
  if (list) {
    // Set list offset of current cell
    dest->list_offsets[dest->curr_sizes.num_list_offsets++] =
        static_cast<int32_t>(index);
    // Always keep the final offset set to the current size.
    // (This is why we have the +2 overflow check above).
    dest->list_offsets[dest->curr_sizes.num_list_offsets] =
        static_cast<int32_t>(index + num_list_values);
  }

  return true;
}

bool InMemoryExporter::add_zero_length_offset(UserBuffer* dest) const {
  // Sanity check
  if (dest->offsets == nullptr)
    throw std::runtime_error(
        "Error adding null value offset; no offsets buffer.");

  // Check for offsets overflow
  if (dest->curr_sizes.num_offsets + 2 > dest->max_num_offsets ||
      dest->curr_sizes.num_offsets > std::numeric_limits<int32_t>::max())
    return false;

  dest->offsets[dest->curr_sizes.num_offsets++] =
      static_cast<int32_t>(dest->curr_sizes.data_nelts);
  dest->offsets[dest->curr_sizes.num_offsets] =
      static_cast<int32_t>(dest->curr_sizes.data_nelts);

  return true;
}

bool InMemoryExporter::copy_alleles_list(
    uint64_t cell_idx, UserBuffer* dest) const {
  // Sanity check buffers
  if (dest->offsets == nullptr || dest->list_offsets == nullptr)
    throw std::runtime_error(
        "Error copying alleles list; no buffer set for offsets or list "
        "offsets.");

  // Find the data and size
  // Use list offsets for index as this is a list datatype
  const int64_t index = dest->curr_sizes.num_offsets;
  const int64_t list_index = dest->curr_sizes.num_list_offsets;
  const Buffer& src = curr_query_results_->buffers()->alleles();
  const uint64_t src_size = curr_query_results_->alleles_size().second;
  void* data = nullptr;
  uint64_t nbytes = 0;
  get_var_attr_value(src, cell_idx, src_size, &data, &nbytes);

  // Note that the alleles data is ingested as a null-terminated CSV list.
  const char* p = static_cast<const char*>(data);
  unsigned num_parts = 0;
  uint64_t idx = 0;
  while (idx < nbytes) {
    // Find the extent of the allele string.
    uint64_t start_idx = idx;
    for (; idx < nbytes; idx++) {
      if (p[idx] == ',' || p[idx] == '\0')
        break;
    }

    // Sanity check
    if (idx <= start_idx)
      throw std::runtime_error("Error copying alleles list; idx <= start_idx");

    // Copy the allele to the user data buffer (and update value offsets).
    const uint64_t len = idx - start_idx;
    if (!copy_cell_data(dest, p + start_idx, len, len, nullptr))
      return false;

    idx++;  // Skip comma
    num_parts++;
  }

  // Add null offset if necessary.
  bool is_null = data == nullptr ||
                 (nbytes == 1 && *static_cast<const char*>(data) == '\0');
  if (is_null && !add_zero_length_offset(dest))
    return false;

  // Update list offsets and bitmap.
  if (!update_cell_list_and_bitmap(dest, index, is_null, num_parts, list_index))
    return false;

  return true;
}

bool InMemoryExporter::copy_filters_list(
    const bcf_hdr_t* hdr, uint64_t cell_idx, UserBuffer* dest) const {
  // Sanity check buffers
  if (dest->offsets == nullptr || dest->list_offsets == nullptr)
    throw std::runtime_error(
        "Error copying filters list; no buffer set for offsets or list "
        "offsets.");

  // Find the data and size
  const Buffer& src = curr_query_results_->buffers()->filter_ids();
  const uint64_t src_size = curr_query_results_->filter_ids_size().second;
  void* data = nullptr;
  uint64_t nbytes = 0;
  get_var_attr_value(src, cell_idx, src_size, &data, &nbytes);

  // Note that the filters data is ingested as a list of int32 IDs.
  const int* int_data = static_cast<const int*>(data);
  int num_filters = *int_data;
  const int* filter_ids = int_data + 1;

  // Use list offsets for index as this is a list datatype
  const int64_t index = dest->curr_sizes.num_offsets;
  const int64_t list_index = dest->curr_sizes.num_list_offsets;
  const bool is_null = num_filters == 0;
  if (is_null) {
    // To adhere to Arrow's offset semantics, a zero-length value still gets
    // an entry in the offsets buffer.
    if (!add_zero_length_offset(dest))
      return false;
  } else {
    // Copy the filter names to the user data buffer (and update value offsets).
    for (int i = 0; i < num_filters; i++) {
      const char* filter_name = bcf_hdr_int2id(hdr, BCF_DT_ID, filter_ids[i]);
      const uint64_t len = strlen(filter_name);
      if (!copy_cell_data(dest, filter_name, len, len, nullptr))
        return false;
    }
  }

  // Update list offsets and bitmap.
  if (!update_cell_list_and_bitmap(
          dest, index, is_null, num_filters, list_index))
    return false;

  return true;
}

bool InMemoryExporter::copy_info_fmt_value(
    uint64_t cell_idx, UserBuffer* dest, const bcf_hdr_t* hdr) const {
  const std::string& field_name = dest->info_fmt_field_name;
  const bool is_gt = field_name == "GT";
  const bool is_iaf = field_name == "TILEDB_IAF";
  const void* src = nullptr;
  uint64_t nbytes = 0, nelts = 0;
  if (is_iaf && !curr_query_results_->af_values.empty()) {
    // assign source pointer, nbytes, and nelts from vector
    src = curr_query_results_->af_values.data();
    nelts = curr_query_results_->af_values.size();
    nbytes = nelts * sizeof(decltype(curr_query_results_->af_values.at(0)));
  } else {
    get_info_fmt_value(dest, cell_idx, &src, &nbytes, &nelts);
  }

  if (is_gt) {
    // Genotype needs special handling to be decoded.
    const int* genotype = reinterpret_cast<const int*>(src);
    int decoded[nelts];
    for (unsigned i = 0; i < nelts; i++)
      decoded[i] = bcf_gt_allele(genotype[i]);
    return copy_cell(dest, decoded, nelts * sizeof(int), nelts, hdr);
  } else {
    return copy_cell(dest, src, nbytes, nelts, hdr);
  }
}

void InMemoryExporter::get_info_fmt_value(
    const UserBuffer* attr_buff,
    uint64_t cell_idx,
    const void** data,
    uint64_t* nbytes,
    uint64_t* nelts) const {
  // Get either the extracted attribute buffer, or the info/fmt blob attribute.
  const std::string& attr_name = attr_buff->attr_name;
  const std::string& field_name = attr_buff->info_fmt_field_name;
  const bool is_info = attr_buff->is_info;
  const Buffer* src = nullptr;
  std::pair<uint64_t, uint64_t> src_size;
  bool is_extracted_attr = false;
  if (curr_query_results_->buffers()->extra_attr(attr_name, &src)) {
    is_extracted_attr = true;

    auto sizes_iter = curr_query_results_->extra_attrs_size().find(attr_name);
    if (sizes_iter == curr_query_results_->extra_attrs_size().end())
      throw std::runtime_error(
          "Could not find size for extra attribute" + attr_name +
          " in get_info_fmt_value");

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
    *data = nullptr;
    *nbytes = 0;
    *nelts = 0;
    return;
  }

  if (is_extracted_attr) {
    int type = *reinterpret_cast<const int*>(ptr);
    ptr += sizeof(int);
    int num_values = *reinterpret_cast<const int*>(ptr);
    ptr += sizeof(int);
    *data = ptr;
    *nbytes = utils::bcf_type_size(type) * num_values;
    *nelts = num_values;
    return;
  } else {
    // Skip initial 'nfmt'/'ninfo' field.
    tot_nbytes -= sizeof(uint32_t);
    ptr += sizeof(uint32_t);

    const char* end = ptr + tot_nbytes;
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
        *nelts = num_values;
        return;
      }

      ptr += num_values * type_size;
    }
  }

  // If we get here, no value for this field; return null value.
  *data = nullptr;
  *nbytes = 0;
  *nelts = 0;
}

}  // namespace vcf
}  // namespace tiledb
