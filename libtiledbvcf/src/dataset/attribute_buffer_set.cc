#include "dataset/attribute_buffer_set.h"
#include "read/in_memory_exporter.h"
#include "utils/logger_public.h"

namespace tiledb {
namespace vcf {

AttributeBufferSet::AttributeBufferSet(bool verbose)
    : verbose_(verbose)
    , number_of_buffers_(0){};

AttributeBufferSet::BufferSizeByType AttributeBufferSet::compute_buffer_size(
    const std::unordered_set<std::string>& attr_names,
    uint64_t mem_budget,
    TileDBVCFDataset* dataset) {
  assert(dataset != nullptr);
  // Get count of number of query buffers being allocated
  size_t num_weighted_buffers = 0;
  uint64_t num_char_buffers = 0;
  uint64_t num_uint8_buffers = 0;
  uint64_t num_int32_buffers = 0;
  uint64_t num_uint64_buffers = 0;
  uint64_t num_float32_buffers = 0;
  uint64_t num_var_length_uint8_buffers = 0;
  for (const auto& s : attr_names) {
    bool var_len, nullable, list;
    tiledb_datatype_t datatype;
    dataset->attribute_datatype_non_fmt_info(
        s, &datatype, &var_len, &nullable, &list);

    if (var_len) {
      num_uint64_buffers += 1;
      num_var_length_uint8_buffers += 1;
    }

    // Currently we don't support list buffers in TileDB schema but this will
    // just work when we do
    if (list) {
      num_uint64_buffers += 1;
    }

    // Currently we don't support nullable buffers in TileDB schema but this
    // will just work when we do
    if (nullable) {
      num_uint8_buffers += 1;
    }

    // For non var length we want to count the datatype
    if (!var_len) {
      switch (datatype) {
        case TILEDB_UINT32:
        case TILEDB_INT32:
          num_int32_buffers += 1;
          break;
        case TILEDB_FLOAT32:
          num_float32_buffers += 1;
          break;
        case TILEDB_STRING_ASCII:
        case TILEDB_CHAR:
          num_char_buffers += 1;
          break;
        case TILEDB_UINT8:
          num_uint8_buffers += 1;
          break;
        default: {
          const char* datatype_str;
          tiledb_datatype_to_str(datatype, &datatype_str);
          throw std::runtime_error(
              "Unsupported datatype in compute_buffer_size: " +
              std::string(datatype_str));
        }
      }
    }
  }

  num_weighted_buffers = num_char_buffers + num_uint8_buffers +
                         (num_int32_buffers * sizeof(int32_t)) +
                         (num_float32_buffers * sizeof(float)) +
                         (num_uint64_buffers * sizeof(uint64_t)) +
                         (num_var_length_uint8_buffers * sizeof(uint64_t));

  // Every buffer alloc gets the same size.
  uint64_t nbytes = mem_budget / num_weighted_buffers;

  // Requesting 0 MB will result in a 1 KB allocation. This is used by the
  // tests to test the path of incomplete TileDB queries.
  if (mem_budget == 0) {
    nbytes = 1024;
  }

  BufferSizeByType sizes(
      nbytes,
      nbytes,
      nbytes * sizeof(int32_t),
      nbytes * sizeof(uint64_t),
      nbytes * sizeof(float),
      nbytes * sizeof(uint64_t));

  return sizes;
}

void AttributeBufferSet::allocate_fixed(
    const std::unordered_set<std::string>& attr_names,
    uint64_t memory_budget,
    TileDBVCFDataset* dataset) {
  clear();
  fixed_alloc_.clear();
  auto version = dataset->metadata().version;

  buffer_size_by_type_ =
      compute_buffer_size(attr_names, memory_budget, dataset);
  uint64_t num_offsets =
      buffer_size_by_type_.uint64_buffer_size / sizeof(uint64_t);

  // Get count of number of query buffers being allocated
  number_of_buffers_ = 0;
  for (const auto& s : attr_names) {
    bool fixed_len = dataset->attribute_is_fixed_len(s);
    number_of_buffers_ += fixed_len ? 1 : 2;
  }

  if (verbose_) {
    std::stringstream ss;
    ss << "Allocating " << attr_names.size() << " fields ("
       << number_of_buffers_ << " buffers) with breakdown of: ";
    if (buffer_size_by_type_.uint64_buffer_size > 0) {
      ss << std::endl
         << "\t"
         << "uint64_t size of " << buffer_size_by_type_.uint64_buffer_size
         << " bytes ("
         << buffer_size_by_type_.uint64_buffer_size / (1024.0f * 1024.0f)
         << "MB)";
    }
    if (buffer_size_by_type_.float32_buffer_size > 0) {
      ss << std::endl
         << "\t"
         << "float32_t size of " << buffer_size_by_type_.float32_buffer_size
         << " bytes ("
         << buffer_size_by_type_.float32_buffer_size / (1024.0f * 1024.0f)
         << "MB)";
    }
    if (buffer_size_by_type_.int32_buffer_size > 0) {
      ss << std::endl
         << "\t"
         << "int32_t size of " << buffer_size_by_type_.int32_buffer_size
         << " bytes ("
         << buffer_size_by_type_.int32_buffer_size / (1024.0f * 1024.0f)
         << "MB)";
    }
    if (buffer_size_by_type_.char_buffer_size > 0) {
      ss << std::endl
         << "\t"
         << "char size of " << buffer_size_by_type_.char_buffer_size
         << " bytes ("
         << buffer_size_by_type_.char_buffer_size / (1024.0f * 1024.0f)
         << "MB)";
    }
    if (buffer_size_by_type_.uint8_buffer_size > 0) {
      ss << std::endl
         << "\t"
         << "uint8_t size of " << buffer_size_by_type_.uint8_buffer_size
         << " bytes ("
         << buffer_size_by_type_.uint8_buffer_size / (1024.0f * 1024.0f)
         << "MB)";
    }
    if (buffer_size_by_type_.var_length_uint8_buffer_size > 0) {
      ss << std::endl
         << "\t"
         << "var length fields size of "
         << buffer_size_by_type_.var_length_uint8_buffer_size << " bytes ("
         << buffer_size_by_type_.var_length_uint8_buffer_size /
                (1024.0f * 1024.0f)
         << "MB)";
    }

    LOG_DEBUG(ss.str());
  }

  using attrNamesV4 = TileDBVCFDataset::AttrNames::V4;
  using attrNamesV3 = TileDBVCFDataset::AttrNames::V3;
  using attrNamesV2 = TileDBVCFDataset::AttrNames::V2;
  using dimNamesV4 = TileDBVCFDataset::DimensionNames::V4;
  using dimNamesV3 = TileDBVCFDataset::DimensionNames::V3;
  using dimNamesV2 = TileDBVCFDataset::DimensionNames::V2;
  for (const auto& s : attr_names) {
    if ((s == dimNamesV3::sample || s == dimNamesV2::sample) &&
        (version == TileDBVCFDataset::Version::V2 ||
         version == TileDBVCFDataset::Version::V3)) {
      sample_.resize(buffer_size_by_type_.int32_buffer_size);
      fixed_alloc_.emplace_back(false, s, &sample_, sizeof(uint32_t));
    } else if (
        s == dimNamesV4::sample && version == TileDBVCFDataset::Version::V4) {
      sample_name_.resize(buffer_size_by_type_.var_length_uint8_buffer_size);
      sample_name_.offsets().resize(num_offsets);
      fixed_alloc_.emplace_back(true, s, &sample_name_, sizeof(char));
    } else if (s == dimNamesV4::contig) {
      contig_.resize(buffer_size_by_type_.var_length_uint8_buffer_size);
      contig_.offsets().resize(num_offsets);
      fixed_alloc_.emplace_back(true, s, &contig_, sizeof(char));
    } else if (s == dimNamesV4::start_pos || s == dimNamesV3::start_pos) {
      start_pos_.resize(buffer_size_by_type_.int32_buffer_size);
      fixed_alloc_.emplace_back(false, s, &start_pos_, sizeof(uint32_t));
    } else if (s == dimNamesV2::end_pos) {
      end_pos_.resize(buffer_size_by_type_.int32_buffer_size);
      fixed_alloc_.emplace_back(false, s, &end_pos_, sizeof(uint32_t));
    } else if (
        s == attrNamesV4::real_start_pos || s == attrNamesV3::real_start_pos) {
      real_start_pos_.resize(buffer_size_by_type_.int32_buffer_size);
      fixed_alloc_.emplace_back(false, s, &real_start_pos_, sizeof(uint32_t));
    } else if (s == attrNamesV3::end_pos) {
      end_pos_.resize(buffer_size_by_type_.int32_buffer_size);
      fixed_alloc_.emplace_back(false, s, &end_pos_, sizeof(uint32_t));
    } else if (s == attrNamesV2::pos) {
      pos_.resize(buffer_size_by_type_.int32_buffer_size);
      fixed_alloc_.emplace_back(false, s, &pos_, sizeof(uint32_t));
    } else if (s == attrNamesV2::real_end) {
      real_end_.resize(buffer_size_by_type_.int32_buffer_size);
      fixed_alloc_.emplace_back(false, s, &real_end_, sizeof(uint32_t));
    } else if (
        s == attrNamesV4::qual || s == attrNamesV3::qual ||
        s == attrNamesV2::qual) {
      qual_.resize(buffer_size_by_type_.float32_buffer_size);
      fixed_alloc_.emplace_back(false, s, &qual_, sizeof(float));
    } else if (
        s == attrNamesV4::alleles || s == attrNamesV3::alleles ||
        s == attrNamesV2::alleles) {
      alleles_.resize(buffer_size_by_type_.var_length_uint8_buffer_size);
      alleles_.offsets().resize(num_offsets);
      fixed_alloc_.emplace_back(true, s, &alleles_, sizeof(char));
    } else if (
        s == attrNamesV4::id || s == attrNamesV3::id || s == attrNamesV2::id) {
      id_.resize(buffer_size_by_type_.var_length_uint8_buffer_size);
      id_.offsets().resize(num_offsets);
      fixed_alloc_.emplace_back(true, s, &id_, sizeof(char));
    } else if (
        s == attrNamesV4::filter_ids || s == attrNamesV3::filter_ids ||
        s == attrNamesV2::filter_ids) {
      filter_ids_.resize(buffer_size_by_type_.int32_buffer_size);
      filter_ids_.offsets().resize(num_offsets);
      fixed_alloc_.emplace_back(true, s, &filter_ids_, sizeof(int32_t));
    } else if (
        s == attrNamesV4::info || s == attrNamesV3::info ||
        s == attrNamesV2::info) {
      info_.resize(buffer_size_by_type_.var_length_uint8_buffer_size);
      info_.offsets().resize(num_offsets);
      fixed_alloc_.emplace_back(true, s, &info_, sizeof(char));
    } else if (
        s == attrNamesV4::fmt || s == attrNamesV3::fmt ||
        s == attrNamesV2::fmt) {
      fmt_.resize(buffer_size_by_type_.var_length_uint8_buffer_size);
      fmt_.offsets().resize(num_offsets);
      fixed_alloc_.emplace_back(true, s, &fmt_, sizeof(char));
    } else {
      Buffer& buff = extra_attrs_[s];
      buff.resize(buffer_size_by_type_.var_length_uint8_buffer_size);
      buff.offsets().resize(num_offsets);
      fixed_alloc_.emplace_back(true, s, &buff, sizeof(char));
    }
  }
}

uint64_t AttributeBufferSet::total_size() const {
  uint64_t total_size = 0;

  // Fixed-len attributes
  total_size += sample_.size();
  total_size += start_pos_.size();
  total_size += real_start_pos_.size();
  total_size += end_pos_.size();
  total_size += pos_.size();
  total_size += real_end_.size();
  total_size += qual_.size();

  // Var-len attributes
  total_size += sample_name_.size();
  total_size += sample_name_.size();
  total_size += contig_.size();
  total_size += contig_.offsets().size() * sizeof(uint64_t);
  total_size += alleles_.size();
  total_size += alleles_.offsets().size() * sizeof(uint64_t);
  total_size += id_.size();
  total_size += id_.offsets().size() * sizeof(uint64_t);
  total_size += filter_ids_.size();
  total_size += filter_ids_.offsets().size() * sizeof(uint64_t);
  total_size += info_.size();
  total_size += info_.offsets().size() * sizeof(uint64_t);
  total_size += fmt_.size();
  total_size += fmt_.offsets().size() * sizeof(uint64_t);

  // Extra attributes (all var-len)
  for (const auto& it : extra_attrs_) {
    const Buffer& buff = it.second;
    total_size += buff.size();
    total_size += buff.offsets().size() * sizeof(uint64_t);
  }

  return total_size;
}

void AttributeBufferSet::clear() {
  // Fixed-len attributes
  sample_.clear();
  start_pos_.clear();
  real_start_pos_.clear();
  end_pos_.clear();
  pos_.clear();
  real_end_.clear();
  qual_.clear();

  // Var-len attributes
  sample_name_.clear();
  sample_name_.offsets().clear();
  contig_.clear();
  contig_.offsets().clear();
  alleles_.clear();
  alleles_.offsets().clear();
  id_.clear();
  id_.offsets().clear();
  filter_ids_.clear();
  filter_ids_.offsets().clear();
  info_.clear();
  info_.offsets().clear();
  fmt_.clear();
  fmt_.offsets().clear();

  // Extra attributes (all var-len)
  for (auto& p : extra_attrs_) {
    p.second.clear();
    p.second.offsets().clear();
  }
}

void AttributeBufferSet::set_buffers(
    tiledb::Query* query, unsigned version) const {
  if (fixed_alloc_.empty()) {
    // Set all buffers
    if (version == TileDBVCFDataset::Version::V4) {
      query->set_buffer(
          TileDBVCFDataset::DimensionNames::V4::sample,
          (uint64_t*)sample_name_.offsets().data(),
          sample_name_.offsets().size(),
          sample_name_.data<void>(),
          sample_name_.nelts<char>());
      query->set_buffer(
          TileDBVCFDataset::DimensionNames::V4::contig,
          (uint64_t*)contig_.offsets().data(),
          contig_.offsets().size(),
          contig_.data<void>(),
          contig_.nelts<char>());
      query->set_buffer(
          TileDBVCFDataset::DimensionNames::V4::start_pos,
          start_pos_.data<void>(),
          start_pos_.nelts<uint32_t>());
      query->set_buffer(
          TileDBVCFDataset::AttrNames::V4::real_start_pos,
          real_start_pos_.data<void>(),
          real_start_pos_.nelts<uint32_t>());
      query->set_buffer(
          TileDBVCFDataset::AttrNames::V4::end_pos,
          end_pos_.data<void>(),
          end_pos_.nelts<uint32_t>());
      query->set_buffer(
          TileDBVCFDataset::AttrNames::V4::qual,
          qual_.data<void>(),
          qual_.nelts<float>());
      query->set_buffer(
          TileDBVCFDataset::AttrNames::V4::alleles,
          (uint64_t*)alleles_.offsets().data(),
          alleles_.offsets().size(),
          alleles_.data<void>(),
          alleles_.nelts<char>());
      query->set_buffer(
          TileDBVCFDataset::AttrNames::V4::id,
          (uint64_t*)id_.offsets().data(),
          id_.offsets().size(),
          id_.data<void>(),
          id_.nelts<char>());
      query->set_buffer(
          TileDBVCFDataset::AttrNames::V4::filter_ids,
          (uint64_t*)filter_ids_.offsets().data(),
          filter_ids_.offsets().size(),
          filter_ids_.data<void>(),
          filter_ids_.nelts<int32_t>());
      query->set_buffer(
          TileDBVCFDataset::AttrNames::V4::info,
          (uint64_t*)info_.offsets().data(),
          info_.offsets().size(),
          info_.data<void>(),
          info_.nelts<uint8_t>());
      query->set_buffer(
          TileDBVCFDataset::AttrNames::V4::fmt,
          (uint64_t*)fmt_.offsets().data(),
          fmt_.offsets().size(),
          fmt_.data<void>(),
          fmt_.nelts<uint8_t>());
    } else if (version == TileDBVCFDataset::Version::V3) {
      query->set_buffer(
          TileDBVCFDataset::DimensionNames::V3::sample,
          sample_.data<void>(),
          sample_.nelts<uint32_t>());
      query->set_buffer(
          TileDBVCFDataset::DimensionNames::V3::start_pos,
          start_pos_.data<void>(),
          start_pos_.nelts<uint32_t>());
      query->set_buffer(
          TileDBVCFDataset::AttrNames::V3::real_start_pos,
          real_start_pos_.data<void>(),
          real_start_pos_.nelts<uint32_t>());
      query->set_buffer(
          TileDBVCFDataset::AttrNames::V3::end_pos,
          end_pos_.data<void>(),
          end_pos_.nelts<uint32_t>());
      query->set_buffer(
          TileDBVCFDataset::AttrNames::V3::qual,
          qual_.data<void>(),
          qual_.nelts<float>());
      query->set_buffer(
          TileDBVCFDataset::AttrNames::V3::alleles,
          (uint64_t*)alleles_.offsets().data(),
          alleles_.offsets().size(),
          alleles_.data<void>(),
          alleles_.nelts<char>());
      query->set_buffer(
          TileDBVCFDataset::AttrNames::V3::id,
          (uint64_t*)id_.offsets().data(),
          id_.offsets().size(),
          id_.data<void>(),
          id_.nelts<char>());
      query->set_buffer(
          TileDBVCFDataset::AttrNames::V3::filter_ids,
          (uint64_t*)filter_ids_.offsets().data(),
          filter_ids_.offsets().size(),
          filter_ids_.data<void>(),
          filter_ids_.nelts<int32_t>());
      query->set_buffer(
          TileDBVCFDataset::AttrNames::V3::info,
          (uint64_t*)info_.offsets().data(),
          info_.offsets().size(),
          info_.data<void>(),
          info_.nelts<uint8_t>());
      query->set_buffer(
          TileDBVCFDataset::AttrNames::V3::fmt,
          (uint64_t*)fmt_.offsets().data(),
          fmt_.offsets().size(),
          fmt_.data<void>(),
          fmt_.nelts<uint8_t>());
    } else {
      assert(version == TileDBVCFDataset::Version::V2);

      query->set_buffer(
          TileDBVCFDataset::DimensionNames::V2::sample,
          sample_.data<void>(),
          sample_.nelts<uint32_t>());
      query->set_buffer(
          TileDBVCFDataset::DimensionNames::V2::end_pos,
          end_pos_.data<void>(),
          end_pos_.nelts<uint32_t>());
      query->set_buffer(
          TileDBVCFDataset::AttrNames::V2::pos,
          pos_.data<void>(),
          pos_.nelts<uint32_t>());
      query->set_buffer(
          TileDBVCFDataset::AttrNames::V2::real_end,
          real_end_.data<void>(),
          real_end_.nelts<uint32_t>());
      query->set_buffer(
          TileDBVCFDataset::AttrNames::V2::qual,
          qual_.data<void>(),
          qual_.nelts<float>());
      query->set_buffer(
          TileDBVCFDataset::AttrNames::V2::alleles,
          (uint64_t*)alleles_.offsets().data(),
          alleles_.offsets().size(),
          alleles_.data<void>(),
          alleles_.nelts<char>());
      query->set_buffer(
          TileDBVCFDataset::AttrNames::V2::id,
          (uint64_t*)id_.offsets().data(),
          id_.offsets().size(),
          id_.data<void>(),
          id_.nelts<char>());
      query->set_buffer(
          TileDBVCFDataset::AttrNames::V2::filter_ids,
          (uint64_t*)filter_ids_.offsets().data(),
          filter_ids_.offsets().size(),
          filter_ids_.data<void>(),
          filter_ids_.nelts<int32_t>());
      query->set_buffer(
          TileDBVCFDataset::AttrNames::V2::info,
          (uint64_t*)info_.offsets().data(),
          info_.offsets().size(),
          info_.data<void>(),
          info_.nelts<uint8_t>());
      query->set_buffer(
          TileDBVCFDataset::AttrNames::V2::fmt,
          (uint64_t*)fmt_.offsets().data(),
          fmt_.offsets().size(),
          fmt_.data<void>(),
          fmt_.nelts<uint8_t>());
    }

    for (const auto& it : extra_attrs()) {
      query->set_buffer(
          it.first,
          (uint64_t*)it.second.offsets().data(),
          it.second.offsets().size(),
          it.second.data<void>(),
          it.second.nelts<uint8_t>());
    }
  } else {
    // For fixed-alloc, set only the allocated buffers.
    for (const auto& p : fixed_alloc_) {
      bool var_num = std::get<0>(p);
      const std::string& name = std::get<1>(p);
      Buffer* buff = std::get<2>(p);
      unsigned datatype_size = std::get<3>(p);
      if (var_num) {
        query->set_buffer(
            name,
            (uint64_t*)buff->offsets().data(),
            buff->offsets().size(),
            buff->data<void>(),
            buff->size() / datatype_size);
      } else {
        query->set_buffer(
            name, buff->data<void>(), buff->size() / datatype_size);
      }
    }
  }
}

const Buffer& AttributeBufferSet::sample_name() const {
  return sample_name_;
}

Buffer& AttributeBufferSet::sample_name() {
  return sample_name_;
}

const Buffer& AttributeBufferSet::sample() const {
  return sample_;
}

Buffer& AttributeBufferSet::sample() {
  return sample_;
}

const Buffer& AttributeBufferSet::contig() const {
  return contig_;
}

Buffer& AttributeBufferSet::contig() {
  return contig_;
}

const Buffer& AttributeBufferSet::start_pos() const {
  return start_pos_;
}

Buffer& AttributeBufferSet::start_pos() {
  return start_pos_;
}

const Buffer& AttributeBufferSet::real_start_pos() const {
  return real_start_pos_;
}

Buffer& AttributeBufferSet::real_start_pos() {
  return real_start_pos_;
}

const Buffer& AttributeBufferSet::end_pos() const {
  return end_pos_;
}

Buffer& AttributeBufferSet::end_pos() {
  return end_pos_;
}

const Buffer& AttributeBufferSet::pos() const {
  return pos_;
}

Buffer& AttributeBufferSet::pos() {
  return pos_;
}

const Buffer& AttributeBufferSet::real_end() const {
  return real_end_;
}

Buffer& AttributeBufferSet::real_end() {
  return real_end_;
}

const Buffer& AttributeBufferSet::qual() const {
  return qual_;
}

Buffer& AttributeBufferSet::qual() {
  return qual_;
}

const Buffer& AttributeBufferSet::alleles() const {
  return alleles_;
}

Buffer& AttributeBufferSet::alleles() {
  return alleles_;
}

const Buffer& AttributeBufferSet::id() const {
  return id_;
}

Buffer& AttributeBufferSet::id() {
  return id_;
}

const Buffer& AttributeBufferSet::filter_ids() const {
  return filter_ids_;
}

Buffer& AttributeBufferSet::filter_ids() {
  return filter_ids_;
}

const Buffer& AttributeBufferSet::info() const {
  return info_;
}

Buffer& AttributeBufferSet::info() {
  return info_;
}

const Buffer& AttributeBufferSet::fmt() const {
  return fmt_;
}

Buffer& AttributeBufferSet::fmt() {
  return fmt_;
}

std::vector<int> AttributeBufferSet::gt(int index) const {
  const Buffer* src = nullptr;
  if (!extra_attr("fmt_GT", &src)) {
    throw std::runtime_error("fmt_GT must be an extracted attribute.");
  }

  // FMT value: type,nvalues,values
  uint64_t offset = src->offsets().at(index);
  const int* ptr = (const int*)(src->data<char>() + offset);
  ptr++;  // skip type
  int num_values = *ptr++;

  std::vector<int> result;
  while (num_values--) {
    result.push_back(bcf_gt_allele(*ptr++));
  }
  return result;
}

const std::unordered_map<std::string, Buffer>& AttributeBufferSet::extra_attrs()
    const {
  return extra_attrs_;
}

std::unordered_map<std::string, Buffer>& AttributeBufferSet::extra_attrs() {
  return extra_attrs_;
}

bool AttributeBufferSet::extra_attr(
    const std::string& name, const Buffer** buffer) const {
  auto it = extra_attrs_.find(name);
  if (it == extra_attrs_.end()) {
    *buffer = nullptr;
    return false;
  } else {
    *buffer = &it->second;
    return true;
  }
}

bool AttributeBufferSet::extra_attr(const std::string& name, Buffer** buffer) {
  auto it = extra_attrs_.find(name);
  if (it == extra_attrs_.end()) {
    *buffer = nullptr;
    return false;
  } else {
    *buffer = &it->second;
    return true;
  }
}

AttributeBufferSet::BufferSizeByType AttributeBufferSet::sizes_per_buffer()
    const {
  return buffer_size_by_type_;
}

uint64_t AttributeBufferSet::nbuffers() const {
  return number_of_buffers_;
}

}  // namespace vcf
}  // namespace tiledb
