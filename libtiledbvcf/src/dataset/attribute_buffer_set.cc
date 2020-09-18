#include "dataset/attribute_buffer_set.h"
#include "read/in_memory_exporter.h"

namespace tiledb {
namespace vcf {

void AttributeBufferSet::allocate_fixed(
    const std::unordered_set<std::string>& attr_names, unsigned mem_budget_mb) {
  clear();
  fixed_alloc_.clear();

  // Get count of number of query buffers being allocated
  size_t num_buffers = 0;
  for (const auto& s : attr_names) {
    bool fixed_len = TileDBVCFDataset::attribute_is_fixed_len(s);
    num_buffers += fixed_len ? 1 : 2;
  }

  // Every buffer alloc gets the same size.
  uint64_t nbytes = (uint64_t(mem_budget_mb) * 1024 * 1024) / num_buffers;
  uint64_t num_offsets = nbytes / sizeof(uint64_t);

  // Requesting 0 MB will result in a 1 KB allocation. This is used by the
  // tests to test the path of incomplete TileDB queries.
  if (mem_budget_mb == 0) {
    nbytes = 1024;
    num_offsets = nbytes / sizeof(uint64_t);
  }

  using attrNamesV4 = TileDBVCFDataset::AttrNames::V4;
  using attrNamesV3 = TileDBVCFDataset::AttrNames::V3;
  using attrNamesV2 = TileDBVCFDataset::AttrNames::V2;
  using dimNamesV4 = TileDBVCFDataset::DimensionNames::V4;
  using dimNamesV3 = TileDBVCFDataset::DimensionNames::V3;
  using dimNamesV2 = TileDBVCFDataset::DimensionNames::V2;
  for (const auto& s : attr_names) {
    if (s == dimNamesV4::sample || s == dimNamesV3::sample ||
        s == dimNamesV2::sample) {
      sample_.resize(nbytes);
      fixed_alloc_.emplace_back(false, s, &sample_, sizeof(uint32_t));
    } else if (s == dimNamesV4::contig) {
      contig_.resize(nbytes);
      contig_.offsets().resize(num_offsets);
      fixed_alloc_.emplace_back(true, s, &contig_, sizeof(char));
    } else if (s == dimNamesV4::start_pos || s == dimNamesV3::start_pos) {
      start_pos_.resize(nbytes);
      fixed_alloc_.emplace_back(false, s, &start_pos_, sizeof(uint32_t));
    } else if (s == dimNamesV2::end_pos) {
      end_pos_.resize(nbytes);
      fixed_alloc_.emplace_back(false, s, &end_pos_, sizeof(uint32_t));
    } else if (s == attrNamesV3::real_start_pos) {
      real_start_pos_.resize(nbytes);
      fixed_alloc_.emplace_back(false, s, &real_start_pos_, sizeof(uint32_t));
    } else if (s == attrNamesV3::end_pos) {
      end_pos_.resize(nbytes);
      fixed_alloc_.emplace_back(false, s, &end_pos_, sizeof(uint32_t));
    } else if (s == attrNamesV2::pos) {
      pos_.resize(nbytes);
      fixed_alloc_.emplace_back(false, s, &pos_, sizeof(uint32_t));
    } else if (s == attrNamesV2::real_end) {
      real_end_.resize(nbytes);
      fixed_alloc_.emplace_back(false, s, &real_end_, sizeof(uint32_t));
    } else if (
        s == attrNamesV4::qual || s == attrNamesV3::qual ||
        s == attrNamesV2::qual) {
      qual_.resize(nbytes);
      fixed_alloc_.emplace_back(false, s, &qual_, sizeof(float));
    } else if (
        s == attrNamesV4::alleles || s == attrNamesV3::alleles ||
        s == attrNamesV2::alleles) {
      alleles_.resize(nbytes);
      alleles_.offsets().resize(num_offsets);
      fixed_alloc_.emplace_back(true, s, &alleles_, sizeof(char));
    } else if (
        s == attrNamesV4::id || s == attrNamesV3::id || s == attrNamesV2::id) {
      id_.resize(nbytes);
      id_.offsets().resize(num_offsets);
      fixed_alloc_.emplace_back(true, s, &id_, sizeof(char));
    } else if (
        s == attrNamesV4::filter_ids || s == attrNamesV3::filter_ids ||
        s == attrNamesV2::filter_ids) {
      filter_ids_.resize(nbytes);
      filter_ids_.offsets().resize(num_offsets);
      fixed_alloc_.emplace_back(true, s, &filter_ids_, sizeof(int32_t));
    } else if (
        s == attrNamesV4::info || s == attrNamesV3::info ||
        s == attrNamesV2::info) {
      info_.resize(nbytes);
      info_.offsets().resize(num_offsets);
      fixed_alloc_.emplace_back(true, s, &info_, sizeof(char));
    } else if (
        s == attrNamesV4::fmt || s == attrNamesV3::fmt ||
        s == attrNamesV2::fmt) {
      fmt_.resize(nbytes);
      fmt_.offsets().resize(num_offsets);
      fixed_alloc_.emplace_back(true, s, &fmt_, sizeof(char));
    } else {
      Buffer& buff = extra_attrs_[s];
      buff.resize(nbytes);
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
          sample_.data<void>(),
          sample_.nelts<uint32_t>());
      query->set_buffer(
          TileDBVCFDataset::DimensionNames::V4::contig,
          (uint64_t*)contig_.offsets().data(),
          contig_.offsets().size(),
          contig_.data<void>(),
          contig_.nelts<uint8_t>());
      query->set_buffer(
          TileDBVCFDataset::DimensionNames::V4::start_pos,
          start_pos_.data<void>(),
          start_pos_.nelts<uint32_t>());
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

}  // namespace vcf
}  // namespace tiledb
