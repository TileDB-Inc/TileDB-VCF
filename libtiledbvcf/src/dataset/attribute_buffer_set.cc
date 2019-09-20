#include "dataset/attribute_buffer_set.h"

namespace tiledb {
namespace vcf {

void AttributeBufferSet::allocate_fixed(
    const std::set<std::string>& attr_names, unsigned size_mb) {
  // Requesting 0 MB will result in a 1 KB allocation. This is used by the
  // tests to test the path of incomplete TileDB queries.
  const uint64_t nbytes = size_mb == 0 ? 1024 : (size_mb * 1024 * 1024);
  const uint64_t num_offsets = nbytes / sizeof(uint64_t);

  clear();
  fixed_alloc_.clear();

  using names = TileDBVCFDataset::AttrNames;
  for (const auto& s : attr_names) {
    if (s == TILEDB_COORDS) {
      coords_.resize(nbytes);
      fixed_alloc_.emplace_back(
          false, TILEDB_COORDS, &coords_, sizeof(uint32_t));
    } else if (s == names::pos) {
      pos_.resize(nbytes);
      fixed_alloc_.emplace_back(false, names::pos, &pos_, sizeof(uint32_t));
    } else if (s == names::real_end) {
      real_end_.resize(nbytes);
      fixed_alloc_.emplace_back(
          false, names::real_end, &real_end_, sizeof(uint32_t));
    } else if (s == names::qual) {
      qual_.resize(nbytes);
      fixed_alloc_.emplace_back(false, names::qual, &qual_, sizeof(float));
    } else if (s == names::alleles) {
      alleles_.resize(nbytes);
      alleles_.offsets().resize(num_offsets);
      fixed_alloc_.emplace_back(true, names::alleles, &alleles_, sizeof(char));
    } else if (s == names::id) {
      id_.resize(nbytes);
      id_.offsets().resize(num_offsets);
      fixed_alloc_.emplace_back(true, names::id, &id_, sizeof(char));
    } else if (s == names::filter_ids) {
      filter_ids_.resize(nbytes);
      filter_ids_.offsets().resize(num_offsets);
      fixed_alloc_.emplace_back(
          true, names::filter_ids, &filter_ids_, sizeof(int32_t));
    } else if (s == names::info) {
      info_.resize(nbytes);
      info_.offsets().resize(num_offsets);
      fixed_alloc_.emplace_back(true, names::info, &info_, sizeof(char));
    } else if (s == names::fmt) {
      fmt_.resize(nbytes);
      fmt_.offsets().resize(num_offsets);
      fixed_alloc_.emplace_back(true, names::fmt, &fmt_, sizeof(char));
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
  total_size += coords_.size();
  total_size += pos_.size();
  total_size += real_end_.size();
  total_size += qual_.size();

  // Var-len attributes
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
  coords_.clear();
  pos_.clear();
  real_end_.clear();
  qual_.clear();

  // Var-len attributes
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

void AttributeBufferSet::set_buffers(tiledb::Query* query) const {
  if (fixed_alloc_.empty()) {
    // Set all buffers
    query->set_buffer(
        TILEDB_COORDS, coords_.data<void>(), coords_.nelts<uint32_t>());
    query->set_buffer(
        TileDBVCFDataset::AttrNames::pos,
        pos_.data<void>(),
        pos_.nelts<uint32_t>());
    query->set_buffer(
        TileDBVCFDataset::AttrNames::real_end,
        real_end_.data<void>(),
        real_end_.nelts<uint32_t>());
    query->set_buffer(
        TileDBVCFDataset::AttrNames::qual,
        qual_.data<void>(),
        qual_.nelts<float>());
    query->set_buffer(
        TileDBVCFDataset::AttrNames::alleles,
        (uint64_t*)alleles_.offsets().data(),
        alleles_.offsets().size(),
        alleles_.data<void>(),
        alleles_.nelts<char>());
    query->set_buffer(
        TileDBVCFDataset::AttrNames::id,
        (uint64_t*)id_.offsets().data(),
        id_.offsets().size(),
        id_.data<void>(),
        id_.nelts<char>());
    query->set_buffer(
        TileDBVCFDataset::AttrNames::filter_ids,
        (uint64_t*)filter_ids_.offsets().data(),
        filter_ids_.offsets().size(),
        filter_ids_.data<void>(),
        filter_ids_.nelts<int32_t>());
    query->set_buffer(
        TileDBVCFDataset::AttrNames::info,
        (uint64_t*)info_.offsets().data(),
        info_.offsets().size(),
        info_.data<void>(),
        info_.nelts<uint8_t>());
    query->set_buffer(
        TileDBVCFDataset::AttrNames::fmt,
        (uint64_t*)fmt_.offsets().data(),
        fmt_.offsets().size(),
        fmt_.data<void>(),
        fmt_.nelts<uint8_t>());

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

const Buffer& AttributeBufferSet::coords() const {
  return coords_;
}

Buffer& AttributeBufferSet::coords() {
  return coords_;
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

const std::map<std::string, Buffer>& AttributeBufferSet::extra_attrs() const {
  return extra_attrs_;
}

std::map<std::string, Buffer>& AttributeBufferSet::extra_attrs() {
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
