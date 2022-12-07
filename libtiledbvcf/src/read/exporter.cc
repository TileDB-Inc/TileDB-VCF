#include "read/exporter.h"

namespace tiledb {
namespace vcf {

Exporter::Exporter()
    : dataset_(nullptr)
    , reusable_rec_(bcf_init1(), bcf_destroy) {
}

void Exporter::set_output_dir(const std::string& output_dir) {
  output_dir_ = output_dir;
}

void Exporter::set_dataset(const TileDBVCFDataset* dataset) {
  dataset_ = dataset;
}

void Exporter::upload_exported_files(
    const VFS& vfs, const std::string& upload_dir) const {
  if (upload_dir.empty())
    return;

  Buffer buffer;
  for (const auto& src_path : all_exported_files_) {
    auto filename = utils::uri_filename(src_path);
    auto dest_uri = utils::uri_join(upload_dir, filename);
    utils::upload_file(vfs, src_path, dest_uri, buffer);
  }
}

void Exporter::recover_record(
    const bcf_hdr_t* hdr,
    const ReadQueryResults& query_results,
    uint64_t cell_idx,
    const std::string& contig_name,
    uint32_t contig_offset,
    bcf1_t* dst) const {
  // First things first, clear the record.
  bcf_clear(dst);

  const auto& results = query_results;
  const auto* buffers = results.buffers();

  dst->rid = bcf_hdr_name2id(hdr, contig_name.c_str());
  if (dst->rid < 0)
    throw std::runtime_error(
        "Record recovery error; no ID for contig name '" + contig_name + "'");

  if (dataset_->metadata().version == TileDBVCFDataset::Version::V4) {
    dst->pos = buffers->real_start_pos().value<uint32_t>(cell_idx);
  } else if (dataset_->metadata().version == TileDBVCFDataset::Version::V3) {
    dst->pos =
        buffers->real_start_pos().value<uint32_t>(cell_idx) - contig_offset;
  } else {
    assert(dataset_->metadata().version == TileDBVCFDataset::Version::V2);
    dst->pos = buffers->pos().value<uint32_t>(cell_idx) - contig_offset;
  }

  dst->qual = buffers->qual().value<float>(cell_idx);
  dst->n_sample = 1;

  const uint64_t alleles_offset = buffers->alleles().offsets()[cell_idx];
  int st = bcf_update_alleles_str(
      hdr, dst, buffers->alleles().data<char>() + alleles_offset);
  if (st < 0)
    throw std::runtime_error(
        "Record recovery error; Error adding alleles, " + std::to_string(st));
  dst->rlen = (int)strlen(dst->d.allele[0]);

  const uint64_t filters_offset = buffers->filter_ids().offsets()[cell_idx];
  int* buff_filters =
      buffers->filter_ids().data<int32_t>() + filters_offset / sizeof(int32_t);
  int nflt = *buff_filters;
  st = bcf_update_filter(hdr, dst, buff_filters + 1, nflt);
  if (st < 0)
    throw std::runtime_error(
        "Record recovery error; Error adding filter IDs, " +
        std::to_string(st));

  int32_t end;
  if (dataset_->metadata().version == TileDBVCFDataset::Version::V4) {
    end = buffers->end_pos().value<uint32_t>(cell_idx);
  } else if (dataset_->metadata().version == TileDBVCFDataset::Version::V3) {
    end = buffers->end_pos().value<uint32_t>(cell_idx) - contig_offset;
  } else {
    assert(dataset_->metadata().version == TileDBVCFDataset::Version::V2);
    end = buffers->real_end().value<uint32_t>(cell_idx) - contig_offset;
  }

  // Only update the END field if it exists in the header
  int inf_id = bcf_hdr_id2int(hdr, BCF_DT_ID, "END");
  if (bcf_hdr_idinfo_exists(hdr, BCF_HL_INFO, inf_id)) {
    end += 1;
    st = bcf_update_info(hdr, dst, "END", &end, 1, BCF_HT_INT);
    if (st < 0)
      throw std::runtime_error(
          "Record recovery error; Error adding END tag, " + std::to_string(st));
  }

  const uint64_t id_offset = buffers->id().offsets()[cell_idx];
  st = bcf_update_id(hdr, dst, buffers->id().data<char>() + id_offset);
  if (st < 0)
    throw std::runtime_error(
        "Record recovery error; Error adding ID, " + std::to_string(st));

  const uint64_t info_offset = buffers->info().offsets()[cell_idx];
  const char* info_ptr = buffers->info().data<char>() + info_offset;
  unsigned num_info_fields = *(uint32_t*)info_ptr;
  info_ptr += sizeof(uint32_t);
  // Reusable buffer for string-valued fields.
  std::string str_buffer;
  for (unsigned i = 0; i < num_info_fields; ++i) {
    const char* key = info_ptr;
    size_t key_nbytes = strlen(key) + 1;
    info_ptr += key_nbytes;

    int type = *(int*)(info_ptr);
    info_ptr += sizeof(int);
    int nvalues = *(int*)(info_ptr);
    info_ptr += sizeof(int);

    // For string types, bcf_update_info requires null-termination.
    if (type == BCF_HT_STR) {
      str_buffer.clear();
      str_buffer.reserve(nvalues + 1);
      str_buffer.append(info_ptr, nvalues);
      str_buffer.push_back('\0');
      st = bcf_update_info(
          hdr, dst, key, str_buffer.data(), str_buffer.size(), type);
    } else {
      st = bcf_update_info(hdr, dst, key, info_ptr, nvalues, type);
    }

    if (st < 0)
      throw std::runtime_error(
          "Record recovery error; Error adding INFO field '" +
          std::string(key) + "', " + std::to_string(st));

    info_ptr += nvalues * utils::bcf_type_size(type);
  }

  const uint64_t fmt_offset = buffers->fmt().offsets()[cell_idx];
  const char* fmt_ptr = buffers->fmt().data<char>() + fmt_offset;
  unsigned num_fmt_fields = *(uint32_t*)fmt_ptr;
  fmt_ptr += sizeof(uint32_t);
  for (unsigned i = 0; i < num_fmt_fields; ++i) {
    const char* key = fmt_ptr;
    size_t key_nbytes = strlen(key) + 1;
    fmt_ptr += key_nbytes;

    int type = *(int*)(fmt_ptr);
    fmt_ptr += sizeof(int);
    int nvalues = *(int*)(fmt_ptr);
    fmt_ptr += sizeof(int);

    // For string types, bcf_update_format requires null-termination.
    if (type == BCF_HT_STR) {
      str_buffer.clear();
      str_buffer.reserve(nvalues + 1);
      str_buffer.append(fmt_ptr, nvalues);
      str_buffer.push_back('\0');
      st = bcf_update_format(
          hdr, dst, key, str_buffer.data(), str_buffer.size(), type);
    } else {
      st = bcf_update_format(hdr, dst, key, fmt_ptr, nvalues, type);
    }

    if (st < 0)
      throw std::runtime_error(
          "Record recovery error; Error adding FMT field '" + std::string(key) +
          "', " + std::to_string(st));

    fmt_ptr += nvalues * utils::bcf_type_size(type);
  }

  for (auto& attr : buffers->extra_attrs()) {
    auto parts = TileDBVCFDataset::split_info_fmt_attr_name(attr.first);
    if ((parts.first != "info" && parts.first != "fmt"))
      throw std::runtime_error(
          "Record recovery error; improper attribute name '" + attr.first +
          "'.");
    const bool is_info = parts.first == "info";
    const auto& field_name = parts.second;

    auto sizes_iter = results.extra_attrs_size().find(attr.first);
    if (sizes_iter == results.extra_attrs_size().end())
      throw std::runtime_error(
          "Could not find size for extra attribute" + attr.first +
          " in recover_record");

    auto sizes = results.extra_attrs_size().at(attr.first);
    const char* field_ptr =
        attr.second.data<char>() + attr.second.offsets()[cell_idx];
    size_t field_nbytes =
        (cell_idx == sizes.first - 1 ? sizes.second :
                                       attr.second.offsets()[cell_idx + 1]) -
        attr.second.offsets()[cell_idx];

    // Check if field exists for this record (check for dummy value).
    if (field_nbytes == 1 && *field_ptr == 0)
      continue;

    int type = *(int*)(field_ptr);
    field_ptr += sizeof(int);
    int nvalues = *(int*)(field_ptr);
    field_ptr += sizeof(int);

    const char* values_ptr = field_ptr;

    // For string types, bcf_update_info/format requires null-termination.
    if (type == BCF_HT_STR) {
      str_buffer.clear();
      str_buffer.reserve(nvalues + 1);
      str_buffer.append(field_ptr, nvalues);
      str_buffer.push_back('\0');
      nvalues = str_buffer.size();
      values_ptr = str_buffer.data();
    }

    if (is_info) {
      st = bcf_update_info(
          hdr, dst, field_name.c_str(), values_ptr, nvalues, type);
      if (st < 0)
        throw std::runtime_error(
            "Record recovery error; Error adding INFO field '" +
            std::string(field_name) + "', " + std::to_string(st));
      num_info_fields++;
    } else {
      st = bcf_update_format(
          hdr, dst, field_name.c_str(), values_ptr, nvalues, type);
      if (st < 0)
        throw std::runtime_error(
            "Record recovery error; Error adding FMT field '" +
            std::string(field_name) + "', " + std::to_string(st));
      num_fmt_fields++;
    }
  }
}

void Exporter::enable_iaf() {
  add_iaf = true;
}

bool Exporter::need_headers() const {
  return need_headers_;
}

}  // namespace vcf
}  // namespace tiledb
