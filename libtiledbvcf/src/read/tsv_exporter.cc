/**
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2019-2021 TileDB, Inc.
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

#include "read/tsv_exporter.h"
#include "vcf/htslib_value.h"

namespace tiledb {
namespace vcf {

TSVExporter::TSVExporter(
    const std::string& output_file,
    const std::vector<std::string>& output_fields)
    : output_initialized_(false)
    , output_file_(output_file) {
  need_headers_ = true;
  for (const auto& f : output_fields) {
    auto parts = utils::split(f, ':');
    if (parts.size() < 2) {
      std::string name = parts[0];
      if (name != "ID" && name != "REF" && name != "ALT" && name != "QUAL" &&
          name != "POS" && name != "CHR" && name != "FILTER" &&
          name != "SAMPLE")
        throw std::invalid_argument(
            "Error initializing TSV export: unknown field '" + f + "'.");
      output_fields_.emplace_back(OutputField::Type::Regular, name);
    } else {
      std::string name = parts[1];
      if (utils::starts_with(f, "I:")) {
        need_headers_ = true;
        output_fields_.emplace_back(OutputField::Type::Info, name);
      } else if (utils::starts_with(f, "F:")) {
        need_headers_ = true;
        output_fields_.emplace_back(OutputField::Type::FmtF, name);
      } else if (utils::starts_with(f, "S:")) {
        need_headers_ = true;
        output_fields_.emplace_back(OutputField::Type::FmtS, name);
      } else if (utils::starts_with(f, "Q:")) {
        output_fields_.emplace_back(OutputField::Type::Query, name);
      } else {
        throw std::invalid_argument(
            "Error initializing TSV export: unknown field '" + f + "'.");
      }
    }
  }
}

TSVExporter::~TSVExporter() {
  close();
}

void TSVExporter::reset() {
  Exporter::reset();
  close();
  output_initialized_ = false;
}

bool TSVExporter::export_record(
    const SampleAndId& sample,
    const bcf_hdr_t* hdr,
    const Region& query_region,
    uint32_t contig_offset,
    const ReadQueryResults& query_results,
    uint64_t cell_idx) {
  init_output_stream();

  recover_record(
      hdr,
      query_results,
      cell_idx,
      query_region.seq_name,
      contig_offset,
      reusable_rec_.get());
  auto rec = reusable_rec_.get();

  std::ostream& os = output_file_.empty() ? std::cout : os_;
  os << sample.sample_name;
  for (auto& field : output_fields_) {
    // skip SAMPLE since it is included by default
    if (field.name == "SAMPLE") {
      continue;
    }
    os << '\t';
    switch (field.type) {
      case OutputField::Type::Regular: {
        if (field.name == "REF") {
          os << rec->d.allele[0];
          break;
        } else if (field.name == "ALT") {
          for (int i = 1; i < rec->n_allele; ++i) {
            os << rec->d.allele[i];
            if (i < rec->n_allele - 1) {
              os << ',';
            }
          }
        } else if (field.name == "ID") {
          os << rec->d.id;
        } else if (field.name == "QUAL") {
          os << rec->qual;
        } else if (field.name == "POS") {
          os << rec->pos + 1;
        } else if (field.name == "CHR") {
          os << bcf_seqname(hdr, rec);
        } else if (field.name == "FILTER") {
          for (int i = 0; i < rec->d.n_flt; i++) {
            os << bcf_hdr_int2id(hdr, BCF_DT_ID, rec->d.flt[i]);
            if (i < rec->d.n_flt - 1)
              os << ";";
          }
        }
        break;
      }
      case OutputField::Type::Info: {
        int tag_id = bcf_hdr_id2int(hdr, BCF_DT_ID, field.name.c_str());
        if (!bcf_hdr_idinfo_exists(hdr, BCF_HL_INFO, tag_id))
          throw std::runtime_error(
              "Error in TSV export: sample " + sample.sample_name +
              " header does not define info field '" + field.name + "'.");
        auto type = bcf_hdr_id2type(hdr, BCF_HL_INFO, tag_id);
        HtslibValueMem val;
        int nvalues = bcf_get_info_values(
            hdr, rec, field.name.c_str(), &val.dst, &val.ndst, type);
        if (nvalues <= 0) {
          os << '.';
          break;
        }
        if (type == BCF_HT_STR) {
          std::string s((const char*)val.dst, nvalues);
          os << s;
        } else {
          for (int i = 0; i < nvalues; ++i) {
            switch (type) {
              case BCF_HT_INT: {
                os << ((int*)val.dst)[i];
                break;
              }
              case BCF_HT_REAL:
                os << ((float*)val.dst)[i];
                break;
              default:
                throw std::runtime_error(
                    "Error in TSV export: unhandled info type " +
                    std::to_string(type));
                break;
            }
            if (i < nvalues - 1)
              os << ',';
          }
        }
        break;
      }
      case OutputField::Type::FmtF:
      case OutputField::Type::FmtS: {
        int tag_id = bcf_hdr_id2int(hdr, BCF_DT_ID, field.name.c_str());
        if (!bcf_hdr_idinfo_exists(hdr, BCF_HL_FMT, tag_id))
          throw std::runtime_error(
              "Error in TSV export: sample " + sample.sample_name +
              " header does not define fmt field '" + field.name + "'.");
        auto type = field.name == "GT" ?
                        BCF_HT_INT :
                        bcf_hdr_id2type(hdr, BCF_HL_FMT, tag_id);
        HtslibValueMem val;
        switch (type) {
          case BCF_HT_INT: {
            int nvalues = bcf_get_format_values(
                hdr, rec, field.name.c_str(), &val.dst, &val.ndst, type);
            if (nvalues <= 0) {
              os << '.';
              break;
            }
            for (int i = 0; i < nvalues; ++i) {
              if (field.name == "GT")
                os << bcf_gt_allele(((int32_t*)val.dst)[i]);
              else
                os << ((int32_t*)val.dst)[i];
              if (i < nvalues - 1)
                os << ',';
            }
            break;
          }
          case BCF_HT_STR: {
            char** s = 0;
            int n = bcf_get_format_string(
                hdr, rec, field.name.c_str(), &s, &val.ndst);
            if (n <= 0) {
              os << '.';
              break;
            }
            std::string str(s[0], val.ndst);
            os << str;
            free(s[0]);
            free(s);
            break;
          }
          case BCF_HT_REAL: {
            int nvalues = bcf_get_format_values(
                hdr, rec, field.name.c_str(), &val.dst, &val.ndst, type);
            if (nvalues <= 0) {
              os << '.';
              break;
            }
            for (int i = 0; i < nvalues; ++i) {
              os << ((float*)val.dst)[i];
              if (i < nvalues - 1)
                os << ',';
            }
            break;
          }
          default:
            break;
        }
        break;
      }
      case OutputField::Type::Query: {
        if (field.name == "POS") {
          os << query_region.min + 1;
        } else if (field.name == "END") {
          os << query_region.max + 1;
        } else if (field.name == "LINE") {
          os << query_region.line;
        } else {
          throw std::runtime_error(
              "Error in TSV export: expected 'Q:' field to be 'POS' or 'END'; "
              "got '" +
              field.name + "'.");
        }
        break;
      }
    }
  }
  os << "\n";

  return true;
}

std::set<std::string> TSVExporter::array_attributes_required() const {
  // TODO: currently we require all attributes for record recovery.
  return dataset_->all_attributes();
}

void TSVExporter::init_output_stream() {
  if (output_initialized_)
    return;

  if (!output_file_.empty()) {
    std::string path = utils::uri_join(output_dir_, output_file_);
    os_.open(path.c_str());
    if (!os_.good() || os_.fail() || os_.bad()) {
      const char* err_c_str = strerror(errno);
      throw std::invalid_argument(
          "Error opening output file '" + path + "'; " +
          std::string(err_c_str));
    }
    all_exported_files_.push_back(path);
  }

  // Write the header. First column is always sample name.
  std::ostream& os = output_file_.empty() ? std::cout : os_;
  os << "SAMPLE";
  for (auto& t : output_fields_) {
    // skip SAMPLE since it is included by default
    if (t.name == "SAMPLE") {
      continue;
    }
    os << "\t";
    switch (t.type) {
      case OutputField::Type::Info:
        os << "I:";
        break;
      case OutputField::Type::FmtF:
        os << "F:";
        break;
      case OutputField::Type::FmtS:
        os << "S:";
        break;
      case OutputField::Type::Query:
        os << "Q:";
        break;
      default:
        break;
    }
    os << t.name;
  }
  os << '\n';

  output_initialized_ = true;
}

void TSVExporter::close() {
  if (output_file_.empty()) {
    std::cout.flush();
  } else {
    os_.flush();
    os_.close();
  }
}

}  // namespace vcf
}  // namespace tiledb
