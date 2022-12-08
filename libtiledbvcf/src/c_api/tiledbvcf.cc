/**
 * @file   tiledbvcf.cc
 *
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
 *
 * @section DESCRIPTION
 *
 * This file defines the C API for TileDB-VCF.
 */

#include "c_api/tiledbvcf.h"
#include "read/reader.h"
#include "utils/logger_public.h"
#include "utils/utils.h"
#include "vcf/bed_file.h"
#include "write/writer.h"

#include <cassert>
#include <iostream>
#include <memory>

using namespace tiledb::vcf;

/* ********************************* */
/*           STRUCT TYPES            */
/* ********************************* */

struct tiledb_vcf_reader_t {
  std::unique_ptr<tiledb::vcf::Reader> reader_;
  std::string saved_errmsg_;
};

struct tiledb_vcf_writer_t {
  std::unique_ptr<tiledb::vcf::Writer> writer_;
  std::string saved_errmsg_;
};

struct tiledb_vcf_error_t {
  std::string errmsg_;
};

struct tiledb_vcf_bed_file_t {
  std::unique_ptr<tiledb::vcf::BedFile> bed_file_;
  std::string saved_errmsg_;
};

/* ********************************* */
/*             HELPERS               */
/* ********************************* */

static void save_error(tiledb_vcf_reader_t* reader, const std::string& error) {
  LOG_ERROR(error);
  reader->saved_errmsg_ = error;
}

static void save_error(
    tiledb_vcf_bed_file_t* bed_file, const std::string& error) {
  LOG_ERROR(error);
  bed_file->saved_errmsg_ = error;
}

static void save_error(tiledb_vcf_writer_t* writer, const std::string& error) {
  LOG_ERROR(error);
  writer->saved_errmsg_ = error;
}

/**
 * Helper macro that executes the given statement, catching all exceptions and
 * saving the error message.
 *
 * @param obj TileDB VCF reader or writer object
 * @param stmt Statement to execute
 * @return True if an error occurred.
 */
#define SAVE_ERROR_CATCH(obj, stmt)                                \
  [&]() {                                                          \
    (obj)->saved_errmsg_.clear();                                  \
    try {                                                          \
      (stmt);                                                      \
    } catch (const std::exception& e) {                            \
      auto err = std::string("TileDB-VCF exception: ") + e.what(); \
      save_error(obj, err);                                        \
      return true;                                                 \
    }                                                              \
    return false;                                                  \
  }()

inline int32_t sanity_check(const tiledb_vcf_reader_t* reader) {
  if (reader == nullptr || reader->reader_ == nullptr) {
    std::string err = "Invalid TileDB VCF reader object";
    LOG_ERROR(err);
    return TILEDB_VCF_ERR;
  }
  return TILEDB_VCF_OK;
}

inline int32_t sanity_check(const tiledb_vcf_bed_file_t* bed_file) {
  if (bed_file == nullptr || bed_file->bed_file_ == nullptr) {
    std::string err = "Invalid TileDB VCF bed_file object";
    LOG_ERROR(err);
    return TILEDB_VCF_ERR;
  }
  return TILEDB_VCF_OK;
}

inline int32_t sanity_check(const tiledb_vcf_writer_t* writer) {
  if (writer == nullptr || writer->writer_ == nullptr) {
    std::string err = "Invalid TileDB VCF writer object";
    LOG_ERROR(err);
    return TILEDB_VCF_ERR;
  }
  return TILEDB_VCF_OK;
}

/* ********************************* */
/*              MISC                 */
/* ********************************* */

void tiledb_vcf_version(const char** version) {
  const std::string& version_str = tiledb::vcf::utils::version_info();
  *version = version_str.c_str();
}

void tiledb_vcf_config_logging(const char* level, const char* logfile) {
  LOG_CONFIG(level, logfile);
}

/* ********************************* */
/*              READER               */
/* ********************************* */

int32_t tiledb_vcf_reader_alloc(tiledb_vcf_reader_t** reader) {
  if (reader == nullptr) {
    std::string err("Null pointer given for TileDB-VCF reader object");
    LOG_ERROR(err);
    return TILEDB_VCF_ERR;
  }

  // Create a reader struct
  *reader = new (std::nothrow) tiledb_vcf_reader_t;
  if (*reader == nullptr) {
    std::string err("Failed to allocate TileDB-VCF reader object");
    LOG_ERROR(err);
    return TILEDB_VCF_ERR;
  }

  // Create a new reader object
  try {
    (*reader)->reader_.reset(new tiledb::vcf::Reader);
  } catch (const std::exception& e) {
    std::string err(
        "Failed to allocate TileDB-VCF reader object: " +
        std::string(e.what()));
    LOG_ERROR(err);
    return TILEDB_VCF_ERR;
  }

  if ((*reader)->reader_ == nullptr) {
    delete *reader;
    std::string err("Failed to allocate TileDB-VCF reader object");
    LOG_ERROR(err);
    return TILEDB_VCF_ERR;
  }

  return TILEDB_VCF_OK;
}

void tiledb_vcf_reader_free(tiledb_vcf_reader_t** reader) {
  if (reader != nullptr && *reader != nullptr) {
    (*reader)->reader_.reset(nullptr);
    delete (*reader);
    *reader = nullptr;
  }
}

int32_t tiledb_vcf_reader_init(
    tiledb_vcf_reader_t* reader, const char* dataset_uri) {
  if (sanity_check(reader) == TILEDB_VCF_ERR || dataset_uri == nullptr)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(reader, reader->reader_->open_dataset(dataset_uri)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_set_samples_file(
    tiledb_vcf_reader_t* reader, const char* uri) {
  if (sanity_check(reader) == TILEDB_VCF_ERR || uri == nullptr)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(reader, reader->reader_->set_samples_file(uri)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_set_bed_file(
    tiledb_vcf_reader_t* reader, const char* uri) {
  if (sanity_check(reader) == TILEDB_VCF_ERR || uri == nullptr)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(reader, reader->reader_->set_bed_file(uri)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_set_samples(
    tiledb_vcf_reader_t* reader, const char* samples) {
  if (sanity_check(reader) == TILEDB_VCF_ERR || samples == nullptr)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(reader, reader->reader_->set_samples(samples)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_set_regions(
    tiledb_vcf_reader_t* reader, const char* regions) {
  if (sanity_check(reader) == TILEDB_VCF_ERR || regions == nullptr)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(reader, reader->reader_->set_regions(regions)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_set_sort_regions(
    tiledb_vcf_reader_t* reader, int32_t sort_regions) {
  if (sanity_check(reader) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          reader, reader->reader_->set_sort_regions(sort_regions == 1)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_set_region_partition(
    tiledb_vcf_reader_t* reader, int32_t partition, int32_t num_partitions) {
  if (sanity_check(reader) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          reader,
          reader->reader_->set_region_partition(partition, num_partitions)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_set_sample_partition(
    tiledb_vcf_reader_t* reader, int32_t partition, int32_t num_partitions) {
  if (sanity_check(reader) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          reader,
          reader->reader_->set_sample_partition(partition, num_partitions)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_set_buffer_values(
    tiledb_vcf_reader_t* reader,
    const char* attribute,
    int64_t buff_size,
    void* buff) {
  if (sanity_check(reader) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          reader,
          reader->reader_->set_buffer_values(attribute, buff, buff_size)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_set_buffer_offsets(
    tiledb_vcf_reader_t* reader,
    const char* attribute,
    int64_t buff_size,
    int32_t* buff) {
  if (sanity_check(reader) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          reader,
          reader->reader_->set_buffer_offsets(attribute, buff, buff_size)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_set_buffer_list_offsets(
    tiledb_vcf_reader_t* reader,
    const char* attribute,
    int64_t buff_size,
    int32_t* buff) {
  if (sanity_check(reader) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          reader,
          reader->reader_->set_buffer_list_offsets(attribute, buff, buff_size)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_set_buffer_validity_bitmap(
    tiledb_vcf_reader_t* reader,
    const char* attribute,
    int64_t buff_size,
    uint8_t* buff) {
  if (sanity_check(reader) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          reader,
          reader->reader_->set_buffer_validity_bitmap(
              attribute, buff, buff_size)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_set_memory_budget(
    tiledb_vcf_reader_t* reader, int32_t memory_mb) {
  if (sanity_check(reader) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(reader, reader->reader_->set_memory_budget(memory_mb)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_set_max_num_records(
    tiledb_vcf_reader_t* reader, int64_t max_num_records) {
  if (sanity_check(reader) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          reader, reader->reader_->set_record_limit(max_num_records)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_set_tiledb_config(
    tiledb_vcf_reader_t* reader, const char* config) {
  if (sanity_check(reader) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(reader, reader->reader_->set_tiledb_config(config)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_set_tiledb_stats_enabled(
    tiledb_vcf_reader_t* reader, const bool stats_enabled) {
  if (sanity_check(reader) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          reader, reader->reader_->set_tiledb_stats_enabled(stats_enabled)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_get_tiledb_stats_enabled(
    tiledb_vcf_reader_t* reader, bool* enabled) {
  if (sanity_check(reader) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(reader, reader->reader_->tiledb_stats_enabled(enabled)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_set_tiledb_stats_enabled_vcf_header_array(
    tiledb_vcf_reader_t* reader, const bool stats_enabled) {
  if (sanity_check(reader) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          reader,
          reader->reader_->set_tiledb_stats_enabled_vcf_header_array(
              stats_enabled)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_get_tiledb_stats_enabled_vcf_header_array(
    tiledb_vcf_reader_t* reader, bool* enabled) {
  if (sanity_check(reader) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          reader,
          reader->reader_->tiledb_stats_enabled_vcf_header_array(enabled)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_get_tiledb_stats(
    tiledb_vcf_reader_t* reader, char** stats) {
  if (sanity_check(reader) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(reader, reader->reader_->tiledb_stats(stats)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

TILEDBVCF_EXPORT int32_t tiledb_vcf_reader_get_af_filter_exists(
    tiledb_vcf_reader_t* reader, bool* present) {
  if (sanity_check(reader) == TILEDB_VCF_ERR || present == nullptr)
    return TILEDB_VCF_ERR;

  *present = reader->reader_->af_filter_enabled();

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_read(tiledb_vcf_reader_t* reader) {
  if (sanity_check(reader) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(reader, reader->reader_->read()))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_get_status(
    tiledb_vcf_reader_t* reader, tiledb_vcf_read_status_t* status) {
  if (sanity_check(reader) == TILEDB_VCF_ERR || status == nullptr)
    return TILEDB_VCF_ERR;

  tiledb::vcf::ReadStatus st = reader->reader_->read_status();
  *status = static_cast<tiledb_vcf_read_status_t>(st);

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_get_result_num_records(
    tiledb_vcf_reader_t* reader, int64_t* num_records) {
  if (sanity_check(reader) == TILEDB_VCF_ERR || num_records == nullptr)
    return TILEDB_VCF_ERR;

  *num_records = reader->reader_->num_records_exported();

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_get_result_size(
    tiledb_vcf_reader_t* reader,
    const char* attribute,
    int64_t* num_offsets,
    int64_t* num_data_elements,
    int64_t* num_data_bytes) {
  if (sanity_check(reader) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          reader,
          reader->reader_->result_size(
              attribute, num_offsets, num_data_elements, num_data_bytes)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_get_num_buffers(
    tiledb_vcf_reader_t* reader, int32_t* num_buffers) {
  if (sanity_check(reader) == TILEDB_VCF_ERR || num_buffers == nullptr)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(reader, reader->reader_->num_buffers(num_buffers)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_get_buffer_values(
    tiledb_vcf_reader_t* reader,
    int32_t buffer_idx,
    const char** name,
    void** buff) {
  if (sanity_check(reader) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          reader, reader->reader_->get_buffer_values(buffer_idx, name, buff)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_get_buffer_offsets(
    tiledb_vcf_reader_t* reader,
    int32_t buffer_idx,
    const char** name,
    int32_t** buff) {
  if (sanity_check(reader) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          reader, reader->reader_->get_buffer_offsets(buffer_idx, name, buff)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_get_buffer_list_offsets(
    tiledb_vcf_reader_t* reader,
    int32_t buffer_idx,
    const char** name,
    int32_t** buff) {
  if (sanity_check(reader) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          reader,
          reader->reader_->get_buffer_list_offsets(buffer_idx, name, buff)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_get_buffer_validity_bitmap(
    tiledb_vcf_reader_t* reader,
    int32_t buffer_idx,
    const char** name,
    uint8_t** buff) {
  if (sanity_check(reader) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          reader,
          reader->reader_->get_buffer_validity_bitmap(buffer_idx, name, buff)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_get_attribute_type(
    tiledb_vcf_reader_t* reader,
    const char* attribute,
    tiledb_vcf_attr_datatype_t* datatype,
    int32_t* var_len,
    int32_t* nullable,
    int32_t* list) {
  if (sanity_check(reader) == TILEDB_VCF_ERR || datatype == nullptr ||
      var_len == nullptr)
    return TILEDB_VCF_ERR;

  tiledb::vcf::AttrDatatype attr_datatype;
  bool is_var_len, is_nullable, is_list;
  if (SAVE_ERROR_CATCH(
          reader,
          reader->reader_->attribute_datatype(
              attribute, &attr_datatype, &is_var_len, &is_nullable, &is_list)))
    return TILEDB_VCF_ERR;

  *datatype = static_cast<tiledb_vcf_attr_datatype_t>(attr_datatype);
  *var_len = is_var_len ? 1 : 0;
  *nullable = is_nullable ? 1 : 0;
  *list = is_list ? 1 : 0;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_get_dataset_version(
    tiledb_vcf_reader_t* reader, int32_t* version) {
  if (sanity_check(reader) == TILEDB_VCF_ERR || version == nullptr)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(reader, reader->reader_->dataset_version(version)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_reset(tiledb_vcf_reader_t* reader) {
  if (sanity_check(reader) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(reader, reader->reader_->reset()))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_reset_buffers(tiledb_vcf_reader_t* reader) {
  if (sanity_check(reader) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(reader, reader->reader_->reset_buffers()))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_get_last_error(
    tiledb_vcf_reader_t* reader, tiledb_vcf_error_t** error) {
  if (sanity_check(reader) == TILEDB_VCF_ERR || error == nullptr)
    return TILEDB_VCF_ERR;

  // Create an error struct
  *error = new (std::nothrow) tiledb_vcf_error_t;
  if (*error == nullptr) {
    std::string err("Failed to allocate TileDB-VCF error object");
    LOG_ERROR(err);
    return TILEDB_VCF_ERR;
  }

  (*error)->errmsg_ = reader->saved_errmsg_;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_get_queryable_attribute_count(
    tiledb_vcf_reader_t* reader, int32_t* count) {
  if (sanity_check(reader) == TILEDB_VCF_ERR || count == nullptr)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          reader, reader->reader_->queryable_attribute_count(count)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_get_queryable_attribute_name(
    tiledb_vcf_reader_t* reader, int32_t index, char** name) {
  if (sanity_check(reader) == TILEDB_VCF_ERR || name == nullptr)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          reader, reader->reader_->queryable_attribute_name(index, name)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_get_materialized_attribute_count(
    tiledb_vcf_reader_t* reader, int32_t* count) {
  if (sanity_check(reader) == TILEDB_VCF_ERR || count == nullptr)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          reader, reader->reader_->materialized_attribute_count(count)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_get_materialized_attribute_name(
    tiledb_vcf_reader_t* reader, int32_t index, char** name) {
  if (sanity_check(reader) == TILEDB_VCF_ERR || name == nullptr)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          reader, reader->reader_->materialized_attribute_name(index, name)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_get_fmt_attribute_count(
    tiledb_vcf_reader_t* reader, int32_t* count) {
  if (sanity_check(reader) == TILEDB_VCF_ERR || count == nullptr)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(reader, reader->reader_->fmt_attribute_count(count)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_get_fmt_attribute_name(
    tiledb_vcf_reader_t* reader, int32_t index, char** name) {
  if (sanity_check(reader) == TILEDB_VCF_ERR || name == nullptr)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          reader, reader->reader_->fmt_attribute_name(index, name)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_get_info_attribute_count(
    tiledb_vcf_reader_t* reader, int32_t* count) {
  if (sanity_check(reader) == TILEDB_VCF_ERR || count == nullptr)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(reader, reader->reader_->info_attribute_count(count)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_get_info_attribute_name(
    tiledb_vcf_reader_t* reader, int32_t index, char** name) {
  if (sanity_check(reader) == TILEDB_VCF_ERR || name == nullptr)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          reader, reader->reader_->info_attribute_name(index, name)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_get_sample_count(
    tiledb_vcf_reader_t* reader, int32_t* count) {
  if (sanity_check(reader) == TILEDB_VCF_ERR || count == nullptr)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(reader, reader->reader_->sample_count(count)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_get_sample_name(
    tiledb_vcf_reader_t* reader, int32_t index, const char** name) {
  if (sanity_check(reader) == TILEDB_VCF_ERR || name == nullptr)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(reader, reader->reader_->sample_name(index, name)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_set_verbose(
    tiledb_vcf_reader_t* reader, const bool verbose) {
  if (sanity_check(reader) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(reader, reader->reader_->set_verbose(verbose)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_set_enable_progress_estimation(
    tiledb_vcf_reader_t* reader, bool enable_progress_estimation) {
  if (sanity_check(reader) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          reader,
          reader->reader_->set_enable_progress_estimation(
              enable_progress_estimation)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_set_buffer_percentage(
    tiledb_vcf_reader_t* reader, const float buffer_percentage) {
  if (sanity_check(reader) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          reader, reader->reader_->set_buffer_percentage(buffer_percentage)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_set_tiledb_tile_cache_percentage(
    tiledb_vcf_reader_t* reader, const float tile_percentage) {
  if (sanity_check(reader) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          reader,
          reader->reader_->set_tiledb_tile_cache_percentage(tile_percentage)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_set_check_samples_exist(
    tiledb_vcf_reader_t* reader, bool check_samples_exist) {
  if (sanity_check(reader) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          reader,
          reader->reader_->set_check_samples_exist(check_samples_exist)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_set_debug_print_vcf_regions(
    tiledb_vcf_reader_t* reader, const bool print_vcf_regions) {
  if (sanity_check(reader) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          reader,
          reader->reader_->set_debug_print_vcf_regions(print_vcf_regions)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_set_debug_print_sample_list(
    tiledb_vcf_reader_t* reader, const bool print_sample_list) {
  if (sanity_check(reader) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          reader,
          reader->reader_->set_debug_print_sample_list(print_sample_list)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_set_debug_print_tiledb_query_ranges(
    tiledb_vcf_reader_t* reader, const bool print_tiledb_query_ranges) {
  if (sanity_check(reader) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          reader,
          reader->reader_->set_debug_print_tiledb_query_ranges(
              print_tiledb_query_ranges)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_set_export_to_disk(
    tiledb_vcf_reader_t* reader, bool export_to_disk) {
  if (sanity_check(reader) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          reader, reader->reader_->set_export_to_disk(export_to_disk)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_set_merge(tiledb_vcf_reader_t* reader, bool merge) {
  if (sanity_check(reader) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(reader, reader->reader_->set_merge(merge)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_set_output_format(
    tiledb_vcf_reader_t* reader, const char* output_format) {
  if (sanity_check(reader) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          reader, reader->reader_->set_output_format(output_format)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_set_output_path(
    tiledb_vcf_reader_t* reader, const char* output_path) {
  if (sanity_check(reader) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(reader, reader->reader_->set_output_path(output_path)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_set_output_dir(
    tiledb_vcf_reader_t* reader, const char* output_dir) {
  if (sanity_check(reader) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(reader, reader->reader_->set_output_dir(output_dir)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_reader_set_af_filter(
    tiledb_vcf_reader_t* reader, const char* af_filter) {
  if (sanity_check(reader) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(reader, reader->reader_->set_af_filter(af_filter)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

/* ********************************* */
/*             BED FILE              */
/* ********************************* */

int32_t tiledb_vcf_bed_file_alloc(tiledb_vcf_bed_file_t** bed_file) {
  if (bed_file == nullptr) {
    std::string err("Null pointer given for TileDB-VCF bed_file object");
    LOG_ERROR(err);
    return TILEDB_VCF_ERR;
  }

  // Create a bed_file struct
  *bed_file = new (std::nothrow) tiledb_vcf_bed_file_t;
  if (*bed_file == nullptr) {
    std::string err("Failed to allocate TileDB-VCF bed_file object");
    LOG_ERROR(err);
    return TILEDB_VCF_ERR;
  }

  // Create a new bed_file object
  try {
    (*bed_file)->bed_file_.reset(new tiledb::vcf::BedFile);
  } catch (const std::exception& e) {
    std::string err(
        "Failed to allocate TileDB-VCF bed_file object: " +
        std::string(e.what()));
    LOG_ERROR(err);
    return TILEDB_VCF_ERR;
  }

  if ((*bed_file)->bed_file_ == nullptr) {
    delete *bed_file;
    std::string err("Failed to allocate TileDB-VCF bed_file object");
    LOG_ERROR(err);
    return TILEDB_VCF_ERR;
  }

  return TILEDB_VCF_OK;
}

void tiledb_vcf_bed_file_free(tiledb_vcf_bed_file_t** bed_file) {
  if (bed_file != nullptr && *bed_file != nullptr) {
    (*bed_file)->bed_file_.reset(nullptr);
    delete (*bed_file);
    *bed_file = nullptr;
  }
}

int32_t tiledb_vcf_bed_file_get_last_error(
    tiledb_vcf_bed_file_t* bed_file, tiledb_vcf_error_t** error) {
  if (sanity_check(bed_file) == TILEDB_VCF_ERR || error == nullptr)
    return TILEDB_VCF_ERR;

  // Create an error struct
  *error = new (std::nothrow) tiledb_vcf_error_t;
  if (*error == nullptr) {
    std::string err("Failed to allocate TileDB-VCF error object");
    LOG_ERROR(err);
    return TILEDB_VCF_ERR;
  }

  (*error)->errmsg_ = bed_file->saved_errmsg_;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_bed_file_parse(
    tiledb_vcf_reader_t* reader,
    tiledb_vcf_bed_file_t* bed_file,
    const char* bed_file_uri) {
  // TODO: the reader is required because we need to make sure the htslib
  // library is inited need to decided on the bed file vs reader hierarchy
  if (sanity_check(reader) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (sanity_check(bed_file) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(bed_file, bed_file->bed_file_->parse(bed_file_uri)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_bed_file_get_contig_count(
    tiledb_vcf_bed_file_t* bed_file, uint64_t* contig_count) {
  if (sanity_check(bed_file) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          bed_file, bed_file->bed_file_->contig_count(contig_count)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_bed_file_get_total_region_count(
    tiledb_vcf_bed_file_t* bed_file, uint64_t* total_region_count) {
  if (sanity_check(bed_file) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          bed_file,
          bed_file->bed_file_->total_region_count(total_region_count)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_bed_file_get_contig_region_count(
    tiledb_vcf_bed_file_t* bed_file,
    uint64_t contig_index,
    uint64_t* region_count) {
  if (sanity_check(bed_file) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          bed_file,
          bed_file->bed_file_->contig_region_count(contig_index, region_count)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_bed_file_get_contig_region(
    tiledb_vcf_bed_file_t* bed_file,
    uint64_t contig_index,
    uint64_t region_index,
    const char** region_str,
    const char** region_contig,
    uint32_t* region_start,
    uint32_t* region_end) {
  if (sanity_check(bed_file) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  tiledb::vcf::Region* region;
  if (SAVE_ERROR_CATCH(
          bed_file,
          bed_file->bed_file_->contig_region(
              contig_index, region_index, &region)))
    return TILEDB_VCF_ERR;

  *region_str = region->region_str.c_str();
  *region_contig = region->seq_name.c_str();
  *region_start = region->min;
  *region_end = region->max;

  return TILEDB_VCF_OK;
}

/* ********************************* */
/*              WRITER               */
/* ********************************* */

int32_t tiledb_vcf_writer_alloc(tiledb_vcf_writer_t** writer) {
  if (writer == nullptr) {
    std::string err("Null pointer given for TileDB-VCF writer object");
    LOG_ERROR(err);
    return TILEDB_VCF_ERR;
  }

  // Create a writer struct
  *writer = new (std::nothrow) tiledb_vcf_writer_t;
  if (*writer == nullptr) {
    std::string err("Failed to allocate TileDB-VCF writer object");
    LOG_ERROR(err);
    return TILEDB_VCF_ERR;
  }

  // Create a new writer object
  try {
    (*writer)->writer_.reset(new tiledb::vcf::Writer);
  } catch (const std::exception& e) {
    std::string err(
        "Failed to allocate TileDB-VCF writer object: " +
        std::string(e.what()));
    LOG_ERROR(err);
    return TILEDB_VCF_ERR;
  }

  if ((*writer)->writer_ == nullptr) {
    delete *writer;
    std::string err("Failed to allocate TileDB-VCF writer object");
    LOG_ERROR(err);
    return TILEDB_VCF_ERR;
  }

  return TILEDB_VCF_OK;
}

void tiledb_vcf_writer_free(tiledb_vcf_writer_t** writer) {
  if (writer != nullptr && *writer != nullptr) {
    (*writer)->writer_.reset(nullptr);
    delete (*writer);
    *writer = nullptr;
  }
}

int32_t tiledb_vcf_writer_init(
    tiledb_vcf_writer_t* writer, const char* dataset_uri) {
  if (sanity_check(writer) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(writer, writer->writer_->init(dataset_uri)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_writer_set_samples(
    tiledb_vcf_writer_t* writer, const char* sample_uris) {
  if (sanity_check(writer) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(writer, writer->writer_->set_sample_uris(sample_uris)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_writer_set_extra_attributes(
    tiledb_vcf_writer_t* writer, const char* attributes) {
  if (sanity_check(writer) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          writer, writer->writer_->set_extra_attributes(attributes)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_writer_set_vcf_attributes(
    tiledb_vcf_writer_t* writer, const char* vcf_uri) {
  if (sanity_check(writer) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(writer, writer->writer_->set_vcf_attributes(vcf_uri)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_writer_set_checksum_type(
    tiledb_vcf_writer_t* writer, tiledb_vcf_checksum_type_t checksum_type) {
  if (sanity_check(writer) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          writer, writer->writer_->set_checksum_type((int)checksum_type)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_writer_set_allow_duplicates(
    tiledb_vcf_writer_t* writer, bool allow_duplicates) {
  if (sanity_check(writer) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          writer, writer->writer_->set_allow_duplicates(allow_duplicates)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_writer_set_tile_capacity(
    tiledb_vcf_writer_t* writer, uint64_t tile_capacity) {
  if (sanity_check(writer) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          writer, writer->writer_->set_tile_capacity(tile_capacity)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_writer_set_anchor_gap(
    tiledb_vcf_writer_t* writer, uint32_t anchor_gap) {
  if (sanity_check(writer) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(writer, writer->writer_->set_anchor_gap(anchor_gap)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_writer_create_dataset(tiledb_vcf_writer_t* writer) {
  if (sanity_check(writer) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(writer, writer->writer_->create_dataset()))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_writer_register(tiledb_vcf_writer_t* writer) {
  if (sanity_check(writer) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(writer, writer->writer_->register_samples()))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_writer_store(tiledb_vcf_writer_t* writer) {
  if (sanity_check(writer) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(writer, writer->writer_->ingest_samples()))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_writer_get_last_error(
    tiledb_vcf_writer_t* writer, tiledb_vcf_error_t** error) {
  if (sanity_check(writer) == TILEDB_VCF_ERR || error == nullptr)
    return TILEDB_VCF_ERR;

  // Create an error struct
  *error = new (std::nothrow) tiledb_vcf_error_t;
  if (*error == nullptr) {
    std::string err("Failed to allocate TileDB-VCF error object");
    LOG_ERROR(err);
    return TILEDB_VCF_ERR;
  }

  (*error)->errmsg_ = writer->saved_errmsg_;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_writer_set_num_threads(
    tiledb_vcf_writer_t* writer, uint32_t threads) {
  if (sanity_check(writer) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(writer, writer->writer_->set_num_threads(threads)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_writer_set_total_memory_budget_mb(
    tiledb_vcf_writer_t* writer, uint32_t total_memory_budget_mb) {
  if (sanity_check(writer) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          writer,
          writer->writer_->set_total_memory_budget_mb(total_memory_budget_mb)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_writer_set_total_memory_percentage(
    tiledb_vcf_writer_t* writer, float total_memory_percentage) {
  if (sanity_check(writer) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          writer,
          writer->writer_->set_total_memory_percentage(
              total_memory_percentage)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_writer_set_ratio_tiledb_memory(
    tiledb_vcf_writer_t* writer, float ratio_tiledb_memory) {
  if (sanity_check(writer) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          writer,
          writer->writer_->set_ratio_tiledb_memory(ratio_tiledb_memory)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_writer_set_max_tiledb_memory_mb(
    tiledb_vcf_writer_t* writer, uint32_t max_tiledb_memory_mb) {
  if (sanity_check(writer) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          writer,
          writer->writer_->set_max_tiledb_memory_mb(max_tiledb_memory_mb)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_writer_set_input_record_buffer_mb(
    tiledb_vcf_writer_t* writer, uint32_t input_record_buffer_mb) {
  if (sanity_check(writer) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          writer,
          writer->writer_->set_input_record_buffer_mb(input_record_buffer_mb)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_writer_set_avg_vcf_record_size(
    tiledb_vcf_writer_t* writer, uint32_t avg_vcf_record_size) {
  if (sanity_check(writer) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          writer,
          writer->writer_->set_avg_vcf_record_size(avg_vcf_record_size)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_writer_set_ratio_task_size(
    tiledb_vcf_writer_t* writer, float ratio_task_size) {
  if (sanity_check(writer) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          writer, writer->writer_->set_ratio_task_size(ratio_task_size)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_writer_set_ratio_output_flush(
    tiledb_vcf_writer_t* writer, float ratio_output_flush) {
  if (sanity_check(writer) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          writer, writer->writer_->set_ratio_output_flush(ratio_output_flush)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_writer_set_thread_task_size(
    tiledb_vcf_writer_t* writer, uint32_t size) {
  if (sanity_check(writer) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(writer, writer->writer_->set_thread_task_size(size)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_writer_set_memory_budget(
    tiledb_vcf_writer_t* writer, uint64_t size) {
  if (sanity_check(writer) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(writer, writer->writer_->set_memory_budget(size)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_writer_set_scratch_space(
    tiledb_vcf_writer_t* writer, const char* path, uint64_t size) {
  if (sanity_check(writer) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(writer, writer->writer_->set_scratch_space(path, size)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_writer_set_max_num_records(
    tiledb_vcf_writer_t* writer, uint64_t max_num_records) {
  if (sanity_check(writer) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          writer, writer->writer_->set_record_limit(max_num_records)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_writer_set_verbose(
    tiledb_vcf_writer_t* writer, const bool verbose) {
  if (sanity_check(writer) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(writer, writer->writer_->set_verbose(verbose)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_writer_set_tiledb_stats_enabled(
    tiledb_vcf_writer_t* writer, const bool stats_enabled) {
  if (sanity_check(writer) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          writer, writer->writer_->set_tiledb_stats_enabled(stats_enabled)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_writer_get_tiledb_stats_enabled(
    tiledb_vcf_writer_t* writer, bool* enabled) {
  if (sanity_check(writer) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(writer, writer->writer_->tiledb_stats_enabled(enabled)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_writer_set_tiledb_stats_enabled_vcf_header_array(
    tiledb_vcf_writer_t* writer, const bool stats_enabled) {
  if (sanity_check(writer) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          writer,
          writer->writer_->set_tiledb_stats_enabled_vcf_header_array(
              stats_enabled)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_writer_get_tiledb_stats_enabled_vcf_header_array(
    tiledb_vcf_writer_t* writer, bool* enabled) {
  if (sanity_check(writer) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          writer,
          writer->writer_->tiledb_stats_enabled_vcf_header_array(enabled)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_writer_get_tiledb_stats(
    tiledb_vcf_writer_t* writer, char** stats) {
  if (sanity_check(writer) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(writer, writer->writer_->tiledb_stats(stats)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_writer_get_dataset_version(
    tiledb_vcf_writer_t* writer, int32_t* version) {
  if (sanity_check(writer) == TILEDB_VCF_ERR || version == nullptr)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(writer, writer->writer_->dataset_version(version)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_writer_set_tiledb_config(
    tiledb_vcf_writer_t* writer, const char* config) {
  if (sanity_check(writer) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(writer, writer->writer_->set_tiledb_config(config)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_writer_set_sample_batch_size(
    tiledb_vcf_writer_t* writer, const uint64_t size) {
  if (sanity_check(writer) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(writer, writer->writer_->set_sample_batch_size(size)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_writer_set_resume_sample_partial_ingestion(
    tiledb_vcf_writer_t* writer, const bool resume) {
  if (sanity_check(writer) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          writer, writer->writer_->set_resume_sample_partial_ingestion(resume)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_writer_set_contig_fragment_merging(
    tiledb_vcf_writer_t* writer, const bool contig_fragment_merging) {
  if (sanity_check(writer) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          writer,
          writer->writer_->set_contig_fragment_merging(
              contig_fragment_merging)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_writer_set_contigs_to_keep_separate(
    tiledb_vcf_writer_t* writer, const char** contigs, const uint64_t len) {
  if (sanity_check(writer) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  // Build set from c-style string array
  std::set<std::string> contig_set;
  for (uint64_t i = 0; i < len; i++)
    contig_set.emplace(contigs[i]);

  if (SAVE_ERROR_CATCH(
          writer, writer->writer_->set_contigs_to_keep_separate(contig_set)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_writer_set_contigs_to_allow_merging(
    tiledb_vcf_writer_t* writer, const char** contigs, const uint64_t len) {
  if (sanity_check(writer) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  // Build set from c-style string array
  std::set<std::string> contig_set;
  for (uint64_t i = 0; i < len; i++)
    contig_set.emplace(contigs[i]);

  if (SAVE_ERROR_CATCH(
          writer, writer->writer_->set_contigs_to_allow_merging(contig_set)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_writer_set_contig_mode(
    tiledb_vcf_writer_t* writer, int contig_mode) {
  if (sanity_check(writer) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(writer, writer->writer_->set_contig_mode(contig_mode)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_writer_set_enable_allele_count(
    tiledb_vcf_writer_t* writer, bool enable) {
  if (sanity_check(writer) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          writer, writer->writer_->set_enable_allele_count(enable)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

int32_t tiledb_vcf_writer_set_enable_variant_stats(
    tiledb_vcf_writer_t* writer, bool enable) {
  if (sanity_check(writer) == TILEDB_VCF_ERR)
    return TILEDB_VCF_ERR;

  if (SAVE_ERROR_CATCH(
          writer, writer->writer_->set_enable_variant_stats(enable)))
    return TILEDB_VCF_ERR;

  return TILEDB_VCF_OK;
}

/* ********************************* */
/*               ERROR               */
/* ********************************* */

int32_t tiledb_vcf_error_get_message(
    tiledb_vcf_error_t* error, const char** errmsg) {
  if (error == nullptr || errmsg == nullptr)
    return TILEDB_ERR;

  if (error->errmsg_.empty())
    *errmsg = nullptr;
  else
    *errmsg = error->errmsg_.c_str();

  return TILEDB_OK;
}

void tiledb_vcf_error_free(tiledb_vcf_error_t** error) {
  if (error != nullptr && *error != nullptr) {
    delete (*error);
    *error = nullptr;
  }
}
