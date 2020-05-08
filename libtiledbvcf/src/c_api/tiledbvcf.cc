/**
 * @file   tiledbvcf.cc
 *
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
 *
 * @section DESCRIPTION
 *
 * This file defines the C API for TileDB-VCF.
 */

#include "c_api/tiledbvcf.h"
#include "read/reader.h"
#include "write/writer.h"

#include <cassert>
#include <iostream>
#include <memory>

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

/* ********************************* */
/*             HELPERS               */
/* ********************************* */

#define LOG_ERROR(s)               \
  do {                             \
    std::cerr << (s) << std::endl; \
  } while (0)

static void save_error(tiledb_vcf_reader_t* reader, const std::string& error) {
  LOG_ERROR(error);
  reader->saved_errmsg_ = error;
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

inline int32_t sanity_check(const tiledb_vcf_writer_t* writer) {
  if (writer == nullptr || writer->writer_ == nullptr) {
    std::string err = "Invalid TileDB VCF writer object";
    LOG_ERROR(err);
    return TILEDB_VCF_ERR;
  }
  return TILEDB_VCF_OK;
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

  if (SAVE_ERROR_CATCH(writer, writer->writer_->set_dataset_uri(dataset_uri)))
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