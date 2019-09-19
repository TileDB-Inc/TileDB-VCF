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

#ifndef TILEDB_VCF_USER_BUFFER_EXPORTER_H
#define TILEDB_VCF_USER_BUFFER_EXPORTER_H

#include "enums/attr_datatype.h"
#include "read/exporter.h"
#include "read_query_results.h"
#include "utils/bitmap.h"

namespace tiledb {
namespace vcf {

/**
 * Export to in-memory columnar buffers. This is the exporter used when
 * exporting via the C API.
 *
 * The attributes exposed by this in-memory exporter ("exportable attributes")
 * are not the same as the underlying attributes in the TileDB array storing the
 * sample data. Instead, this class (and therefore the C API) provides a more
 * user-friendly set of attributes, including the ability to extract individual
 * info/fmt fields into columnar buffers.
 *
 * Note this class is currently not threadsafe.
 */
class InMemoryExporter : public Exporter {
 public:
  /**
   * Sets a user-defined buffer for receiving exported data.
   *
   * The predefined attribute names are:
   *
   * - "sample_name": The sample name (var-len char)
   * - "contig": The contig name (var-len char)
   * - "pos_start": The 1-based record start position (int32)
   * - "pos_end": The 1-based record end position (int32)
   * - "query_bed_start": The 0-based BED query start position (int32)
   * - "query_bed_end": The 1-based BED query end position (int32)
   * - "alleles": CSV string of alleles (var-len char)
   * - "id": ID string (var-len char)
   * - "filters": CSV string of filter names (var-len char)
   * - "qual": The quality value (float)
   * - "info_*": A specific INFO field value (var-len uint8, see below)
   * - "fmt_*": A specific FMT field value (var-len uint8, see below)
   * - "fmt": Format byte blob of non-attribute fields (var-len uint8)
   * - "info": Info byte blob of non-attribute fields (var-len uint8)
   *
   * In general to access specific INFO or FMT field values, you should
   * use the special `fmt_*` / `info_*` attribute names. For example, to
   * retrieve the values of the `MIN_DP` format field, set a buffer for
   * attribute `fmt_MIN_DP`. The generic `fmt` and `info` byte blob attributes
   * are mostly available as an escape hatch.
   *
   * When retrieving info/fmt fields, the values stored in the buffers are typed
   * according to the actual field type. For example, if an INFO field `foo` is
   * listed in the BCF header as being a floating-point field, then the bytes
   * stored in the buffer `info_foo` will be floating-point values.
   *
   * If a record does not contain a value for the specified INFO or FMT field,
   * the value stored in the result buffer is a special null sentinel value
   * indicating "no value".
   *
   * @param attribute Name of attribute
   * @param offsets Offsets buffer, ignored for fixed-len attributes.
   * @param max_num_offsets Size of offsets buffer (in num elements).
   * @param data Data buffer.
   * @param max_data_bytes Size of data buffer (in bytes).
   */
  void set_buffer(
      const std::string& attribute,
      int32_t* offsets,
      int64_t max_num_offsets,
      void* data,
      int64_t max_data_bytes);

  void set_validity_bitmap(
      const std::string& attribute,
      uint8_t* bitmap_buff,
      int64_t bitmap_buff_size);

  std::set<std::string> array_attributes_required() const override;

  void reset() override;

  bool export_record(
      const SampleAndId& sample,
      const bcf_hdr_t* hdr,
      const Region& query_region,
      uint32_t contig_offset,
      const ReadQueryResults& query_results,
      uint64_t cell_idx) override;

  /**
   * Returns the size of the result copied for the given attribute.
   */
  void result_size(
      const std::string& attribute,
      int64_t* num_offsets,
      int64_t* num_data_elements,
      int64_t* num_data_bytes) const;

  /** Returns the number of in-memory user buffers that have been set. */
  void num_buffers(int32_t* num_buffers) const;

  /** Gets information about the given buffer (by index). */
  void get_buffer(
      int32_t buffer_idx,
      const char** name,
      int32_t** offset_buff,
      int64_t* offset_buff_size,
      void** data_buff,
      int64_t* data_buff_size) const;

  /** Gets information about the given buffer (by index). */
  void get_bitmap_buffer(
      int32_t buffer_idx,
      uint8_t** bitmap_buff,
      int64_t* bitmap_buff_size) const;

  /** Resets the "current" (i.e. copied so far) sizes for all user buffers. */
  void reset_current_sizes();

  /**
   * Gets the datatype of a particular exportable attribute.
   *
   * @param dataset Dataset (for metadata)
   * @param attribute Attribute name
   * @param datatype Set to the datatype of the attribute
   * @param var_len Set to true if the attribute is variable-length
   * @param nullable Set to true if the attribute is nullable
   */
  static void attribute_datatype(
      const TileDBVCFDataset* dataset,
      const std::string& attribute,
      AttrDatatype* datatype,
      bool* var_len,
      bool* nullable);

 private:
  /* ********************************* */
  /*           PRIVATE DATATYPES       */
  /* ********************************* */

  /** The exportable attributes. */
  enum class ExportableAttribute {
    SampleName,
    Contig,
    PosStart,
    PosEnd,
    QueryBedStart,
    QueryBedEnd,
    Alleles,
    Id,
    Filters,
    Qual,
    Fmt,
    Info,
    InfoOrFmt
  };

  /** User-allocated buffer to store exportable attribute data */
  struct UserBuffer {
    UserBuffer()
        : attr(ExportableAttribute::InfoOrFmt)
        , attr_name("")
        , is_info(false)
        , info_fmt_field_name("")
        , data(nullptr)
        , max_data_bytes(0)
        , curr_data_bytes(0)
        , curr_data_nelts(0)
        , offsets(nullptr)
        , max_num_offsets(0)
        , curr_num_offsets(0)
        , bitmap_buff(nullptr)
        , max_bitmap_bytes(0)
        , curr_bitmap_bytes(0)
        , bitmap(nullptr) {
    }

    /** The attribute */
    ExportableAttribute attr;

    /** The name of the attribute */
    std::string attr_name;

    /** If type is InfoOrFmt, true if it's info, false if fmt. */
    bool is_info;

    /** If type is InfoOrFmt, the field name. */
    std::string info_fmt_field_name;

    /** Pointer to user's buffer. */
    void* data;
    /** Size of user's buffer (in bytes) */
    int64_t max_data_bytes;
    /** Currently used number of bytes in user's buffer. */
    int64_t curr_data_bytes;
    /** Current number of elements in user's buffer. */
    int64_t curr_data_nelts;

    /** Pointer to user's offset buffer (null for fixed-len) */
    int32_t* offsets;
    /** Size, in num offsets, of user's offset buffer. */
    int64_t max_num_offsets;
    /** Currently used number of offsets in user's offset buffer. */
    int64_t curr_num_offsets;

    /** Pointer to user's bitmap buffer (null for non-nullable) */
    uint8_t* bitmap_buff;
    /** Size, in num bytes, of user's bitmap buffer. */
    int64_t max_bitmap_bytes;
    /** Currently used number of bytes in user's bitmap buffer. */
    int64_t curr_bitmap_bytes;
    /** Convenience wrapper around the bitmap buffer. */
    std::unique_ptr<Bitmap> bitmap;
  };

  /* ********************************* */
  /*          PRIVATE ATTRIBUTES       */
  /* ********************************* */

  /** The external user buffers set for exporting. */
  std::map<std::string, UserBuffer> user_buffers_;

  /** Helper map for looking up buffers by index. */
  std::vector<UserBuffer*> user_buffers_by_idx_;

  /**
   * For convenience, the current query results for the record being exported.
   */
  const ReadQueryResults* curr_query_results_;

  /** Reusable string buffer for temp results. */
  std::string str_buff_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /**
   * Returns the ExportableAttribute corresponding to the given attribute name.
   */
  static ExportableAttribute attr_name_to_enum(const std::string& name);

  /** Returns true if the given exportable attribute is fixed-length. */
  static bool fixed_len_attr(const std::string& attr);

  /** Returns true if the given exportable attribute is nullable. */
  static bool nullable_attr(const std::string& attr);

  /** Gets the datatype for a specific info_/fmt_ attribute. */
  static AttrDatatype get_info_fmt_datatype(
      const TileDBVCFDataset* dataset, const std::string& attr);

  /** Exports/copies the given cell into the user buffers. */
  bool copy_cell(
      const bcf_hdr_t* hdr,
      const Region& region,
      uint32_t contig_offset,
      uint64_t cell_idx);

  /** Copies the given data to a user buffer. */
  bool copy_to_user_buff(
      UserBuffer* dest,
      const void* data,
      uint64_t nbytes,
      uint64_t nelts) const;

  /** Helper method to export an info_/fmt_ attribute. */
  bool copy_info_fmt_value(uint64_t cell_idx, UserBuffer* dest) const;

  /**
   * Gets a pointer to the variable-length attribute data in the given source
   * buffer.
   */
  void get_var_attr_value(
      const Buffer& src,
      uint64_t cell_idx,
      uint64_t buff_var_size,
      void** data,
      uint64_t* nbytes) const;

  /**
   * Gets a pointer to a particular info_/fmt_ field's data in the query result
   * buffers.
   */
  void get_info_fmt_value(
      const UserBuffer* attr_buff,
      uint64_t cell_idx,
      const void** data,
      uint64_t* nbytes,
      uint64_t* nelts) const;

  /**
   * Constructs a string CSV list of filter names from the given filter data.
   */
  void make_csv_filter_list(
      const bcf_hdr_t* hdr,
      const void* data,
      uint64_t nbytes,
      std::string* dest) const;
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_BCF_EXPORTER_H
