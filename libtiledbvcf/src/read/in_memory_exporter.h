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
  /** Sets the values buffer pointer and size (in bytes) for an attribute. */
  void set_buffer_values(
      const std::string& attribute, void* buff, int64_t buff_size);

  /** Sets the offsets buffer pointer and size (in bytes) for an attribute. */
  void set_buffer_offsets(
      const std::string& attribute, int32_t* buff, int64_t buff_size);

  /**
   * Sets the list offsets buffer pointer and size (in bytes) for an attribute.
   */
  void set_buffer_list_offsets(
      const std::string& attribute, int32_t* buff, int64_t buff_size);

  /** Sets the bitmap buffer pointer and size (in bytes) for an attribute. */
  void set_buffer_validity_bitmap(
      const std::string& attribute, uint8_t* buff, int64_t buff_size);

  /**
   * Based on the buffers that have been set, returns the list of array
   * attributes that must be read from the TileDB array.
   */
  std::set<std::string> array_attributes_required() const override;

  /** Resets any state of the exporter. */
  void reset() override;

  /** Reset user buffers. Used to reuse a reader but for different attributes.
   */
  void reset_buffers();

  /**
   * Exports a cell by copying to the user's buffers.
   *
   * @param sample Sample that the record belongs to
   * @param hdr BCF header instance for the sample
   * @param query_region Original query region that intersects the cell
   * @param contig_offset Offset of the cell's contig
   * @param query_results Handle on the query results / buffers
   * @param cell_idx Index of cell to export
   * @return True if export succeeded; false if the user buffers ran out of
   *    space.
   */
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

  void get_buffer_values(
      int32_t buffer_idx, const char** name, void** data_buff) const;

  void get_buffer_offsets(
      int32_t buffer_idx, const char** name, int32_t** buff) const;

  void get_buffer_list_offsets(
      int32_t buffer_idx, const char** name, int32_t** buff) const;

  void get_buffer_validity_bitmap(
      int32_t buffer_idx, const char** name, uint8_t** buff) const;

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
   * @param nullable Set to true if the attribute is var-len list
   */
  static void attribute_datatype(
      const TileDBVCFDataset* dataset,
      const std::string& attribute,
      AttrDatatype* datatype,
      bool* var_len,
      bool* nullable,
      bool* list,
      bool add_iaf);

 private:
  /* ********************************* */
  /*           PRIVATE DATATYPES       */
  /* ********************************* */

  /** The exportable attributes. */
  enum class ExportableAttribute {
    SampleName,
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
    InfoOrFmt,
    Contig,
    QueryBedLine,
  };

  /** Struct holding size info of user buffers. */
  struct UserBufferSizes {
    /** Number of bytes in user's data/values buffer. */
    int64_t data_bytes = 0;
    /** Number of elements in user's data/values buffer. */
    int64_t data_nelts = 0;
    /** Number of offsets in user's offsets buffer. */
    int64_t num_offsets = 0;
    /** Number of offsets in user's list offsets buffer. */
    int64_t num_list_offsets = 0;
    /** Number of bytes in user's bitmap buffer. */
    int64_t bitmap_bytes = 0;
  };

  /** User-allocated buffer to store exportable attribute data */
  struct UserBuffer {
    UserBuffer()
        : attr(ExportableAttribute::InfoOrFmt)
        , is_info(false)
        , data(nullptr)
        , max_data_bytes(0)
        , offsets(nullptr)
        , max_num_offsets(0)
        , list_offsets(nullptr)
        , max_num_list_offsets(0)
        , bitmap_buff(nullptr)
        , max_bitmap_bytes(0)
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

    /** Current sizes of user buffers. */
    UserBufferSizes curr_sizes;

    /** Pointer to user's buffer. */
    void* data;
    /** Size of user's buffer allocation (in bytes) */
    int64_t max_data_bytes;

    /** Pointer to user's offset buffer (null for fixed-len) */
    int32_t* offsets;
    /** Size, in num offsets, of user's offset buffer. */
    int64_t max_num_offsets;

    /** Pointer to user's list offset buffer. */
    int32_t* list_offsets;
    /** Size, in num offsets, of user's list offset buffer. */
    int64_t max_num_list_offsets;

    /** Pointer to user's bitmap buffer (null for non-nullable) */
    uint8_t* bitmap_buff;
    /** Size, in num bytes, of user's bitmap buffer. */
    int64_t max_bitmap_bytes;
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

  /**
   * Returns true if the given exportable attribute is a variable-length "list"
   * attribute.
   */
  static bool var_len_list_attr(const std::string& attr);

  /** Returns true if the given exportable attribute is nullable. */
  static bool nullable_attr(const std::string& attr);

  /** Gets the datatype for a specific info_/fmt_ attribute. */
  static AttrDatatype get_info_fmt_datatype(
      const TileDBVCFDataset* dataset,
      const std::string& attr,
      const bcf_hdr_t* hdr,
      bool add_iaf);

  UserBuffer* get_buffer(const std::string& attribute);

  /** Copies the given cell data to a user buffer. */
  bool copy_cell(
      UserBuffer* dest,
      const void* data,
      uint64_t nbytes,
      uint64_t nelts,
      const bcf_hdr_t* hdr) const;

  /** Copies the cell value, and updates the offsets for var-len attributes. */
  bool copy_cell_data(
      UserBuffer* dest,
      const void* data,
      uint64_t nbytes,
      uint64_t nelts,
      const bcf_hdr_t* hdr) const;

  /**
   * Updates the attribute list offsets and validity bitmap. This should be
   * called after copy_cell_data().
   */
  bool update_cell_list_and_bitmap(
      UserBuffer* dest,
      int64_t index,
      bool is_null,
      int32_t num_list_values,
      int64_t list_index) const;

  /**
   * Adds an offset for a 0-length var-len value.
   *
   * To adhere to Arrow's offset semantics, a zero-length value still gets an
   * entry in the offsets buffer.
   */
  bool add_zero_length_offset(UserBuffer* dest) const;

  /** Helper method to export the alleles attribute. */
  bool copy_alleles_list(uint64_t cell_idx, UserBuffer* dest) const;

  /** Helper method to export the filters attribute. */
  bool copy_filters_list(
      const bcf_hdr_t* hdr, uint64_t cell_idx, UserBuffer* dest) const;

  /** Helper method to export an info_/fmt_ attribute. */
  bool copy_info_fmt_value(
      uint64_t cell_idx, UserBuffer* dest, const bcf_hdr_t* hdr) const;

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
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_BCF_EXPORTER_H
