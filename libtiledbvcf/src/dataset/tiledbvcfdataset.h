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

#ifndef TILEDB_VCF_TILEVCFDATASET_H
#define TILEDB_VCF_TILEVCFDATASET_H

#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <htslib/vcf.h>
#include <tiledb/tiledb>

#include "utils/sample_utils.h"

namespace tiledb {
namespace vcf {

/* ********************************* */
/*       AUXILIARY DATATYPES         */
/* ********************************* */

/** Arguments/params for dataset creation. */
struct CreationParams {
  std::string uri;
  std::vector<std::string> extra_attributes;
  uint64_t tile_capacity = 10000;
  uint32_t row_tile_extent = 10;
  uint32_t anchor_gap = 1000;
  std::vector<std::string> tiledb_config;
  tiledb_filter_type_t checksum = TILEDB_FILTER_CHECKSUM_SHA256;
  bool allow_duplicates = true;
};

/** Arguments/params for dataset registration. */
struct RegistrationParams {
  std::string uri;
  std::string sample_uris_file;
  std::vector<std::string> sample_uris;
  ScratchSpaceInfo scratch_space;
  std::vector<std::string> tiledb_config;
};

/** Arguments/params for the list operation. */
struct ListParams {
  std::string uri;
  std::vector<std::string> tiledb_config;
};

/** Arguments/params for the stat operation. */
struct StatParams {
  std::string uri;
  std::vector<std::string> tiledb_config;
};

/* ********************************* */
/*         TILEDBVCFDATASET          */
/* ********************************* */

/**
 * This is the main class through which sample data can be registered and
 * ingested into a TileDB VCF dataset. It also serves as a handle for accessing
 * dataset metadata during exports.
 */
class TileDBVCFDataset {
 public:
  /* ********************************* */
  /*         PUBLIC DATATYPES          */
  /* ********************************* */

  /**
   * General metadata for a dataset. This should be kept relatively small.
   */
  struct Metadata {
    unsigned version = TILEVCF_ARRAY_VERSION;
    uint64_t tile_capacity;
    uint32_t row_tile_extent;
    uint32_t anchor_gap;
    std::vector<std::string> extra_attributes;

    uint32_t free_sample_id;
    std::vector<std::string> all_samples;

    /**
     * Mapping of sample name -> sample ID (row coord).
     */
    std::map<std::string, uint32_t> sample_ids;

    /**
     * Mapping of sample ID (row coord) -> sample name. This field is derived,
     * not serialized.
     */
    std::vector<std::string> sample_names;

    /** Mapping of contig name -> global genomic offset. */
    std::map<std::string, uint32_t> contig_offsets;

    /** Mapping of contig name -> length. */
    std::map<std::string, uint32_t> contig_lengths;

    /** Sum of lengths of all contigs. */
    uint32_t total_contig_length;
  };

  /**
   * String names for built-in attribute names in the data array.
   */
  struct DimensionNames {
    static const std::string sample;
    static const std::string end_pos;
  };

  /**
   * String names for built-in attribute names in the data array.
   */
  struct AttrNames {
    static const std::string pos;
    static const std::string real_end;
    static const std::string qual;
    static const std::string alleles;
    static const std::string id;
    static const std::string filter_ids;
    static const std::string info;
    static const std::string fmt;
  };

  /* ********************************* */
  /*            PUBLIC API             */
  /* ********************************* */

  TileDBVCFDataset();

  static void create(const CreationParams& params);

  void open(
      const std::string& uri,
      const std::vector<std::string>& tiledb_config = {});

  void register_samples(const RegistrationParams& params);

  void print_samples_list();

  void print_dataset_stats();

  const Metadata& metadata() const;

  std::string data_uri() const;

  std::vector<SafeBCFHdr> fetch_vcf_headers(
      const tiledb::Context& ctx,
      uint32_t sample_id_min,
      uint32_t sample_id_max) const;

  std::string first_contig() const;

  /**
   * Returns a list of regions, one per contig (spanning the entire contig),
   * sorted on global contig offset.
   */
  std::vector<Region> all_contigs() const;

  /**
   * Returns a pair of (offset, length) for the contig containing the given
   * global column coordinate value.
   */
  std::pair<uint32_t, uint32_t> contig_from_column(uint32_t col) const;

  /**
   * Splits an attribute name like 'info_X' or 'fmt_Y' into a pair ('info', 'X')
   * or ('fmt', 'Y').
   */
  static std::pair<std::string, std::string> split_info_fmt_attr_name(
      const std::string& attr_name);

  /** Return a set of the attribute names for the "builtin" attributes. */
  static std::set<std::string> builtin_attributes();

  /** Returns true if the builtin attribute is fixed-len in the schema. */
  static bool attribute_is_fixed_len(const std::string& attr);

  /** Returns a set of all the attribute names in the dataset. */
  std::set<std::string> all_attributes() const;

  /** Returns the BCF_HT_ type for the info field of the given name. */
  int info_field_type(const std::string& name) const;

  /** Returns the BCF_HT_ type for the format field of the given name. */
  int fmt_field_type(const std::string& name) const;

 private:
  /** Current version of the TileDBVCF format. */
  static const unsigned TILEVCF_ARRAY_VERSION = 2;

  /* ********************************* */
  /*          PRIVATE ATTRIBUTES       */
  /* ********************************* */

  /** The URI of the dataset root directory (which is a TileDB group). */
  std::string root_uri_;

  /** Set to true when the dataset is opened. */
  bool open_;

  /** The dataset's general metadata (does not contain sample header data). */
  Metadata metadata_;

  /** Map of info field name -> hstlib type. */
  std::map<std::string, int> info_field_types_;

  /** Map of fmt field name -> hstlib type. */
  std::map<std::string, int> fmt_field_types_;

  /* ********************************* */
  /*          STATIC METHODS           */
  /* ********************************* */

  /**
   * Checks the given extracted attribute names for validity on dataset
   * creation.
   */
  static void check_attribute_names(const std::vector<std::string>& attribues);

  /**
   * Creates the metadata for a new dataset.
   *
   * @param ctx TileDB context
   * @param root_uri Root URI of the dataset
   * @param metadata General dataset metadata to write
   * @param checksum optional checksum filter
   */
  static void create_empty_metadata(
      const Context& ctx,
      const std::string& root_uri,
      const Metadata& metadata,
      const tiledb_filter_type_t& checksum);

  /**
   * Creates the empty sample data array for a new dataset.
   *
   * @param ctx TileDB context
   * @param root_uri Root URI of the dataset
   * @param metadata Dataset metadata containing tile capacity etc. to use
   * @param checksum optional checksum filter
   */
  static void create_empty_data_array(
      const Context& ctx,
      const std::string& root_uri,
      const Metadata& metadata,
      const tiledb_filter_type_t& checksum,
      const bool allow_duplicates);

  /**
   * Creates the empty sample header array for a new dataset.
   *
   * While technically considered metadata, the sample headers are quite large
   * and require efficient slicing (indexed by sample ID), so they are stored
   * in a separate TileDB array.
   *
   * @param ctx TileDB context
   * @param root_uri Root URI of the dataset
   * @param checksum optional checksum filter
   */
  static void create_sample_header_array(
      const Context& ctx,
      const std::string& root_uri,
      const tiledb_filter_type_t& checksum);

  /**
   * Write the given Metadata instance into the dataset.
   *
   * @param ctx TileDB context
   * @param root_uri Root URI of the dataset
   * @param metadata Metadata instance to write
   */
  static void write_metadata(
      const Context& ctx,
      const std::string& root_uri,
      const Metadata& metadata);

  /**
   * Reads the Metadata from a given dataset.
   *
   * @param ctx TileDB context
   * @param root_uri Root URI of the dataset
   * @return The dataset's Metadata.
   */
  static Metadata read_metadata(
      const Context& ctx, const std::string& root_uri);

  /**
   * Writes the given sample header data to the separate sample header array in
   * the dataset.
   *
   * @param ctx TileDB context
   * @param root_uri Root URI of the dataset
   * @param vcf_headers Map of sample name -> header string to write.
   */
  static void write_vcf_headers(
      const Context& ctx,
      const std::string& root_uri,
      const std::map<uint32_t, std::string>& vcf_headers);

  /**
   * Registers a set of samples. Registration updates the given metadata
   * instance and several other output parameters.
   *
   * @param headers Samples to be registered
   * @param metadata Metadata to be updated
   * @param sample_set Set of sample names to be updated
   * @param sample_headers Map of sample ID -> header string to be updated
   */
  static void register_samples_helper(
      const std::vector<SafeBCFHdr>& headers,
      Metadata* metadata,
      std::set<std::string>* sample_set,
      std::map<uint32_t, std::string>* sample_headers);

  /** Returns the URI of the sample data array for the dataset. */
  static std::string data_array_uri(
      const std::string& root_uri, bool check_for_cloud = true);

  /** Returns the URI of the VCF header data array for the dataset. */
  static std::string vcf_headers_uri(
      const std::string& root_uri, bool check_for_cloud = true);

  /** Returns true if the array starts with the tiledb:// URI **/
  static bool cloud_dataset(std::string root_uri);

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /**
   * Populate the metadata maps of info/fmt field name -> htslib types.
   */
  void load_field_type_maps(const tiledb::Context& ctx);
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_TILEVCFDATASET_H
