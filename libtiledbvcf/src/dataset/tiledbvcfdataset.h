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
#include <unordered_set>
#include <vector>

#include <htslib/vcf.h>
#include <tiledb/tiledb>

#include "utils/rwlock.h"
#include "utils/sample_utils.h"
#include "utils/unique_rwlock.h"
#include "vcf/region.h"

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

struct UtilsParams {
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
  /*         PUBLIC ATTRIBUTES         */
  /* ********************************* */

  struct DimensionNames {
    struct V4 {
      static const std::string sample;
      static const std::string contig;
      static const std::string start_pos;
    };

    struct V3 {
      static const std::string sample;
      static const std::string start_pos;
    };

    struct V2 {
      static const std::string sample;
      static const std::string end_pos;
    };
  };

  struct AttrNames {
    struct V4 {
      static const std::string real_start_pos;
      static const std::string end_pos;
      static const std::string qual;
      static const std::string alleles;
      static const std::string id;
      static const std::string filter_ids;
      static const std::string info;
      static const std::string fmt;
    };

    struct V3 {
      static const std::string real_start_pos;
      static const std::string end_pos;
      static const std::string qual;
      static const std::string alleles;
      static const std::string id;
      static const std::string filter_ids;
      static const std::string info;
      static const std::string fmt;
    };

    struct V2 {
      static const std::string pos;
      static const std::string real_end;
      static const std::string qual;
      static const std::string alleles;
      static const std::string id;
      static const std::string filter_ids;
      static const std::string info;
      static const std::string fmt;
    };
  };

  /* ********************************* */
  /*         PUBLIC DATATYPES          */
  /* ********************************* */

  /**
   * The format version.
   */
  enum Version { V2 = 2, V3, V4 };

  /**
   * General metadata for a dataset. This should be kept relatively small.
   */
  struct Metadata {
    Metadata()
        : tile_capacity(0)
        , ingestion_sample_batch_size(0)
        , anchor_gap(0)
        , free_sample_id(0)
        , total_contig_length(0) {
    }

    Metadata(Metadata&& metadata) noexcept {
      vcf::utils::UniqueReadLock lck_(&metadata.sample_names_rw_lock_);
      version = metadata.version;
      tile_capacity = metadata.tile_capacity;
      ingestion_sample_batch_size = metadata.ingestion_sample_batch_size;
      anchor_gap = metadata.anchor_gap;
      extra_attributes = metadata.extra_attributes;
      free_sample_id = metadata.free_sample_id;
      all_samples = metadata.all_samples;
      sample_ids = metadata.sample_ids;
      sample_names_ = metadata.sample_names_;
      contig_offsets = metadata.contig_offsets;
      contig_lengths = metadata.contig_lengths;
      total_contig_length = metadata.total_contig_length;
    }

    Metadata(const Metadata& metadata) {
      vcf::utils::UniqueReadLock lck_(&metadata.sample_names_rw_lock_);
      version = metadata.version;
      tile_capacity = metadata.tile_capacity;
      ingestion_sample_batch_size = metadata.ingestion_sample_batch_size;
      anchor_gap = metadata.anchor_gap;
      extra_attributes = metadata.extra_attributes;
      free_sample_id = metadata.free_sample_id;
      all_samples = metadata.all_samples;
      sample_ids = metadata.sample_ids;
      sample_names_ = metadata.sample_names_;
      contig_offsets = metadata.contig_offsets;
      contig_lengths = metadata.contig_lengths;
      total_contig_length = metadata.total_contig_length;
    }

    Metadata& operator=(const Metadata& metadata) {
      vcf::utils::UniqueReadLock lck_(&metadata.sample_names_rw_lock_);
      version = metadata.version;
      tile_capacity = metadata.tile_capacity;
      ingestion_sample_batch_size = metadata.ingestion_sample_batch_size;
      anchor_gap = metadata.anchor_gap;
      extra_attributes = metadata.extra_attributes;
      free_sample_id = metadata.free_sample_id;
      all_samples = metadata.all_samples;
      sample_ids = metadata.sample_ids;
      sample_names_ = metadata.sample_names_;
      contig_offsets = metadata.contig_offsets;
      contig_lengths = metadata.contig_lengths;
      total_contig_length = metadata.total_contig_length;

      return *this;
    }

    unsigned version = Version::V4;
    uint64_t tile_capacity;
    uint32_t ingestion_sample_batch_size;
    uint32_t anchor_gap;
    std::vector<std::string> extra_attributes;

    uint32_t free_sample_id;
    mutable std::vector<std::string> all_samples;

    /**
     * Mapping of sample name -> sample ID (row coord).
     */
    mutable std::map<std::string, uint32_t> sample_ids;

    /**
     * Mapping of sample ID (row coord) -> sample name. This field is derived,
     * not serialized.
     */
    mutable std::vector<std::string> sample_names_;

    /** Lock for accessing and manipulating sample_names */
    mutable utils::RWLock sample_names_rw_lock_;

    /** Mapping of contig name -> global genomic offset. */
    std::map<std::string, uint32_t> contig_offsets;

    /** Mapping of contig name -> length. */
    std::map<std::string, uint32_t> contig_lengths;

    /** Sum of lengths of all contigs. */
    uint32_t total_contig_length;
  };

  /* ********************************* */
  /*            PUBLIC API             */
  /* ********************************* */

  TileDBVCFDataset();
  ~TileDBVCFDataset();

  static void create(const CreationParams& params);

  void open(
      const std::string& uri,
      const std::vector<std::string>& tiledb_config = {});

  void register_samples(const RegistrationParams& params);

  void print_samples_list();

  void print_dataset_stats();

  const Metadata& metadata() const;

  std::string data_uri() const;

  std::string root_uri() const;

  std::unordered_map<uint32_t, SafeBCFHdr> fetch_vcf_headers(
      const std::vector<SampleAndId>& samples) const;

  /**
   * Fetch VCF headers
   * @param samples List of samples, if list is empty then we'll fetch just one
   * @param lookup_map
   * @return
   */
  std::unordered_map<uint32_t, SafeBCFHdr> fetch_vcf_headers_v4(
      const std::vector<SampleAndId>& samples,
      std::unordered_map<std::string, size_t>* lookup_map,
      uint64_t memory_budget = 10485760) const;

  /**
   * Returns a list of regions, one per contig (spanning the entire contig),
   * sorted on global contig offset.
   */
  std::vector<Region> all_contigs() const;

  /**
   * Returns a list of regions, one per contig (spanning the entire contig),
   * sorted on global contig offset.
   *
   * Note: This works by using the first VCF header. If different VCF headers
   * exist in the array this will not correctly return all contig regions
   *
   * @param Context contex
   * @return List of all regions and contigs
   */
  std::vector<Region> all_contigs_v4() const;

  /**
   * Returns a list of regions, one per contig (spanning the entire contig),
   * sorted on global contig offset.
   */
  std::list<Region> all_contigs_list() const;

  /**
   * Returns a list of regions, one per contig (spanning the entire contig),
   * sorted on global contig offset.
   *
   * @param Context contex
   * @return List of all regions and contigs
   */
  std::list<Region> all_contigs_list_v4() const;

  /**
   * Returns a pair of (offset, length) for the contig containing the given
   * global column coordinate value.
   */
  std::tuple<uint32_t, uint32_t, std::string> contig_from_column(
      uint32_t col) const;

  /**
   * Splits an attribute name like 'info_X' or 'fmt_Y' into a pair ('info', 'X')
   * or ('fmt', 'Y').
   */
  static std::pair<std::string, std::string> split_info_fmt_attr_name(
      const std::string& attr_name);

  /** Return a set of the v4 attribute names for the "builtin" attributes. */
  static std::set<std::string> builtin_attributes_v4();

  /** Return a set of the v3 attribute names for the "builtin" attributes. */
  static std::set<std::string> builtin_attributes_v3();

  /** Return a set of the v2 attribute names for the "builtin" attributes. */
  static std::set<std::string> builtin_attributes_v2();

  /** Returns true if the builtin attribute is fixed-len in the schema. */
  static bool attribute_is_fixed_len(const std::string& attr);

  /** Returns a set of all the attribute names in the dataset. This is a set as
   * we rely on the order for the attribute_index function */
  std::set<std::string> all_attributes() const;

  /** Returns the BCF_HT_ type for the info field of the given name. */
  int info_field_type(const std::string& name, const bcf_hdr_t* hdr) const;

  /** Returns the BCF_HT_ type for the format field of the given name. */
  int fmt_field_type(const std::string& name, const bcf_hdr_t* hdr) const;

  /** Map of info field name -> hstlib type. */
  std::map<std::string, int> info_field_types() const;

  /** Map of fmt field name -> hstlib type. */
  std::map<std::string, int> fmt_field_types() const;

  /**
   * Get queryable attribute count
   * @return
   */
  int32_t queryable_attribute_count() const;

  /**
   * Get attribute name by index
   * @param index
   * @return
   */
  const char* queryable_attribute_name(int32_t index) const;

  /**
   * Get materialized attribute count
   * @return
   */
  int32_t materialized_attribute_count() const;

  /**
   * Get materialized attribute name by index
   * @param index
   * @return
   */
  const char* materialized_attribute_name(int32_t index) const;

  /**
   * Get sample name by index
   * @param index
   * @return
   */
  const char* sample_name(int32_t index) const;

  /**
   * Writes the given sample header data to the separate sample header array in
   * the dataset.
   *
   * @param ctx TileDB context
   * @param vcf_headers Map of sample name -> header string to write.
   */
  void write_vcf_headers_v4(
      const Context& ctx,
      const std::map<std::string, std::string>& vcf_headers) const;

  /**
   * Fetch all sample ids from the vcf header array
   *
   * @return vector of all sample names
   */
  std::vector<std::string> get_all_samples_from_vcf_headers(
      const uint64_t memory_budget = 10485760) const;

  std::shared_ptr<tiledb::Array> data_array() const;

  /**
   * Returns if the core tiledb stats are enabled or not
   * @return tiledb stats enabled
   */
  const bool tiledb_stats_enabled() const;

  /**
   * Sets whether the core tiledb stats are enabled or not
   * @param stats_enabled
   */
  void set_tiledb_stats_enabled(const bool stats_enabled);

  /**
   * Retuns if core tiledb stats are enabled or not for the vcf header array
   * @return stats enabled for vcf header array
   */
  const bool tiledb_stats_enabled_vcf_header() const;

  /**
   * Sets whether the core tiledb stats are enabled or not for the vcf header
   * array
   * @param stats_enabled
   */
  void set_tiledb_stats_enabled_vcf_header(const bool stats_enabled);

  /**
   * Consolidate fragment metadata of the vcf header array
   * @param params
   */
  void consolidate_vcf_header_array_fragment_metadata(
      const UtilsParams& params);

  /**
   * Consolidate fragment metadata of the data array
   * @param params
   */
  void consolidate_data_array_fragment_metadata(const UtilsParams& params);

  /**
   * Consolidate fragment metadata of all arrays (vcf header array and data
   * array)
   * @param params
   */
  void consolidate_fragment_metadata(const UtilsParams& params);

  /**
   * Consolidate fragments  of the vcf header array
   * @param params
   */
  void consolidate_vcf_header_array_fragments(const UtilsParams& params);

  /**
   * Consolidate fragments  of the data array
   * @param params
   */
  void consolidate_data_array_fragments(const UtilsParams& params);

  /**
   * Consolidate fragments of all arrays (vcf header array and data array)
   * @param params
   */
  void consolidate_fragments(const UtilsParams& params);

  /**
   * Vacuum fragment metadata of the vcf header array
   * @param params
   */
  void vacuum_vcf_header_array_fragment_metadata(const UtilsParams& params);

  /**
   * Vacuum fragment metadata of the data array
   * @param params
   */
  void vacuum_data_array_fragment_metadata(const UtilsParams& params);

  /**
   * Vacuum fragment metadata of all arrays (vcf header array and data array)
   * @param params
   */
  void vacuum_fragment_metadata(const UtilsParams& params);

  /**
   * Vacuum fragments  of the vcf header array
   * @param params
   */
  void vacuum_vcf_header_array_fragments(const UtilsParams& params);

  /**
   * Vacuum fragments  of the data array
   * @param params
   */
  void vacuum_data_array_fragments(const UtilsParams& params);

  /**
   * Vacuum fragments of all arrays (vcf header array and data array)
   * @param params
   */
  void vacuum_fragments(const UtilsParams& params);

  /**
   * Return sample name vector
   * @return
   */
  std::vector<std::vector<char>> sample_names() const;

  /**
   * Load sample names
   */
  void load_sample_names_v4() const;

 private:
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
  mutable std::map<std::string, int> info_field_types_;

  /** Map of fmt field name -> hstlib type. */
  mutable std::map<std::string, int> fmt_field_types_;

  /** List of all attributes of vcf for querying */
  mutable std::vector<std::vector<char>> vcf_attributes_;

  /** List of all materialzied attributes of vcf for querying */
  mutable std::vector<std::vector<char>> materialized_vcf_attributes_;

  /** List of sample names for exporting */
  mutable std::vector<std::vector<char>> sample_names_;

  /** Pointer to hold an open vcf header array. This avoid opening the array
   * multiple times in multiple places */
  std::unique_ptr<tiledb::Array> vcf_header_array_;

  /** Pointer to hold an open data array. This avoid opening the array multiple
   * times in multiple places */
  std::shared_ptr<tiledb::Array> data_array_;

  /** TileDB config used for open dataset */
  tiledb::Config cfg_;

  /** TileDB Context for dataset */
  tiledb::Context ctx_;

  /** TileDB stats enablement */
  bool tiledb_stats_enabled_;

  /** TileDB stats enablement for vcf header array */
  bool tiledb_stats_enabled_vcf_header_;

  /** Are sample names loaded */
  mutable bool sample_names_loaded_;

  /** RWLock for building type field maps for info/fmt */
  utils::RWLock type_field_rw_lock_;

  /** flag for if info_field_types/fmt_field_types has been loaded */
  mutable bool info_fmt_field_types_loaded_;

  /** RWLock for building list of queryable attributes */
  utils::RWLock queryable_attribute_lock_;

  /** flag for if queryable attributes have been computed or not */
  mutable bool queryable_attribute_loaded_;

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
   * Write the given Metadata instance into the dataset.
   *
   * @param ctx TileDB context
   * @param root_uri Root URI of the dataset
   * @param metadata Metadata instance to write
   */
  static void write_metadata_v4(
      const Context& ctx,
      const std::string& root_uri,
      const Metadata& metadata);

  /**
   * Writes the given sample header data to the separate sample header array in
   * the dataset.
   *
   * @param ctx TileDB context
   * @param root_uri Root URI of the dataset
   * @param vcf_headers Map of sample name -> header string to write.
   */
  static void write_vcf_headers_v4(
      const Context& ctx,
      const std::string& root_uri,
      const std::map<std::string, std::string>& vcf_headers);

  /**
   * Writes the given sample header data to the separate sample header array in
   * the dataset.
   *
   * @param ctx TileDB context
   * @param root_uri Root URI of the dataset
   * @param vcf_headers Map of sample name -> header string to write.
   */
  static void write_vcf_headers_v2(
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
  void load_field_type_maps() const;

  /**
   * Populate the metadata maps of info/fmt field name -> htslib types.
   */
  void load_field_type_maps_v4(const bcf_hdr_t* hdr) const;

  /**
   * Open the VCF header array
   *
   * @param query_type query type
   * @return Unique ptr to open array
   */
  std::unique_ptr<tiledb::Array> open_vcf_array(tiledb_query_type_t query_type);

  /**
   * Open the data array
   *
   * @param query_type query type
   * @return Unique ptr to open array
   */
  std::shared_ptr<tiledb::Array> open_data_array(
      tiledb_query_type_t query_type);

  /**
   * Reads the Metadata from a given dataset.
   *
   * @param root_uri Root URI of the dataset
   * @return The dataset's Metadata.
   */
  void read_metadata();

  /**
   * Reads the Metadata from a given dataset.
   *
   * @param root_uri Root URI of the dataset
   * @return The dataset's Metadata.
   */
  void read_metadata_v4();

  /**
   * Build list of queryable attributes
   */
  void build_queryable_attributes() const;
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_TILEVCFDATASET_H
