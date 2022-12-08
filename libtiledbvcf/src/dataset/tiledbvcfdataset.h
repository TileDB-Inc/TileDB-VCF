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

#ifndef TILEDB_VCF_TILEVCFDATASET_H
#define TILEDB_VCF_TILEVCFDATASET_H

#include <map>
#include <memory>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include <htslib/vcf.h>
#include <future>
#include <tiledb/tiledb>
#include <tiledb/tiledb_experimental>  // for the new group api

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
  std::string log_level;
  std::string log_file;
  std::vector<std::string> extra_attributes;
  uint64_t tile_capacity = 10000;
  uint32_t anchor_gap = 1000;
  std::vector<std::string> tiledb_config;
  tiledb_filter_type_t checksum = TILEDB_FILTER_CHECKSUM_SHA256;
  bool allow_duplicates = true;
  std::string vcf_uri;
  bool enable_allele_count = false;
  bool enable_variant_stats = false;
  bool compress_sample_dim = false;
};

/** Arguments/params for dataset registration. */
struct RegistrationParams {
  std::string uri;
  std::string log_level;
  std::string log_file;
  std::string sample_uris_file;
  std::vector<std::string> sample_uris;
  ScratchSpaceInfo scratch_space;
  std::vector<std::string> tiledb_config;
};

/** Arguments/params for the list operation. */
struct ListParams {
  std::string uri;
  std::string log_level;
  std::string log_file;
  std::vector<std::string> tiledb_config;
};

/** Arguments/params for the stat operation. */
struct StatParams {
  std::string uri;
  std::string log_level;
  std::string log_file;
  std::vector<std::string> tiledb_config;
};

struct UtilsParams {
  std::string uri;
  std::string log_level;
  std::string log_file;
  std::vector<std::string> tiledb_config;
};

// Only for pairs of std::hash-able types for simplicity.
// You can of course template this struct to allow other hash functions
// inspired by https://stackoverflow.com/a/32685618/11562375
struct pair_hash {
  template <class T1, class T2>
  std::size_t operator()(const std::pair<T1, T2>& p) const {
    auto h1 = std::hash<T1>{}(p.first);
    auto h2 = std::hash<T2>{}(p.second);

    // Mainly for demonstration purposes, i.e. works but is overly simple
    // In the real world, use sth. like boost.hash_combine
    return h1 ^ h2;
  }
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

  TileDBVCFDataset(std::shared_ptr<tiledb::Context> ctx);
  TileDBVCFDataset(const tiledb::Config& config);
  ~TileDBVCFDataset();

  static void create(const CreationParams& params);

  void open(
      const std::string& uri,
      const std::vector<std::string>& tiledb_config = {},
      const bool prefetch_data_array_fragment_info = false);

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
      bool all_samples,
      bool first_sample) const;

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
  bool attribute_is_fixed_len(const std::string& attr);

  /** Returns a set of all the attribute names in the dataset. This is a set as
   * we rely on the order for the attribute_index function */
  std::set<std::string> all_attributes() const;

  /** Returns the BCF_HT_ type for the info field of the given name. */
  int info_field_type(
      const std::string& name,
      const bcf_hdr_t* hdr,
      bool add_iaf = false) const;

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

  bool is_attribute_materialized(const std::string& attr) const;

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
  std::vector<std::string> get_all_samples_from_vcf_headers() const;

  /**
   * Get open data array
   * @return shared_pointer to opened data array
   */
  std::shared_ptr<tiledb::Array> data_array() const;

  /**
   * Returns the FragmentInfo for the data array. Fetches fragment info if it
   * isn't loaded
   *
   * @return Data array FragmentInfo
   */
  std::shared_ptr<tiledb::FragmentInfo> data_array_fragment_info();

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
   * Consolidate commits of the vcf header array
   * @param params
   */
  void consolidate_vcf_header_array_commits(const UtilsParams& params);

  /**
   * Consolidate commits of the data array
   * @param params
   */
  void consolidate_data_array_commits(const UtilsParams& params);

  /**
   * Consolidate commits of all arrays (vcf header array and data
   * array)
   * @param params
   */
  void consolidate_commits(const UtilsParams& params);

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
   * Vacuum commits of the vcf header array
   * @param params
   */
  void vacuum_vcf_header_array_commits(const UtilsParams& params);

  /**
   * Vacuum commits of the data array
   * @param params
   */
  void vacuum_data_array_commits(const UtilsParams& params);

  /**
   * Vacuum fragment metadata of all arrays (vcf header array and data array)
   * @param params
   */
  void vacuum_commits(const UtilsParams& params);

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

  /**
   * Check if a attr is an info field or not
   * @param attr
   * @return true if "info_" prefixed
   */
  bool is_info_field(const std::string& attr) const;

  /**
   * Check if a attr is an fmt field or not
   * @param attr
   * @return true if "fmt_" prefixed
   */
  bool is_fmt_field(const std::string& attr) const;

  /** Returns true if the dataset is tiledb cloud URI. */
  bool tiledb_cloud_dataset() const;

  /**
   * Gets the datatype of a particular exportable attribute that is not fmt or
   * info
   *
   * @param dataset Dataset (for metadata)
   * @param attribute Attribute name
   * @param datatype Set to the datatype of the attribute
   * @param var_len Set to true if the attribute is variable-length
   * @param nullable Set to true if the attribute is nullable
   * @param nullable Set to true if the attribute is var-len list
   */
  void attribute_datatype_non_fmt_info(
      const std::string& attribute,
      tiledb_datatype_t* datatype,
      bool* var_len,
      bool* nullable,
      bool* list);

  /**
   * Gets if the attribute is nullable. Nulls are not currently included in
   * schema but will be eventually
   * @param attr
   * @return true if nullable
   */
  static bool attribute_is_nullable(const std::string& attr);

  /**
   * Gets if the attribute is list. Lists are not currently included in schema
   * but will be eventually
   * @param attr
   * @return true if a list
   */
  static bool attribute_is_list(const std::string& attr);

  /**
   * Load the data array fragment info as a background task
   */
  void preload_data_array_fragment_info();

  /**
   *
   * @return vector of contig and sample start/end list
   */
  std::unordered_map<
      std::pair<std::string, std::string>,
      std::vector<std::pair<std::string, std::string>>,
      tiledb::vcf::pair_hash>
  fragment_sample_contig_list();
  std::unordered_map<
      std::pair<std::string, std::string>,
      std::vector<std::pair<std::string, std::string>>,
      tiledb::vcf::pair_hash>
  fragment_sample_contig_list_v4();

 private:
  /* ********************************* */
  /*          PRIVATE ATTRIBUTES       */
  /* ********************************* */

  inline static const std::string DATA_ARRAY = "data";
  inline static const std::string METADATA_GROUP = "metadata";
  inline static const std::string VCF_HEADER_ARRAY = "vcf_headers";

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
  std::shared_ptr<tiledb::Array> vcf_header_array_;

  /** Pointer to hold an open data array. This avoid opening the array multiple
   * times in multiple places */
  std::shared_ptr<tiledb::Array> data_array_;

  /** Pointer to hold fragment info for the data array. This lets us fetch it
   * async. */
  std::shared_ptr<tiledb::FragmentInfo> data_array_fragment_info_;

  /** Is the fragment info for data array loaded */
  bool data_array_fragment_info_loaded_;

  /** Mutex for loading data array fragment info */
  std::mutex data_array_fragment_info_mtx_;

  /** TileDB config used for open dataset */
  tiledb::Config cfg_;

  /** TileDB Context for dataset */
  std::shared_ptr<tiledb::Context> ctx_;

  /** TileDB stats enablement */
  bool tiledb_stats_enabled_;

  /** TileDB stats enablement for vcf header array */
  bool tiledb_stats_enabled_vcf_header_;

  /** Are sample names loaded */
  mutable bool sample_names_loaded_;

  /** RWLock for dataset to serialize access to TileDB */
  utils::RWLock dataset_lock_;

  /** RWLock for building type field maps for info/fmt */
  utils::RWLock type_field_rw_lock_;

  /** flag for if info_field_types/fmt_field_types has been loaded */
  mutable bool info_fmt_field_types_loaded_;

  /** flag for whether the IAF field type has been added */
  mutable bool info_iaf_field_type_added_;

  /** RWLock for building list of queryable attributes */
  utils::RWLock queryable_attribute_lock_;

  /** flag for if queryable attributes have been computed or not */
  mutable bool queryable_attribute_loaded_;

  /** RWLock for building list of materialized attributes */
  utils::RWLock materialized_attribute_lock_;

  /** flag for if materialized attributes have been computed or not */
  mutable bool materialized_attribute_loaded_;

  /** RWLock for data array to prevent destruction if in use */
  utils::RWLock data_array_lock_;

  /** RWLock for vcf header array to prevent destruction if in use */
  utils::RWLock vcf_header_array_lock_;

  /** Future for preloading non_empty_domain of data array */
  std::future<void> data_array_preload_non_empty_domain_thread_;

  /** Future for preloading non_empty_domain of data array */
  std::future<void> data_array_preload_fragment_info_thread_;

  /** Future for preloading non_empty_domain of vcf header array */
  std::future<void> vcf_header_array_preload_non_empty_domain_thread_;

  /* ********************************* */
  /*          STATIC METHODS           */
  /* ********************************* */

  /**
   * Creates a list of all info and format fields in the provided VCF file.
   */
  static std::vector<std::string> get_vcf_attributes(std::string uri);

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
      const bool allow_duplicates,
      const bool compress_sample_dim);

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
      const std::string& root_uri, bool relative = false, bool legacy = false);

  /** Returns the URI of the metadata group for the dataset. */
  static std::string metadata_group_uri(
      const std::string& root_uri, bool relative = false, bool legacy = false);

  /** Returns the URI of the VCF header data array for the dataset. */
  static std::string vcf_headers_uri(
      const std::string& root_uri, bool relative = false, bool legacy = false);

  /** Returns true if the array starts with the tiledb:// URI **/
  static bool cloud_dataset(const std::string& root_uri);

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /** Block until it's safe to delete or close the data array */
  void lock_and_join_data_array();

  /** Block until it's safe to delete or close the vcf header array */
  void lock_and_join_vcf_header_array();

  /**
   * Populate the metadata maps of info/fmt field name -> htslib types.
   */
  void load_field_type_maps() const;

  /**
   * Populate the metadata maps of info/fmt field name -> htslib types.
   */
  void load_field_type_maps_v4(
      const bcf_hdr_t* hdr, bool add_iaf = false) const;

  /**
   * @brief Open an array using the uri from the root group member with name
   * `member_name`. If the group member does not exist or is a tiledb:// uri
   *  when the root uri is not a tiledb:// uri, open the array at the
   * `uri_path`. If these uris fail, try the `legacy_uri` before throwing and
   * error.
   *
   * @param query_type query type
   * @param member_name member name in root group
   * @param uri_path non tiledb:// uri for the array
   * @param legacy_uri legacy tiledb:// uri
   * @return std::unique_ptr<tiledb::Array> unique ptr to open array
   */
  std::unique_ptr<tiledb::Array> open_array(
      tiledb_query_type_t query_type,
      const std::string& member_name,
      const std::string& uri_path,
      const std::string& legacy_uri);

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

  /**
   * Build list of materialized attributes
   */
  void build_materialized_attributes() const;

  /**
   * Preload the non empty domain async so that its available when needed later
   * @return
   */
  void preload_data_array_non_empty_domain();
  void preload_data_array_non_empty_domain_v2_v3();
  void preload_data_array_non_empty_domain_v4();

  /**
   * Preload the non empty domain async so that its available when needed later
   * @return
   */
  void preload_vcf_header_array_non_empty_domain();
  void preload_vcf_header_array_non_empty_domain_v2_v3();
  void preload_vcf_header_array_non_empty_domain_v4();

  /**
   * Load the data array fragment info
   */
  void data_array_fragment_info_load();
};

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_TILEVCFDATASET_H
