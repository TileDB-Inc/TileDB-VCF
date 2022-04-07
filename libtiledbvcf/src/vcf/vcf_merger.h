#include <htslib/hts.h>
#include <htslib/vcf.h>
#include <vcf/vcf_utils.h>
#include <cstdint>
#include <deque>
#include <map>
#include <string>
#include <tuple>
#include <unordered_set>
#include <variant>

#include "utils/logger_public.h"
#include "utils/utils.h"

namespace tiledb {
namespace vcf {

class MergedData {
 public:
  // CHROM: contig id of the merged record
  int rid;

  // POS: position of the merged record
  hts_pos_t pos;

  // ID: vector of unique IDs in the merged record
  std::vector<std::string> ids;

  // QUAL: max quality of merged records
  float qual;

  // REF: reference bases
  std::string ref;

  // ALT: vector of alleles
  std::vector<std::string> alleles;

  // allele suffix for normalizing multiallelic records
  std::string suffix;

  // AC: allele count in GT
  std::vector<int> allele_count;

  // AN: total number of alleles in GT
  int allele_total;

  // DP: total read depth
  int depth_total;

  // vector of unique filters in the merged record
  std::vector<int> filters;

  // vector of GT values (currently assumes diploid)
  std::vector<int> gts;

  // map: info id -> vector of info number values (int and float)
  std::unordered_map<int, std::vector<uint32_t>> info;

  // format keys present in the merged data
  std::vector<int> format_keys;

  // map: sample_num -> map: local allele index -> merged allele index
  std::unordered_map<int, std::unordered_map<int, int>> allele_maps;

  // vector of {sample_num, rec} in the merged record
  std::vector<std::tuple<int, SafeBCFRec>> samples;

  // set of sample numbers included in the merged data
  std::unordered_set<int> merged_samples;

  /**
   * @brief Clear the data structure and prepare to merge.
   *
   * @param num_samples total number of samples
   */
  void reset(int num_samples) {
    rid = -1;
    pos = -1;
    ids.clear();
    bcf_float_set_missing(qual);
    ref = "";
    alleles.clear();
    allele_count.clear();
    allele_total = 0;
    depth_total = 0;
    filters.clear();
    gts.resize(2 * num_samples);
    std::fill(gts.begin(), gts.end(), 0);
    info.clear();
    format_keys.clear();
    allele_maps.clear();
    samples.clear();
    merged_samples.clear();
  }
};

class VCFMerger {
 public:
  VCFMerger();

  ~VCFMerger();

  void init(
      const std::vector<std::pair<std::string, size_t>>& sorted_hdrs,
      const std::unordered_map<uint32_t, SafeBCFHdr>& hdr_map);

  void reset();

  /**
   * @brief Finish merging data in the merge buffer. Call after writing the last
   * record to finish merging.
   *
   */
  void finish();

  /**
   * @brief Close the VCFMerger.
   *
   */
  void close();

  /**
   * @brief Write record to merge buffer.
   *
   * @param sample_name sample name
   * @param rec sample record
   */
  void write(const std::string& sample_name, SafeBCFRec rec);

  /**
   * @brief Read next merged record from output buffer.
   *
   * @return SafeBCFRec merged record
   */
  SafeBCFRec read();

  /**
   * @brief Check if output buffer of merged records is empty.
   *
   * @return true output buffer is empty
   * @return false output buffer is not empty
   */
  bool is_empty();

  /**
   * @brief Get a pointer to the merged VCF header for an htslib function call
   *
   * @return bcf_hdr_t* merged VCF header
   */
  bcf_hdr_t* get_header() {
    return hdr_.get();
  }

 private:
  struct SampleRecord {
    SafeBCFRec record;
    int sample_num = -1;
    int variant_type = -1;
  };

  /**
   * @brief Return number, type, and number of values for a INFO/FORMAT field
   * based on the merged header and number of alleles in the merged data.
   *
   * @param id int id of INFO/FORMAT field
   * @param hdr_type BCF_HL_INFO | BCF_HL_FMT
   * @param sample_num sample number for VCF header lookup, if -1 use merged
   * header
   * @return std::tuple<int, int, int> [number, type, values]
   */
  std::tuple<int, int, int> get_number_type_values(
      int id, int hdr_type, int sample_num = -1);

  /**
   * @brief Get the missing vector end object
   *
   * @param type field type (BCF_HT_*)
   * @return std::tuple<int, int> [missing, vector_end]
   */
  std::tuple<int, int> get_missing_vector_end(int type);

  /**
   * @brief Try to merge records in the merge buffer
   *
   * @param flush if true, merge until merge buffer is empty
   */
  void try_merge(bool flush);

  /**
   * @brief Merge records at the same CHROM,POS
   *
   */
  void merge_records();

  /**
   * @brief Merge alleles from new record into the merged data alleles.
   * Normalize REF and ALT allelesand create an allele index map
   * for multiallelic records.
   *
   * @param sample_num sample number of input record
   * @param input sample record to be merged
   */
  void merge_alleles(int sample_num, bcf1_t* input);

  /**
   * @brief Check if record can be merged into the existing merged data.
   *
   * @param record VCF record
   * @return true Record can be merged
   * @return false Record cannot be merged
   */
  bool can_merge_record(SafeBCFRec& record);

  /**
   * @brief Merge input record with the merged data structure.
   *
   * @param sample_num sample number of input record
   * @param input sample record to be merged
   */
  void merge_record(int sample_num, SafeBCFRec input);

  /**
   * @brief Finish merging INFO fields.
   *
   * @param rec merged record
   */
  void finish_info(SafeBCFRec& rec);

  void finish_format(SafeBCFRec& rec);
  /**
   * @brief Call after all records at the current CHROM,POS have been merged
   * with merge_record.
   *
   */
  void finish_merge();

  // reusable htslib buffer for bcf_get_* functions
  int* dst_ = nullptr;

  // reusable htslib buffer size for bcf_get_* functions
  int ndst_ = 0;

  // reusable buffer for merging data
  Buffer buffer_;

  // map of sample name to sample_num
  std::unordered_map<std::string, int> sample_map_;

  // combined VCF header
  SafeBCFHdr hdr_;

  // vector of sample VCF headers, indexed by sample number
  // TODO: update ReadState to use shared_ptrs
  std::vector<bcf_hdr_t*> hdrs_;

  // combined VCF record
  SafeBCFRec rec_;

  // cached id of PASS filter to avoid multiple lookups
  int pass_filter_id_;

  // merged data
  MergedData md_;

  // number of samples being combined
  int num_samples_ = -1;

  // contig of data currently being merged
  int contig_ = -1;

  // records ready to be merged
  std::deque<SampleRecord> merge_buffer_;

  // merged records
  std::deque<SafeBCFRec> output_buffer_;

  // counters
  uint32_t read_count_ = 0;
  uint32_t write_count_ = 0;
};

}  // namespace vcf
}  // namespace tiledb
