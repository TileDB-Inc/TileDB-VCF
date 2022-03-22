#ifndef TILEDB_VCF_QC_ARRAYS_H
#define TILEDB_VCF_QC_ARRAYS_H

#include <atomic>
#include <map>
#include <mutex>
#include <string>
#include <vector>

#include <htslib/vcf.h>
#include <tiledb/tiledb>

namespace tiledb::vcf {

// Future expansion of ingestion tasks
class IngestionTask {};

class QCArrays : public IngestionTask {
 public:
  QCArrays();

  ~QCArrays();

  // Create array
  static void create(
      Context& ctx, std::string root_uri, tiledb_filter_type_t checksum);

  // Open array and create query object
  static void init(std::shared_ptr<Context> ctx, std::string root_uri);

  // Finalize open query
  static void finalize();

  // Close array
  static void close();

  // Add record to buffer
  void process(
      bcf_hdr_t* hdr,
      const std::string& sample_name,
      const std::string& contig,
      uint32_t pos,
      bcf1_t* record);

  // Write results to array and reset buffers
  void flush();

 private:
  inline static const std::string VARIANT_QC_URI = "variant_qc";

  enum Dim { CONTIG, POS, ALLELE };
  inline static const std::vector<std::string> DIM_STR = {
      "contig", "pos", "allele"};

  enum Attr { AC = 0, N_HOM, N_CALLED, N_PASS, LAST_ };
  inline static const std::vector<std::string> ATTR_STR = {
      "ac", "n_hom", "n_called", "n_pass"};

  static std::atomic_int contig_records_;
  static std::unique_ptr<Array> array_;
  static std::shared_ptr<Query> query_;
  static std::mutex query_lock_;

  // map allele -> (map attr -> value)
  std::map<std::string, std::unordered_map<int, int32_t>> values_;
  std::string contig_;
  uint32_t pos_;

  // Buffers for tiledb write
  std::string contig_buffer_;
  std::vector<uint64_t> contig_offsets_;
  std::vector<int32_t> pos_buffer_;
  std::string allele_buffer_;
  std::vector<uint64_t> allele_offsets_;
  std::unordered_map<int, std::vector<int32_t>> attr_buffers_;

  // Reusable htslib buffer for bcf_get_* functions
  int* dst_ = nullptr;

  // Reusable htslib buffer size for bcf_get_* functions
  int ndst_ = 0;

  // Update results with data at the previous locus
  void update_results();

  void debug(std::string msg);
};

}  // namespace tiledb::vcf

#endif
