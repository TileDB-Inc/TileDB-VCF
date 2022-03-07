#ifndef TILEDB_VCF_ALLELE_COUNTER_H
#define TILEDB_VCF_ALLELE_COUNTER_H

#include <map>
#include <string>
#include <vector>

#include <htslib/vcf.h>
#include <tiledb/tiledb>

namespace tiledb::vcf {

// Future expansion of ingestion tasks
class IngestionTask {};

class AlleleCounter : public IngestionTask {
 public:
  AlleleCounter();

  ~AlleleCounter();

  // create array
  static void create(
      Context& ctx, std::string root_uri, tiledb_filter_type_t checksum);

  // open array
  void init(std::shared_ptr<Context> ctx, std::string root_uri);

  void process(
      bcf_hdr_t* hdr,
      const std::string& sample_name,
      const std::string& contig,
      uint32_t pos,
      bcf1_t* record);

  // create query, write, finalize
  void flush();

  void finalize();

 private:
  inline static const std::string AC_URI = "allele_count";
  inline static const std::string AC_CONTIG = "contig";  // dim: str
  inline static const std::string AC_POS = "pos";        // dim: int
  inline static const std::string AC_ALLELE = "allele";  // attr: str
  inline static const std::string AC_COUNT = "count";    // attr: int

  std::shared_ptr<Context> ctx_ = nullptr;
  std::unique_ptr<Array> array_ = nullptr;
  std::map<std::string, int> allele_count_;
  std::string contig_;
  uint32_t pos_;

  // Buffers for tiledb write
  std::string ac_contig_;
  std::vector<uint64_t> ac_contig_offsets_;
  std::vector<int32_t> ac_pos_;
  std::string ac_allele_;
  std::vector<uint64_t> ac_allele_offsets_;
  std::vector<int32_t> ac_count_;

  // Reusable htslib buffer for bcf_get_* functions
  int* dst_ = nullptr;

  // Reusable htslib buffer size for bcf_get_* functions
  int ndst_ = 0;

  // Update results with data at the previous locus
  void update_results();
};

}  // namespace tiledb::vcf

#endif
