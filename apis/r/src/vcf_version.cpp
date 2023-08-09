
#include <tiledbvcf/tiledbvcf.h>
#include <Rcpp.h>

// [[Rcpp::export]]
std::string vcf_version() {
    const char* version_str;
    tiledb_vcf_version(&version_str);
    return version_str;
}
