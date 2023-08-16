
#include <Rcpp.h>
#include <tiledbvcf/tiledbvcf.h>
#include "reader.h"

// Test via ag
// Rscript -e 'library(tiledbvcf); \
//     vcf_read("../../libtiledbvcf/test/inputs/arrays/v2/ingested_1sample")'

// [[Rcpp::export]]
bool vcf_read(std::string& uri,
              Rcpp::Nullable<Rcpp::CharacterVector> attributes = R_NilValue,
              Rcpp::Nullable<std::string> regions = R_NilValue,
              Rcpp::Nullable<std::string> samples = R_NilValue) {

    tiledbvcf::config_logging("debug", ""); // debug level in libtiledbvcf
    tiledbvcf::LOG_SET_LEVEL("debug");      // debug level in this R package

    tiledbvcf::LOG_TRACE("[vcf_read] before instantianting reader");
    tiledbvcf::Reader reader;
    tiledbvcf::LOG_TRACE("[vcf_read] after instantianting reader");
    reader.init(uri);

    if (!attributes.isNull()) {
        std::vector<std::string> attrs = Rcpp::as<std::vector<std::string>>(attributes);
        reader.set_attributes(attrs);
    } else {
        // cf apis/python/src/tiledbvcf/dataset.py
        std::vector<std::string> attrs = { "sample_name", "contig", "pos_start", "alleles", "fmt_GT" };
        reader.set_attributes(attrs);
    }

    if (!regions.isNull()) {
        std::string reg = Rcpp::as<std::string>(regions);
        reader.set_regions(reg);
    }

    if (!samples.isNull()) {
        std::string samp = Rcpp::as<std::string>(samples);
        reader.set_samples(samp);
    }

    reader.read(true);
    return true;
}
