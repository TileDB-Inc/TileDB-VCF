
#include <Rcpp.h>
#include <tiledbvcf/tiledbvcf.h>
#include "reader.h"

// [[Rcpp::export]]
bool vcf_read(std::string& uri,
              Rcpp::Nullable<Rcpp::CharacterVector> attributes = R_NilValue,
              Rcpp::Nullable<std::string> regions = R_NilValue,
              Rcpp::Nullable<std::string> samples = R_NilValue) {

    tiledbvcf::config_logging("trace", "");

    tiledbvcf::Reader reader;
    reader.init(uri);

    if (!attributes.isNull()) {
        std::vector<std::string> attrs = Rcpp::as<std::vector<std::string>>(attributes);
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

    reader.read(false);
    return true;
}
