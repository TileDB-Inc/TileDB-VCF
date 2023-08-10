
#include <Rcpp.h>
#include <tiledbvcf/tiledbvcf.h>
#include "reader.h"

// [[Rcpp::export]]
bool vcf_read(std::string& uri,
              std::vector<std::string>& attributes,
              std::string& regions,
              std::string& samples) {

    tiledbvcf::Reader reader;
    reader.init(uri);
    if (attributes.size() > 0) reader.set_attributes(attributes);
    if (regions != "") reader.set_regions(regions);
    if (samples != "") reader.set_samples(samples);

    return true;
}
