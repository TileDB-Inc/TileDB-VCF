package io.tiledb.vcf;

import java.net.URI;
import java.util.Optional;
import org.apache.log4j.Logger;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;

/** Class defining the TileDB-VCF Spark DataSource. */
public class VCFDataSource implements DataSourceV2, ReadSupport {
  private static Logger log = Logger.getLogger(VCFDataSource.class.getName());

  @Override
  public DataSourceReader createReader(DataSourceOptions options) {
    VCFDataSourceOptions vcfOptions = new VCFDataSourceOptions(options);
    Optional<String> uriString = options.get("uri");
    if (!uriString.isPresent()) {
      throw new RuntimeException(
          "Error creating VCFDataSourceReader; dataset URI option must be set.");
    }

    URI uri;
    try {
      uri = URI.create(uriString.get());
    } catch (Exception err) {
      throw new RuntimeException(
          "Error creating VCFDataSourceReader; cannot parse dataset URI option into valid URI object");
    }

    log.info("Creating VCF DataSource Reader for '" + uriString + "'");
    return new VCFDataSourceReader(uri, vcfOptions);
  }
}
