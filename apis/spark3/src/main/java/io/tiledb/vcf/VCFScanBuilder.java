package io.tiledb.vcf;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

public class VCFScanBuilder
    implements ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns {

  static Logger log = Logger.getLogger(VCFScanBuilder.class.getName());

  private final VCFSparkSchema schema;
  private final Map<String, String> properties;
  private final VCFDataSourceOptions options;
  protected static List<String> pushedSampleNames;
  private List<Filter> pushedFilters;

  public VCFScanBuilder(Map<String, String> properties, VCFDataSourceOptions options)
      throws URISyntaxException {

    this.schema = new VCFSparkSchema(options.getDatasetURI().get(), options); // TODO warning fix
    this.properties = properties;
    this.options = options;
    this.pushedSampleNames = new ArrayList<>();
    this.pushedFilters = new ArrayList<>();
  }

  @Override
  public Scan build() {
    return new VCFScan(schema, properties, options);
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    List<Filter> nonPushedFilters = new ArrayList<>();
    pushedFilters.clear();
    pushedSampleNames.clear();
    for (Filter f : filters) {
      boolean pushed = false;
      if (f instanceof EqualTo) {
        if (((EqualTo) f).attribute().equals("sampleName")) {
          if (options.getSamples().isPresent() || options.getSampleURI().isPresent()) {
            throw new UnsupportedOperationException(
                "Cannot have a sampleName in where clause while also having samples or sampleFile in options list");
          }
          String value = (String) ((EqualTo) f).value();
          pushedSampleNames.add(value);
          pushed = true;
        }
      }

      if (pushed) {
        pushedFilters.add(f);
      } else {
        nonPushedFilters.add(f);
      }
    }

    Filter[] arr = new Filter[nonPushedFilters.size()];
    nonPushedFilters.toArray(arr);
    return arr;
  }

  @Override
  public Filter[] pushedFilters() {
    Filter[] arr = new Filter[pushedFilters.size()];
    pushedFilters.toArray(arr);
    return arr;
  }

  @Override
  public void pruneColumns(StructType pushDownSchema) {
    log.trace("Pushdown columns: " + pushDownSchema);
    this.schema.setPushDownSchema(pushDownSchema);
  }
}
