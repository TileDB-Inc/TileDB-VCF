package io.tiledb.vcf;

import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import scala.collection.mutable.StringBuilder;

/** Class holding option values for TileDB-VCF. */
public class VCFDataSourceOptions implements Serializable {
  private HashMap<String, String> options;

  /**
   * Parses TileDB-VCF specific options from generic Spark Datasource options
   *
   * @param options Spark Datasource options
   */
  public VCFDataSourceOptions(DataSourceOptions options) {
    this.options = new HashMap<String, String>();
    this.options.putAll(options.asMap());
  }

  /** @return TileDB-VCF dataset URI */
  public Optional<URI> getDatasetURI() {
    if (options.containsKey("uri")) {
      return Optional.of(URI.create(options.get("uri")));
    }
    return Optional.empty();
  }

  /** @return Optional array of provided sample names to read */
  public Optional<String[]> getSamples() {
    if (options.containsKey("samples")) {
      return Optional.of(options.get("samples").split("\\s*,[,\\s]*"));
    }
    return Optional.empty();
  }

  /** @return Optional array of contig regions to read */
  public Optional<String[]> getRanges() {
    if (options.containsKey("ranges")) {
      return Optional.of(options.get("ranges").split("\\s*,[,\\s]*"));
    }
    return Optional.empty();
  }

  /** @return Optional maximum memory budget (MB) */
  public Optional<Integer> getMemoryBudget() {
    if (options.containsKey("memory")) {
      return Optional.of(Integer.parseInt(options.get("memory")));
    }
    return Optional.empty();
  }

  /** @return Optional uri of BED file */
  public Optional<URI> getBedURI() {
    if (options.containsKey("bedfile")) {
      return Optional.of(URI.create(options.get("bedfile")));
    }
    return Optional.empty();
  }

  /** @return Optional uri of SampleFile file */
  public Optional<URI> getSampleURI() {
    if (options.containsKey("samplefile")) {
      return Optional.of(URI.create(options.get("samplefile")));
    }
    return Optional.empty();
  }

  /** @return Optional number of partitions */
  public Optional<Integer> getPartitions() {
    if (options.containsKey("partitions")) {
      return Optional.of(Integer.parseInt(options.get("partitions")));
    }
    return Optional.empty();
  }

  /** @return Optional number of range partitions */
  public Optional<Integer> getRangePartitions() {
    if (options.containsKey("range_partitions")) {
      return Optional.of(Integer.parseInt(options.get("range_partitions")));
    }
    return Optional.empty();
  }

  /** @return Optional number of sample partitions */
  public Optional<Integer> getSamplePartitions() {
    if (options.containsKey("sample_partitions")) {
      return Optional.of(Integer.parseInt(options.get("sample_partitions")));
    }
    return Optional.empty();
  }

  /** @return Optional CSV String of config parameters */
  public Optional<String> getConfigCSV() {
    ArrayList<String> configEntries = new ArrayList<>();
    Iterator<Map.Entry<String, String>> entries = options.entrySet().iterator();
    while (entries.hasNext()) {
      Map.Entry<String, String> entry = entries.next();
      String key = entry.getKey();
      if (key.startsWith("tiledb.")) {
        StringBuilder kv = new StringBuilder();
        kv.append(key.substring(7));
        kv.append("=");
        kv.append(entry.getValue());
        configEntries.add(kv.toString());
      }
    }
    Collections.sort(configEntries);
    int numEntries = configEntries.size();
    if (numEntries == 0) {
      return Optional.empty();
    }
    StringBuilder configCSV = new StringBuilder();
    for (int i = 0; i < numEntries; i++) {
      configCSV.append(configEntries.get(i));
      if (i < (numEntries - 1)) {
        configCSV.append(",");
      }
    }
    return Optional.of(configCSV.toString());
  }
}
