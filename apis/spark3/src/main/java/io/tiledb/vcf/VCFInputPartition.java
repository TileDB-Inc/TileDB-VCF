package io.tiledb.vcf;

import java.net.URI;
import java.util.List;
import org.apache.spark.sql.connector.read.InputPartition;

public class VCFInputPartition implements InputPartition {

  private VCFSparkSchema schema;
  private VCFDataSourceOptions options;
  private URI uri;
  private List<String> samples;
  private VCFPartitionInfo rangePartitionInfo;
  private VCFPartitionInfo samplePartitionInfo;

  public VCFInputPartition(
      URI uri,
      VCFSparkSchema schema,
      VCFDataSourceOptions options,
      List<String> samples,
      VCFPartitionInfo rangePartitionInfo,
      VCFPartitionInfo samplePartitionInfo) {
    this.uri = uri;
    this.schema = schema;
    this.options = options;
    this.samples = samples;
    this.rangePartitionInfo = rangePartitionInfo;
    this.samplePartitionInfo = samplePartitionInfo;
  }

  public VCFSparkSchema getSchema() {
    return schema;
  }

  public VCFDataSourceOptions getOptions() {
    return options;
  }

  public URI getUri() {
    return uri;
  }

  public List<String> getSamples() {
    return samples;
  }

  public VCFPartitionInfo getRangePartitionInfo() {
    return rangePartitionInfo;
  }

  public VCFPartitionInfo getSamplePartitionInfo() {
    return samplePartitionInfo;
  }
}
