package io.tiledb.vcf;

import java.util.Map;
import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.streaming.ContinuousStream;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.types.StructType;

public class VCFScan implements Scan {
  private final VCFSparkSchema schema;
  private final Map<String, String> properties;
  private final VCFDataSourceOptions options;

  public VCFScan(
      VCFSparkSchema schema, Map<String, String> properties, VCFDataSourceOptions options) {
    this.schema = schema;
    this.properties = properties;
    this.options = options;
  }

  @Override
  public StructType readSchema() {
    return schema.getSparkSchema();
  }

  @Override
  public String description() {
    return Scan.super.description();
  } // TODO add description

  @Override
  public Batch toBatch() {
    return new VCFBatch(schema, properties, options);
  }

  @Override
  public MicroBatchStream toMicroBatchStream(String checkpointLocation) {
    return Scan.super.toMicroBatchStream(checkpointLocation);
  }

  @Override
  public ContinuousStream toContinuousStream(String checkpointLocation) {
    return Scan.super.toContinuousStream(checkpointLocation);
  }

  @Override
  public CustomMetric[] supportedCustomMetrics() {
    return Scan.super.supportedCustomMetrics();
  }
}
