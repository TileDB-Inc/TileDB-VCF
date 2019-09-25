package io.tiledb.vcf;

import java.net.URI;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class VCFInputPartition implements InputPartition<ColumnarBatch> {

  static Logger log = Logger.getLogger(VCFInputPartition.class.getName());

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

  @Override
  public InputPartitionReader<ColumnarBatch> createPartitionReader() {
    VCFInputPartitionReader reader;
    try {
      reader =
          new VCFInputPartitionReader(
              uri, schema, options, samples, rangePartitionInfo, samplePartitionInfo);
    } catch (Exception err) {
      throw new RuntimeException(err);
    }
    return reader;
  }
}
