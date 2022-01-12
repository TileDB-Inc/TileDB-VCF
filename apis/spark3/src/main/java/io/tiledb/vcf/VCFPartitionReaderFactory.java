package io.tiledb.vcf;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class VCFPartitionReaderFactory implements PartitionReaderFactory {

  // This method should be implemented only if a row-reader is available
  @Override
  public PartitionReader<InternalRow> createReader(InputPartition partition) {
    return null;
  }

  @Override
  public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition inputPartition) {
    VCFInputPartition vcfInputPartition = (VCFInputPartition) inputPartition;
    return new VCFPartitionReader(
        vcfInputPartition.getUri(),
        vcfInputPartition.getSchema(),
        vcfInputPartition.getOptions(),
        vcfInputPartition.getSamples(),
        vcfInputPartition.getRangePartitionInfo(),
        vcfInputPartition.getSamplePartitionInfo());
  }

  @Override
  public boolean supportColumnarReads(InputPartition partition) {
    return true;
  }
}
