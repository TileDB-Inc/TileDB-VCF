package io.tiledb.vcf;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import org.apache.log4j.Logger;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.reader.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class VCFDataSourceReader
    implements DataSourceReader,
        SupportsPushDownRequiredColumns,
        SupportsScanColumnarBatch,
        SupportsPushDownFilters {

  static Logger log = Logger.getLogger(VCFDataSourceReader.class.getName());

  private URI uri;
  private VCFDataSourceOptions options;
  private VCFSparkSchema schema;
  private List<String> pushedSamples;

  public VCFDataSourceReader(URI uri, VCFDataSourceOptions options) {
    this.uri = uri;
    this.options = options;
    this.schema = new VCFSparkSchema();
    this.pushedSamples = new ArrayList<>();
  }

  @Override
  public void pruneColumns(StructType pushDownSchema) {
    log.trace("Pushdown columns: " + pushDownSchema);
    this.schema.setPushDownSchema(pushDownSchema);
  }

  @Override
  public StructType readSchema() {
    StructType readSchema = schema.getSparkSchema();
    log.trace("Read schema: + " + readSchema);
    return readSchema;
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    pushedSamples.clear();
    for (Filter f : filters) {
      if (f instanceof EqualTo) {
        if (((EqualTo) f).attribute() == "sampleName") {
          String value = (String) ((EqualTo) f).value();
          pushedSamples.add(value);
        }
      }
    }
    return filters;
  }

  @Override
  public Filter[] pushedFilters() {
    return new Filter[] {};
  }

  @Override
  public boolean enableBatchRead() {
    // always read in batch mode
    return true;
  }

  @Override
  public List<InputPartition<ColumnarBatch>> planBatchInputPartitions() {
    HashSet<String> dedupSamples = new HashSet<>();
    dedupSamples.addAll(pushedSamples);
    Optional<String[]> optionSamples = options.getSamples();
    if (optionSamples.isPresent()) {
      for (String sample : optionSamples.get()) {
        dedupSamples.add(sample);
      }
    }

    // Get number of range partitions (if specified)
    List<String> samples = new ArrayList<>(dedupSamples);
    Optional<Integer> optionRangePartitions = options.getRangePartitions();
    int numRangePartitions = 1;
    if (optionRangePartitions.isPresent()) {
      numRangePartitions = optionRangePartitions.get();
      if (numRangePartitions < 1) {
        throw new RuntimeException("Invalid number of range partitions: " + numRangePartitions);
      }
    }

    // Get number of sample partitions (if specified)
    Optional<Integer> optionSamplePartitions = options.getSamplePartitions();
    int numSamplePartitions = 1;
    if (optionSamplePartitions.isPresent()) {
      numSamplePartitions = optionSamplePartitions.get();
      if (numSamplePartitions < 1) {
        throw new RuntimeException("Invalid number of sample partitions: " + numSamplePartitions);
      }
    }

    // Try to add additional partitions if partitions is also specified
    Optional<Integer> optionPartitions = options.getPartitions();
    if (optionPartitions.isPresent()) {
      int numPartitions = optionPartitions.get();
      if (numPartitions < 1) {
        throw new RuntimeException("Invalid number of partitions: " + numPartitions);
      }
      if (optionRangePartitions.isPresent() && optionSamplePartitions.isPresent()) {
        if ((numRangePartitions * numSamplePartitions) != numPartitions) {
          throw new RuntimeException(
              "Cannot partition domain given "
                  + numRangePartitions
                  + " range partitions, "
                  + numSamplePartitions
                  + " sample partitions and "
                  + numPartitions
                  + " domain partitions");
        }
      } else if (optionRangePartitions.isPresent()) {
        if (numPartitions % numRangePartitions != 0) {
          throw new RuntimeException(
              "Cannot partition domain given "
                  + numRangePartitions
                  + " range partitions and "
                  + numPartitions
                  + " domain partitions");
        }
        numSamplePartitions = numPartitions / numRangePartitions;
      } else if (optionSamplePartitions.isPresent()) {
        if (numPartitions % numSamplePartitions != 0) {
          throw new RuntimeException(
              "Cannot partition domain given "
                  + numRangePartitions
                  + " range partitions and "
                  + numPartitions
                  + " domain partitions");
        }
        numRangePartitions = numPartitions / numSamplePartitions;
      } else {
        // only domain partitions specified, default to partitioning by ranges as that is the most
        // performant default given an unknown number of samples and ranges
        numRangePartitions = numPartitions;
      }
    }

    // Create Spark input partitions
    List<InputPartition<ColumnarBatch>> inputPartitions =
        new ArrayList<>(numRangePartitions * numSamplePartitions);
    for (int r = 0; r < numRangePartitions; r++) {
      for (int s = 0; s < numSamplePartitions; s++) {
        inputPartitions.add(
            new VCFInputPartition(
                uri,
                schema,
                options,
                samples,
                new VCFPartitionInfo(r, numRangePartitions),
                new VCFPartitionInfo(s, numSamplePartitions)));
      }
    }

    return inputPartitions;
  }
}
