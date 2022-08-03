package io.tiledb.vcf;

import io.netty.buffer.ArrowBuf;
import io.tiledb.libvcfnative.VCFReader;
import io.tiledb.util.CredentialProviderUtils;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.execution.arrow.ArrowUtils;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/** This class implements a Spark batch reader from a TileDB-VCF data source. */
public class VCFInputPartitionReader implements InputPartitionReader<ColumnarBatch> {
  private final Logger log;

  /**
   * Default memory budget (in MB). The Spark connector uses half for allocating columnar buffers,
   * the other half goes to TileDB-VCF native for its own buffers.
   */
  private static final int DEFAULT_MEM_BUDGET_MB = 512;

  /** URI of the TileDB-VCF dataset. */
  private URI datasetURI;

  /** Options for the Spark reader. */
  private VCFDataSourceOptions options;

  /** The Spark schema being used. */
  private VCFSparkSchema schema;

  /** The reader instance used to interface with the native TileDB-VCF library. */
  private VCFReader vcfReader;

  /** List of the allocated Arrow vectors used to hold columnar data. */
  private List<ArrowColumnVector> arrowVectors;

  /** The current batch of results. */
  private ColumnarBatch resultBatch;

  /** Partitioning info on ranges for this reader. */
  private VCFPartitionInfo rangePartitionInfo;

  /** Partitioning info on samples for this reader. */
  private VCFPartitionInfo samplePartitionInfo;

  /** Unique ID of the range/sample partition of this reader. */
  private int partitionId;

  /** Flag (initially false) set to false when the VCF read query is complete. */
  private boolean hasNext;

  /** The sample names that are included in the VCF read query. */
  private String[] samples;

  /** If true, prints extra stats log messages. */
  private boolean enableStatsLogging;

  /** Stats counter: number of VCF read submits. */
  private long statsSubmissions;

  /** Stats counter: number of bytes in allocated buffers. */
  private long statsTotalBufferBytes;

  private Level enableStatsLogLevel;

  /**
   * Creates a TileDB-VCF reader.
   *
   * @param uri URI of the TileDB-VCF dataset to read from.
   * @param schema Spark schema of result dataframe
   * @param options Any VCF options to be used
   * @param samples List of sample names to include in the read query
   * @param rangePartitionInfo Partition info (ranges)
   * @param samplePartitionInfo Partition info (samples)
   */
  public VCFInputPartitionReader(
      URI uri,
      VCFSparkSchema schema,
      VCFDataSourceOptions options,
      List<String> samples,
      VCFPartitionInfo rangePartitionInfo,
      VCFPartitionInfo samplePartitionInfo) {
    this.datasetURI = uri;
    this.schema = schema;
    this.arrowVectors = new ArrayList<>();
    this.options = options;
    this.rangePartitionInfo = rangePartitionInfo;
    this.samplePartitionInfo = samplePartitionInfo;
    this.partitionId =
        rangePartitionInfo.getIndex() * samplePartitionInfo.getNumPartitions()
            + samplePartitionInfo.getIndex()
            + 1;
    this.hasNext = false;
    this.enableStatsLogging = true;
    this.statsSubmissions = 0;
    this.statsTotalBufferBytes = 0;
    if (samples.size() > 0) {
      this.samples = new String[samples.size()];
      samples.toArray(this.samples);
    } else {
      this.samples = new String[] {};
    }

    this.enableStatsLogLevel = Level.OFF;
    if (this.options.getTileDBStatsLogLevel().isPresent()) {
      // If an invalid log level is set, the default is DEBUG
      this.enableStatsLogLevel = Level.toLevel(this.options.getTileDBStatsLogLevel().get());
    }

    TaskContext task = TaskContext.get();
    log =
        Logger.getLogger(
            VCFInputPartitionReader.class.getName()
                + " (stage:"
                + task.stageId()
                + "/taskID:"
                + task.partitionId()
                + ")");
    log.info(
        "Task Stage ID - "
            + task.stageId()
            + ", Task Partition ID - "
            + task.partitionId()
            + " for TileDB-VCF Partition "
            + partitionId
            + " started");

    task.addTaskCompletionListener(
        context -> {
          log.info(
              "Task Stage ID - "
                  + task.stageId()
                  + ", Task Partition ID - "
                  + task.partitionId()
                  + " for TileDB-VCF Partition "
                  + partitionId
                  + " completed");
        });

    log.info("options set: " + options.toString());
  }

  @Override
  public boolean next() {
    // first submission
    if (vcfReader == null) {
      initVCFReader();
      hasNext = true;
    } else {
      vcfReader.resetBuffers();
    }
    if (!hasNext) {
      return false;
    }
    boolean prevNext = hasNext;
    long t1 = System.currentTimeMillis();
    vcfReader.submit();
    long t2 = System.currentTimeMillis();
    if (enableStatsLogging) {
      statsSubmissions++;
      log.info(
          "STATS Partition "
              + (this.partitionId)
              + " Submit vcfReader: "
              + statsSubmissions
              + " time (ms): "
              + (t2 - t1));
    }

    VCFReader.Status status = vcfReader.getStatus();
    if (status != VCFReader.Status.COMPLETED && status != VCFReader.Status.INCOMPLETE)
      throw new RuntimeException("Unexpected VCF vcfReader status " + status);

    hasNext = vcfReader.getStatus() == VCFReader.Status.INCOMPLETE;
    return prevNext;
  }

  @Override
  public ColumnarBatch get() {
    long t1 = System.nanoTime();

    long numRecords = vcfReader.getNumRecords();
    if (numRecords == 0 && vcfReader.getStatus() == VCFReader.Status.INCOMPLETE)
      throw new RuntimeException("Unexpected VCF incomplete vcfReader with 0 results.");

    // First batch of results
    if (resultBatch == null) {
      ColumnVector[] colVecs = new ColumnVector[arrowVectors.size()];
      for (int i = 0; i < arrowVectors.size(); i++) colVecs[i] = arrowVectors.get(i);
      resultBatch = new ColumnarBatch(colVecs);
    }
    resultBatch.setNumRows(Util.longToInt(numRecords));
    long t2 = System.nanoTime();

    if (enableStatsLogging) {
      log.info(
          String.format(
              "STATS Partition %d Copy vcfReader: %d time (nanos): %d for %d record(s)",
              this.partitionId, statsSubmissions, t2 - t1, numRecords));
    }

    return resultBatch;
  }

  @Override
  public void close() {
    log.info("Closing VCFReader for partition " + (this.partitionId));

    if (!this.enableStatsLogLevel.equals(Level.OFF)) {
      log.log(this.enableStatsLogLevel, this.vcfReader.stats());
    }

    if (vcfReader != null) {
      vcfReader.close();
      vcfReader = null;
    }

    if (resultBatch != null) {
      resultBatch.close();
      resultBatch = null;
    }

    releaseArrowVectors();

    // Notify garbage collector there is trash to take out
    System.gc();
  }

  /** Closes any allocated Arrow vectors and clears the list. */
  private void releaseArrowVectors() {
    if (arrowVectors != null) {
      for (ArrowColumnVector v : arrowVectors) v.close();
      arrowVectors.clear();
    }
  }

  /**
   * Initializes the native TileDB-VCF reader instance by setting partitioning info, allocating
   * buffers, etc.
   */
  private void initVCFReader() {
    log.info(
        "Init VCFReader for partition "
            + this.partitionId
            + ": range partition "
            + (rangePartitionInfo.getIndex() + 1)
            + " of "
            + rangePartitionInfo.getNumPartitions()
            + ", sample partition "
            + (samplePartitionInfo.getIndex() + 1)
            + " of "
            + samplePartitionInfo.getNumPartitions()
            + " (range_partition_index="
            + rangePartitionInfo.getIndex()
            + ", range_partitions="
            + rangePartitionInfo.getNumPartitions()
            + ", sample_partition_index="
            + samplePartitionInfo.getIndex()
            + ", sample_partitions="
            + samplePartitionInfo.getNumPartitions()
            + ")");
    String uriString = datasetURI.toString();

    Optional<String> credentialsCsv =
        options
            .getCredentialsProvider()
            .map(CredentialProviderUtils::buildConfigMap)
            .flatMap(VCFDataSourceOptions::getConfigCSV);

    Optional<String> configCsv =
        VCFDataSourceOptions.combineCsvOptions(options.getConfigCSV(), credentialsCsv);

    vcfReader = new VCFReader(uriString, samples, options.getSampleURI(), configCsv);

    if (!options.getNewPartitionMethod().orElse(false)) {
      log.info("No ranges on spark partition info, using bed file/user passed regions instead");
      // Set ranges
      Optional<String[]> ranges = options.getRanges();
      if (ranges.isPresent()) {
        vcfReader.setRanges(ranges.get());
      }

      // Set BED file
      Optional<URI> bedURI = options.getBedURI();
      if (bedURI.isPresent()) {
        vcfReader.setBedFile(bedURI.get().toString());
      }
    } else {
      if (rangePartitionInfo.getRegions().isEmpty()) {
        throw new RuntimeException(
            "rangePartitionInfo missing regions but new_partition_method is set");
      }
      log.info("Setting " + rangePartitionInfo.getRegions().size() + " ranges from partition info");
      String[] regions = new String[rangePartitionInfo.getRegions().size()];
      regions = rangePartitionInfo.getRegions().toArray(regions);
      vcfReader.setRanges(regions);
    }

    // Set sort regions
    Optional<Boolean> sortRegions = options.getSortRegions();
    if (sortRegions.isPresent()) {
      vcfReader.setSortRegions(sortRegions.get());
    }

    // Set verbose
    Optional<Boolean> verbose = options.getVerbose();
    if (verbose.isPresent()) {
      vcfReader.setVerbose(verbose.get());
    }

    // Set enableProgressEstimation
    Optional<Boolean> enableProgressEstimation = options.getEnableProgressEstimation();
    if (enableProgressEstimation.isPresent()) {
      vcfReader.setEnableProgressEstimation(enableProgressEstimation.get());
    }

    // Set TileDB buffer percentage
    Optional<Float> tiledbBufferPercentage = options.getTileDBBufferPercentage();
    if (tiledbBufferPercentage.isPresent()) {
      vcfReader.setBufferPercentage(tiledbBufferPercentage.get());
    }

    // Set TileDB tile cache percentage
    Optional<Float> tiledbTileCachePercentage = options.getTileDBTileCachePercentage();
    if (tiledbTileCachePercentage.isPresent()) {
      vcfReader.setTileDBTileCachePercentage(tiledbTileCachePercentage.get());
    }

    // Set debugPrintVCFRegions
    Optional<Boolean> debugPrintVCFRegions = options.getDebugPrintVCFRegions();
    if (debugPrintVCFRegions.isPresent()) {
      vcfReader.setDebugPrintVCFRegions(debugPrintVCFRegions.get());
    }

    // Set debugPrintSampleList
    Optional<Boolean> debugPrintSampleList = options.getDebugPrintSampleList();
    if (debugPrintSampleList.isPresent()) {
      vcfReader.setDebugPrintSampleList(debugPrintSampleList.get());
    }

    // Set debugPrintTileDBQueryRanges
    Optional<Boolean> debugPrintTileDBQueryRanges = options.getDebugPrintTileDBQueryRanges();
    if (debugPrintTileDBQueryRanges.isPresent()) {
      vcfReader.setDebugPrintTileDBQueryRanges(debugPrintTileDBQueryRanges.get());
    }

    // Enable VCFReader stats
    if (!this.enableStatsLogLevel.equals(Level.OFF)) this.vcfReader.setStatsEnabled(true);

    // Set logical partition in array
    if (rangePartitionInfo.getRegions().isEmpty()) {
      vcfReader.setRangePartition(
          rangePartitionInfo.getNumPartitions(), rangePartitionInfo.getIndex());
    }
    vcfReader.setSamplePartition(
        samplePartitionInfo.getNumPartitions(), samplePartitionInfo.getIndex());

    // Get a count of the number of buffers we need to allocate.
    StructField[] sparkFields = schema.getSparkFields();
    String[] attrNames = schema.getVCFAttributes();
    int numColumns = schema.getNumColumns();
    int nBuffers = 0;
    for (int idx = 0; idx < numColumns; idx++) {
      nBuffers += numBuffersForField(attrNames[idx]);
    }

    // Get the memory budget, if specified.
    Optional<Integer> memoryBudget = options.getMemoryBudget();
    long memBudgetMB = DEFAULT_MEM_BUDGET_MB;
    if (memoryBudget.isPresent()) {
      memBudgetMB = memoryBudget.get();
    }
    long vcfMemBudgetMB = memBudgetMB;

    // Given fixed memory budget and required attributes, compute buffer sizes. Note that if
    // nBuffers is 0,
    // this is just a counting operation, and no buffers need to be allocated.
    if (nBuffers > 0) {
      // We get 1/3rd, the other 2/3rds goes to libtiledbvcf.
      vcfMemBudgetMB = ((Double) (memBudgetMB / 3.0 * 2)).longValue();
      memBudgetMB /= 3;

      // Compute allocation size; check against some reasonable minimum.
      long bufferSizeMB = ((memBudgetMB * 1024 * 1024) / nBuffers) / (1024 * 1024);

      if (bufferSizeMB < 10) {
        log.warn(
            "Warning: TileDB-VCF-Spark buffer allocation of "
                + bufferSizeMB
                + " is small. Increase the memory budget from its current setting of "
                + (memBudgetMB * 3)
                + " MB. Or reduce fields current selection of "
                + numColumns
                + " fields ("
                + nBuffers
                + " buffers)");
      }

      if (bufferSizeMB > 2048) {
        memBudgetMB = nBuffers * 2048;
        bufferSizeMB = 2048;
        log.warn(
            String.format(
                ""
                    + "Size of individual buffers is larger than 2048 MB which is not supported by Arrow 0.10.0 (Spark 2.4). "
                    + "Using %d buffers, a reasonable memory budget is %d."
                    + "Setting buffer size to 2048 instead.",
                nBuffers, memBudgetMB));
      }

      log.info(
          "Initializing "
              + numColumns
              + " columns with "
              + nBuffers
              + " buffers of size "
              + bufferSizeMB
              + "MB");

      long bufferSizeBytes = bufferSizeMB * (1024 * 1024);

      releaseArrowVectors();
      for (int idx = 0; idx < numColumns; idx++) {
        allocateAndSetBuffer(sparkFields[idx].name(), attrNames[idx], bufferSizeBytes);
        this.statsTotalBufferBytes += numBuffersForField(attrNames[idx]) * bufferSizeBytes;
      }
    }

    // Set the VCF c++ memory budget to 2/3rds of total budget
    vcfReader.setMemoryBudget(Util.longToInt(vcfMemBudgetMB));

    if (enableStatsLogging) {
      log.info(
          "STATS Partition "
              + (this.partitionId)
              + " offheap buffer bytes allocated: "
              + this.statsTotalBufferBytes);
    }
  }

  /**
   * Allocates an appropriately-typed Arrow buffer for an attribute (dataframe column), then sets
   * that buffer on the VCF Reader object.
   *
   * @param fieldName Name of the Spark field for the column
   * @param attrName Name of the TileDB-VCF attribute for the column
   * @param attributeBufferSize Size (in bytes) of the buffer allocation. Note that var-len and list
   *     attributes have multiple buffers allocated (data values, offsets), in which case this size
   *     is used for all buffers individually.
   */
  private void allocateAndSetBuffer(String fieldName, String attrName, long attributeBufferSize) {
    VCFReader.AttributeTypeInfo info = vcfReader.getAttributeDatatype(attrName);

    // Allocate an Arrow-backed buffer for the attribute.
    ValueVector valueVector = makeArrowVector(fieldName, info);

    long maxRowsL = (attributeBufferSize / Util.getDefaultRecordByteCount(valueVector.getClass()));

    // Max number of rows is nbytes / sizeof(int32_t), i.e. the max number of offsets that can be
    // stored.
    int maxNumRows = Util.longToInt(maxRowsL);

    if (valueVector instanceof ListVector) {
      ((ListVector) valueVector).setInitialCapacity(maxNumRows, 1);
    } else {
      valueVector.setInitialCapacity(maxNumRows);
    }
    valueVector.allocateNew();

    // Get the underlying data values buffer.
    ByteBuffer data;
    if (valueVector instanceof ListVector) {
      ListVector lv = (ListVector) valueVector;
      ArrowBuf arrowData = lv.getDataVector().getDataBuffer();
      data = arrowData.nioBuffer(0, arrowData.capacity());

      // For null list entries, TileDB-VCF will set the outer bitmap.
      // The inner (values) bitmap should be initialized to all non-null.
      ArrowBuf valueBitmap = lv.getDataVector().getValidityBuffer();
      int nbytes = valueBitmap.capacity();
      for (int i = 0; i < nbytes; i++) {
        valueBitmap.setByte(i, 0xff);
      }
    } else {
      ArrowBuf arrowData = valueVector.getDataBuffer();
      data = arrowData.nioBuffer(0, arrowData.capacity());
    }

    // Set the value buffer (and offsets if applicable).
    vcfReader.setBuffer(attrName, data);

    if (info.isList) {
      ListVector lv = (ListVector) valueVector;
      ArrowBuf arrowListOffsets = lv.getOffsetBuffer();
      ByteBuffer listOffsets = arrowListOffsets.nioBuffer(0, arrowListOffsets.capacity());
      ArrowBuf arrowOffsets = lv.getDataVector().getOffsetBuffer();
      ByteBuffer offsets = arrowOffsets.nioBuffer(0, arrowOffsets.capacity());
      vcfReader.setBufferOffsets(attrName, offsets);
      vcfReader.setBufferListOffsets(attrName, listOffsets);
    } else if (info.isVarLen) {
      ArrowBuf arrowOffsets = valueVector.getOffsetBuffer();
      ByteBuffer offsets = arrowOffsets.nioBuffer(0, arrowOffsets.capacity());
      vcfReader.setBufferOffsets(attrName, offsets);
    }

    // Set the validity bitmap buffer.
    ArrowBuf arrowBitmap = valueVector.getValidityBuffer();
    ByteBuffer bitmap = arrowBitmap.nioBuffer(0, arrowBitmap.capacity());
    if (info.isNullable) {
      vcfReader.setBufferValidityBitmap(attrName, bitmap);
    } else {
      int nbytes = arrowBitmap.capacity();
      for (int i = 0; i < nbytes; i++) {
        arrowBitmap.setByte(i, 0xff);
      }
    }

    this.arrowVectors.add(new ArrowColumnVector(valueVector));
  }

  /**
   * Creates and returns an empty Arrow vector with the appropriate type for the given field.
   *
   * @param fieldName Name of the Spark field for the column
   * @param typeInfo Type information for the Arrow vector
   * @return Arrow ValueVector
   */
  private static ValueVector makeArrowVector(
      String fieldName, VCFReader.AttributeTypeInfo typeInfo) {
    // TODO: any way to get Arrow to not allocate bitmaps for non-nullable attributes?
    RootAllocator allocator = ArrowUtils.rootAllocator();
    ArrowType arrowType;
    ValueVector valueVector;
    switch (typeInfo.datatype) {
      case CHAR:
        if (!typeInfo.isVarLen)
          throw new RuntimeException("Unhandled fixed-len char buffer for attribute " + fieldName);
        if (typeInfo.isList) {
          // Nested list (list of UTF8 which is already a list type)
          ListVector lv = ListVector.empty(fieldName, allocator);
          lv.addOrGetVector(FieldType.nullable(new ArrowType.Utf8()));
          valueVector = lv;
        } else {
          valueVector = new VarCharVector(fieldName, allocator);
        }
        break;
      case UINT8:
        // Because there are no unsigned datatypes, the uint8_t fields must be binary blobs, not
        // scalars.
        if (!typeInfo.isVarLen)
          throw new RuntimeException(
              "Unhandled fixed-len uint8_t buffer for attribute " + fieldName);
        // None of the attributes from TileDB-VCF currently can be a nested list except for strings.
        if (typeInfo.isList)
          throw new RuntimeException("Unhandled nested list for attribute " + fieldName);
        valueVector = new VarBinaryVector(fieldName, allocator);
        break;
      case INT32:
        // None of the attributes from TileDB-VCF currently can be a nested list except for strings.
        if (typeInfo.isVarLen && typeInfo.isList)
          throw new RuntimeException("Unhandled nested list for attribute " + fieldName);
        arrowType = new ArrowType.Int(32, true);
        if (typeInfo.isVarLen) {
          ListVector lv = ListVector.empty(fieldName, allocator);
          lv.addOrGetVector(FieldType.nullable(arrowType));
          valueVector = lv;
        } else {
          valueVector = new IntVector(fieldName, FieldType.nullable(arrowType), allocator);
        }
        break;
      case FLOAT32:
        // None of the attributes from TileDB-VCF currently can be a nested list except for strings.
        if (typeInfo.isVarLen && typeInfo.isList)
          throw new RuntimeException("Unhandled nested list for attribute " + fieldName);
        arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        if (typeInfo.isVarLen) {
          ListVector lv = ListVector.empty(fieldName, allocator);
          lv.addOrGetVector(FieldType.nullable(arrowType));
          valueVector = lv;
        } else {
          valueVector = new Float4Vector(fieldName, FieldType.nullable(arrowType), allocator);
        }
        break;
      default:
        throw new RuntimeException("Unhandled datatype for Arrow buffer, attribute " + fieldName);
    }

    return valueVector;
  }

  /**
   * Returns the number of buffers needed to hold results for the attribute at the given schema
   * index.
   */
  private int numBuffersForField(String attrName) {
    VCFReader.AttributeTypeInfo info = vcfReader.getAttributeDatatype(attrName);
    int numBuffers = 1;
    numBuffers += info.isVarLen ? 1 : 0; // Offsets buffer
    numBuffers += info.isNullable ? 1 : 0; // Nullable bitmap
    numBuffers += info.isList ? 1 : 0; // List offsets bitmap
    return numBuffers;
  }
}
