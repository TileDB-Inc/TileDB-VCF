package io.tiledb.vcf;

import io.netty.buffer.ArrowBuf;
import io.tiledb.java.api.*;
import io.tiledb.libvcfnative.VCFReader;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.*;
//import org.apache.arrow.memory.RootAllocator;
//import org.apache.arrow.vector.Float4Vector;
//import org.apache.arrow.vector.IntVector;
//import org.apache.arrow.vector.ValueVector;
//import org.apache.arrow.vector.VarBinaryVector;
//import org.apache.arrow.vector.VarCharVector;
//import org.apache.arrow.vector.complex.ListVector;
//import org.apache.arrow.vector.types.FloatingPointPrecision;
//import org.apache.arrow.vector.types.pojo.ArrowType;
//import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.log4j.Logger;
import org.apache.spark.sql.execution.arrow.ArrowUtils;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.types.StructField;
//import org.apache.spark.sql.vectorized.ArrowColumnVector;
//import org.apache.spark.sql.vectorized.O;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/** This class implements a Spark batch reader from a TileDB-VCF data source. */
public class VCFInputPartitionReader implements InputPartitionReader<ColumnarBatch> {
  private static Logger log = Logger.getLogger(VCFInputPartitionReader.class.getName());

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
//  private List<ArrowColumnVector> arrowVectors;
//  private List<> arrowVectors;
  private OnHeapColumnVector[] resultVectors;
  /**
   * List of NativeArray buffers used in the query object. This is indexed based on columnHandles
   * indexing (aka query field indexes)
   */
//  private ArrayList<Pair<NativeArray, NativeArray>> queryBuffers;

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

  private Context ctx;
  private Long numRecords;

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
    try {
      this.ctx = new Context();
    } catch (TileDBError tileDBError) {
      tileDBError.printStackTrace();
    }
    this.datasetURI = uri;
    this.schema = schema;
//    this.queryBuffers = new ArrayList<>();
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
    log.info("vcfReader, buffer sizes: " + vcfReader.getBuffers().size());
    for (Map.Entry<String, VCFReader.BufferInfo> buffers : vcfReader.getBuffers().entrySet()) {
      VCFReader.BufferInfo buffer = buffers.getValue();
      log.info("Buffer " + buffers.getKey() + " has value size: " + buffer.values.getSize());
      if (buffer.offsets != null) {
        log.info("Buffer " + buffers.getKey() + " has offset size: " + buffer.offsets.getSize());
      }
      if (buffer.listOffsets != null) {
        log.info("Buffer " + buffers.getKey() + " has listOffsets size: " + buffer.listOffsets.getSize());
      }
      if (buffer.bitmap != null) {
        log.info("Buffer " + buffers.getKey() + " has bitmap size: " + buffer.bitmap.getSize());
      }
    }
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

    try {
      numRecords = vcfReader.getNumRecords();
      if (numRecords == 0 && vcfReader.getStatus() == VCFReader.Status.INCOMPLETE)
        throw new RuntimeException("Unexpected VCF incomplete vcfReader with 0 results.");

      // First batch of results
//    if (resultBatch == null) {
//      ColumnVector[] colVecs = new ColumnVector[arrowVectors.size()];
//      for (int i = 0; i < arrowVectors.size(); i++) colVecs[i] = arrowVectors.get(i);
//      resultBatch = new ColumnarBatch(colVecs);
//    }

      // loop over all Spark attributes (DataFrame columns) and copy the query result set
      int index = 0;
      for (StructField field : schema.getSparkFields()) {
        getColumnBatch(field, index);
        index++;
      }

      resultBatch.setNumRows(numRecords.intValue());
      long t2 = System.nanoTime();

      if (enableStatsLogging) {
        log.info(
                String.format(
                        "STATS Partition %d Copy vcfReader: %d time (nanos): %d for %d record(s)",
                        this.partitionId, statsSubmissions, t2 - t1, numRecords));
      }
    } catch (TileDBError tileDBError) {
      tileDBError.printStackTrace();
    }

    return resultBatch;
  }

  @Override
  public void close() {
    log.info("Closing VCFReader for partition " + (this.partitionId));


//    releaseArrowVectors();
    closeQueryNativeArrays();

    if (vcfReader != null) {
      vcfReader.close();
      vcfReader = null;
    }

    if (resultBatch != null) {
      resultBatch.close();
      resultBatch = null;
    }

    // Close out spark buffers
    closeOnHeapColumnVectors();
  }

/*  *//** Closes any allocated Arrow vectors and clears the list. *//*
  private void releaseArrowVectors() {
    if (arrowVectors != null) {
      for (ArrowColumnVector v : arrowVectors) v.close();
      arrowVectors.clear();
    }
  }*/
  /** Close out all the NativeArray objects */
  private void closeQueryNativeArrays() {
      if (vcfReader != null) {
        Map<String, VCFReader.BufferInfo> buffers = vcfReader.getBuffers();
        if (buffers != null) {
          for (Map.Entry<String, VCFReader.BufferInfo> bufferSet : buffers.entrySet()) {
            if (bufferSet == null) {
              continue;
            }
            VCFReader.BufferInfo bufferInfo = bufferSet.getValue();
            if (bufferInfo.values != null) {
              bufferInfo.values.close();
            }
            if (bufferInfo.offsets != null) {
              bufferInfo.offsets.close();
            }
            if (bufferInfo.listOffsets != null) {
              bufferInfo.listOffsets.close();
            }
            if (bufferInfo.bitmap != null) {
              bufferInfo.bitmap.close();
            }
          }
        }
      }
  }

  /** Close out onheap column vectors */
  private void closeOnHeapColumnVectors() {
    // Close the OnHeapColumnVector buffers
    for (OnHeapColumnVector buff : resultVectors) {
      buff.close();
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
            + samplePartitionInfo.getNumPartitions());
    String uriString = datasetURI.toString();
    vcfReader = new VCFReader(uriString, samples, options.getSampleURI(), options.getConfigCSV());

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

    // Set sort regions
    Optional<Boolean> sortRegions = options.getSortRegions();
    if (sortRegions.isPresent()) {
      vcfReader.setSortRegions(sortRegions.get().booleanValue());
    }

    // Set logical partition in array
    vcfReader.setRangePartition(
        rangePartitionInfo.getNumPartitions(), rangePartitionInfo.getIndex());
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

    // Given fixed memory budget and required attributes, compute buffer sizes. Note that if
    // nBuffers is 0,
    // this is just a counting operation, and no buffers need to be allocated.
    if (nBuffers > 0) {
      // We get half, the other half goes to libtiledbvcf.
      memBudgetMB /= 2;

      // Compute allocation size; check against some reasonable minimum.
      long bufferSizeMB = ((memBudgetMB * 1024 * 1024) / nBuffers) / (1024 * 1024);
      if (bufferSizeMB < 10) {
        log.warn(
            "Warning: TileDB-VCF-Spark buffer allocation of "
                + bufferSizeMB
                + " is small. Increase the memory budget from its current setting of "
                + (memBudgetMB * 2)
                + " MB.");
      }

      long bufferSizeBytes = bufferSizeMB * (1024 * 1024);

//      releaseArrowVectors();
      try {
        closeQueryNativeArrays();
      for (int idx = 0; idx < numColumns; idx++) {
//        allocateAndSetBuffer(sparkFields[idx].name(), attrNames[idx], bufferSizeBytes);
          allocateQuerybuffer(sparkFields[idx].name(), attrNames[idx], bufferSizeBytes);
        this.statsTotalBufferBytes += numBuffersForField(attrNames[idx]) * bufferSizeBytes;
      }
      } catch (TileDBError tileDBError) {
        tileDBError.printStackTrace();
      }

      int maxNumRows = (int) (bufferSizeBytes / 4);
      // Allocate result set batch based on the estimated (upper bound) number of rows / cells
      resultVectors = OnHeapColumnVector.allocateColumns(maxNumRows, schema.getSparkSchema());
      resultBatch = new ColumnarBatch(resultVectors);
    }

    vcfReader.setMemoryBudget((int) memBudgetMB);

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
 /* private void allocateAndSetBuffer(String fieldName, String attrName, long attributeBufferSize) {
    VCFReader.AttributeTypeInfo info = vcfReader.getAttributeDatatype(attrName);

    // Max number of rows is nbytes / sizeof(int32_t), i.e. the max number of offsets that can be
    // stored.
    int maxNumRows = (int) (attributeBufferSize / 4);

    // Allocate an Arrow-backed buffer for the attribute.
    ValueVector valueVector = makeArrowVector(fieldName, info);
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
  }*/

  /**
   *
   * @param fieldName
   * @param attrName
   * @param readBufferSize
   * @throws TileDBError
   */
  private void allocateQuerybuffer(String fieldName, String attrName, long readBufferSize) throws TileDBError {
    int maxNumRows = (int) (readBufferSize / 4);
    VCFReader.AttributeTypeInfo info = vcfReader.getAttributeDatatype(attrName);
//          int nvalues = Math.toIntExact(readBufferSize / attr.getType().getNativeSize());
        NativeArray data = new NativeArray(ctx, maxNumRows, info.datatype.toTileDBDatatype());
          // attribute is variable length, init the varlen result buffers using the est num offsets
    vcfReader.setBufferNativeArray(attrName, data);
    if (info.isList) {
//      int noffsets = Math.toIntExact(readBufferSize / TILEDB_UINT64.getNativeSize());
      NativeArray offsets = new NativeArray(ctx, maxNumRows, Datatype.TILEDB_INT32);
      NativeArray listOffsets = new NativeArray(ctx, maxNumRows, Datatype.TILEDB_INT32);
//            query.setBuffer(name, offsets, data);
      vcfReader.setBufferOffsetsNativeArray(attrName, offsets);
      vcfReader.setBufferListOffsetsNativeArray(attrName, listOffsets);
//      queryBuffers.add(new Pair<>(offsets, data));
    } else if (info.isVarLen) {
//            int noffsets = Math.toIntExact(readBufferSize / TILEDB_UINT64.getNativeSize());
            NativeArray offsets = new NativeArray(ctx, maxNumRows, Datatype.TILEDB_INT32);
//            query.setBuffer(name, offsets, data);
      vcfReader.setBufferOffsetsNativeArray(attrName, offsets);
//      queryBuffers.add(new Pair<>(offsets, data));
//          } else {
            // attribute is fixed length, use the result size estimate for allocation
//            query.setBuffer(name, new NativeArray(ctx, nvalues, attr.getType()));
//            queryBuffers.add(new Pair<>(null, data));
          }
    if (info.isNullable) {
      NativeArray bitmap = new NativeArray(ctx, maxNumRows, Datatype.TILEDB_UINT8);
      vcfReader.setBufferValidityBitmapNativeArray(attrName, bitmap);
    }
  }

    /**
     * Creates and returns an empty Arrow vector with the appropriate type for the given field.
     *
     * @param fieldName Name of the Spark field for the column
     * @param typeInfo Type information for the Arrow vector
     * @return Arrow ValueVector
     */
/*  private static ValueVector makeArrowVector(
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
  }*/

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


  /**
   * For a given Spark field name, dispatch between attribute and dimension buffer copying
   *
   * @param field Spark field to copy query result set
   * @return number of values copied into the columnar batch result buffers
   * @throws TileDBError A TileDB exception
   */
  private int getColumnBatch(StructField field, int index) throws TileDBError {
//    String name = field.name();
    String[] attrNames = schema.getVCFAttributes();
    return getAttributeColumn(attrNames[index], index);
  }

  /**
   * For a given attribute name, dispatch between variable length and scalar buffer copying
   *
   * @param name Attribute name
   * @param index The Attribute index in the columnar buffer array
   * @return number of values copied into the columnar batch result buffers
   * @throws TileDBError A TileDB exception
   */
  private int getAttributeColumn(String name, int index) throws TileDBError {
    VCFReader.AttributeTypeInfo info = vcfReader.getAttributeDatatype(name);
      if (info.isVarLen || info.isList) {
        // variable length values added as arrays
        return getVarLengthAttributeColumn(name, info, index);
      } else {
        // one value per cell
        return getScalarValueAttributeColumn(name, info, index);
      }
  }

  private int getScalarValueAttributeColumn(String name, VCFReader.AttributeTypeInfo info, int index)
          throws TileDBError {
    int numValues;
    int bufferLength;
    VCFReader.BufferInfo bufferInfo = vcfReader.getBuffer(name);
    switch (info.datatype) {
      case FLOAT32:
      {
        float[] buff = (float[]) bufferInfo.values.toJavaArray();
        bufferLength = buff.length;
        numValues = bufferLength;
        resultVectors[index].reset();
        resultVectors[index].putFloats(0, bufferLength, buff, 0);
        break;
      }
      case CHAR:
      {
        byte[] buff = (byte[]) bufferInfo.values.toJavaArray();
        bufferLength = buff.length;
        numValues = bufferLength;
        resultVectors[index].reset();
        resultVectors[index].putBytes(0, bufferLength, buff, 0);
        break;
      }
      case UINT8:
      {
        short[] buff = (short[]) bufferInfo.values.toJavaArray();
        bufferLength = buff.length;
        numValues = bufferLength;
        resultVectors[index].reset();
        resultVectors[index].putShorts(0, bufferLength, buff, 0);
        break;
      }
      case INT32:
      {
        int[] buff = (int[]) bufferInfo.values.toJavaArray();
        bufferLength = buff.length;
        numValues = bufferLength;
        resultVectors[index].reset();
        resultVectors[index].putInts(0, bufferLength, buff, 0);
        break;
      }
      default:
      {
        throw new TileDBError("Not supported type " + info.datatype);
      }
    }
    return numValues;
  }

  private int getVarLengthAttributeColumn(String name, VCFReader.AttributeTypeInfo info, int index)
          throws TileDBError {
    int numValues = 0;
    int bufferLength = 0;
    // reset columnar batch start index
    resultVectors[index].reset();
    resultVectors[index].getChild(0).reset();
    VCFReader.BufferInfo bufferInfo = vcfReader.getBuffer(name);
    StructField field = schema.getSparkFields()[index];
    log.info("buffer_name: " + name + ", field_index_name=" + field.name());
    log.info(resultVectors[index]);
    switch (info.datatype) {
      case FLOAT32:
      {
        float[] buff = (float[]) bufferInfo.values.toJavaArray();
        bufferLength = buff.length;
        resultVectors[index].getChild(0).reserve(bufferLength);
        resultVectors[index].getChild(0).putFloats(0, bufferLength, buff, 0);
        break;
      }
      case CHAR:
      {
        int[] listOffsets = (int[]) bufferInfo.listOffsets.toJavaArray();
        int[] offsets = (int[]) bufferInfo.offsets.toJavaArray();
        if (info.isList) {
          resultVectors[index].getChild(0).reserve(bufferLength);
//          resultVectors[index].getChild(0).
            for (int i = 0; i < numRecords; i++) {
              int offset = listOffsets[i];
              int end = listOffsets[i + 1];
              if(offset >= bufferInfo.values.getSize() || end >= bufferInfo.values.getSize() || offset == 2231685 || end == 2231685) {
                log.info(i);
                log.info(offsets[i]);
                log.info(listOffsets[i]);
              }
              byte[] buff = (byte[]) bufferInfo.values.toJavaArray(offset, end-offset);
              resultVectors[index].getChild(0).putByteArray(i, buff);
            }
        } else {
          byte[] buff = (byte[]) bufferInfo.values.toJavaArray();
          bufferLength = buff.length;
          resultVectors[index].getChild(0).reserve(bufferLength);
          resultVectors[index].getChild(0).putBytes(0, bufferLength, buff, 0);
        }
        break;
      }
      case UINT8:
      {
        short[] buff = (short[]) bufferInfo.values.toJavaArray();
        bufferLength = buff.length;
        resultVectors[index].getChild(0).reserve(bufferLength);
        resultVectors[index].getChild(0).putShorts(0, bufferLength, buff, 0);
        break;
      }
      case INT32:
      {
        int[] buff = (int[]) bufferInfo.values.toJavaArray();
        bufferLength = buff.length;
        resultVectors[index].getChild(0).reserve(bufferLength);
          resultVectors[index].getChild(0).putInts(0, bufferLength, buff, 0);
        break;
      }
      default:
      {
        throw new TileDBError("Not supported type " + info.datatype);
      }
    }

    if (info.isNullable) {
      short[] bitmap = (short[])bufferInfo.bitmap.toJavaArray();
        long bits = 0;
        for(int i = 0; i < bitmap.length; i++) {
          short bit = bitmap[i];
          if (bits >= numRecords) {
            break;
          }

          for (int j = 0; j < Integer.SIZE; j++) {
            if (bits >= numRecords) {
              break;
            }
              if ((bit & 0x1) == 1) {
                resultVectors[index].putNull(Math.toIntExact(bits));
              }
            bit >>= 1;
            bits++;
          }
        }
    }

    // TODO: this isList section I don't think is right..
    if (info.isList) {
      int[] offsets = (int[]) bufferInfo.offsets.toJavaArray();
      numValues = offsets.length;
      // number of bytes per (scalar) element in
      int typeSize = info.datatype.toTileDBDatatype().getNativeSize();
      long numBytes = bufferLength * typeSize;
      for (int j = 0; j < numValues; j++) {
        int off1 = Math.toIntExact(offsets[j] / typeSize);
        int off2 = Math.toIntExact((j < numValues - 1 ? offsets[j + 1] : numBytes) / typeSize);
        resultVectors[index].putArray(j, off1, off2 - off1);
//        Long off1 = (offsets[j] / typeSize);
//        Long off2 = ((j < numValues - 1 ? offsets[j + 1] : numBytes) / typeSize);
//        resultVectors[index].putArray(j, off1.intValue(), ((Long)(off2 - off1)).intValue());
      }
    } else {
      // add var length offsets
      int[] offsets = (int[]) bufferInfo.offsets.toJavaArray();
      numValues = offsets.length;
      // number of bytes per (scalar) element in
      int typeSize = info.datatype.toTileDBDatatype().getNativeSize();
      long numBytes = bufferLength * typeSize;
      for (int j = 0; j < numValues; j++) {
        int off1 = Math.toIntExact(offsets[j] / typeSize);
        int off2 = Math.toIntExact((j < numValues - 1 ? offsets[j + 1] : numBytes) / typeSize);
        resultVectors[index].putArray(j, off1, off2 - off1);
//        Long off1 = (offsets[j] / typeSize);
//        Long off2 = ((j < numValues - 1 ? offsets[j + 1] : numBytes) / typeSize);
//        resultVectors[index].putArray(j, off1.intValue(), ((Long)(off2 - off1)).intValue());
      }
    }
//    } else {
//      bufferInfo.listOffsets;
//       fixed sized array attribute
//      int cellNum = (int) attribute.getCellValNum();
//      numValues = bufferLength / cellNum;
//      for (int j = 0; j < numValues; j++) {
//        resultVectors[index].putArray(j, cellNum * j, cellNum);
//      }
//    }
    return numValues;
  }
}
