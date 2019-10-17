package io.tiledb.libvcfnative;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/** This class wraps the low level TileDB-VCF C API with a Java API. */
public class VCFReader implements AutoCloseable {
  private long readerPtr;

  private class BufferInfo {
    public ByteBuffer values;
    public ByteBuffer offsets;
    public ByteBuffer listOffsets;
    public ByteBuffer bitmap;

    public BufferInfo(
        ByteBuffer values, ByteBuffer offsets, ByteBuffer listOffsets, ByteBuffer bitmap) {
      this.values = values;
      this.offsets = offsets;
      this.listOffsets = listOffsets;
      this.bitmap = bitmap;
    }
  }

  private Map<String, BufferInfo> buffers;

  public enum Status {
    FAILED,
    COMPLETED,
    INCOMPLETE,
    UNINITIALIZED
  }

  public enum AttributeDatatype {
    CHAR,
    UINT8,
    INT32,
    FLOAT32
  }

  public class AttributeTypeInfo {
    public AttributeDatatype datatype;
    public boolean isVarLen;
    public boolean isNullable;
    public boolean isList;

    public AttributeTypeInfo(
        AttributeDatatype datatype, boolean isVarLen, boolean isNullable, boolean isList) {
      this.datatype = datatype;
      this.isVarLen = isVarLen;
      this.isNullable = isNullable;
      this.isList = isList;
    }
  }

  public VCFReader(
      String uri, String[] samples, Optional<URI> samplesURI, Optional<String> config) {
    long[] readerPtrArray = new long[1];
    int rc = LibVCFNative.tiledb_vcf_reader_alloc(readerPtrArray);
    if (rc != 0 || readerPtrArray[0] == 0) {
      throw new RuntimeException("Error allocating reader object");
    }

    if (config.isPresent()) {
      rc = LibVCFNative.tiledb_vcf_reader_set_tiledb_config(readerPtrArray[0], config.get());
      if (rc != 0) {
        String msg = getLastErrorMessage();
        LibVCFNative.tiledb_vcf_reader_free(readerPtrArray[0]);
        if (msg == null) {
          msg = "";
        }
        throw new RuntimeException("Error setting TileDB config options on reader object: " + msg);
      }
    }

    rc = LibVCFNative.tiledb_vcf_reader_init(readerPtrArray[0], uri);
    if (rc != 0) {
      String msg = getLastErrorMessage();
      LibVCFNative.tiledb_vcf_reader_free(readerPtrArray[0]);
      if (msg == null) {
        msg = "";
      }
      throw new RuntimeException("Error initializing reader object: " + msg);
    }

    if (samples.length > 0) {
      String samplesCSV = String.join(",", samples);
      rc = LibVCFNative.tiledb_vcf_reader_set_samples(readerPtrArray[0], samplesCSV);
      if (rc != 0) {
        String msg = getLastErrorMessage();
        LibVCFNative.tiledb_vcf_reader_free(readerPtrArray[0]);
        if (msg == null) {
          msg = "";
        }
        throw new RuntimeException("Error setting samples list on reader object: " + msg);
      }
    }

    if (samplesURI.isPresent()) {
      String sampleURIStr = samplesURI.get().toString();
      rc = LibVCFNative.tiledb_vcf_reader_set_samples_file(readerPtrArray[0], sampleURIStr);
      if (rc != 0) {
        String msg = getLastErrorMessage();
        LibVCFNative.tiledb_vcf_reader_free(readerPtrArray[0]);
        if (msg == null) {
          msg = "";
        }
        throw new RuntimeException("Error setting samples file on reader object: " + msg);
      }
    }

    readerPtr = readerPtrArray[0];
    buffers = new HashMap<>();
  }

  private String getLastErrorMessage() {
    String msg = LibVCFNative.tiledb_vcf_reader_get_last_error_message(readerPtr);
    if (msg == null) {
      return "";
    }
    return msg;
  }

  public VCFReader setRanges(String[] ranges) {
    String rangesCSV = String.join(",", ranges);
    int rc = LibVCFNative.tiledb_vcf_reader_set_regions(readerPtr, rangesCSV);
    if (rc != 0) {
      String msg = getLastErrorMessage();
      throw new RuntimeException("Error setting query ranges: " + msg);
    }
    return this;
  }

  public VCFReader setBedFile(String uri) {
    int rc = LibVCFNative.tiledb_vcf_reader_set_bed_file(readerPtr, uri);
    if (rc != 0) {
      String msg = getLastErrorMessage();
      throw new RuntimeException("Error setting query bed file '" + uri + "': " + msg);
    }
    return this;
  }

  public VCFReader setSortRegions(boolean sortRegions) {
    int rc = LibVCFNative.tiledb_vcf_reader_set_sort_regions(readerPtr, sortRegions);
    if (rc != 0) {
      String msg = getLastErrorMessage();
      throw new RuntimeException("Error setting sort regions parameter: " + msg);
    }
    return this;
  }

  public VCFReader setRangePartition(int numPartitions, int partition) {
    int rc =
        LibVCFNative.tiledb_vcf_reader_set_region_partition(readerPtr, partition, numPartitions);
    if (rc != 0) {
      String msg = getLastErrorMessage();
      throw new RuntimeException(
          "Error setting query range partition ("
              + partition
              + " of "
              + numPartitions
              + "): "
              + msg);
    }
    return this;
  }

  public VCFReader setSamplePartition(int numPartitions, int partition) {
    int rc =
        LibVCFNative.tiledb_vcf_reader_set_sample_partition(readerPtr, partition, numPartitions);
    if (rc != 0) {
      String msg = getLastErrorMessage();
      throw new RuntimeException(
          "Error setting query sample partition ("
              + partition
              + " of"
              + numPartitions
              + "): "
              + msg);
    }
    return this;
  }

  public VCFReader setVariantFilters(String csvTypes, boolean include) {
    int rc = LibVCFNative.tiledb_vcf_reader_set_variant_filter(readerPtr, include, csvTypes);
    if (rc != 0) {
      String msg = getLastErrorMessage();
      throw new RuntimeException("Error setting variant filters: " + msg);
    }
    return this;
  }

  public VCFReader setMemoryBudget(Integer mb) {
    if (mb < 1) {
      throw new RuntimeException("memory budget must be >= 1 MB");
    }
    int budget = mb.intValue();
    int rc = LibVCFNative.tiledb_vcf_reader_set_memory_budget(this.readerPtr, budget);
    if (rc != 0) {
      String msg = getLastErrorMessage();
      throw new RuntimeException("Error setting query memory budget: " + msg);
    }
    return this;
  }

  public VCFReader setMaxNumRecords(Integer max_num_records) {
    long n = max_num_records.longValue();
    int rc = LibVCFNative.tiledb_vcf_reader_set_max_num_records(this.readerPtr, n);
    if (rc != 0) {
      throw new RuntimeException("Error setting libtiledbvcf query max num records");
    }
    return this;
  }

  public VCFReader setBuffer(String attribute, java.nio.ByteBuffer buffer) {
    if (!buffer.isDirect()) {
      throw new RuntimeException("Error setting buffer, buffer not a direct ByteBuffer");
    }
    if (buffer.capacity() == 0) {
      throw new RuntimeException("Error setting buffer, buffer has 0 capacity");
    }
    int rc = LibVCFNative.tiledb_vcf_reader_set_buffer_values(this.readerPtr, attribute, buffer);
    if (rc != 0) {
      String msg = getLastErrorMessage();
      throw new RuntimeException("Error setting buffer (attribute: " + attribute + "): " + msg);
    }

    BufferInfo info = buffers.getOrDefault(attribute, new BufferInfo(buffer, null, null, null));
    info.values = buffer;

    return this;
  }

  public VCFReader setBufferOffsets(String attribute, java.nio.ByteBuffer buffer) {
    if (!buffer.isDirect()) {
      throw new RuntimeException("Error setting offsets buffer, buffer not a direct ByteBuffer");
    }
    if (buffer.capacity() == 0) {
      throw new RuntimeException("Error setting offsets buffer, buffer has 0 capacity");
    }
    int rc = LibVCFNative.tiledb_vcf_reader_set_buffer_offsets(this.readerPtr, attribute, buffer);
    if (rc != 0) {
      String msg = getLastErrorMessage();
      throw new RuntimeException(
          "Error setting offsets buffer (attribute: " + attribute + "): " + msg);
    }

    BufferInfo info = buffers.getOrDefault(attribute, new BufferInfo(null, buffer, null, null));
    info.offsets = buffer;

    return this;
  }

  public VCFReader setBufferListOffsets(String attribute, java.nio.ByteBuffer buffer) {
    if (!buffer.isDirect()) {
      throw new RuntimeException(
          "Error setting list offsets buffer, buffer not a direct ByteBuffer");
    }
    if (buffer.capacity() == 0) {
      throw new RuntimeException("Error setting list offsets buffer, buffer has 0 capacity");
    }
    int rc =
        LibVCFNative.tiledb_vcf_reader_set_buffer_list_offsets(this.readerPtr, attribute, buffer);
    if (rc != 0) {
      String msg = getLastErrorMessage();
      throw new RuntimeException(
          "Error setting list offsets buffer (attribute: " + attribute + "): " + msg);
    }

    BufferInfo info = buffers.getOrDefault(attribute, new BufferInfo(null, null, buffer, null));
    info.listOffsets = buffer;

    return this;
  }

  public VCFReader setBufferValidityBitmap(String attribute, java.nio.ByteBuffer buffer) {
    if (!buffer.isDirect()) {
      throw new RuntimeException("Error setting bitmap buffer, buffer not a direct ByteBuffer");
    }
    int rc =
        LibVCFNative.tiledb_vcf_reader_set_buffer_validity_bitmap(
            this.readerPtr, attribute, buffer);
    if (rc != 0) {
      String msg = getLastErrorMessage();
      throw new RuntimeException(
          "Error setting bitmap buffer (attribute: " + attribute + "): " + msg);
    }

    BufferInfo info = buffers.getOrDefault(attribute, new BufferInfo(null, null, null, buffer));
    info.bitmap = buffer;

    return this;
  }

  public void submit() {
    int rc = LibVCFNative.tiledb_vcf_reader_read(this.readerPtr);
    if (rc != 0) {
      String msg = getLastErrorMessage();
      throw new RuntimeException("Error in submission of query: " + msg);
    }
  }

  public VCFReader.Status getStatus() {
    int[] status = new int[1];
    int rc = LibVCFNative.tiledb_vcf_reader_get_status(this.readerPtr, status);
    if (rc != 0) {
      String msg = getLastErrorMessage();
      throw new RuntimeException("Error getting query status: " + msg);
    }
    switch (status[0]) {
      case 0:
        return Status.FAILED;
      case 1:
        return Status.COMPLETED;
      case 2:
        return Status.INCOMPLETE;
      case 3:
        return Status.UNINITIALIZED;
      default:
        throw new RuntimeException("Unknown query status value " + status[0]);
    }
  }

  public long getNumRecords() {
    long[] numRecords = new long[1];
    int rc = LibVCFNative.tiledb_vcf_reader_get_result_num_records(this.readerPtr, numRecords);
    if (rc != 0) {
      String msg = getLastErrorMessage();
      throw new RuntimeException("Error getting number of reader result records: " + msg);
    }
    return numRecords[0];
  }

  public AttributeTypeInfo getAttributeDatatype(String attribute) {
    int[] datatype = new int[1];
    int[] varLen = new int[1];
    int[] nullable = new int[1];
    int[] list = new int[1];
    int rc =
        LibVCFNative.tiledb_vcf_reader_get_attribute_type(
            this.readerPtr, attribute, datatype, varLen, nullable, list);
    if (rc != 0) {
      String msg = getLastErrorMessage();
      throw new RuntimeException("Error getting attribute datatype: " + msg);
    }

    boolean isVarLen = varLen[0] == 1;
    boolean isNullable = nullable[0] == 1;
    boolean isList = list[0] == 1;
    switch (datatype[0]) {
      case 0:
        return new AttributeTypeInfo(AttributeDatatype.CHAR, isVarLen, isNullable, isList);
      case 1:
        return new AttributeTypeInfo(AttributeDatatype.UINT8, isVarLen, isNullable, isList);
      case 2:
        return new AttributeTypeInfo(AttributeDatatype.INT32, isVarLen, isNullable, isList);
      case 3:
        return new AttributeTypeInfo(AttributeDatatype.FLOAT32, isVarLen, isNullable, isList);
      default:
        throw new RuntimeException("Unknown attribute datatype " + datatype[0]);
    }
  }

  public VCFReader resetBuffers() {
    Iterator it = buffers.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry pair = (Map.Entry) it.next();
      BufferInfo info = (BufferInfo) pair.getValue();
      if (info.values != null) info.values.position(0);
      if (info.offsets != null) info.offsets.position(0);
      if (info.listOffsets != null) info.listOffsets.position(0);
      if (info.bitmap != null) info.bitmap.position(0);
    }
    return this;
  }

  @Override
  public void close() {
    // release held references to NIO ByteBuffers
    Iterator it = buffers.entrySet().iterator();
    while (it.hasNext()) {
      it.next();
      it.remove();
    }
    if (readerPtr != 0l) {
      LibVCFNative.tiledb_vcf_reader_free(readerPtr);
    }
    readerPtr = 0l;
  }
}
