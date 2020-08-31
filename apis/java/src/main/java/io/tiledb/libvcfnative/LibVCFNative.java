package io.tiledb.libvcfnative;

import static io.tiledb.libvcfnative.NativeLibLoader.loadNativeHTSLib;
import static io.tiledb.libvcfnative.NativeLibLoader.loadNativeTileDB;
import static io.tiledb.libvcfnative.NativeLibLoader.loadNativeTileDBVCF;
import static io.tiledb.libvcfnative.NativeLibLoader.loadNativeTileDBVCFJNI;

import java.nio.ByteBuffer;

public class LibVCFNative {

  static {
    try {
      // Load native libraries in order
      loadNativeTileDB();
      loadNativeHTSLib();
      loadNativeTileDBVCF();
      loadNativeTileDBVCFJNI();
    } catch (Exception e) {
      System.err.println("Native code library failed to load. \n");
      e.printStackTrace();
      System.exit(1);
    }
  }

  public static final native int tiledb_vcf_reader_alloc(long[] readerPtr);

  public static final native int tiledb_vcf_reader_free(long readerPtr);

  public static final native int tiledb_vcf_reader_init(long readerPtr, String datasetUri);

  public static final native int tiledb_vcf_reader_set_samples_file(long readerPtr, String uri);

  public static final native int tiledb_vcf_reader_set_bed_file(long readerPtr, String uri);

  public static final native int tiledb_vcf_reader_set_samples(long readerPtr, String samplesCSV);

  public static final native int tiledb_vcf_reader_set_regions(long readerPtr, String regionsCSV);

  public static final native int tiledb_vcf_reader_set_sort_regions(
      long readerPtr, boolean shouldSort);

  public static final native int tiledb_vcf_reader_set_region_partition(
      long readerPtr, int partition, int numPartitions);

  public static final native int tiledb_vcf_reader_set_sample_partition(
      long readerPtr, int partition, int numPartitions);

  public static final native int tiledb_vcf_reader_set_buffer_values(
      long queryPtr, String attribute, ByteBuffer buffer);

  public static final native int tiledb_vcf_reader_set_buffer_offsets(
      long queryPtr, String attribute, ByteBuffer buffer);

  public static final native int tiledb_vcf_reader_set_buffer_list_offsets(
      long queryPtr, String attribute, ByteBuffer buffer);

  public static final native int tiledb_vcf_reader_set_buffer_validity_bitmap(
      long queryPtr, String attribute, ByteBuffer buffer);

  public static final native int tiledb_vcf_reader_set_memory_budget(long readerPtr, int memoryMB);

  public static final native int tiledb_vcf_reader_set_max_num_records(
      long readerPtr, long maxNumRecords);

  public static final native int tiledb_vcf_reader_set_tiledb_config(
      long readerPtr, String configCSV);

  public static final native int tiledb_vcf_reader_read(long readerPtr);

  public static final native int tiledb_vcf_reader_get_status(long readerPtr, int[] status);

  public static final native int tiledb_vcf_reader_get_result_num_records(
      long readerPtr, long[] numRecords);

  public static final native int tiledb_vcf_reader_get_result_size(
      long readerPtr,
      String attribute,
      long[] numOffsets,
      long[] numDataElements,
      long[] numDataBytes);

  public static final native int tiledb_vcf_reader_get_attribute_type(
      long readerPtr, String attribute, int[] datatype, int[] varLen, int[] nullable, int[] isList);

  public static final native int tiledb_vcf_reader_get_attribute_count(long readerPtr, int[] count);

  public static final native int tiledb_vcf_reader_get_attribute_name(
      long readerPtr, int index, byte[] name);

  public static final native int tiledb_vcf_reader_get_fmt_attribute_count(
      long readerPtr, int[] count);

  public static final native int tiledb_vcf_reader_get_fmt_attribute_name(
      long readerPtr, int index, byte[] name);

  public static final native int tiledb_vcf_reader_get_info_attribute_count(
      long readerPtr, int[] count);

  public static final native int tiledb_vcf_reader_get_info_attribute_name(
      long readerPtr, int index, byte[] name);

  public static final native int tiledb_vcf_reader_get_dataset_version(
      long readerPtr, int[] version);

  public static final native int tiledb_vcf_reader_set_verbose(long readerPtr, boolean verbose);

  public static final native String tiledb_vcf_reader_get_last_error_message(long readerPtr);

  public static final native int tiledb_vcf_reader_set_tiledb_stats_enabled(
      long readerPtr, boolean statsEnabled);

  public static final native boolean tiledb_vcf_reader_tiledb_stats_enabled(long readerPtr);

  public static final native String tiledb_vcf_reader_tiledb_stats(long readerPtr);
}
