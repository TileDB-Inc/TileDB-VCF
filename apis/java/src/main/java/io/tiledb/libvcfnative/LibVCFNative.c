#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "LibVCFNative.h"
#include "tiledbvcf/tiledbvcf.h"

static int set_out_param_int32(JNIEnv* env, int32_t value, jintArray valueOut) {
  jint* c_value = (*env)->GetIntArrayElements(env, valueOut, NULL);
  if (c_value == NULL) {
    return -1;
  }

  // Check that an array of length 1 was passed.
  if ((*env)->GetArrayLength(env, valueOut) != 1) {
    (*env)->ReleaseIntArrayElements(env, valueOut, c_value, 0);
    return -1;
  }

  // Cast value to jint and set result value array
  c_value[0] = (jint)value;
  (*env)->SetIntArrayRegion(env, valueOut, 0, 1, c_value);

  (*env)->ReleaseIntArrayElements(env, valueOut, c_value, 0);

  return TILEDB_VCF_OK;
}

static int set_out_param_int64(
    JNIEnv* env, int64_t value, jlongArray valueOut) {
  jlong* c_value = (*env)->GetLongArrayElements(env, valueOut, NULL);
  if (c_value == NULL) {
    return -1;
  }

  // Check that an array of length 1 was passed.
  if ((*env)->GetArrayLength(env, valueOut) != 1) {
    (*env)->ReleaseLongArrayElements(env, valueOut, c_value, 0);
    return -1;
  }

  // Cast value to jlong and set result value array
  c_value[0] = (jlong)value;
  (*env)->SetLongArrayRegion(env, valueOut, 0, 1, c_value);

  (*env)->ReleaseLongArrayElements(env, valueOut, c_value, 0);

  return TILEDB_VCF_OK;
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1alloc(
    JNIEnv* env, jclass self, jlongArray readerPtrOut) {
  (void)self;

  // Check that the passed in array is not null
  jlong* c_query = (*env)->GetLongArrayElements(env, readerPtrOut, NULL);
  if (c_query == NULL) {
    return -1;
  }
  if ((*env)->GetArrayLength(env, readerPtrOut) != 1) {
    (*env)->ReleaseLongArrayElements(env, readerPtrOut, c_query, 0);
    return -1;
  }

  // Allocate a new reader
  tiledb_vcf_reader_t* reader_ptr = NULL;
  int rc = tiledb_vcf_reader_alloc(&reader_ptr);
  if (rc != TILEDB_VCF_OK || reader_ptr == NULL) {
    (*env)->ReleaseLongArrayElements(env, readerPtrOut, c_query, 0);
    return rc;
  }

  // Store the address of the reader struct
  c_query[0] = (jlong)reader_ptr;
  (*env)->SetLongArrayRegion(env, readerPtrOut, 0, 1, c_query);

  // Un-pin memory region
  (*env)->ReleaseLongArrayElements(env, readerPtrOut, c_query, 0);

  return rc;
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1free(
    JNIEnv* env, jclass self, jlong readerPtr) {
  (void)self;

  if (readerPtr == 0) {
    return 0;
  }

  tiledb_vcf_reader_t* reader_ptr = (tiledb_vcf_reader_t*)readerPtr;
  tiledb_vcf_reader_free(&reader_ptr);

  return 0;
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1init(
    JNIEnv* env, jclass self, jlong readerPtr, jstring datasetUri) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return TILEDB_VCF_ERR;
  }

  const char* c_uri = (*env)->GetStringUTFChars(env, datasetUri, 0);
  if (c_uri == NULL) {
    return TILEDB_VCF_ERR;
  }

  int rc = tiledb_vcf_reader_init(reader, c_uri);
  (*env)->ReleaseStringUTFChars(env, datasetUri, c_uri);
  return rc;
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1set_1samples_1file(
    JNIEnv* env, jclass self, jlong readerPtr, jstring uri) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return TILEDB_VCF_ERR;
  }

  const char* c_uri = (*env)->GetStringUTFChars(env, uri, 0);
  if (c_uri == NULL) {
    return TILEDB_VCF_ERR;
  }

  int rc = tiledb_vcf_reader_set_samples_file(reader, c_uri);
  (*env)->ReleaseStringUTFChars(env, uri, c_uri);
  return rc;
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1set_1bed_1file(
    JNIEnv* env, jclass self, jlong readerPtr, jstring uri) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return TILEDB_VCF_ERR;
  }

  const char* c_uri = (*env)->GetStringUTFChars(env, uri, 0);
  if (c_uri == NULL) {
    return TILEDB_VCF_ERR;
  }

  int rc = tiledb_vcf_reader_set_bed_file(reader, c_uri);
  (*env)->ReleaseStringUTFChars(env, uri, c_uri);
  return rc;
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1set_1samples(
    JNIEnv* env, jclass self, jlong readerPtr, jstring samples) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return TILEDB_VCF_ERR;
  }

  const char* c_samples = (*env)->GetStringUTFChars(env, samples, 0);
  if (c_samples == NULL) {
    return TILEDB_VCF_ERR;
  }

  int rc = tiledb_vcf_reader_set_samples(reader, c_samples);
  (*env)->ReleaseStringUTFChars(env, samples, c_samples);
  return rc;
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1set_1regions(
    JNIEnv* env, jclass self, jlong readerPtr, jstring regions) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return TILEDB_VCF_ERR;
  }

  const char* c_regions = (*env)->GetStringUTFChars(env, regions, 0);
  if (c_regions == NULL) {
    return TILEDB_VCF_ERR;
  }

  int rc = tiledb_vcf_reader_set_regions(reader, c_regions);
  (*env)->ReleaseStringUTFChars(env, regions, c_regions);
  return rc;
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1set_1sort_1regions(
    JNIEnv* env, jclass self, jlong readerPtr, jboolean shouldSort) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return TILEDB_VCF_ERR;
  }

  int32_t sort = shouldSort ? 1 : 0;
  return tiledb_vcf_reader_set_sort_regions(reader, sort);
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1set_1region_1partition(
    JNIEnv* env,
    jclass self,
    jlong readerPtr,
    jint partition,
    jint numPartitions) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return TILEDB_VCF_ERR;
  }

  return tiledb_vcf_reader_set_region_partition(
      reader, partition, numPartitions);
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1set_1sample_1partition(
    JNIEnv* env,
    jclass self,
    jlong readerPtr,
    jint partition,
    jint numPartitions) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return TILEDB_VCF_ERR;
  }

  return tiledb_vcf_reader_set_sample_partition(
      reader, partition, numPartitions);
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1set_1buffer_1values(
    JNIEnv* env,
    jclass self,
    jlong readerPtr,
    jstring attribute,
    jobject buffer) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return TILEDB_VCF_ERR;
  }

  jlong buffer_size = (*env)->GetDirectBufferCapacity(env, buffer);
  if (buffer_size == -1) {
    return -1;
  }
  int64_t c_buffer_size = (int64_t)buffer_size;
  void* c_buffer = (*env)->GetDirectBufferAddress(env, buffer);

  const char* c_attribute = (*env)->GetStringUTFChars(env, attribute, 0);
  int rc = tiledb_vcf_reader_set_buffer_values(
      reader, c_attribute, c_buffer_size, c_buffer);
  (*env)->ReleaseStringUTFChars(env, attribute, c_attribute);

  return rc;
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1set_1buffer_1offsets(
    JNIEnv* env,
    jclass self,
    jlong readerPtr,
    jstring attribute,
    jobject buffer) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return TILEDB_VCF_ERR;
  }

  jlong buffer_size = (*env)->GetDirectBufferCapacity(env, buffer);
  if (buffer_size == -1) {
    return -1;
  }
  int64_t c_buffer_size = (int64_t)buffer_size;
  int32_t* c_buffer = (int32_t*)(*env)->GetDirectBufferAddress(env, buffer);

  const char* c_attribute = (*env)->GetStringUTFChars(env, attribute, 0);
  int rc = tiledb_vcf_reader_set_buffer_offsets(
      reader, c_attribute, c_buffer_size, c_buffer);
  (*env)->ReleaseStringUTFChars(env, attribute, c_attribute);

  return rc;
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1set_1buffer_1list_1offsets(
    JNIEnv* env,
    jclass self,
    jlong readerPtr,
    jstring attribute,
    jobject buffer) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return TILEDB_VCF_ERR;
  }

  jlong buffer_size = (*env)->GetDirectBufferCapacity(env, buffer);
  if (buffer_size == -1) {
    return -1;
  }
  int64_t c_buffer_size = (int64_t)buffer_size;
  int32_t* c_buffer = (int32_t*)(*env)->GetDirectBufferAddress(env, buffer);

  const char* c_attribute = (*env)->GetStringUTFChars(env, attribute, 0);
  int rc = tiledb_vcf_reader_set_buffer_list_offsets(
      reader, c_attribute, c_buffer_size, c_buffer);
  (*env)->ReleaseStringUTFChars(env, attribute, c_attribute);

  return rc;
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1set_1buffer_1validity_1bitmap(
    JNIEnv* env,
    jclass self,
    jlong readerPtr,
    jstring attribute,
    jobject bitmap) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return TILEDB_VCF_ERR;
  }

  jlong buffer_size = (*env)->GetDirectBufferCapacity(env, bitmap);
  if (buffer_size == -1) {
    return -1;
  }
  int64_t c_buffer_size = (int64_t)buffer_size;
  uint8_t* c_buffer = (uint8_t*)((*env)->GetDirectBufferAddress(env, bitmap));

  const char* c_attribute = (*env)->GetStringUTFChars(env, attribute, 0);
  int rc = tiledb_vcf_reader_set_buffer_validity_bitmap(
      reader, c_attribute, c_buffer_size, c_buffer);
  (*env)->ReleaseStringUTFChars(env, attribute, c_attribute);

  return rc;
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1set_1memory_1budget(
    JNIEnv* env, jclass self, jlong readerPtr, jint memoryBudget) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return TILEDB_VCF_ERR;
  }

  return tiledb_vcf_reader_set_memory_budget(reader, memoryBudget);
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1set_1max_1num_1records(
    JNIEnv* env, jclass self, jlong readerPtr, jlong maxNumRecords) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return TILEDB_VCF_ERR;
  }

  return tiledb_vcf_reader_set_max_num_records(reader, maxNumRecords);
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1set_1tiledb_1config(
    JNIEnv* env, jclass self, jlong readerPtr, jstring config) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return TILEDB_VCF_ERR;
  }

  const char* c_config = (*env)->GetStringUTFChars(env, config, 0);
  if (c_config == NULL) {
    return TILEDB_VCF_ERR;
  }

  int rc = tiledb_vcf_reader_set_tiledb_config(reader, c_config);
  (*env)->ReleaseStringUTFChars(env, config, c_config);
  return rc;
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1read(
    JNIEnv* env, jclass self, jlong readerPtr) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return TILEDB_VCF_ERR;
  }

  return tiledb_vcf_reader_read(reader);
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1get_1status(
    JNIEnv* env, jclass self, jlong readerPtr, jintArray statusOut) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return TILEDB_VCF_ERR;
  }

  tiledb_vcf_read_status_t status = TILEDB_VCF_FAILED;
  int32_t rc = tiledb_vcf_reader_get_status(reader, &status);
  if (rc == TILEDB_VCF_OK) {
    return set_out_param_int32(env, (int32_t)status, statusOut);
  }

  return rc;
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1get_1result_1num_1records(
    JNIEnv* env, jclass self, jlong readerPtr, jlongArray numResultsOut) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return TILEDB_VCF_ERR;
  }

  int64_t num_results = 0;
  int32_t rc = tiledb_vcf_reader_get_result_num_records(reader, &num_results);
  if (rc == TILEDB_VCF_OK) {
    return set_out_param_int64(env, num_results, numResultsOut);
  }

  return rc;
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1get_1result_1size(
    JNIEnv* env,
    jclass self,
    jlong readerPtr,
    jstring attribute,
    jlongArray numOffsetsOut,
    jlongArray numDataElementsOut,
    jlongArray numDataBytesOut) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return TILEDB_VCF_ERR;
  }

  const char* c_attribute = (*env)->GetStringUTFChars(env, attribute, 0);
  if (c_attribute == NULL) {
    return TILEDB_VCF_ERR;
  }

  int64_t num_offsets = 0, num_data_elts = 0, num_data_bytes = 0;
  int32_t rc = tiledb_vcf_reader_get_result_size(
      reader, c_attribute, &num_offsets, &num_data_elts, &num_data_bytes);
  (*env)->ReleaseStringUTFChars(env, attribute, c_attribute);
  if (rc == TILEDB_VCF_OK) {
    if (set_out_param_int64(env, num_offsets, numOffsetsOut) != TILEDB_VCF_OK)
      return TILEDB_VCF_ERR;
    if (set_out_param_int64(env, num_data_elts, numDataElementsOut) !=
        TILEDB_VCF_OK)
      return TILEDB_VCF_ERR;
    if (set_out_param_int64(env, num_data_bytes, numDataBytesOut) !=
        TILEDB_VCF_OK)
      return TILEDB_VCF_ERR;
  }

  return rc;
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1get_1attribute_1type(
    JNIEnv* env,
    jclass self,
    jlong readerPtr,
    jstring attribute,
    jintArray datatypeOut,
    jintArray varLenOut,
    jintArray nullableOut,
    jintArray isListOut) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return TILEDB_VCF_ERR;
  }

  const char* c_attribute = (*env)->GetStringUTFChars(env, attribute, 0);
  if (c_attribute == NULL) {
    return TILEDB_VCF_ERR;
  }

  tiledb_vcf_attr_datatype_t datatype;
  int32_t var_len = 0, nullable = 0, is_list = 0;
  int32_t rc = tiledb_vcf_reader_get_attribute_type(
      reader, c_attribute, &datatype, &var_len, &nullable, &is_list);
  (*env)->ReleaseStringUTFChars(env, attribute, c_attribute);
  if (rc == TILEDB_VCF_OK) {
    if (set_out_param_int32(env, datatype, datatypeOut) != TILEDB_VCF_OK)
      return TILEDB_VCF_ERR;
    if (set_out_param_int32(env, var_len, varLenOut) != TILEDB_VCF_OK)
      return TILEDB_VCF_ERR;
    if (set_out_param_int32(env, nullable, nullableOut) != TILEDB_VCF_OK)
      return TILEDB_VCF_ERR;
    if (set_out_param_int32(env, is_list, isListOut) != TILEDB_VCF_OK)
      return TILEDB_VCF_ERR;
  }

  return rc;
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1get_1attribute_1count(
    JNIEnv* env, jclass self, jlong readerPtr, jintArray countOut) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return TILEDB_VCF_ERR;
  }

  int32_t count;
  int32_t rc = tiledb_vcf_reader_get_queryable_attribute_count(reader, &count);
  if (rc == TILEDB_VCF_OK) {
    return set_out_param_int32(env, count, countOut);
  }

  return rc;
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1get_1attribute_1name(
    JNIEnv* env, jclass self, jlong readerPtr, jint index, jbyteArray nameOut) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return TILEDB_VCF_ERR;
  }

  char* buf;
  int32_t rc =
      tiledb_vcf_reader_get_queryable_attribute_name(reader, index, &buf);
  if (rc == TILEDB_VCF_OK) {
    int length = strlen(buf);
    (*env)->SetByteArrayRegion(env, nameOut, 0, length, buf);
  }

  return rc;
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1get_1materialized_1attribute_1count(
    JNIEnv* env, jclass self, jlong readerPtr, jintArray countOut) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return TILEDB_VCF_ERR;
  }

  int32_t count;
  int32_t rc =
      tiledb_vcf_reader_get_materialized_attribute_count(reader, &count);
  if (rc == TILEDB_VCF_OK) {
    return set_out_param_int32(env, count, countOut);
  }

  return rc;
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1get_1materialized_1attribute_1name(
    JNIEnv* env, jclass self, jlong readerPtr, jint index, jbyteArray nameOut) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return TILEDB_VCF_ERR;
  }

  char* buf;
  int32_t rc =
      tiledb_vcf_reader_get_materialized_attribute_name(reader, index, &buf);
  if (rc == TILEDB_VCF_OK) {
    int length = strlen(buf);
    (*env)->SetByteArrayRegion(env, nameOut, 0, length, buf);
  }

  return rc;
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1get_1fmt_1attribute_1count(
    JNIEnv* env, jclass self, jlong readerPtr, jintArray countOut) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return TILEDB_VCF_ERR;
  }

  int32_t count;
  int32_t rc = tiledb_vcf_reader_get_fmt_attribute_count(reader, &count);
  if (rc == TILEDB_VCF_OK) {
    return set_out_param_int32(env, count, countOut);
  }

  return rc;
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1get_1fmt_1attribute_1name(
    JNIEnv* env, jclass self, jlong readerPtr, jint index, jbyteArray nameOut) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return TILEDB_VCF_ERR;
  }

  char* buf;
  int32_t rc = tiledb_vcf_reader_get_fmt_attribute_name(reader, index, &buf);
  if (rc == TILEDB_VCF_OK) {
    int length = strlen(buf);
    (*env)->SetByteArrayRegion(env, nameOut, 0, length, buf);
  }

  return rc;
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1get_1info_1attribute_1count(
    JNIEnv* env, jclass self, jlong readerPtr, jintArray countOut) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return TILEDB_VCF_ERR;
  }

  int32_t count;
  int32_t rc = tiledb_vcf_reader_get_info_attribute_count(reader, &count);
  if (rc == TILEDB_VCF_OK) {
    return set_out_param_int32(env, count, countOut);
  }

  return rc;
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1get_1info_1attribute_1name(
    JNIEnv* env, jclass self, jlong readerPtr, jint index, jbyteArray nameOut) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return TILEDB_VCF_ERR;
  }

  char* buf;
  int32_t rc = tiledb_vcf_reader_get_info_attribute_name(reader, index, &buf);
  if (rc == TILEDB_VCF_OK) {
    int length = strlen(buf);
    (*env)->SetByteArrayRegion(env, nameOut, 0, length, buf);
  }

  return rc;
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1get_1dataset_1version(
    JNIEnv* env, jclass self, jlong readerPtr, jintArray versionOut) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return TILEDB_VCF_ERR;
  }

  int32_t version = 0;
  int32_t rc = tiledb_vcf_reader_get_dataset_version(reader, &version);
  if (rc == TILEDB_VCF_OK) {
    return set_out_param_int32(env, version, versionOut);
  }

  return rc;
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1set_1verbose(
    JNIEnv* env, jclass self, jlong readerPtr, jboolean verbose) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return TILEDB_VCF_ERR;
  }

  return tiledb_vcf_reader_set_verbose(reader, verbose);
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1set_1buffer_1percentage(
    JNIEnv* env, jclass self, jlong readerPtr, jfloat buffer_percentage) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return TILEDB_VCF_ERR;
  }

  return tiledb_vcf_reader_set_buffer_percentage(reader, buffer_percentage);
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1set_1tiledb_1tile_1cache_1percentage(
    JNIEnv* env, jclass self, jlong readerPtr, jfloat tile_cache_percentage) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return TILEDB_VCF_ERR;
  }

  return tiledb_vcf_reader_set_tiledb_tile_cache_percentage(
      reader, tile_cache_percentage);
}

JNIEXPORT jstring JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1get_1last_1error_1message(
    JNIEnv* env, jclass self, jlong readerPtr) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return NULL;
  }

  const char* msg = "Error getting error message via JNI.";
  tiledb_vcf_error_t* error = NULL;
  int32_t rc = tiledb_vcf_reader_get_last_error(reader, &error);
  if (rc == TILEDB_VCF_OK) {
    tiledb_vcf_error_get_message(error, &msg);
  } else {
    error = NULL;
  }

  jstring result = (*env)->NewStringUTF(env, msg);
  tiledb_vcf_error_free(&error);
  return result;
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1set_1tiledb_1stats_1enabled(
    JNIEnv* env, jclass self, jlong readerPtr, jboolean statsEnabled) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return TILEDB_VCF_ERR;
  }

  const bool stats_enabled = statsEnabled ? true : false;

  int32_t rc =
      tiledb_vcf_reader_set_tiledb_stats_enabled(reader, stats_enabled);

  return rc;
}

JNIEXPORT jboolean JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1tiledb_1stats_1enabled(
    JNIEnv* env, jclass self, jlong readerPtr) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return TILEDB_VCF_ERR;
  }

  bool stats_enabled;

  int rc = tiledb_vcf_reader_get_tiledb_stats_enabled(reader, &stats_enabled);

  return stats_enabled;
}

JNIEXPORT jstring JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1tiledb_1stats(
    JNIEnv* env, jclass self, jlong readerPtr) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return NULL;
  }

  char* stats;
  tiledb_vcf_reader_get_tiledb_stats(reader, &stats);

  jstring result = (*env)->NewStringUTF(env, stats);
  return result;
}

JNIEXPORT jstring JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1version(
    JNIEnv* env, jclass self) {
  (void)self;

  const char* version;
  tiledb_vcf_version(&version);

  jstring result = (*env)->NewStringUTF(env, version);
  return result;
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1bed_1file_1alloc(
    JNIEnv* env, jclass self, jlongArray bedFilePtrOut) {
  (void)self;

  // Check that the passed in array is not null
  jlong* c_query = (*env)->GetLongArrayElements(env, bedFilePtrOut, NULL);
  if (c_query == NULL) {
    return -1;
  }
  if ((*env)->GetArrayLength(env, bedFilePtrOut) != 1) {
    (*env)->ReleaseLongArrayElements(env, bedFilePtrOut, c_query, 0);
    return -1;
  }

  // Allocate a new bed_file
  tiledb_vcf_bed_file_t* bed_file_ptr = NULL;
  int rc = tiledb_vcf_bed_file_alloc(&bed_file_ptr);
  if (rc != TILEDB_VCF_OK || bed_file_ptr == NULL) {
    (*env)->ReleaseLongArrayElements(env, bedFilePtrOut, c_query, 0);
    return rc;
  }

  // Store the address of the bed_file struct
  c_query[0] = (jlong)bed_file_ptr;
  (*env)->SetLongArrayRegion(env, bedFilePtrOut, 0, 1, c_query);

  // Un-pin memory region
  (*env)->ReleaseLongArrayElements(env, bedFilePtrOut, c_query, 0);

  return rc;
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1bed_1file_1free(
    JNIEnv* env, jclass self, jlong bedFilePtr) {
  (void)self;

  if (bedFilePtr == 0) {
    return 0;
  }

  tiledb_vcf_bed_file_t* bed_file_ptr = (tiledb_vcf_bed_file_t*)bedFilePtr;
  tiledb_vcf_bed_file_free(&bed_file_ptr);

  return 0;
}

JNIEXPORT jstring JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1bed_1file_1get_1last_1error_1message(
    JNIEnv* env, jclass self, jlong bedFilePtr) {
  (void)self;
  tiledb_vcf_bed_file_t* bed_file = (tiledb_vcf_bed_file_t*)bedFilePtr;
  if (bed_file == 0) {
    return NULL;
  }

  const char* msg = "Error getting error message via JNI.";
  tiledb_vcf_error_t* error = NULL;
  int32_t rc = tiledb_vcf_bed_file_get_last_error(bed_file, &error);
  if (rc == TILEDB_VCF_OK) {
    tiledb_vcf_error_get_message(error, &msg);
  } else {
    error = NULL;
  }

  jstring result = (*env)->NewStringUTF(env, msg);
  tiledb_vcf_error_free(&error);
  return result;
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1set_1enable_1progress_1estimation(
    JNIEnv* env,
    jclass self,
    jlong readerPtr,
    jboolean enableProgressEstimation) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return TILEDB_VCF_ERR;
  }

  int32_t rc = tiledb_vcf_reader_set_enable_progress_estimation(
      reader, enableProgressEstimation);

  return rc;
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1set_1debug_1print_1vcf_1regions(
    JNIEnv* env, jclass self, jlong readerPtr, jboolean printVCFRegions) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return TILEDB_VCF_ERR;
  }

  int32_t rc =
      tiledb_vcf_reader_set_debug_print_vcf_regions(reader, printVCFRegions);

  return rc;
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1bed_1file_1parse(
    JNIEnv* env,
    jclass self,
    jlong readerPtr,
    jlong bedFilePtr,
    jstring bedFileURI) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return TILEDB_VCF_ERR;
  }
  tiledb_vcf_bed_file_t* bed_file = (tiledb_vcf_bed_file_t*)bedFilePtr;
  if (bed_file == 0) {
    return TILEDB_VCF_ERR;
  }

  const char* c_uri = (*env)->GetStringUTFChars(env, bedFileURI, 0);
  if (c_uri == NULL) {
    return TILEDB_VCF_ERR;
  }

  int rc = tiledb_vcf_bed_file_parse(reader, bed_file, c_uri);
  (*env)->ReleaseStringUTFChars(env, bedFileURI, c_uri);
  return rc;
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1bed_1file_1get_1contig_1count(
    JNIEnv* env, jclass self, jlong bedFilePtr, jlongArray countOut) {
  (void)self;
  tiledb_vcf_bed_file_t* bed_file = (tiledb_vcf_bed_file_t*)bedFilePtr;
  if (bed_file == 0) {
    return TILEDB_VCF_ERR;
  }

  uint64_t count;
  int32_t rc = tiledb_vcf_bed_file_get_contig_count(bed_file, &count);
  if (rc == TILEDB_VCF_OK) {
    return set_out_param_int64(env, count, countOut);
  }

  return rc;
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1set_1debug_1print_1sample_1list(
    JNIEnv* env, jclass self, jlong readerPtr, jboolean printSampleList) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return TILEDB_VCF_ERR;
  }

  int32_t rc =
      tiledb_vcf_reader_set_debug_print_sample_list(reader, printSampleList);

  return rc;
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1bed_1file_1get_1total_1region_1count(
    JNIEnv* env, jclass self, jlong bedFilePtr, jlongArray countOut) {
  (void)self;
  tiledb_vcf_bed_file_t* bed_file = (tiledb_vcf_bed_file_t*)bedFilePtr;
  if (bed_file == 0) {
    return TILEDB_VCF_ERR;
  }

  uint64_t count;
  int32_t rc = tiledb_vcf_bed_file_get_total_region_count(bed_file, &count);
  if (rc == TILEDB_VCF_OK) {
    return set_out_param_int64(env, count, countOut);
  }

  return rc;
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1reader_1set_1debug_1print_1tiledb_1query_1ranges(
    JNIEnv* env,
    jclass self,
    jlong readerPtr,
    jboolean printTileDBQueryRanges) {
  (void)self;
  tiledb_vcf_reader_t* reader = (tiledb_vcf_reader_t*)readerPtr;
  if (reader == 0) {
    return TILEDB_VCF_ERR;
  }

  int32_t rc = tiledb_vcf_reader_set_debug_print_tiledb_query_ranges(
      reader, printTileDBQueryRanges);

  return rc;
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1bed_1file_1get_1contig_1region_1count(
    JNIEnv* env,
    jclass self,
    jlong bedFilePtr,
    jlong contigIndex,
    jlongArray countOut) {
  (void)self;
  tiledb_vcf_bed_file_t* bed_file = (tiledb_vcf_bed_file_t*)bedFilePtr;
  if (bed_file == 0) {
    return TILEDB_VCF_ERR;
  }

  uint64_t count;
  int32_t rc = tiledb_vcf_bed_file_get_contig_region_count(
      bed_file, contigIndex, &count);
  if (rc == TILEDB_VCF_OK) {
    return set_out_param_int64(env, count, countOut);
  }

  return rc;
}

JNIEXPORT jint JNICALL
Java_io_tiledb_libvcfnative_LibVCFNative_tiledb_1vcf_1bed_1file_1get_1contig_1region(
    JNIEnv* env,
    jclass self,
    jlong bedFilePtr,
    jlong contigIndex,
    jlong regionIndex,
    jbyteArray regionStrOut,
    jbyteArray regionContigOut,
    jlongArray regionStartOut,
    jlongArray regionEndOut) {
  (void)self;
  tiledb_vcf_bed_file_t* bed_file = (tiledb_vcf_bed_file_t*)bedFilePtr;
  if (bed_file == 0) {
    return TILEDB_VCF_ERR;
  }

  const char* region_str;
  const char* region_contig;
  uint32_t region_start, region_end;
  int32_t rc = tiledb_vcf_bed_file_get_contig_region(
      bed_file,
      contigIndex,
      regionIndex,
      &region_str,
      &region_contig,
      &region_start,
      &region_end);
  if (rc == TILEDB_VCF_OK) {
    int length = strlen(region_str) + 1;  // add 1 to copy null terminator
    (*env)->SetByteArrayRegion(env, regionStrOut, 0, length, region_str);

    int length2 = strlen(region_contig) + 1;  // add 1 to copy null terminator
    (*env)->SetByteArrayRegion(env, regionContigOut, 0, length2, region_contig);

    // Upcast to 64bit because java doesn't have unsigned 32bit ints
    int64_t region_start_long = region_start;
    rc = set_out_param_int64(env, region_start_long, regionStartOut);
    if (rc != 0)
      return rc;

    // Upcast to 64bit because java doesn't have unsigned 32bit ints
    int64_t region_end_long = region_end;
    rc = set_out_param_int64(env, region_end_long, regionEndOut);
    if (rc != 0)
      return rc;
  }

  return rc;
}
