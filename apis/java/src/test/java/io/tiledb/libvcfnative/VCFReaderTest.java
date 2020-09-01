package io.tiledb.libvcfnative;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

public class VCFReaderTest {
  private int TOTAL_EXPECTED_RECORDS = 14;
  private int BED_FILE_EXPECTED_RECORDS = 10;
  private int SINGLE_SAMPLE_EXPECTED_RECORDS = 3;

  /**
   * Given a file name constructs a URI
   *
   * @param fileName The file name
   * @return The resulting URI
   */
  private String constructUri(String fileName) {
    Path path = Paths.get("src", "test", "resources", "arrays", "v3", fileName);
    return path.toAbsolutePath().toString();
  }

  /**
   * Constructs the .bed file URI
   *
   * @return The .bed file URI
   */
  private String constructBEDURI() {
    Path path = Paths.get("src", "test", "resources", "simple.bed");
    return path.toAbsolutePath().toString();
  }

  /**
   * Returns an array of the samples read from the sample_names.txt
   *
   * @return A String array of sample names
   * @throws IOException
   */
  private String[] getSamples() throws IOException {
    Path path = Paths.get("src", "test", "resources", "sample_names.txt");
    Object[] lines =
        FileUtils.readLines(new File(path.toAbsolutePath().toString()), "UTF8").toArray();
    String[] samples = new String[lines.length];

    for (int i = 0; i < lines.length; ++i) samples[i] = (String) lines[i];

    return samples;
  }

  /**
   * Returns a VCFReader instance
   *
   * @param inputSamples Optional. An input samples array
   * @return The VCFReader instance
   * @throws IOException
   */
  private VCFReader getVFCReader(Optional<String[]> inputSamples, Optional<String> bedFile)
      throws IOException {
    String samples[] = inputSamples.orElse(getSamples());

    VCFReader reader =
        new VCFReader(
            constructUri("ingested_2samples"), samples, Optional.empty(), Optional.empty());

    if (bedFile.isPresent()) reader.setBedFile(bedFile.get());

    return reader;
  }

  /**
   * Tests if the read with the default settings completes
   *
   * @throws IOException
   */
  @Test
  public void testReadCompletes() throws IOException {
    VCFReader reader = getVFCReader(Optional.empty(), Optional.empty());

    int results = 0;

    reader.submit();
    results += reader.getNumRecords();

    while (reader.getStatus().equals(VCFReader.Status.INCOMPLETE)) reader.submit();

    Assert.assertEquals(results, TOTAL_EXPECTED_RECORDS);
  }

  /**
   * Tests validity of the reader values
   *
   * @throws IOException
   */
  @Test
  public void testReaderValues() throws IOException {
    VCFReader reader = getVFCReader(Optional.empty(), Optional.empty());

    reader.setRanges("1:12100-13360,1:13500-17350".split(","));

    String[] attributes =
        new String[] {
          "info",
          "fmt",
          "pos_start",
          "pos_end",
          "sample_name",
          "fmt_MIN_DP",
          "fmt_SB",
          "fmt_AD",
          "fmt_GQ",
          "fmt_GT",
          "fmt_PL",
          "fmt_DP",
          "info_END",
          "info_DP",
          "info_ReadPosRankSum",
          "info_ClippingRankSum"
        };
    // Set the buffers
    for (String key : attributes) {
      reader.setBuffer(key, ByteBuffer.allocateDirect(1024).order(ByteOrder.nativeOrder()));
    }

    reader.setBufferOffsets(
        "sample_name", ByteBuffer.allocateDirect(1024).order(ByteOrder.nativeOrder()));

    reader.setBufferOffsets(
        "info_END", ByteBuffer.allocateDirect(1024).order(ByteOrder.nativeOrder()));

    reader.setBufferValidityBitmap(
        "fmt", ByteBuffer.allocateDirect(1024).order(ByteOrder.nativeOrder()));
    reader.setBufferValidityBitmap(
        "fmt", ByteBuffer.allocateDirect(1024).order(ByteOrder.nativeOrder()));
    reader.setBufferValidityBitmap(
        "info", ByteBuffer.allocateDirect(1024).order(ByteOrder.nativeOrder()));
    reader.setBufferValidityBitmap(
        "fmt_MIN_DP", ByteBuffer.allocateDirect(1024).order(ByteOrder.nativeOrder()));
    reader.setBufferValidityBitmap(
        "fmt_DP", ByteBuffer.allocateDirect(1024).order(ByteOrder.nativeOrder()));
    reader.setBufferOffsets(
        "fmt_PL", ByteBuffer.allocateDirect(1024).order(ByteOrder.nativeOrder()));
    reader.setBufferValidityBitmap(
        "fmt_PL", ByteBuffer.allocateDirect(1024).order(ByteOrder.nativeOrder()));
    reader.setBufferValidityBitmap(
        "fmt_GQ", ByteBuffer.allocateDirect(1024).order(ByteOrder.nativeOrder()));
    reader.setBufferValidityBitmap(
        "fmt_GT", ByteBuffer.allocateDirect(1024).order(ByteOrder.nativeOrder()));
    reader.setBufferOffsets(
        "fmt_GT", ByteBuffer.allocateDirect(1024).order(ByteOrder.nativeOrder()));
    reader.setBufferValidityBitmap(
        "info_END", ByteBuffer.allocateDirect(1024).order(ByteOrder.nativeOrder()));

    int[] fmt_MIN_DP_expected = new int[] {0, 0, 0, 0, 14, 30, 7, 4, 0, 0};
    int[] fmt_DP_expected = new int[] {0, 0, 0, 0, 15, 64, 10, 6, 0, 0};
    int[] fmt_GQ_expected = new int[] {0, 0, 0, 0, 42, 99, 30, 12, 0, 0};
    int[] fmt_PL_expected =
        new int[] {
          0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 24, 360, 0, 66, 990, 0, 21, 210, 0, 6, 90, 0, 0, 0,
          0, 0, 0
        };
    int[] fmt_GT_expected = new int[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

    int[] pos_start_expected = {
      12141, 12141, 12546, 12546, 13354, 13354, 13452, 13520, 13545, 17319
    };
    int[] pos_end_expected =
        new int[] {12277, 12277, 12771, 12771, 13374, 13389, 13519, 13544, 13689, 17479};

    int totalResults = 0;
    long results = 0;

    int PLIdx = 0;
    int PLBitmapIdx = 0;
    int GTIdx = 0;
    int GTBitmapIdx = 0;

    while (!reader.getStatus().equals(VCFReader.Status.COMPLETED)) {
      reader.submit();
      results = reader.getNumRecords();
      totalResults += results;

      int cnt = 0;

      // FMT buffers
      ByteBuffer pos_start_buffer = reader.getBuffer("pos_start");
      ByteBuffer pos_end_buffer = reader.getBuffer("pos_end");

      // fixed-sized
      ByteBuffer fmt_MIN_DP_buffer = reader.getBuffer("fmt_MIN_DP");
      ByteBuffer fmt_DP_buffer = reader.getBuffer("fmt_DP");
      ByteBuffer fmt_GQ_buffer = reader.getBuffer("fmt_GQ");

      // var-sized
      ByteBuffer fmt_PL_buffer = reader.getBuffer("fmt_PL");
      ByteBuffer fmt_GT_buffer = reader.getBuffer("fmt_GT");

      BitSet fmt_MIN_DP_bitmap = BitSet.valueOf(reader.getBitMap("fmt_MIN_DP"));
      BitSet fmt_DP_bitmap = BitSet.valueOf(reader.getBitMap("fmt_DP"));
      BitSet fmt_PL_bitmap = BitSet.valueOf(reader.getBitMap("fmt_PL"));
      BitSet fmt_GQ_bitmap = BitSet.valueOf(reader.getBitMap("fmt_GQ"));
      BitSet fmt_GT_bitmap = BitSet.valueOf(reader.getBitMap("fmt_GT"));

      // Info Buffers
      ByteBuffer info_DP_buffer = reader.getBuffer("info_DP");
      ByteBuffer info_ClippingRankSum_buffer = reader.getBuffer("info_ClippingRankSum");
      ByteBuffer info_ReadPosRankSum_buffer = reader.getBuffer("info_ReadPosRankSum");

      for (int i = 0; i < results; ++i) {
        // Check fixed-sized first
        Assert.assertTrue(fmt_MIN_DP_bitmap.get(i));
        Assert.assertTrue(fmt_DP_bitmap.get(i));
        Assert.assertTrue(fmt_GQ_bitmap.get(i));

        boolean PLValidity = fmt_PL_bitmap.get(i);
        boolean GTValidity = fmt_GT_bitmap.get(i);

        // Check var-sized bitmaps
        Assert.assertTrue(PLValidity);
        Assert.assertTrue(GTValidity);

        Assert.assertEquals(pos_start_expected[cnt], pos_start_buffer.getInt());
        Assert.assertEquals(pos_end_expected[cnt], pos_end_buffer.getInt());
        Assert.assertEquals(fmt_MIN_DP_expected[cnt], fmt_MIN_DP_buffer.getInt());

        Assert.assertEquals(fmt_DP_expected[cnt], fmt_DP_buffer.getInt());
        Assert.assertEquals(fmt_GQ_expected[cnt], fmt_GQ_buffer.getInt());

        Assert.assertEquals(0, info_DP_buffer.getInt());
        Assert.assertEquals(0, info_ClippingRankSum_buffer.getInt());
        Assert.assertEquals(0, info_ReadPosRankSum_buffer.getInt());

        for (int arrayIdx = 0; arrayIdx < 3; ++arrayIdx) {
          int PLValue = fmt_PL_buffer.getInt();
          Assert.assertEquals(fmt_PL_expected[PLIdx], PLValue);
          PLIdx++;
          PLBitmapIdx++;

          System.out.printf("%d", PLValue);
          if (arrayIdx < 2) System.out.printf(", ");
          else System.out.printf(" -> " + PLValidity + "\n");
        }

        for (int arrayIdx = 0; arrayIdx < 2; ++arrayIdx) {
          int GTValue = fmt_GT_buffer.getInt();
          Assert.assertEquals(fmt_GT_expected[GTIdx], GTValue);
          GTIdx++;
          GTBitmapIdx++;

          System.out.printf("%d", GTValue);
          if (arrayIdx < 1) System.out.printf(", ");
          else System.out.printf(" -> " + GTValidity + "\n");
        }

        cnt++;
      }
    }
  }

  /**
   * * Tests if the result returned is the same for different number of range partitions
   *
   * @throws IOException
   */
  @Test
  public void testNumResultsMultipleRangePartitions() throws IOException {
    int[] partitionsToCheck = {1, 2, 5, 10, 50, 100};

    for (int numPartitions : partitionsToCheck) {
      VCFReader reader = getVFCReader(Optional.empty(), Optional.empty());

      int results = 0;

      for (int partition = 0; partition < numPartitions; ++partition) {
        reader.setRangePartition(numPartitions, partition);

        reader.submit();
        results += reader.getNumRecords();

        while (reader.getStatus().equals(VCFReader.Status.INCOMPLETE)) reader.submit();
      }

      Assert.assertEquals(results, TOTAL_EXPECTED_RECORDS);

      reader.close();
    }
  }

  /**
   * * Checks the number of results returned by providing a single sample
   *
   * @throws IOException
   */
  @Test
  public void testSingleSample() throws IOException {
    VCFReader reader = getVFCReader(Optional.of(new String[] {getSamples()[0]}), Optional.empty());

    int results = 0;

    reader.submit();
    results += reader.getNumRecords();

    while (reader.getStatus().equals(VCFReader.Status.INCOMPLETE)) reader.submit();

    Assert.assertEquals(results, SINGLE_SAMPLE_EXPECTED_RECORDS);
  }

  /**
   * * Checks the number of results returned by providing the .bed file
   *
   * @throws IOException
   */
  @Test
  public void testBEDFile() throws IOException {
    VCFReader reader = getVFCReader(Optional.empty(), Optional.of(constructBEDURI()));

    int results = 0;

    reader.submit();

    results += reader.getNumRecords();

    while (reader.getStatus().equals(VCFReader.Status.INCOMPLETE)) reader.submit();

    Assert.assertEquals(results, BED_FILE_EXPECTED_RECORDS);
  }

  /**
   * * Checks the number of results returned by providing the .bed file
   *
   * @throws IOException
   */
  @Test
  public void testSetSingleBuffer() throws IOException {
    VCFReader reader = getVFCReader(Optional.empty(), Optional.of(constructBEDURI()));
    ByteBuffer data = ByteBuffer.allocateDirect(1024);
    reader.setBuffer("sample_name", data);

    int results = 0;

    reader.submit();

    results += reader.getNumRecords();

    while (reader.getStatus().equals(VCFReader.Status.INCOMPLETE)) reader.submit();

    Assert.assertEquals(results, BED_FILE_EXPECTED_RECORDS);
  }

  @Test
  public void testSetStatsEnabled() throws IOException {
    VCFReader reader = getVFCReader(Optional.empty(), Optional.of(constructBEDURI()));

    reader.setStatsEnabled(true);
  }

  @Test
  public void testGetStatsEnabled() throws IOException {
    VCFReader reader = getVFCReader(Optional.empty(), Optional.of(constructBEDURI()));

    Assert.assertFalse(reader.getStatsEnabled());
    reader.setStatsEnabled(true);
    Assert.assertTrue(reader.getStatsEnabled());
    reader.setStatsEnabled(false);
    Assert.assertFalse(reader.getStatsEnabled());
  }

  @Test
  public void testStats() throws IOException {
    VCFReader reader = getVFCReader(Optional.empty(), Optional.of(constructBEDURI()));
    reader.setStatsEnabled(true);
    Assert.assertNotNull(reader.stats());
  }

  /**
   * * Checks that the reader attribute details are initialized in constructor
   *
   * @throws IOException
   */
  @Test
  public void testAttributes() throws IOException {
    VCFReader reader = getVFCReader(Optional.of(new String[] {getSamples()[0]}), Optional.empty());

    Assert.assertTrue(reader.attributes.size() > 0);
    Assert.assertTrue(reader.fmtAttributes.size() > 0);
    Assert.assertTrue(reader.infoAttributes.size() > 0);
  }
}
