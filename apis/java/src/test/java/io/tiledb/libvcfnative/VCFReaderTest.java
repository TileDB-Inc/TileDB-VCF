package io.tiledb.libvcfnative;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
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
    Path path = Paths.get("src", "test", "resources", "arrays", fileName);
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
}
