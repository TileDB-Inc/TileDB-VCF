package io.tiledb.vcf;

import java.net.URI;
import java.util.HashMap;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;

public class VCFDataSourceOptionsTest {

  @Test
  public void testArrayURIOptionMissing() {
    VCFDataSourceOptions options = new VCFDataSourceOptions(new DataSourceOptions(new HashMap<>()));
    Assert.assertFalse(options.getDatasetURI().isPresent());
  }

  @Test
  public void testArrayURIOption() {
    HashMap<String, String> optionMap = new HashMap<>();
    optionMap.put("uri", "s3://foo/bar");
    VCFDataSourceOptions options = new VCFDataSourceOptions(new DataSourceOptions(optionMap));
    Optional<URI> uri = options.getDatasetURI();
    Assert.assertTrue(uri.isPresent());
    Assert.assertEquals(uri.get(), URI.create("s3://foo/bar"));
  }

  @Test
  public void testSamplesOptionMissing() {
    VCFDataSourceOptions options = new VCFDataSourceOptions(new DataSourceOptions(new HashMap<>()));
    Assert.assertFalse(options.getSamples().isPresent());
  }

  @Test
  public void testSamplesOption() {
    HashMap<String, String> optionMap = new HashMap<>();
    optionMap.put("samples", "sample1, sample2 , sample3  , sample4");
    VCFDataSourceOptions options = new VCFDataSourceOptions(new DataSourceOptions(optionMap));
    Optional<String[]> samples = options.getSamples();
    Assert.assertTrue(samples.isPresent());
    Assert.assertArrayEquals(
        new String[] {"sample1", "sample2", "sample3", "sample4"}, samples.get());
  }

  @Test
  public void testRangesOptionMissing() {
    VCFDataSourceOptions options = new VCFDataSourceOptions(new DataSourceOptions(new HashMap<>()));
    Assert.assertFalse(options.getRanges().isPresent());
  }

  @Test
  public void testRangesOption() {
    HashMap<String, String> optionMap = new HashMap<>();
    optionMap.put("ranges", "CHR1:1-100, CHR2:2-200");
    VCFDataSourceOptions options = new VCFDataSourceOptions(new DataSourceOptions(optionMap));
    Optional<String[]> ranges = options.getRanges();
    Assert.assertTrue(ranges.isPresent());
    Assert.assertArrayEquals(new String[] {"CHR1:1-100", "CHR2:2-200"}, ranges.get());
  }

  @Test
  public void testRegionsOption() {
    HashMap<String, String> optionMap = new HashMap<>();
    optionMap.put("regions", "CHR1:1-100, CHR2:2-200");
    VCFDataSourceOptions options = new VCFDataSourceOptions(new DataSourceOptions(optionMap));
    Optional<String[]> ranges = options.getRanges();
    Assert.assertTrue(ranges.isPresent());
    Assert.assertArrayEquals(new String[] {"CHR1:1-100", "CHR2:2-200"}, ranges.get());
  }

  @Test
  public void testMemoryBudgetOptionMissing() {
    VCFDataSourceOptions options = new VCFDataSourceOptions(new DataSourceOptions(new HashMap<>()));
    Assert.assertFalse(options.getMemoryBudget().isPresent());
  }

  @Test
  public void testMemoryBudgetOption() {
    HashMap<String, String> optionMap = new HashMap<>();
    optionMap.put("memory", "10");
    VCFDataSourceOptions options = new VCFDataSourceOptions(new DataSourceOptions(optionMap));
    Optional<Integer> memory = options.getMemoryBudget();
    Assert.assertTrue(memory.isPresent());
    Assert.assertEquals((Integer) 10, memory.get());
  }

  @Test
  public void testBedURIOptionMissing() {
    VCFDataSourceOptions options = new VCFDataSourceOptions(new DataSourceOptions(new HashMap<>()));
    Assert.assertFalse(options.getBedURI().isPresent());
  }

  @Test
  public void testBedURIOption() {
    URI expectedURI = URI.create("s3://foo/bar");
    HashMap<String, String> optionMap = new HashMap<>();
    optionMap.put("bedfile", expectedURI.toString());
    VCFDataSourceOptions options = new VCFDataSourceOptions(new DataSourceOptions(optionMap));
    Assert.assertTrue(options.getBedURI().isPresent());
    Assert.assertEquals(expectedURI, options.getBedURI().get());
  }

  @Test
  public void testSampleURIOptionMissing() {
    VCFDataSourceOptions options = new VCFDataSourceOptions(new DataSourceOptions(new HashMap<>()));
    Assert.assertFalse(options.getBedURI().isPresent());
  }

  @Test
  public void testSampleURIOption() {
    URI expectedURI = URI.create("s3://foo/bar");
    HashMap<String, String> optionMap = new HashMap<>();
    optionMap.put("samplefile", expectedURI.toString());
    VCFDataSourceOptions options = new VCFDataSourceOptions(new DataSourceOptions(optionMap));
    Assert.assertTrue(options.getSampleURI().isPresent());
    Assert.assertEquals(expectedURI, options.getSampleURI().get());
  }

  @Test
  public void testPartitionsOptionMissing() {
    VCFDataSourceOptions options = new VCFDataSourceOptions(new DataSourceOptions(new HashMap<>()));
    Assert.assertFalse(options.getPartitions().isPresent());
  }

  @Test
  public void testPartitionsOption() {
    HashMap<String, String> optionMap = new HashMap<>();
    optionMap.put("partitions", "10");
    VCFDataSourceOptions options = new VCFDataSourceOptions(new DataSourceOptions(optionMap));
    Optional<Integer> partitions = options.getPartitions();
    Assert.assertTrue(partitions.isPresent());
    Assert.assertEquals((Integer) 10, partitions.get());
  }

  @Test
  public void testConfigOptionMissing() {
    VCFDataSourceOptions options = new VCFDataSourceOptions(new DataSourceOptions(new HashMap<>()));
    Assert.assertFalse(options.getConfigCSV().isPresent());
  }

  @Test
  public void testConfigOption() {
    HashMap<String, String> optionMap = new HashMap<>();
    optionMap.put("tiledb.sm.compute_concurrency_level", "5");
    VCFDataSourceOptions options = new VCFDataSourceOptions(new DataSourceOptions(optionMap));
    Optional<String> config = options.getConfigCSV();
    Assert.assertTrue(config.isPresent());
    Assert.assertEquals("sm.compute_concurrency_level=5", config.get());
  }

  @Test
  public void testCombineCSV() {
    Assert.assertEquals(
        VCFDataSourceOptions.combineCsvOptions(Optional.empty(), Optional.of("a")),
        Optional.of("a"));
    Assert.assertEquals(
        VCFDataSourceOptions.combineCsvOptions(Optional.of("b"), Optional.empty()),
        Optional.of("b"));
    Assert.assertEquals(
        VCFDataSourceOptions.combineCsvOptions(Optional.of("a"), Optional.of("b")),
        Optional.of("a,b"));
  }
}
