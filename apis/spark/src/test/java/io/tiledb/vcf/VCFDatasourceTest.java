package io.tiledb.vcf;

import static org.apache.spark.sql.functions.callUDF;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Assert;
import org.junit.Test;

public class VCFDatasourceTest extends SharedJavaSparkSession {

  private String testSampleGroupURI(String sampleGroupName) {
    Path arraysPath = Paths.get("src", "test", "resources", "arrays", sampleGroupName);
    return "file://".concat(arraysPath.toAbsolutePath().toString());
  }

  private String testSimpleBEDFile() {
    Path path = Paths.get("src", "test", "resources", "simple.bed");
    return "file://".concat(path.toAbsolutePath().toString());
  }

  private String testSampleFile() {
    Path path = Paths.get("src", "test", "resources", "sample_names.txt");
    return "file://".concat(path.toAbsolutePath().toString());
  }

  private Dataset<Row> testSampleDataset() {
    Dataset<Row> dfRead =
        session()
            .read()
            .format("io.tiledb.vcf")
            .option("uri", testSampleGroupURI("ingested_2samples"))
            .option("samples", "HG01762,HG00280")
            .option("ranges", "1:12100-13360,1:13500-17350")
            .option("tiledb.vfs.num_threads", 1)
            .load();
    return dfRead;
  }

  @Test
  public void testSchema() {
    SparkSession spark = session();
    Dataset<Row> dfRead =
        spark.read().format("io.tiledb.vcf.VCFDataSource").option("uri", "s3://foo/bar").load();

    dfRead.createOrReplaceTempView("vcf");

    long numColumns = spark.sql("SHOW COLUMNS FROM vcf").count();
    Assert.assertEquals(numColumns, 14l);

    List<Row> colNameList = spark.sql("SHOW COLUMNS FROM vcf").collectAsList();
    List<String> colNames =
        colNameList.stream().map(r -> r.getString(0)).collect(Collectors.toList());
    Assert.assertEquals(
        colNames,
        Arrays.asList(
            "sampleName",
            "contig",
            "posStart",
            "posEnd",
            "alleles",
            "filter",
            "genotype",
            "fmt_DP",
            "fmt_GQ",
            "fmt_MIN_DP",
            "fmt",
            "info",
            "queryBedStart",
            "queryBedEnd"));
  }

  @Test(expected = org.apache.spark.sql.AnalysisException.class)
  public void testSchemaBadColumnname() {
    SparkSession spark = session();
    Dataset<Row> dfRead = testSampleDataset();
    dfRead.createOrReplaceTempView("vcf");
    spark.sql("SELECT foo FROM vcf").collect();
  }

  @Test
  public void testSchemaCount() {
    SparkSession spark = session();
    Dataset<Row> dfRead = testSampleDataset();
    Assert.assertEquals(10, dfRead.count());
  }

  @Test
  public void testPartition() {
    Dataset<Row> dfRead =
        session()
            .read()
            .format("io.tiledb.vcf")
            .option("uri", testSampleGroupURI("ingested_2samples"))
            .option("samples", "HG01762,HG00280")
            .option("ranges", "1:12100-13360,1:13500-17350")
            .option("partitions", 2)
            .load();
    Assert.assertEquals(2, dfRead.select("sampleName").rdd().getNumPartitions());
    List<Row> rows = dfRead.select("sampleName").collectAsList();
    Assert.assertEquals(10, rows.size());
    Dataset<Row> dfExpected = testSampleDataset();
    List<Row> expectedRows = dfRead.select("sampleName").collectAsList();
    for (int i = 0; i < rows.size(); i++) {
      Assert.assertEquals(rows.get(i).getString(0), expectedRows.get(i).getString(0));
    }
  }

  @Test
  public void testSamplePartition() {
    Dataset<Row> dfRead =
        session()
            .read()
            .format("io.tiledb.vcf")
            .option("uri", testSampleGroupURI("ingested_2samples"))
            .option("samples", "HG01762,HG00280")
            .option("ranges", "1:12100-13360,1:13500-17350")
            .option("sample_partitions", 2)
            .load();
    Assert.assertEquals(2, dfRead.select("sampleName").rdd().getNumPartitions());
    List<Row> rows = dfRead.select("sampleName").collectAsList();
    Assert.assertEquals(10, rows.size());
    Dataset<Row> dfExpected = testSampleDataset();
    List<Row> expectedRows = dfRead.select("sampleName").collectAsList();
    for (int i = 0; i < rows.size(); i++) {
      Assert.assertEquals(rows.get(i).getString(0), expectedRows.get(i).getString(0));
    }
  }

  @Test
  public void testBedFile() {
    System.out.println("BED: " + testSampleFile());
    Dataset<Row> dfRead =
        session()
            .read()
            .format("io.tiledb.vcf")
            .option("uri", testSampleGroupURI("ingested_2samples"))
            .option("samplefile", testSampleFile())
            .option("bedfile", testSimpleBEDFile())
            .load();
    List<Row> rows = dfRead.select("sampleName").collectAsList();
    Assert.assertEquals(10, rows.size());
  }

  @Test
  public void testSchemaShowTopN() {
    Dataset<Row> dfRead = testSampleDataset();
    dfRead.show(10);
  }

  @Test
  public void testSchemaPushDownSamples() {
    Dataset<Row> dfRead =
        session()
            .read()
            .format("io.tiledb.vcf")
            .option("uri", testSampleGroupURI("ingested_2samples"))
            .option("ranges", "1:12100-13360,1:13500-17350")
            .option("tiledb.vfs.num_threads", 1)
            .load();
    dfRead.createOrReplaceTempView("vcf");
    List<Row> rows =
        sparkSession
            .sql("SELECT sampleName FROM vcf WHERE vcf.sampleName='HG01762'")
            .collectAsList();
    Assert.assertEquals(3, rows.size());
    Assert.assertEquals(rows.get(0).getString(0), "HG01762");
    Assert.assertEquals(rows.get(1).getString(0), "HG01762");
    Assert.assertEquals(rows.get(2).getString(0), "HG01762");
  }

  @Test
  public void testSchemaSampleName() {
    Dataset<Row> dfRead = testSampleDataset();
    List<Row> rows = dfRead.select("sampleName").collectAsList();
    Assert.assertEquals(10, rows.size());
    String[] expected =
        new String[] {
          "HG00280", "HG01762", "HG00280", "HG01762", "HG00280", "HG01762", "HG00280", "HG00280",
          "HG00280", "HG00280"
        };
    String[] results = new String[rows.size()];
    for (int i = 0; i < rows.size(); i++) {
      results[i] = rows.get(i).getString(0);
    }
    Assert.assertArrayEquals(expected, results);
  }

  @Test
  public void testContig() {
    Dataset<Row> dfRead = testSampleDataset();
    List<Row> rows = dfRead.select("contig").collectAsList();
    Assert.assertEquals(10, rows.size());
    String[] expected = new String[] {"1", "1", "1", "1", "1", "1", "1", "1", "1", "1"};
    String[] results = new String[rows.size()];
    for (int i = 0; i < rows.size(); i++) {
      results[i] = rows.get(i).getString(0);
    }
    Assert.assertArrayEquals(expected, results);
  }

  @Test
  public void testSchemaPosStart() {
    Dataset<Row> dfRead = testSampleDataset();
    List<Row> rows = dfRead.select("posStart").collectAsList();
    Assert.assertEquals(10, rows.size());
    int[] expected =
        new int[] {12141, 12141, 12546, 12546, 13354, 13354, 13452, 13520, 13545, 17319};
    int[] results = new int[rows.size()];
    for (int i = 0; i < rows.size(); i++) {
      results[i] = rows.get(i).getInt(0);
    }
    Assert.assertArrayEquals(expected, results);
  }

  @Test
  public void testSchemaPosEnd() {
    Dataset<Row> dfRead = testSampleDataset();
    List<Row> rows = dfRead.select("posEnd").collectAsList();
    Assert.assertEquals(10, rows.size());
    int[] expected =
        new int[] {12277, 12277, 12771, 12771, 13374, 13389, 13519, 13544, 13689, 17479};
    int[] results = new int[rows.size()];
    for (int i = 0; i < rows.size(); i++) {
      results[i] = rows.get(i).getInt(0);
    }
    Assert.assertArrayEquals(expected, results);
  }

  @Test
  public void testAlleles() {
    Dataset<Row> dfRead = testSampleDataset();
    List<Row> rows = dfRead.select("alleles").collectAsList();
    Assert.assertEquals(10, rows.size());
    String[] expected =
        new String[] {
          "C,<NON_REF>",
          "C,<NON_REF>",
          "G,<NON_REF>",
          "G,<NON_REF>",
          "T,<NON_REF>",
          "T,<NON_REF>",
          "G,<NON_REF>",
          "G,<NON_REF>",
          "G,<NON_REF>",
          "T,<NON_REF>"
        };
    for (int i = 0; i < rows.size(); i++) {
      List<String> row = rows.get(i).getList(0);
      String[] expectedAlleles = expected[i].split(",");
      Assert.assertEquals(expectedAlleles[0], row.get(0));
      Assert.assertEquals(expectedAlleles[1], row.get(1));
    }
  }

  @Test
  public void testFilter() {
    Dataset<Row> dfRead = testSampleDataset();
    List<Row> rows = dfRead.select("filter").collectAsList();
    Assert.assertEquals(10, rows.size());
    // check null values
    for (int i = 0; i < rows.size(); i++) {
      boolean isNull = rows.get(i).isNullAt(0);
      if (i == 4) {
        Assert.assertFalse(isNull);
        List<String> row = rows.get(i).getList(0);
        Assert.assertEquals(row.size(), 1);
        Assert.assertEquals(row.get(0), "LowQual");
      } else {
        Assert.assertTrue(isNull);
      }
    }
  }

  @Test
  public void testGenotype() {
    Dataset<Row> dfRead = testSampleDataset();
    List<Row> rows = dfRead.select("genotype").collectAsList();
    Assert.assertEquals(10, rows.size());
    List<Integer> expected = Arrays.asList(0, 0);
    for (int i = 0; i < rows.size(); i++) {
      List<Integer> genotype = rows.get(i).getList(0);
      Assert.assertEquals(expected, genotype);
    }
  }

  @Test
  public void testInfo() {
    Dataset<Row> dfRead = testSampleDataset();
    List<Row> rows = dfRead.select("info").collectAsList();
    Assert.assertEquals(10, rows.size());
    // For this dataset there are no extra info fields, so the first integer (4 bytes) should be 0
    // (indicating no info fields stored).
    for (int i = 0; i < rows.size(); i++) {
      byte[] byteArr = (byte[]) rows.get(i).get(0);
      ByteBuffer bytes = ByteBuffer.allocateDirect(byteArr.length).order(ByteOrder.nativeOrder());
      bytes.position(0);
      for (byte b : byteArr) {
        bytes.put(b);
      }
      bytes.position(0);
      Assert.assertEquals(0, bytes.getInt());
    }
  }

  @Test
  public void testFmt() {
    Dataset<Row> dfRead = testSampleDataset();
    List<Row> rows = dfRead.select("fmt").collectAsList();
    Assert.assertEquals(10, rows.size());
    // Note: the GT values here are not what you would expect because htslib internally encodes
    // them. To get the decoded values, use the `fmt_GT` column instead.
    String[] expectedGT =
        new String[] {"2,2", "2,2", "2,2", "2,2", "2,2", "2,2", "2,2", "2,2", "2,2", "2,2"};
    String[] expectedDP = new String[] {"0", "0", "0", "0", "15", "64", "10", "6", "0", "0"};
    String[] expectedGQ = new String[] {"0", "0", "0", "0", "42", "99", "30", "12", "0", "0"};
    String[] expectedMIN_DP = new String[] {"0", "0", "0", "0", "14", "30", "7", "4", "0", "0"};
    String[] expectedPL =
        new String[] {
          "0,0,0",
          "0,0,0",
          "0,0,0",
          "0,0,0",
          "0,24,360",
          "0,66,990",
          "0,21,210",
          "0,6,90",
          "0,0,0",
          "0,0,0"
        };

    for (int i = 0; i < rows.size(); i++) {
      byte[] bytesArr = (byte[]) rows.get(i).get(0);
      ByteBuffer bytes = ByteBuffer.allocateDirect(bytesArr.length).order(ByteOrder.nativeOrder());
      bytes.position(0);
      for (byte b : bytesArr) {
        bytes.put(b);
      }
      Map<String, String> fmtFields = VCFInfoFmtDecoder.decodeInfoFmtBytes(bytes);
      Assert.assertEquals(expectedGT[i], fmtFields.get("GT"));
      Assert.assertEquals(expectedDP[i], fmtFields.get("DP"));
      Assert.assertEquals(expectedGQ[i], fmtFields.get("GQ"));
      Assert.assertEquals(expectedMIN_DP[i], fmtFields.get("MIN_DP"));
      Assert.assertEquals(expectedPL[i], fmtFields.get("PL"));
    }

    // Check UDF
    VCFInfoFmtUDF udf = new VCFInfoFmtUDF();
    session()
        .udf()
        .register(
            "fmt_to_map", udf, DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));
    dfRead = dfRead.withColumn("fmt_map", callUDF("fmt_to_map", dfRead.col("fmt")));
    rows = dfRead.select("fmt_map").collectAsList();
    for (int i = 0; i < rows.size(); i++) {
      Map<String, String> fmtFields = rows.get(i).getJavaMap(0);
      Assert.assertEquals(expectedGT[i], fmtFields.get("GT"));
      Assert.assertEquals(expectedDP[i], fmtFields.get("DP"));
      Assert.assertEquals(expectedGQ[i], fmtFields.get("GQ"));
      Assert.assertEquals(expectedMIN_DP[i], fmtFields.get("MIN_DP"));
      Assert.assertEquals(expectedPL[i], fmtFields.get("PL"));
    }
  }

  @Test
  public void testBedRanges() {
    Dataset<Row> dfRead = testSampleDataset();
    List<Row> rows = dfRead.select("queryBedStart", "queryBedEnd").collectAsList();
    Assert.assertEquals(10, rows.size());
    int[] expectedStart =
        new int[] {12099, 12099, 12099, 12099, 12099, 12099, 13499, 13499, 13499, 13499};
    int[] expectedEnd =
        new int[] {13360, 13360, 13360, 13360, 13360, 13360, 17350, 17350, 17350, 17350};

    int[] resultStart = new int[rows.size()];
    int[] resultEnd = new int[rows.size()];
    for (int i = 0; i < rows.size(); i++) {
      resultStart[i] = rows.get(i).getInt(0);
      resultEnd[i] = rows.get(i).getInt(1);
    }
    Assert.assertArrayEquals(expectedStart, resultStart);
    Assert.assertArrayEquals(expectedEnd, resultEnd);
  }

  // TODO(ttd) commenting out because AD is not in the default schema
  //  @Test
  //  public void testADNull() {
  //    Dataset<Row> dfRead = testSampleDataset();
  //    List<Row> rows = dfRead.select("fmt_AD").collectAsList();
  //    Assert.assertEquals(10, rows.size());
  //    // assert all are null
  //    for (int i = 0; i < rows.size(); i++) {
  //      Assert.assertTrue(rows.get(i).isNullAt(0));
  //    }
  //  }

  @Test
  public void testFmtInfoUdf() {
    Dataset<Row> dfRead = testSampleDataset();
    VCFInfoFmtUDF udf = new VCFInfoFmtUDF();
    session()
        .udf()
        .register(
            "fmt_to_map", udf, DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));
    dfRead = dfRead.withColumn("fmt_map", callUDF("fmt_to_map", dfRead.col("fmt")));
    dfRead.select("fmt_map").show(10, false);
  }
}
