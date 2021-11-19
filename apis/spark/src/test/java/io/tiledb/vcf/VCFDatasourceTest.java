package io.tiledb.vcf;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.callUDF;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.*;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.log4j.Level;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Assert;
import org.junit.Test;
import scala.collection.JavaConverters;
import scala.collection.immutable.HashMap;
import scala.collection.mutable.WrappedArray;

public class VCFDatasourceTest extends SharedJavaSparkSession {

  private String testSampleGroupURI(String sampleGroupName) {
    Path arraysPath = Paths.get("src", "test", "resources", "arrays", "v3", sampleGroupName);
    return "file://".concat(arraysPath.toAbsolutePath().toString());
  }

  private String testSampleGroupURI(String sampleGroupName, String version) {
    Path arraysPath = Paths.get("src", "test", "resources", "arrays", version, sampleGroupName);
    return "file://".concat(arraysPath.toAbsolutePath().toString());
  }

  private String testSimpleBEDFile() {
    Path path = Paths.get("src", "test", "resources", "simple.bed");
    return "file://".concat(path.toAbsolutePath().toString());
  }

  private String testLargeBEDFile() {
    Path path = Paths.get("src", "test", "resources", "E001_15_coreMarks_dense.bed.gz");
    return "file://".concat(path.toAbsolutePath().toString());
  }

  private String testSampleFile() {
    Path path = Paths.get("src", "test", "resources", "sample_names.txt");
    return "file://".concat(path.toAbsolutePath().toString());
  }

  private Dataset<Row> testSampleDataset() {
    return session()
        .read()
        .format("io.tiledb.vcf")
        .option("uri", testSampleGroupURI("ingested_2samples"))
        .option("samples", "HG01762,HG00280")
        .option("ranges", "1:12100-13360,1:13500-17350")
        .option("tiledb.vfs.num_threads", 1)
        .option("tiledb_stats_log_level", Level.INFO.toString())
        .load();
  }

  @Test
  public void testSchema() {
    SparkSession spark = session();
    Dataset<Row> dfRead =
        spark
            .read()
            .format("io.tiledb.vcf.VCFDataSource")
            .option("uri", testSampleGroupURI("ingested_2samples"))
            .load();

    dfRead.createOrReplaceTempView("vcf");

    long numColumns = spark.sql("SHOW COLUMNS FROM vcf").count();
    Assert.assertEquals(numColumns, 33L);

    List<Row> colNameList = spark.sql("SHOW COLUMNS FROM vcf").collectAsList();
    Set<String> colNames =
        colNameList.stream().map(r -> r.getString(0)).collect(Collectors.toSet());
    Assert.assertEquals(
        new HashSet<>(
            Arrays.asList(
                "fmt_SB",
                "fmt_MIN_DP",
                "info_DP",
                "info_ClippingRankSum",
                "info_ReadPosRankSum",
                "fmt",
                "queryBedStart",
                "fmt_AD",
                "posStart",
                "info_BaseQRankSum",
                "info_MLEAF",
                "posEnd",
                "fmt_GQ",
                "info_MLEAC",
                "genotype",
                "id",
                "alleles",
                "info",
                "sampleName",
                "info_MQ",
                "queryBedEnd",
                "queryBedLine",
                "info_MQ0",
                "fmt_PL",
                "filter",
                "info_HaplotypeScore",
                "contig",
                "info_DS",
                "info_InbreedingCoeff",
                "info_END",
                "fmt_DP",
                "info_MQRankSum",
                "qual")),
        colNames);
  }

  @Test
  public void testMaterializedSchema() {
    SparkSession spark = session();
    Dataset<Row> dfRead =
        spark
            .read()
            .format("io.tiledb.vcf.VCFDataSource")
            .option("uri", testSampleGroupURI("ingested_2samples"))
            .option("only_materialized_fields", true)
            .load();

    dfRead.createOrReplaceTempView("vcf");

    long numColumns = spark.sql("SHOW COLUMNS FROM vcf").count();
    Assert.assertEquals(numColumns, 13L);

    List<Row> colNameList = spark.sql("SHOW COLUMNS FROM vcf").collectAsList();
    Set<String> colNames =
        colNameList.stream().map(r -> r.getString(0)).collect(Collectors.toSet());
    Assert.assertEquals(
        new HashSet<>(
            Arrays.asList(
                "queryBedStart",
                "posStart",
                "queryBedEnd",
                "queryBedLine",
                "posEnd",
                "qual",
                "filter",
                "id",
                "fmt",
                "alleles",
                "contig",
                "info",
                "sampleName")),
        colNames);
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
            .option("tiledb.config.logging_level", 5)
            .option("tiledb.vcf.log_level", "TRACE")
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
  public void testSamplePartitionIndex() {
    Dataset<Row> dfRead =
        session()
            .read()
            .format("io.tiledb.vcf")
            .option("uri", testSampleGroupURI("ingested_2samples"))
            .option("samples", "HG01762,HG00280")
            .option("ranges", "1:12100-13360,1:13500-17350")
            .option("sample_partitions", 2)
            .option("sample_partition_index", 0)
            .load();
    Assert.assertEquals(1, dfRead.select("sampleName").rdd().getNumPartitions());
    List<Row> rows = dfRead.select("sampleName").collectAsList();
    Assert.assertEquals(7, rows.size());
    Dataset<Row> dfExpected = testSampleDataset();
    List<Row> expectedRows = dfRead.select("sampleName").collectAsList();
    // This works because partitioning size we are grabing the first set of data, so it works to
    // generically compared to expectedRows which is ALL partitions
    for (int i = 0; i < rows.size(); i++) {
      Assert.assertEquals(rows.get(i).getString(0), expectedRows.get(i).getString(0));
    }
  }

  @Test
  public void testRangePartitionIndex() {
    Dataset<Row> dfRead =
        session()
            .read()
            .format("io.tiledb.vcf")
            .option("uri", testSampleGroupURI("ingested_2samples"))
            .option("samples", "HG01762,HG00280")
            .option("ranges", "1:12100-13360,1:13500-17350")
            .option("range_partitions", 2)
            .option("range_partition_index", 0)
            .load();
    Assert.assertEquals(1, dfRead.select("sampleName").rdd().getNumPartitions());
    List<Row> rows = dfRead.select("sampleName").collectAsList();
    Assert.assertEquals(6, rows.size());
    Dataset<Row> dfExpected = testSampleDataset();
    // This works because partitioning size we are grabing the first set of data, so it works to
    // generically compared to expectedRows which is ALL partitions
    List<Row> expectedRows = dfRead.select("sampleName").collectAsList();
    for (int i = 0; i < rows.size(); i++) {
      Assert.assertEquals(rows.get(i).getString(0), expectedRows.get(i).getString(0));
    }
  }

  @Test
  public void testRangeAndSamplePartitionIndex() {
    Dataset<Row> dfRead =
        session()
            .read()
            .format("io.tiledb.vcf")
            .option("uri", testSampleGroupURI("ingested_2samples"))
            .option("samples", "HG01762,HG00280")
            .option("ranges", "1:12100-13360,1:13500-17350")
            .option("range_partitions", 2)
            .option("range_partition_index", 0)
            .option("sample_partitions", 2)
            .option("sample_partition_index", 0)
            .load();
    Assert.assertEquals(1, dfRead.select("sampleName").rdd().getNumPartitions());
    List<Row> rows = dfRead.select("sampleName").collectAsList();
    Assert.assertEquals(3, rows.size());
    Dataset<Row> dfExpected = testSampleDataset();
    // This works because partitioning size we are grabing the first set of data, so it works to
    // generically compared to expectedRows which is ALL partitions
    List<Row> expectedRows = dfRead.select("sampleName").collectAsList();
    for (int i = 0; i < rows.size(); i++) {
      Assert.assertEquals(rows.get(i).getString(0), expectedRows.get(i).getString(0));
    }
  }

  @Test
  public void testDecoder() {
    Dataset<Row> dfRead =
        session()
            .read()
            .format("io.tiledb.vcf")
            .option("uri", testSampleGroupURI("ingested_2samples"))
            .option("samples", "HG01762,HG00280")
            // .option("ranges", "1:12100-13360,1:13500-17350")
            .option("partitions", 2)
            .load();

    // Create and register the UDF
    UDF1 decoderUDF = new VCFInfoFmtUDF();
    session()
        .sqlContext()
        .udf()
        .register(
            "decode",
            decoderUDF,
            DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));

    Row[] fmt = (Row[]) dfRead.select(col("fmt"), callUDF("decode", col("fmt"))).collect();

    List<byte[]> fmtList =
        Arrays.stream(fmt)
            .map(
                x -> {
                  return (byte[]) x.get(0);
                })
            .collect(Collectors.toList());
    List<HashMap> decodedFmtList =
        Arrays.stream(fmt)
            .map(
                x -> {
                  return (HashMap) x.get(1);
                })
            .collect(Collectors.toList());

    int idx = 0;
    for (HashMap r : decodedFmtList) {
      byte[] fmtBytes = fmtList.get(idx);

      ByteBuffer bytes = ByteBuffer.allocateDirect(fmtBytes.length).order(ByteOrder.nativeOrder());
      bytes.position(0);
      for (byte b : fmtBytes) {
        bytes.put(b);
      }

      Map<String, String> tmp = VCFInfoFmtDecoder.decodeInfoFmtBytes(bytes);

      for (String key : tmp.keySet()) {
        Assert.assertEquals(tmp.get(key), r.get(key).get());
      }

      ++idx;
    }
  }

  @Test
  public void testDecoder2() {
    Dataset<Row> dfRead =
        session()
            .read()
            .format("io.tiledb.vcf")
            .option("uri", testSampleGroupURI("ingested_2samples"))
            .option("samples", "HG01762,HG00280")
            // .option("ranges", "1:12100-13360,1:13500-17350")
            .option("partitions", 2)
            .load();

    // Create and register the UDF
    UDF1 decoderUDF = new VCFInfoFmtUDF();
    session()
        .sqlContext()
        .udf()
        .register(
            "decode",
            decoderUDF,
            DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));

    List<Row> rows = dfRead.collectAsList();

    Row[] fmt = (Row[]) dfRead.select(col("fmt"), callUDF("decode", col("fmt"))).collect();

    String[] columns = dfRead.columns();

    List<String> fmtColumns =
        Arrays.asList(new String[] {"fmt_MIN_DP", "fmt_GQ", "fmt_DP", "fmt_PL", "fmt_GT"});

    int idx = 0;

    int fmtSchemaPosition = 5;
    int infoSchemaPosition = 17;
    int sampleNameSchemaPosition = 18;

    for (Row row : rows) {
      System.out.println("Sample Name: " + row.get(sampleNameSchemaPosition).toString());
      byte[] fmtBytes = (byte[]) row.get(fmtSchemaPosition);
      byte[] infoBytes = (byte[]) row.get(infoSchemaPosition);

      ByteBuffer fmtBuffer =
          ByteBuffer.allocateDirect(fmtBytes.length).order(ByteOrder.nativeOrder());
      for (byte b : fmtBytes) {
        fmtBuffer.put(b);
      }

      ByteBuffer infoBuffer =
          ByteBuffer.allocateDirect(infoBytes.length).order(ByteOrder.nativeOrder());
      for (byte b : infoBytes) {
        infoBuffer.put(b);
      }

      Map<String, String> fmtMap = VCFInfoFmtDecoder.decodeInfoFmtBytes(fmtBuffer);
      Map<String, String> infoMap = VCFInfoFmtDecoder.decodeInfoFmtBytes(infoBuffer);

      // Keep track of the column position in the schema, so we can get
      // the corresponding value of each row.
      int colPosition = 0;
      for (String column : columns) {
        if (fmtColumns.contains(column)) {

          // The decoder returns a HashMap, with keys like "MIN_DP", "GQ", and so on. Thus, we need
          // to take
          // the second part of the column name, that is, "DP" from "fmt_DP", "GQ" of "fmt_GQ" and
          // so on.
          String[] columnSplit = column.split("_");
          columnSplit = Arrays.copyOfRange(columnSplit, 1, columnSplit.length);
          String key = Arrays.stream(columnSplit).reduce((x, y) -> x + "_" + y).get();

          String decoderValue = fmtMap.get(key);
          Object attributeValue = row.get(colPosition);

          // If the attribute value is a WrappedArray, convert it to a string of the form "0,0,0"
          // so we can compare it with the attribute value.
          if (attributeValue instanceof WrappedArray) {
            WrappedArray array = (WrappedArray) attributeValue;
            Object[] javaArray = JavaConverters.asJavaCollection(array).toArray();
            String arrayStr = "";

            for (int i = 0; i < javaArray.length; ++i) {
              arrayStr += javaArray[i].toString();

              if (i < javaArray.length - 1) arrayStr += ",";
            }

            attributeValue = arrayStr;
          } else {
            attributeValue = attributeValue.toString();
          }

          System.out.println(
              column + " - Decoder: " + decoderValue + " Attribute: " + attributeValue);
          Assert.assertEquals(decoderValue, attributeValue);
        }

        colPosition++;
      }
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
  public void testNewPartition() {
    int rangePartitions = 32;
    int samplePartitions = 2;
    Dataset<Row> dfRead =
        session()
            .read()
            .format("io.tiledb.vcf")
            .option("uri", testSampleGroupURI("ingested_2samples", "v4"))
            .option("bedfile", testLargeBEDFile())
            .option("new_partition_method", true)
            .option("range_partitions", rangePartitions)
            .option("sample_partitions", samplePartitions)
            .option("tiledb.vcf.log_level", "TRACE")
            .load();

    Assert.assertEquals(
        rangePartitions * samplePartitions, dfRead.select("sampleName").rdd().getNumPartitions());

    List<Row> rows =
        dfRead
            .select(
                "sampleName",
                "contig",
                "posStart",
                "posEnd",
                "queryBedStart",
                "queryBedEnd",
                "queryBedLine")
            .collectAsList();

    // query from bed file line 184134 (0-indexed line numbers)
    // 1	10600	540400	15_Quies	0	.	10600	540400	255,255,255

    // NOTE: queryBedEnd returns the half-open value from the bed file,
    //       not the inclusive value used by tiledb-vcf
    int expectedBedStart = 10600; // 0-indexed
    int expectedBedEnd = 540400; // half-open
    int expectedBedLine = 184134;

    for (int i = 0; i < rows.size(); i++) {
      System.out.println(
          String.format(
              "*** %s, %s, pos=%d-%d, query=%d-%d, bedLine=%d",
              rows.get(i).getString(0),
              rows.get(i).getString(1),
              rows.get(i).getInt(2),
              rows.get(i).getInt(3),
              rows.get(i).getInt(4),
              rows.get(i).getInt(5),
              rows.get(i).getInt(6)));
      Assert.assertEquals(expectedBedStart, rows.get(i).getInt(4));
      Assert.assertEquals(expectedBedEnd, rows.get(i).getInt(5));
      Assert.assertEquals(expectedBedLine, rows.get(i).getInt(6));
    }
  }

  @Test
  public void testBedFile() {
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

  @Test(expected = UnsupportedOperationException.class)
  public void testSchemaPushDownSamplesError() {
    Dataset<Row> dfRead =
        session()
            .read()
            .format("io.tiledb.vcf")
            .option("uri", testSampleGroupURI("ingested_2samples"))
            .option("ranges", "1:12100-13360,1:13500-17350")
            .option("samples", "HG01762")
            .option("tiledb.vfs.num_threads", 1)
            .load();
    dfRead.createOrReplaceTempView("vcf");
    List<Row> rows =
        sparkSession
            .sql("SELECT sampleName FROM vcf WHERE vcf.sampleName='HG01762'")
            .collectAsList();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testSchemaPushDownSamplesError2() {
    Dataset<Row> dfRead =
        session()
            .read()
            .format("io.tiledb.vcf")
            .option("uri", testSampleGroupURI("ingested_2samples"))
            .option("ranges", "1:12100-13360,1:13500-17350")
            .option("samplefile", testSampleFile())
            .option("tiledb.vfs.num_threads", 1)
            .load();
    dfRead.createOrReplaceTempView("vcf");
    List<Row> rows =
        sparkSession
            .sql("SELECT sampleName FROM vcf WHERE vcf.sampleName='HG01762'")
            .collectAsList();
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
    // expect empty filters for all records except HG00280's variant at 13354
    for (int i = 0; i < rows.size(); i++) {
      boolean isNull = rows.get(i).isNullAt(0);
      if (i == 4) {
        Assert.assertFalse(isNull);
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
    List<Row> rows = dfRead.select("queryBedStart", "queryBedEnd", "queryBedLine").collectAsList();
    Assert.assertEquals(10, rows.size());
    int[] expectedStart =
        new int[] {12099, 12099, 12099, 12099, 12099, 12099, 13499, 13499, 13499, 13499};
    int[] expectedEnd =
        new int[] {13360, 13360, 13360, 13360, 13360, 13360, 17350, 17350, 17350, 17350};
    int expectedLine = -1; // query is not from a bed file

    int[] resultStart = new int[rows.size()];
    int[] resultEnd = new int[rows.size()];
    int resultLine;
    for (int i = 0; i < rows.size(); i++) {
      resultStart[i] = rows.get(i).getInt(0);
      resultEnd[i] = rows.get(i).getInt(1);
      resultLine = rows.get(i).getInt(2);
      Assert.assertEquals(expectedLine, resultLine);
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

  @Test
  public void testDebugOptions() {
    Dataset<Row> dfRead =
        session()
            .read()
            .format("io.tiledb.vcf")
            .option("uri", testSampleGroupURI("ingested_2samples"))
            .option("ranges", "1:12100-13360,1:13500-17350")
            .option("tiledb.vfs.num_threads", 1)
            .option("debug.print_vcf_regions", true)
            .option("debug.print_sample_list", true)
            .option("debug.print_tiledb_query_ranges", true)
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
  public void testTimeTravel() throws java.text.ParseException {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-M-dd hh:mm:ss");
    Long start_ms = sdf.parse("2000-08-01 12:00:00").getTime();
    Long end_ms = sdf.parse("2030-08-01 16:00:00").getTime();

    Dataset<Row> dfRead =
        session()
            .read()
            .format("io.tiledb.vcf")
            .option("uri", testSampleGroupURI("ingested_2samples"))
            .option("ranges", "1:12100-13360,1:13500-17350")
            .option("tiledb.vfs.num_threads", 1)
            .option("tiledb.vcf.start_timestamp", start_ms)
            .option("tiledb.vcf.end_timestamp", end_ms)
            .option("tiledb.vcf.log_level", "INFO")
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
}
