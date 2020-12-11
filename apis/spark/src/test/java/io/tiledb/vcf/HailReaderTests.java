package io.tiledb.vcf;

import is.hail.HailContext;
import is.hail.annotations.Region;
import is.hail.annotations.RegionPool;
import is.hail.backend.spark.SparkBackend;
import is.hail.expr.ir.ExecuteContext;
import is.hail.expr.ir.Interpret;
import is.hail.expr.ir.MatrixRead;
import is.hail.expr.ir.MatrixRowsTable;
import is.hail.expr.ir.TableIR;
import is.hail.expr.ir.TableValue;
import is.hail.io.fs.HadoopFS;
import is.hail.utils.ExecutionTimer;
import is.hail.utils.SerializableHadoopConfiguration;
import is.hail.variant.Locus;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.junit.Assert;
import org.junit.Test;
import scala.collection.JavaConversions;

public class HailReaderTests extends SharedJavaSparkSession {

  private String testSampleGroupURI(String sampleGroupName) {
    Path arraysPath = Paths.get("src", "test", "resources", "arrays", "v3", sampleGroupName);
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
            .option("tiledb_stats_log_level", Level.INFO.toString())
            .load();
    return dfRead;
  }

  @Test
  public void hailReader() {
    Map<String, String> optionsMap = new HashMap<>();
    optionsMap.put("uri", testSampleGroupURI("ingested_2samples"));
    optionsMap.put("samples", "HG01762,HG00280");
    optionsMap.put("ranges", "1:12100-13360,1:13500-17350");
    optionsMap.put("partitions", "10");
    optionsMap.put("tiledb.vfs.num_threads", "1");
    optionsMap.put("tiledb_stats_log_level", Level.INFO.toString());

    DataSourceOptions dataSourceOptions = new DataSourceOptions(optionsMap);
    VCFDataSourceOptions opts = new VCFDataSourceOptions(dataSourceOptions);

    SparkBackend sb = new SparkBackend("/tmp", "file:///tmp", sparkSession.sparkContext());
    HailContext hc = HailContext.apply(sb, "/tmp/hail.log", false, false, 50, false, 3);

    TileDBHailVCFReader reader =
        new TileDBHailVCFReader(
            URI.create(testSampleGroupURI("ingested_2samples")), opts, testSampleDataset().toDF());

    ExecuteContext ctx =
        new ExecuteContext(
            "/tmp",
            "file:///tmp",
            hc.backend(),
            new HadoopFS(new SerializableHadoopConfiguration(new Configuration())),
            Region.apply(Region.REGULAR(), RegionPool.apply(false)),
            new ExecutionTimer("HailSuite"));

    MatrixRead r = new MatrixRead(reader.fullMatrixType(), false, false, reader);
    TableIR mt = new MatrixRowsTable(r);

    TableValue tv = Interpret.apply(mt, ctx);

    Row[] rows = (Row[]) tv.rvd().toRows().collect();

    Row[] expectedRows =
        new GenericRow[] {
          new GenericRow(
              new Object[] {
                Locus.apply("1", 12141),
                JavaConversions.asScalaBuffer(Arrays.asList(new Object[] {"C", "<NON_REF>"}))
                    .toSeq()
              }),
          new GenericRow(
              new Object[] {
                Locus.apply("1", 12141),
                JavaConversions.asScalaBuffer(Arrays.asList(new Object[] {"C", "<NON_REF>"}))
                    .toSeq()
              }),
          new GenericRow(
              new Object[] {
                Locus.apply("1", 12546),
                JavaConversions.asScalaBuffer(Arrays.asList(new Object[] {"G", "<NON_REF>"}))
                    .toSeq()
              }),
          new GenericRow(
              new Object[] {
                Locus.apply("1", 12546),
                JavaConversions.asScalaBuffer(Arrays.asList(new Object[] {"G", "<NON_REF>"}))
                    .toSeq()
              }),
          new GenericRow(
              new Object[] {
                Locus.apply("1", 13354),
                JavaConversions.asScalaBuffer(Arrays.asList(new Object[] {"T", "<NON_REF>"}))
                    .toSeq()
              }),
          new GenericRow(
              new Object[] {
                Locus.apply("1", 13354),
                JavaConversions.asScalaBuffer(Arrays.asList(new Object[] {"T", "<NON_REF>"}))
                    .toSeq()
              }),
          new GenericRow(
              new Object[] {
                Locus.apply("1", 13452),
                JavaConversions.asScalaBuffer(Arrays.asList(new Object[] {"G", "<NON_REF>"}))
                    .toSeq()
              }),
          new GenericRow(
              new Object[] {
                Locus.apply("1", 13520),
                JavaConversions.asScalaBuffer(Arrays.asList(new Object[] {"G", "<NON_REF>"}))
                    .toSeq()
              }),
          new GenericRow(
              new Object[] {
                Locus.apply("1", 13545),
                JavaConversions.asScalaBuffer(Arrays.asList(new Object[] {"G", "<NON_REF>"}))
                    .toSeq()
              }),
          new GenericRow(
              new Object[] {
                Locus.apply("1", 17319),
                JavaConversions.asScalaBuffer(Arrays.asList(new Object[] {"T", "<NON_REF>"}))
                    .toSeq()
              }),
        };

    Assert.assertArrayEquals(expectedRows, rows);

    for (Row row : rows) {
      System.out.println(row.getClass());
      System.out.println(row.toString());
    }
  }
}
