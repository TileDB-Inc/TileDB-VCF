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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.junit.Assert;
import org.junit.Test;
import scala.Option;

public class TileDBHailReaderTest extends SharedJavaSparkSession {

  private String testVCFUri(String sampleGroupName) {
    Path arraysPath = Paths.get("src", "test", "resources", "arrays", "v4", sampleGroupName);
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

  private Dataset<Row> testSampleDataset(String samples) {
    Dataset<Row> dfRead =
        session()
            .read()
            .format("io.tiledb.vcf")
            .option("uri", testVCFUri("ingested_2samples"))
            .option("samples", samples)
            .option("ranges", "1:12100-13360,1:13500-17350")
            .option("tiledb.vfs.num_threads", 1)
            .option("tiledb_stats_log_level", Level.INFO.toString())
            .load();
    return dfRead;
  }

  @Test
  public void hailReader() {
    SparkBackend sb = new SparkBackend("/tmp", "file:///tmp", sparkSession.sparkContext());
    HailContext hc = HailContext.apply(sb, "/tmp/hail.log", false, false, 50, false, 3);

    ExecuteContext ctx =
        new ExecuteContext(
            "/tmp",
            "file:///tmp",
            hc.backend(),
            new HadoopFS(new SerializableHadoopConfiguration(new Configuration())),
            Region.apply(Region.REGULAR(), RegionPool.apply(false)),
            new ExecutionTimer("HailSuite"),
            null);

    // Test with sample HG01762
    String sample = "HG01762";

    TileDBVCFHailReader reader =
        TileDBVCFHailReader.build(
            testSampleDataset(sample), testVCFUri("ingested_2samples"), Option.apply(sample));

    MatrixRead r = new MatrixRead(reader.fullMatrixType(), false, false, reader);
    TableIR mt = new MatrixRowsTable(r);

    TableValue tv = Interpret.apply(mt, ctx);

    List<GenericRow> rows =
        tv.rvd()
            .toRows()
            .toJavaRDD()
            .map(
                x -> {
                  return new GenericRow(new Object[] {x.get(0), x.get(1)});
                })
            .collect();

    List<GenericRow> expectedRows =
        testSampleDataset(sample)
            .select("contig", "posStart", "alleles")
            .javaRDD()
            .map(
                x -> {
                  return new GenericRow(
                      new Object[] {Locus.apply((String) x.get(0), (int) x.get(1)), x.get(2)});
                })
            .collect();

    Assert.assertArrayEquals(expectedRows.toArray(), rows.toArray());

    // Test sample HG00280
    sample = "HG00280";

    reader =
        TileDBVCFHailReader.build(
            testSampleDataset(sample), testVCFUri("ingested_2samples"), Option.apply(sample));

    r = new MatrixRead(reader.fullMatrixType(), false, false, reader);
    mt = new MatrixRowsTable(r);

    tv = Interpret.apply(mt, ctx);

    rows =
        tv.rvd()
            .toRows()
            .toJavaRDD()
            .map(
                x -> {
                  return new GenericRow(new Object[] {x.get(0), x.get(1)});
                })
            .collect();

    expectedRows =
        testSampleDataset(sample)
            .select("contig", "posStart", "alleles")
            .javaRDD()
            .map(
                x -> {
                  return new GenericRow(
                      new Object[] {Locus.apply((String) x.get(0), (int) x.get(1)), x.get(2)});
                })
            .collect();

    Assert.assertArrayEquals(expectedRows.toArray(), rows.toArray());
  }
}
