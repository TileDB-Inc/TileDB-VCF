package io.tiledb.vcf.hail

import java.nio.file.Paths
import io.tiledb.vcf.TileDBHailVCFReader
import is.hail.HailContext
import is.hail.annotations.{Region, RegionPool}
import is.hail.backend.spark.SparkBackend
import is.hail.expr.ir.{ExecuteContext, Interpret, MatrixIR, MatrixRead, MatrixRowsTable, Ref, SelectFields, TableCount, TableIR, TableJoin, TableMapRows, TableRead, TableValue}
import is.hail.io.fs.HadoopFS
import is.hail.io.vcf.MatrixVCFReader
import is.hail.types.virtual.TFloat64
import is.hail.utils.{ExecutionTimer, SerializableHadoopConfiguration, TextInputFilterAndReplace, fatal, partition, using}
import is.hail.variant.ReferenceGenome
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Level
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.SparkSession
import org.scalatest.Assertions.assertResult
import org.scalatest.funsuite.AnyFunSuite
import org.testng.annotations.Test

class TileDBHailVCFReaderTests {
  val sparkSession = SparkSession
    .builder()
    .master("local[1]")
    .appName("hail-integration")
    .getOrCreate()

  val sc = sparkSession.sparkContext
  val sb = new SparkBackend("/tmp", "file:///tmp", sc)
  val hc: HailContext = HailContext.apply(sb, "/tmp/hail.log", false, false, 50, false, 3)

  val ctx = new ExecuteContext("/tmp", "file:///tmp", hc.backend,
    new HadoopFS(new SerializableHadoopConfiguration(new Configuration)),
    Region.apply(Region.REGULAR, RegionPool.apply(false)),
    new ExecutionTimer("HailSuite"))

  private def testTileDBVCFURI(sampleGroupName: String) = {
    val arraysPath = Paths.get("src", "test", "resources", sampleGroupName)
    "file://".concat(arraysPath.toAbsolutePath.toString)
  }

  private def testVCFURI(sampleGroupName: String) = {
    val arraysPath = Paths.get("src", "test", "resources", sampleGroupName)
    arraysPath.toAbsolutePath.toString
  }

  private def testSampleDataset(array: String, samples: Seq[String]) = {
    val dfRead = sparkSession.read.format("io.tiledb.vcf")
      .option("uri", testTileDBVCFURI(array))
      .option("samples", samples.reduce(_ + "," + _))
      //.option("ranges", "1:12100-13360,1:13500-17350")
      .option("tiledb.vfs.num_threads", 1)
      .option("tiledb_stats_log_level", Level.INFO.toString).load

    dfRead
  }

  def importVCF(ctx: ExecuteContext, file: String, force: Boolean = false,
                forceBGZ: Boolean = false,
                headerFile: Option[String] = None,
                nPartitions: Option[Int] = None,
                blockSizeInMB: Option[Int] = None,
                minPartitions: Option[Int] = None,
                dropSamples: Boolean = false,
                callFields: Set[String] = Set.empty[String],
                rg: Option[ReferenceGenome] = Some(ReferenceGenome.GRCh37),
                contigRecoding: Option[Map[String, String]] = None,
                arrayElementsRequired: Boolean = true,
                skipInvalidLoci: Boolean = false,
                partitionsJSON: String = null): MatrixIR = {
    rg.foreach { referenceGenome =>
      ReferenceGenome.addReference(referenceGenome)
    }
    val entryFloatType = TFloat64._toPretty

    val reader = MatrixVCFReader(ctx,
      Array(file),
      callFields,
      entryFloatType,
      headerFile,
      nPartitions,
      blockSizeInMB,
      minPartitions,
      rg.map(_.name),
      contigRecoding.getOrElse(Map.empty[String, String]),
      arrayElementsRequired,
      skipInvalidLoci,
      forceBGZ,
      force,
      TextInputFilterAndReplace(),
      partitionsJSON)
    MatrixRead(reader.fullMatrixType, dropSamples, false, reader)
  }

  def testMethod() = 0

  @Test def tileDBVCFHailCompare() {
    println(testVCFURI("hail_vcf/regressionLogistic.vcf"))
    val vcf = importVCF(ctx, testVCFURI("hail_vcf/regressionLogistic/G.vcf"))
    val t1: TableIR = MatrixRowsTable(vcf)


    val reader = TileDBHailVCFReader.build(testVCFURI("hail_vcf/regressionLogistic/regressionLogistic") , Some("G"))
    val tileDBMatrixRead = MatrixRead(reader.fullMatrixType, false, false, reader)
    val tileDBMRT = MatrixRowsTable(tileDBMatrixRead)

    val tv1 = Interpret.apply(t1, ctx)
    val tileDBTV = Interpret.apply(tileDBMRT, ctx)

    val expected = tv1.rvd.toRows.collect().map(x =>
      new GenericRow(Array(x.get(0), x.get(1))))
    val actual = tileDBTV.rvd.toRows.collect()

    assertResult(expected.size)(actual.size)
    assertResult(expected)(actual)
  }

  @Test def tileDBVCFHailCompareHailDataset() {
    val vcf = importVCF(ctx, testVCFURI("hail_vcf/pedigree/vcf/Dad1.vcf"))
    val t1: TableIR = MatrixRowsTable(vcf)
    val tv1 = Interpret.apply(t1, ctx)

    val reader = TileDBHailVCFReader.build(testVCFURI("hail_vcf/pedigree/tiledb/pedigree"), Some("Dad1"))

    val tileDBMatrixRead = MatrixRead(reader.fullMatrixType, false, false, reader)
    val tileDBMRT = MatrixRowsTable(tileDBMatrixRead)
    val tileDBTV = Interpret.apply(tileDBMRT, ctx)

    val expected = tv1.rvd.toRows.collect().map(x =>
      new GenericRow(Array(x.get(0), x.get(1))))

    val actual = tileDBTV.rvd.toRows.collect()

    assertResult(expected.size)(actual.size)
    assertResult(expected)(actual)
  }

  @Test def tileDBVCFHailJoin() {
    val vcf = importVCF(ctx, testVCFURI("small.vcf"))
    val hailMRT: TableIR = MatrixRowsTable(vcf)
    val hailTV = Interpret.apply(hailMRT, ctx)

    val reader = TileDBHailVCFReader.build(testTileDBVCFURI("arrays/v3/ingested_2samples"), Some("HG01762"))
    val tileDBMatrixRead = MatrixRead(reader.fullMatrixType, false, false, reader)
    val tileDBMRT = MatrixRowsTable(tileDBMatrixRead)
    val tileDBTV = Interpret.apply(tileDBMRT, ctx)

    val hail = TableMapRows(hailMRT, SelectFields(Ref("row", hailMRT.typ.rowType), Seq("locus", "alleles")))
    val tiledb = TableMapRows(tileDBMRT, SelectFields(Ref("row", tileDBMRT.typ.rowType), Seq("locus", "alleles")))

    val join: TableIR = TableJoin(hail, tiledb, "inner", 2)
    val joinResultTV = Interpret.apply(join, ctx)

    assertResult(joinResultTV.rvd.count())(hailTV.rvd.count())
    assertResult(joinResultTV.rvd.count())(tileDBTV.rvd.count())
  }
//
//  test("TileDBVCFHailJoin2") {
//    val vcf = importVCF(ctx, testVCFURI("hail_vcf/pedigree/vcf/Dad1.vcf"))
//    val hailMRT: TableIR = MatrixRowsTable(vcf)
//
//    val samples = Option("Mom1")
//    val reader = TileDBHailVCFReader.build(testTileDBVCFURI("hail_vcf/pedigree/tiledb/pedigree"), samples)
//    val tileDBMatrixRead = MatrixRead(reader.fullMatrixType, false, false, reader)
//    val tileDBMRT = MatrixRowsTable(tileDBMatrixRead)
//
//    val hail = TableMapRows(hailMRT, SelectFields(Ref("row", hailMRT.typ.rowType), Seq("locus", "alleles")))
//    val tiledb = TableMapRows(tileDBMRT, SelectFields(Ref("row", tileDBMRT.typ.rowType), Seq("locus", "alleles")))
//    val hailTableMapRowsTV = Interpret.apply(hail, ctx)
//    val tileDBTableMapRowsTV = Interpret.apply(tiledb, ctx)
//
//    val join: TableIR = TableJoin(hail, tiledb, "inner", 2)
//    val joinResultTV = Interpret.apply(join, ctx)
//
//    val intersection = tileDBTableMapRowsTV.rvd.toRows.intersection(hailTableMapRowsTV.rvd.toRows)
//
//    assertResult(joinResultTV.rvd.toRows.collect().toSet)(intersection.collect().toSet)
//  }
}
