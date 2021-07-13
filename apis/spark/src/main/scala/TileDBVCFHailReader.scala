package io.tiledb.vcf

import is.hail.annotations.BroadcastRow
import is.hail.expr.ir.{ExecuteContext, LowerMatrixIR, MatrixHybridReader, TableRead, TableValue}
import is.hail.rvd.{RVD, RVDType}
import is.hail.sparkextras.ContextRDD
import is.hail.types.physical.{PCanonicalCall, PCanonicalSet, PCanonicalString, PCanonicalStruct, PField, PFloat32, PFloat64, PInt32, PStruct, PType}
import is.hail.types.virtual._
import is.hail.types.{MatrixType, TableType}
import is.hail.utils.{FastIndexedSeq, toRichContextRDDRow, toRichIterable}
import is.hail.variant.{Call, Call2, Locus, ReferenceGenome}
import org.apache.spark.sql.{Column, DataFrame, Row, functions}
import org.json4s.JsonAST.JValue
import org.json4s.{DefaultFormats, Formats}
import io.tiledb.libvcfnative.VCFReader
import io.tiledb.libvcfnative.VCFReader.AttributeDatatype

import java.util.Optional
import scala.collection.mutable


class TileDBHailVCFReader(var uri: String = null, var samples: Option[String] = Option.empty) extends MatrixHybridReader {
  override def pathsUsed: Seq[String] = Seq.empty
  override def columnCount: Option[Int] = None
  override def partitionCounts: Option[IndexedSeq[Long]] = None

  var df: DataFrame = null

  //  val fmt = vcfReader.fmtAttributes.keySet.toArray.map{ fmtAttr =>
  //    val key = fmtAttr.toString
  //    val dt = {
  //      if (key.contains("GT")) {
  //        PCanonicalCall()
  //      }
  //      else {
  //        vcfReader.fmtAttributes.get(key).datatype match {
  //          case AttributeDatatype.UINT8 => PInt32()
  //          case AttributeDatatype.INT32 => PInt32()
  //          case AttributeDatatype.CHAR => PCanonicalString()
  //          case AttributeDatatype.FLOAT32 => PFloat32()
  //        }
  //      }
  //    }
  //    (fmtAttr.toString.split("_")(1) -> dt.virtualType)
  //  }
  //

  val sampleList = {
    if (samples.isDefined) samples.get.split(",")
    else Array[String]()
  }

  val entryType = TStruct(
    "AD" -> TArray(PInt32().virtualType),
    "DP" -> PInt32().virtualType,
    "GQ" -> PInt32().virtualType,
    "GT" -> PCanonicalCall().virtualType,
    "MIN_DP" -> PInt32().virtualType,
    "PL" -> TArray(PInt32().virtualType),
    "SB" -> TArray(PInt32().virtualType))

  val vcfReader: VCFReader = new VCFReader(uri, sampleList, Optional.empty(), Optional.empty())

  val info = vcfReader.infoAttributes.keySet.toArray.map{ infoAttr =>
    val key = infoAttr.toString
    val dt = {
      if (key.contains("GT"))
        PCanonicalCall(false)
      else {
        vcfReader.infoAttributes.get(key).datatype match {
          case AttributeDatatype.UINT8 => PInt32()
          case AttributeDatatype.INT32 => PInt32()
          case AttributeDatatype.CHAR => PCanonicalString()
          case AttributeDatatype.FLOAT32 => PFloat32()
        }
      }
    }
    (infoAttr.toString.split("_")(1) -> dt.virtualType)
  }

  val vaSignature = PCanonicalStruct(Array(
    PField("rsid", PCanonicalString(), 0),
    PField("qual", PFloat64(), 1),
    PField("filters", PCanonicalSet(PCanonicalString(true)), 2)), true)

  val fullMatrixType: MatrixType = MatrixType(
    globalType = TStruct.empty,
    colType = TStruct("s" -> TString),
    colKey = Array("s"),
    rowType = TStruct(
      "locus" -> TLocus(ReferenceGenome.GRCh37),
      "alleles" -> TArray(TString)) ++ vaSignature.virtualType ++ (TStruct("info" -> TStruct(info:_*))),
    rowKey = Array("locus", "alleles"),
    entryType = entryType)

  override def apply(tr: TableRead, ctx: ExecuteContext): TableValue = {
    val requestedType = tr.typ
    val rowType = tr.typ.rowType

    val entryType: TStruct = rowType.fieldOption(LowerMatrixIR.entriesFieldName) match {
      case Some(entriesArray) => entriesArray.typ.asInstanceOf[TArray].elementType.asInstanceOf[TStruct]
      case None => TStruct.empty
    }

    if (df == null) {
      df = ctx.backend.asSpark("spark")
        .sparkSession.read
        .format("io.tiledb.vcf")
        .option("samples", samples.get)
        .option("uri", uri)
        .load()
    }

    //    var projection = df.groupBy("contig","posStart", "alleles")
    //      .agg(functions.collect_list("genotype").as("GT"),
    //        functions.collect_list("fmt_DP").as("DP"),
    //        functions.collect_list("fmt_PL").as("PL"))

    var projection = df
      .withColumnRenamed("fmt_AD", "AD")
      .withColumnRenamed("fmt_DP", "DP")
      .withColumnRenamed("fmt_GQ", "GQ")
      .withColumnRenamed("genotype", "GT")
      .withColumnRenamed("fmt_MIN_DP", "MIN_DP")
      .withColumnRenamed("fmt_PL", "PL")
      .withColumnRenamed("fmt_SB", "SB")


    var columns = Seq[String]("contig", "posStart", "alleles")

    // Construct a map of info fields and row indices
    var idx = 3
    val infoFieldIdx: mutable.HashMap[String, Int] = mutable.HashMap()

    // Check if the row has an "info" field
    val hasInfo = rowType.hasField("info")
    val infoFieldNames = info.map(_._1)
    if (hasInfo) {
      info.foreach { infoField =>
        columns = columns :+ s"info_${infoField._1}"
        infoFieldIdx.put(infoField._1, idx)
        idx += 1
      }
    }

    val hasRsid = rowType.hasField("rsid")
    val hasQual = rowType.hasField("qual")
    val hasFilter = rowType.hasField("filters")

    // Parse info entry fields
    val infoFields = Seq("AD", "DP", "GQ", "GT", "MIN_DP", "PL", "SB")
    infoFields.foreach(infoField =>
      if (entryType.hasField(infoField)){
        columns = columns :+ infoField
        infoFieldIdx.put(infoField, idx)
        idx += 1
      })

    // Add all the columns
    projection = projection.select(columns.map(new Column(_)) :  _ *)

    val rdd = projection.rdd.map{
      row => {
        val locus = Locus(row.getString(0), row.getInt(1))
        val alleles = row.get(2)

        // Set locus and alleles
        var rowFields = Seq(locus, alleles)

        if (hasRsid)
          rowFields = rowFields :+ ""
        if (hasQual)
          rowFields = rowFields :+ 0.0
        if (hasFilter)
          rowFields = rowFields :+ Set.empty

        if (hasInfo) {
          rowFields = rowFields :+ Row(infoFieldNames.map { x =>
            val idx = infoFieldIdx.get(x).get
            val v = row.get(idx)
            v
          }.toSeq: _*)
        }

        // Parse Entry fields (GT, DP and PL)
        val entryValues = entryType.fieldNames.map { fieldName =>
          val rid = infoFieldIdx.get(fieldName).get
          fieldName match {
            case "GT" => {
              val r = row.get(rid).asInstanceOf[mutable.WrappedArray[Int]]
              val GT = Call2(r(0), r(1))
              GT
            }
            case _ => row.get(rid)
          }
        }

        if (entryValues.nonEmpty) {
          rowFields = rowFields :+ mutable.WrappedArray.make(Array(Row(entryValues :_* )))
        }

        Row(rowFields : _ *)
      }
    }

    val tt = fullMatrixType.toTableType(LowerMatrixIR.entriesFieldName, LowerMatrixIR.colsFieldName)

    val globals = {
      if (samples.isDefined)
        Row(samples.get.split(",").map(Row(_)).toFastIndexedSeq)
      else
        Row(FastIndexedSeq.empty)
    }
    val g = { (requestedGlobalsType: Type) =>
      val subset = tt.globalType.valueSubsetter(requestedGlobalsType)
      subset(globals).asInstanceOf[Row]
    }

    val crdd = ContextRDD.weaken(rdd).toRegionValues(requestedType.canonicalRowPType)
    val rvd = RVD.coerce(ctx, requestedType.canonicalRVDType, crdd)

    val tv = TableValue(ctx,
      requestedType,
      BroadcastRow(ctx, g(requestedType.globalType), requestedType.globalType),
      rvd)

    tv
  }

  override def rowAndGlobalPTypes(ctx: ExecuteContext, requestedType: TableType): (PStruct, PStruct) = {
    requestedType.canonicalRowPType -> PType.canonical(requestedType.globalType).asInstanceOf[PStruct]
  }
}

case class TileDBHail(uri: String, samples: String)

object TileDBHailVCFReader {
  def fromJValue(jv: JValue) = {
    implicit val formats: Formats = DefaultFormats
    val params = jv.extract[TileDBHail]

    val reader = new TileDBHailVCFReader(params.uri, Option(params.samples))

    reader
  }

  def build(uri: String, samples: Option[String]): TileDBHailVCFReader = {
    val reader = new TileDBHailVCFReader(uri, samples)

    reader
  }

  def build(df: DataFrame, uri: String, samples: Option[String]): TileDBHailVCFReader = {
    val reader = new TileDBHailVCFReader(uri, samples)
    reader.df = df

    reader
  }

  def build(uri: String): TileDBHailVCFReader = {
    build(uri, Option.empty)
  }
}