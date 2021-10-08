package io.tiledb.vcf

import java.util.Optional

import io.tiledb.libvcfnative.VCFReader
import io.tiledb.libvcfnative.VCFReader.AttributeDatatype
import is.hail.annotations.BroadcastRow
import is.hail.expr.ir.{ExecuteContext, LowerMatrixIR, MatrixHybridReader, TableRead, TableValue}
import is.hail.rvd.RVD
import is.hail.sparkextras.ContextRDD
import is.hail.types.physical.{PCanonicalCall, PCanonicalSet, PCanonicalString, PCanonicalStruct, PField, PFloat32, PFloat64, PInt32, PStruct, PType}
import is.hail.types.virtual._
import is.hail.types.{MatrixType, TableType}
import is.hail.utils.{FastIndexedSeq, toRichContextRDDRow, toRichIterable}
import is.hail.variant.{Call2, Locus, ReferenceGenome}
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.json4s.JsonAST.JValue
import org.json4s.{DefaultFormats, Formats}

import scala.collection.mutable


class TileDBVCFHailReader(var uri: String = null, var samples: Option[String] = Option.empty, var df: DataFrame = null) extends MatrixHybridReader {
  override def pathsUsed: Seq[String] = Seq.empty
  override def columnCount: Option[Int] = None
  override def partitionCounts: Option[IndexedSeq[Long]] = None

  //  var df: DataFrame = dataframe

  val sampleList = {
    if (samples != null && samples.isDefined) samples.get.split(",")
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

  val fmt = vcfReader.fmtAttributes.keySet.toArray.map{ fmtAttr =>
    val key = fmtAttr.toString
    val dt = {
      if (key.contains("GT"))
        PCanonicalCall(false)
      else {
        vcfReader.fmtAttributes.get(key).datatype match {
          case AttributeDatatype.UINT8 => PInt32()
          case AttributeDatatype.INT32 => PInt32()
          case AttributeDatatype.CHAR => PCanonicalString()
          case AttributeDatatype.FLOAT32 => PFloat32()
        }
      }
    }
    (fmtAttr.toString.split("_")(1) -> dt.virtualType)
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
//      "locus" -> TLocus(ReferenceGenome.GRCh38),
      "locus" -> TLocus.schemaFromRG(None),
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

    var projection = df

    entryType.fieldNames.foreach { fieldName =>
      fieldName match {
        case "genotype" => projection = projection.withColumnRenamed("genotype", "GT")
        case _ => projection = projection.withColumnRenamed(s"info_${fieldName}", fieldName)
      }
    }

    // We extract contig and posStart in order to construct the locus field as a pair of the first two
    // The alleles field is also required to execute the basic operations in a matrix table
    var columns = Seq[String]("contig", "posStart", "alleles")

    // Construct a map of info fields and row indices
    var idx = 3
    val infoFieldIdx: mutable.HashMap[String, Int] = mutable.HashMap()
    val fmtFieldIdx: mutable.HashMap[String, Int] = mutable.HashMap()

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

    // Check if the row has an "fmt" field
    val hasFmt = rowType.hasField("info")
    val fmtFieldNames = fmt.map(_._1)
    if (hasFmt) {
      fmt.foreach { fmtField =>
        columns = columns :+ s"fmt_${fmtField._1}"
        fmtFieldIdx.put(fmtField._1, idx)
        idx += 1
      }
    }

    val hasRsid = rowType.hasField("rsid")
    val hasQual = rowType.hasField("qual")
    val hasFilter = rowType.hasField("filters")

    // Parse info entry fields
    infoFieldNames.foreach(infoField =>
      if (entryType.hasField(infoField)){
        columns = columns :+ infoField
        infoFieldIdx.put(infoField, idx)
        idx += 1
      })

    // Parse fmt entry fields
    fmtFieldNames.foreach(fmtField =>
      if (entryType.hasField(fmtField)){
        columns = columns :+ fmtField
        fmtFieldIdx.put(fmtField, idx)
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

        if (hasFmt) {
          rowFields = rowFields :+ Row(fmtFieldNames.map { x =>
            val idx = fmtFieldIdx.get(x).get
            val v = row.get(idx)
            v
          }.toSeq: _*)
        }

        // Parse Entry fields (GT, DP and PL)
        val entryValuesInfo = entryType.fieldNames.map { fieldName =>
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

        val entryValuesFmt = entryType.fieldNames.map { fieldName =>
          val rid = fmtFieldIdx.get(fieldName).get
          fieldName match {
            case "GT" => {
              val r = row.get(rid).asInstanceOf[mutable.WrappedArray[Int]]
              val GT = Call2(r(0), r(1))
              GT
            }
            case _ => row.get(rid)
          }
        }

        if (entryValuesInfo.nonEmpty) {
          rowFields = rowFields :+ mutable.WrappedArray.make(Array(Row(entryValuesInfo :_* )))
        }

        if (entryValuesFmt.nonEmpty) {
          rowFields = rowFields :+ mutable.WrappedArray.make(Array(Row(entryValuesFmt :_* )))
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

    val reader = new TileDBVCFHailReader(params.uri, Option(params.samples))

    reader
  }

  def build(uri: String, samples: Option[String]): TileDBVCFHailReader = {
    val reader = new TileDBVCFHailReader(uri, samples)

    reader
  }

  def build(df: DataFrame, uri: String, samples: Option[String]): TileDBVCFHailReader = {
    val reader = new TileDBVCFHailReader(uri, samples, df)
    //    reader.df = df

    reader
  }

  def build(uri: String): TileDBVCFHailReader = {
    build(uri, Option.empty)
  }
}