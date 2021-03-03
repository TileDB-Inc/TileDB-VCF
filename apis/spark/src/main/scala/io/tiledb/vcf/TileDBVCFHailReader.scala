package io.tiledb.vcf

import is.hail.annotations.BroadcastRow
import is.hail.expr.ir.{ExecuteContext, LowerMatrixIR, MatrixHybridReader, TableRead, TableValue}
import is.hail.rvd.{RVD, RVDType}
import is.hail.sparkextras.ContextRDD
import is.hail.types.physical.{PCanonicalCall, PCanonicalString, PField, PFloat32, PInt32, PStruct, PType}
import is.hail.types.virtual._
import is.hail.types.{MatrixType, TableType}
import is.hail.utils.{FastIndexedSeq, toRichContextRDDRow, toRichIterable}
import is.hail.variant.{Call, Call2, Locus, ReferenceGenome}
import org.apache.spark.sql.{Column, DataFrame, Row, functions}
import org.json4s.JsonAST.JValue
import org.json4s.{DefaultFormats, Formats}

import java.util.Optional
import scala.collection.mutable


class TileDBHailVCFReader() extends MatrixHybridReader {
  override def pathsUsed: Seq[String] = Seq.empty
  override def columnCount: Option[Int] = None
  override def partitionCounts: Option[IndexedSeq[Long]] = None

  var df: DataFrame = null
  var uri: String = null
  var samples: Option[String] = Option.empty

  //  val sampleList = {
  //    if (samples.isDefined) samples.get.split(",")
  //    else Array[String]()
  //  }
  //
  //  val vcfReader: VCFReader = new VCFReader(uri, sampleList, Optional.empty(), Optional.empty())
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
  //  val info = vcfReader.infoAttributes.keySet.toArray.map{ infoAttr =>
  //    val key = infoAttr.toString
  //    val dt = {
  //      if (key.contains("GT"))
  //        PCanonicalCall(false)
  //      else {
  //        vcfReader.infoAttributes.get(key).datatype match {
  //          case AttributeDatatype.UINT8 => PInt32()
  //          case AttributeDatatype.INT32 => PInt32()
  //          case AttributeDatatype.CHAR => PCanonicalString()
  //          case AttributeDatatype.FLOAT32 => PFloat32()
  //        }
  //      }
  //    }
  //    (infoAttr.toString.split("_")(1) -> dt.virtualType)
  //  }

  val entryType = TStruct("DP" -> PInt32().virtualType, "GT" -> PCanonicalCall().virtualType, "PL" -> TArray(PInt32().virtualType))

  val fullMatrixType: MatrixType = MatrixType(
    globalType = TStruct.empty,
    colType = TStruct("s" -> TString),
    colKey = Array("s"),
    rowType = TStruct(
      "locus" -> TLocus(ReferenceGenome.GRCh37),
      "alleles" -> TArray(TString)),
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

    var projection = df.groupBy("contig","posStart", "alleles")
      .agg(functions.collect_list("genotype").as("GT"),
        functions.collect_list("fmt_DP").as("DP"),
        functions.collect_list("fmt_PL").as("PL"))

    var columns = Seq[String]("contig", "posStart", "alleles")

    val infoFields = Seq("GT", "DP", "PL")

    infoFields.foreach(infoField => if (entryType.hasField(infoField)) columns = columns :+ infoField)

    projection = projection.select(columns.map(new Column(_)) :  _ *)

    val rdd = projection.rdd.map{
      row => {
        val locus = Locus(row.getString(0), row.getInt(1))
        val alleles = row.get(2)

        var rowFields = Seq(locus, alleles)
        
        // Parse Entry fields (GT, DP and PL)
        var idx = rowFields.size + 1
        entryType.fieldNames.foreach { fieldName =>
          fieldName match {
            case "GT" => {
              val GT = row.get(idx).asInstanceOf[mutable.WrappedArray[mutable.WrappedArray[Int]]]
                .map(x => Row(Call2(x(0), x(1))))
              rowFields = rowFields :+ GT
            }
            case "DP" => {
              val DP = row.get(idx).asInstanceOf[mutable.WrappedArray[Int]]
                .map(x => Row(x))
              rowFields = rowFields :+ DP
            }
            case "PL" => {
              val PL = row.get(idx).asInstanceOf[mutable.WrappedArray[mutable.WrappedArray[Int]]]
                .map(x => Row(x))
              rowFields = rowFields :+ PL
            }
            case _ => {}
          }
          idx += 1
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

    val reader = new TileDBHailVCFReader()

    reader.uri = params.uri
    reader.samples = Option(params.samples)

  }

  def build(uri: String, samples: Option[String]): TileDBHailVCFReader = {
    val reader = new TileDBHailVCFReader
    reader.uri = uri
    reader.samples = samples

    reader
  }

  def build(df: DataFrame): TileDBHailVCFReader = {
    val reader = new TileDBHailVCFReader()
    reader.df = df

    reader
  }

  def build(uri: String): TileDBHailVCFReader = {
    build(uri, Option.empty)
  }
}