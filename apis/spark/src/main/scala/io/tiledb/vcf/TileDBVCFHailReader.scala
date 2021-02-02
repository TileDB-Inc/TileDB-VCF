package io.tiledb.vcf

import io.tiledb.libvcfnative.VCFReader
import io.tiledb.libvcfnative.VCFReader.AttributeDatatype
import is.hail.annotations.BroadcastRow

import java.net.URI
import is.hail.expr.SparkAnnotationImpex
import is.hail.expr.ir.{ExecuteContext, LowerMatrixIR, MatrixHybridReader, TableRead, TableValue}
import is.hail.io.vcf.MatrixVCFReaderParameters
import is.hail.rvd.{RVD, RVDType}
import is.hail.sparkextras.ContextRDD
import is.hail.types.physical.{PCanonicalCall, PCanonicalString, PFloat32, PInt32, PStruct, PType}
import is.hail.types.virtual.{TArray, TCall, TLocus, TString, TStruct, Type}
import is.hail.types.{MatrixType, TableType}
import is.hail.utils.{toRichContextRDDRow, toRichIterable}
import is.hail.variant.{Locus, ReferenceGenome}
import org.apache.spark.sql.{DataFrame, Row}
import org.json4s.{DefaultFormats, Formats}
import org.json4s.JsonAST.JValue

import java.util.Optional
import scala.collection.mutable


class TileDBHailVCFReader(val uri: String, val samples: String) extends MatrixHybridReader {
  override def pathsUsed: Seq[String] = Seq.empty
  override def columnCount: Option[Int] = None
  override def partitionCounts: Option[IndexedSeq[Long]] = None

  val vcfReader: VCFReader = new VCFReader(uri, samples.split(","), Optional.empty(), Optional.empty())

  val fmt = vcfReader.fmtAttributes.keySet.toArray.map{ fmtAttr =>
    val key = fmtAttr.toString
    val dt = {
      if (key.contains("GT")) {
        PCanonicalCall()
      }
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

  //val entryType = TStruct((fmt):_ *)
  //val entryType = TStruct(Array("GT" -> PCanonicalCall().virtualType):_*)
  //val entryType = TStruct("GT" -> TCall)
  val entryType = TStruct("DP" -> PInt32().virtualType, "PL" -> PInt32().virtualType)

  val fullMatrixType: MatrixType = MatrixType(
    globalType = TStruct.empty,
    colType = TStruct("s" -> TString),
    colKey = Array("s"),
    rowType = TStruct(
      "locus" -> TLocus(ReferenceGenome.GRCh37),
      "alleles" -> TArray(TString),
      "info" -> entryType),
    rowKey = Array("locus", "alleles"),
    entryType = entryType)

  val str = ""

  override def apply(tr: TableRead, ctx: ExecuteContext): TableValue = {
    val df = ctx.backend.asSpark("spark")
      .sparkSession.read
      .format("io.tiledb.vcf")
      .option("samples", samples)
      .option("uri", uri)
      .load()

    //val tt = TableType(fullMatrixType.rowType, fullMatrixType.rowType.fieldNames, TStruct.empty)
    val tt = fullMatrixType.toTableType(LowerMatrixIR.entriesFieldName, LowerMatrixIR.colsFieldName)

    val rdd = df.select("contig", "posStart", "alleles", "fmt_DP", "fmt_PL").rdd.map{
      //      r => Row(Locus(r.getString(0), r.getInt(1)), r.get(2).asInstanceOf[mutable.WrappedArray[Int]].toIndexedSeq, IndexedSeq(Row(r.get(3), 1, 1, 1, 1, 1, 1)))
      r => Row(Locus(r.getString(0), r.getInt(1)), r.get(2).asInstanceOf[mutable.WrappedArray[Int]].toIndexedSeq, Row(1, 1))
    }

    TableValue(ctx, tr.typ.rowType, tr.typ.key, rdd)
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

    new TileDBHailVCFReader(params.uri, params.samples)
  }

  def apply(ctx: ExecuteContext, uri: String, samples: String): TileDBHailVCFReader = {
    new TileDBHailVCFReader(uri, samples)
  }
}