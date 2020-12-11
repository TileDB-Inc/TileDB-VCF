package io.tiledb.vcf

import java.net.URI

import is.hail.expr.SparkAnnotationImpex
import is.hail.expr.ir.{ExecuteContext, MatrixHybridReader, TableRead, TableValue}
import is.hail.types.physical.{PStruct, PType}
import is.hail.types.virtual.{TArray, TLocus, TString, TStruct}
import is.hail.types.{MatrixType, TableType}
import is.hail.variant.{Locus, ReferenceGenome}
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable


class TileDBHailVCFReader(val uri: URI, val options: VCFDataSourceOptions, val df: DataFrame) extends MatrixHybridReader {
  val reader: VCFDataSourceReader = new VCFDataSourceReader(uri, options)
  val inputPartitions = reader.planBatchInputPartitions();

//  val types = df.schema.fields.slice(1, 2).map(field => (field.name -> SparkAnnotationImpex.importType(field.dataType).virtualType))
//  val types = df.schema.fields.map(field => (field.name -> SparkAnnotationImpex.importType(field.dataType).virtualType))
//  val rowType = TStruct(types: _*)

  override def pathsUsed: Seq[String] = Seq.empty

  override def columnCount: Option[Int] = None

  override def partitionCounts: Option[IndexedSeq[Long]] = None

  val fullMatrixType: MatrixType = MatrixType(
    TStruct.empty,
    colType = TStruct("s" -> TString),
    colKey = Array("s"),
    rowType = TStruct(
      "locus" -> TLocus(ReferenceGenome.GRCh37),
      "alleles" -> TArray(TString)),
    rowKey = Array("locus", "alleles"),
    entryType = TStruct.empty)

  override def apply(tr: TableRead, ctx: ExecuteContext): TableValue = {
    val tt = TableType(fullMatrixType.rowType, fullMatrixType.rowType.fieldNames, TStruct.empty)

    val rdd = df.select("contig", "posStart", "alleles").rdd.map{
      r => Row(Locus(r.getString(0), r.getInt(1)), r.get(2).asInstanceOf[mutable.WrappedArray[Int]].toIndexedSeq)
    }
    TableValue(ctx, tt.rowType, tt.key, rdd)
  }

  override def rowAndGlobalPTypes(ctx: ExecuteContext, requestedType: TableType): (PStruct, PStruct) = {
    requestedType.canonicalRowPType -> PType.canonical(requestedType.globalType).asInstanceOf[PStruct]
  }
}