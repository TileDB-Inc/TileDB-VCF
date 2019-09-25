package io.tiledb.vcf;

import java.io.Serializable;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class VCFSparkSchema implements Serializable {

  private StructType defaultSchema;
  private StructType pushDownSchema;

  public VCFSparkSchema() {
    this.defaultSchema = defaultSparkSchema();
  }

  public VCFSparkSchema setPushDownSchema(StructType pushDownSchema) {
    this.pushDownSchema = pushDownSchema;
    return this;
  }

  public StructType getSparkSchema() {
    if (pushDownSchema == null) {
      return defaultSchema;
    }
    return pushDownSchema;
  }

  public StructField[] getSparkFields() {
    return getSparkSchema().fields();
  }

  public String[] getVCFAttributes() {
    int numAttr = getNumColumns();
    StructField[] fields = getSparkFields();
    String[] vcfAttrNames = new String[numAttr];
    for (int i = 0; i < numAttr; i++) {
      vcfAttrNames[i] = fieldToVCFAttribute(fields[i]);
    }
    return vcfAttrNames;
  }

  public int getNumColumns() {
    return getSparkFields().length;
  }

  private String fieldToVCFAttribute(StructField field) {
    String fieldName = field.name();
    switch (fieldName) {
      case "sampleName":
        return "sample_name";
      case "contig":
        return "contig";
      case "posStart":
        return "pos_start";
      case "posEnd":
        return "pos_end";
      case "alleles":
        return "alleles";
      case "filter":
        return "filters";
      case "genotype":
        return "fmt_GT";
      case "fmt":
        return "fmt";
      case "fmt_AD":
        return "fmt_AD";
      case "fmt_DP":
        return "fmt_DP";
      case "fmt_GQ":
        return "fmt_GQ";
      case "fmt_MIN_DP":
        return "fmt_MIN_DP";
      case "info":
        return "info";
      case "queryBedStart":
        return "query_bed_start";
      case "queryBedEnd":
        return "query_bed_end";
      default:
        throw new RuntimeException("Unknown VCF schema field name: " + fieldName);
    }
  }

  private StructType defaultSparkSchema() {
    MetadataBuilder metadata = new MetadataBuilder();
    StructType schema = new StructType();
    // sampleName
    metadata.putString("comment", "sampleName");
    schema =
        schema.add(new StructField("sampleName", DataTypes.StringType, false, metadata.build()));
    // CHR field in BCF
    metadata = new MetadataBuilder();
    metadata.putString("comment", "CHR field in BCF");
    schema = schema.add(new StructField("contig", DataTypes.StringType, false, metadata.build()));
    // POS field in BCF
    metadata = new MetadataBuilder();
    metadata.putString("comment", "POS field in BCF");
    schema =
        schema.add(new StructField("posStart", DataTypes.IntegerType, false, metadata.build()));
    // POS + END -1 if END is defined, else POS
    metadata = new MetadataBuilder();
    metadata.putString("comment", "POS + END -1 if END is defined, else POS");
    schema = schema.add(new StructField("posEnd", DataTypes.IntegerType, false, metadata.build()));
    // List of REF, ALT fields in BCF
    metadata = new MetadataBuilder();
    metadata.putString("comment", "List of REF, ALT fields in BCF");
    schema =
        schema.add(
            new StructField(
                "alleles",
                DataTypes.createArrayType(DataTypes.StringType, false),
                false,
                metadata.build()));
    // FILTER field in BCF
    metadata = new MetadataBuilder();
    metadata.putString("comment", "List of FILTER strings in BCF");
    schema =
        schema.add(
            new StructField(
                "filter",
                DataTypes.createArrayType(DataTypes.StringType, false),
                true,
                metadata.build()));
    // Numeric representation of GT, eg [0, 1] for "0/1"
    metadata = new MetadataBuilder();
    metadata.putString("comment", "Numeric representation of GT, eg [0, 1] for \"0/1\"");
    schema =
        schema.add(
            new StructField(
                "genotype",
                DataTypes.createArrayType(DataTypes.IntegerType, false),
                true,
                metadata.build()));
    // AD field in FORMAT block of BCF
    metadata = new MetadataBuilder();
    metadata.putString("comment", "AD field in FORMAT block of BCF");
    schema =
        schema.add(
            new StructField(
                "fmt_AD",
                DataTypes.createArrayType(DataTypes.IntegerType, false),
                true,
                metadata.build()));
    // DP field in FORMAT block of BCF
    metadata = new MetadataBuilder();
    metadata.putString("comment", "DP field in FORMAT block of BCF");
    schema =
        schema.add(
            new StructField(
                "fmt_DP",
                DataTypes.createArrayType(DataTypes.IntegerType, false),
                true,
                metadata.build()));
    // GQ field in FORMAT block of BCF
    metadata = new MetadataBuilder();
    metadata.putString("comment", "GQ field in FORMAT block of BCF");
    schema =
        schema.add(
            new StructField(
                "fmt_GQ",
                DataTypes.createArrayType(DataTypes.IntegerType, false),
                true,
                metadata.build()));
    // MIN_DP field in FORMAT block of BCF
    metadata = new MetadataBuilder();
    metadata.putString("comment", "MIN_DP field in FORMAT block of BCF");
    schema =
        schema.add(
            new StructField(
                "fmt_MIN_DP",
                DataTypes.createArrayType(DataTypes.IntegerType, false),
                true,
                metadata.build()));
    // Non-attribute FORMAT fields
    metadata = new MetadataBuilder();
    metadata.putString("comment", "Non-attribute FORMAT fields");
    schema = schema.add(new StructField("fmt", DataTypes.BinaryType, true, metadata.build()));
    // Non-attribute INFO fields
    metadata = new MetadataBuilder();
    metadata.putString("comment", "Non-attribute INFO fields");
    schema = schema.add(new StructField("info", DataTypes.BinaryType, true, metadata.build()));
    // BED start position (0-based) of the query associated with this record
    metadata = new MetadataBuilder();
    metadata.putString("comment", "BED start position (0-based) of query");
    schema =
        schema.add(new StructField("queryBedStart", DataTypes.IntegerType, true, metadata.build()));
    // BED end position (0-based) of the query associated with this record
    metadata = new MetadataBuilder();
    metadata.putString("comment", "BED end position (1-based) of query");
    schema =
        schema.add(new StructField("queryBedEnd", DataTypes.IntegerType, true, metadata.build()));
    return schema;
  }
}
