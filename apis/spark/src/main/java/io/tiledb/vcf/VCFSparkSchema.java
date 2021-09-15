package io.tiledb.vcf;

import io.tiledb.libvcfnative.VCFReader;
import io.tiledb.util.CredentialProviderUtils;
import java.io.Serializable;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.annotation.meta.field;

public class VCFSparkSchema implements Serializable {

  private StructType schema;
  private StructType pushDownSchema;
  private VCFDataSourceOptions options;

  public VCFSparkSchema(URI uri, VCFDataSourceOptions options) {

    this.options = options;
    Optional<String> credentialsCsv =
        options
            .getCredentialsProvider()
            .map(CredentialProviderUtils::buildConfigMap)
            .flatMap(VCFDataSourceOptions::getConfigCSV);

    Optional<String> configCsv =
        VCFDataSourceOptions.combineCsvOptions(options.getConfigCSV(), credentialsCsv);

    VCFReader vcfReader = new VCFReader(uri.toString(), null, options.getSampleURI(), configCsv);
    schema = buildSchema(vcfReader);
    vcfReader.close();
  }

  public VCFSparkSchema setPushDownSchema(StructType pushDownSchema) {
    this.pushDownSchema = pushDownSchema;
    return this;
  }

  public StructType getSparkSchema() {
    if (pushDownSchema == null) {
      return schema;
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
      vcfAttrNames[i] = fieldToVCFAttribute(fields[i].name());
    }
    return vcfAttrNames;
  }

  public int getNumColumns() {
    return getSparkFields().length;
  }

  private String fieldToVCFAttribute(String fieldName) {
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
      case "queryBedLine":
        return "query_bed_line";
      case "qual":
        return "qual";
      case "id":
        return "id";
      default:
        // Handle all fmt and info fields
        if (fieldName.startsWith("fmt_") || fieldName.startsWith("info_")) return fieldName;
        throw new RuntimeException("Unknown VCF schema field name: " + fieldName);
    }
  }

  private String vcfAttributeToField(String fieldName) {
    switch (fieldName) {
      case "sample_name":
        return "sampleName";
      case "contig":
        return "contig";
      case "pos_start":
        return "posStart";
      case "pos_end":
        return "posEnd";
      case "alleles":
        return "alleles";
      case "filters":
        return "filter";
      case "fmt_GT":
        return "genotype";
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
      case "query_bed_start":
        return "queryBedStart";
      case "query_bed_end":
        return "queryBedEnd";
      case "query_bed_line":
        return "queryBedLine";
      case "qual":
        return "qual";
      case "id":
        return "id";
      default:
        // Handle all fmt and info fields
        if (fieldName.startsWith("fmt_") || fieldName.startsWith("info_")) return fieldName;
        throw new RuntimeException("Unknown VCF schema field name: " + fieldName);
    }
  }

  /**
   * Build a spark schema based on the VCF dataset fields
   *
   * @return spark schema struct
   * @param vcfReader
   */
  private StructType buildSchema(VCFReader vcfReader) {
    StructType schema = new StructType();

    if (options.getOnlyMaterializedFields().orElse(false)) {
      for (Map.Entry<String, VCFReader.AttributeTypeInfo> attrSet :
          vcfReader.materializedAttributes.entrySet()) {
        String name = attrSet.getKey();
        VCFReader.AttributeTypeInfo typeInfo = attrSet.getValue();
        schema = schema.add(schemaField(name, typeInfo));
      }
    } else { // List all fields
      for (Map.Entry<String, VCFReader.AttributeTypeInfo> attrSet :
          vcfReader.attributes.entrySet()) {
        String name = attrSet.getKey();
        VCFReader.AttributeTypeInfo typeInfo = attrSet.getValue();
        schema = schema.add(schemaField(name, typeInfo));
      }
    }

    return schema;
  }

  /**
   * Create a new field for the spark schema
   *
   * @param name field name to add
   * @param typeInfo field type info
   * @return StructField
   */
  private StructField schemaField(String name, VCFReader.AttributeTypeInfo typeInfo) {
    // If we have a list or a variable length non-string we treat it as a spark array type
    if (typeInfo.isList
        || (typeInfo.isVarLen && typeInfo.datatype != VCFReader.AttributeDatatype.CHAR)) {
      return schemaFieldArrayType(name, typeInfo);
    }

    return schemaFieldSingle(name, typeInfo);
  }

  /**
   * Create a new field for the spark schema
   *
   * @param name field name to add
   * @param typeInfo field type info
   * @return StructField
   */
  private StructField schemaFieldSingle(String name, VCFReader.AttributeTypeInfo typeInfo) {

    name = vcfAttributeToField(name);

    MetadataBuilder metadata = new MetadataBuilder();
    metadata.putString("comment", fieldCommentLookup(name));

    // fmt and info are binary
    if (name.equals("fmt") || name.equals("info")) {
      return new StructField(name, DataTypes.BinaryType, typeInfo.isNullable, metadata.build());
    }

    switch (typeInfo.datatype) {
      case CHAR:
        return new StructField(name, DataTypes.StringType, typeInfo.isNullable, metadata.build());
      case INT32:
        return new StructField(name, DataTypes.IntegerType, typeInfo.isNullable, metadata.build());
      case UINT8:
        return new StructField(name, DataTypes.ShortType, typeInfo.isNullable, metadata.build());
      case FLOAT32:
        return new StructField(name, DataTypes.FloatType, typeInfo.isNullable, metadata.build());
    }

    throw new RuntimeException(
        "Unsupported field "
            + name
            + " with unsupported datatype "
            + typeInfo.datatype
            + " in spark schema");
  }

  /**
   * Create a new array field for the spark schema
   *
   * @param name field name to add
   * @param typeInfo field type info
   * @return StructField
   */
  private StructField schemaFieldArrayType(String name, VCFReader.AttributeTypeInfo typeInfo) {

    name = vcfAttributeToField(name);

    MetadataBuilder metadata = new MetadataBuilder();
    metadata.putString("comment", fieldCommentLookup(name));

    // fmt and info are binary
    if (name.equals("fmt") || name.equals("info")) {
      return new StructField(name, DataTypes.BinaryType, typeInfo.isNullable, metadata.build());
    }

    switch (typeInfo.datatype) {
      case CHAR:
        return new StructField(
            name,
            DataTypes.createArrayType(DataTypes.StringType, false),
            typeInfo.isNullable,
            metadata.build());
      case INT32:
        return new StructField(
            name,
            DataTypes.createArrayType(DataTypes.IntegerType, false),
            typeInfo.isNullable,
            metadata.build());
      case UINT8:
        return new StructField(
            name,
            DataTypes.createArrayType(DataTypes.ShortType, false),
            typeInfo.isNullable,
            metadata.build());
      case FLOAT32:
        return new StructField(
            name,
            DataTypes.createArrayType(DataTypes.FloatType, false),
            typeInfo.isNullable,
            metadata.build());
    }

    throw new RuntimeException(
        "Unsupported field "
            + name
            + " with unsupported datatype "
            + typeInfo.datatype
            + " in spark schema");
  }

  /**
   * Custom comments for various fields
   *
   * @param field to get comment for
   * @return comment string
   */
  private static String fieldCommentLookup(String field) {
    if (field.equals("sampleName")) return "sampleName";
    else if (field.equals("contig")) return "CHR field in BCF";
    else if (field.equals("posStart")) return "POS field in BCF";
    else if (field.equals("posEnd")) return "POS + END -1 if END is defined, else POS";
    else if (field.equals("alleles")) return "List of REF, ALT fields in BCF";
    else if (field.equals("filter")) return "List of FILTER strings in BCF";
    else if (field.equals("genotype")) return "Numeric representation of GT, eg [0, 1] for \"0/1\"";
    else if (field.equals("fmt")) return "Non-attribute FORMAT fields";
    else if (field.equals("info")) return "Non-attribute INFO fields";
    else if (field.startsWith("fmt_")) {
      String fieldPart = field.substring(4);
      return fieldPart.toUpperCase() + " field in FORMAT block of BCF";
    } else if (field.startsWith("info_")) {
      String fieldPart = field.substring(5);
      return fieldPart.toUpperCase() + " field in INFO block of BCF";
    } else if (field.equals("queryBedStart")) return "BED start position (0-based) of query";
    else if (field.equals("queryBedEnd")) return "BED end position (half-open) of query";
    else if (field.equals("queryBedLine")) return "BED file line number of query";

    return field;
  }
}
