package io.tiledb.vcf;

import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class TileDBTable implements SupportsRead {
  private final StructType schema;
  private final Map<String, String> properties;
  private Set<TableCapability> capabilities;

  public TileDBTable(StructType schema, Map<String, String> properties) {
    this.schema = schema;
    this.properties = properties;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    try {
      VCFDataSourceOptions VCFOptions = Util.asVCFOptions(options);
      return new VCFScanBuilder(properties, VCFOptions);
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public String name() {
    return properties.get("uri");
  }

  @Override
  public StructType schema() {
    return schema;
  }

  @Override
  public Set<TableCapability> capabilities() {
    if (capabilities == null) {
      this.capabilities = new HashSet<>();
      capabilities.add(TableCapability.BATCH_READ);
    }
    return capabilities;
  }
}
