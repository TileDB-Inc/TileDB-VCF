package io.tiledb.vcf;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import org.apache.log4j.Logger;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** Class defining the TileDB-VCF Spark DataSource. */
public class VCFDataSource implements TableProvider {
  private static Logger log = Logger.getLogger(VCFDataSource.class.getName());
  private VCFDataSourceOptions VCFOptions;

  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    VCFOptions = Util.asVCFOptions(options);

    VCFSparkSchema vcfSparkSchema = null;
    try {
      vcfSparkSchema = new VCFSparkSchema(new URI(options.get("uri")), VCFOptions);
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
    assert vcfSparkSchema != null;
    return vcfSparkSchema.getSparkSchema();
  }

  @Override
  public Table getTable(
      StructType schema, Transform[] partitioning, Map<String, String> properties) {
    return new TileDBTable(schema, properties);
  }

  @Override
  public boolean supportsExternalMetadata() {
    return true;
  }
}
