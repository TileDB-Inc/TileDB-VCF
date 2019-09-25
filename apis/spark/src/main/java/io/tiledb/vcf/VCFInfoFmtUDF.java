package io.tiledb.vcf;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.api.java.UDF1;

public class VCFInfoFmtUDF implements UDF1<byte[], Map<String, String>> {

  @Override
  public Map<String, String> call(byte[] bytes) {
    if (bytes == null) {
      return new HashMap<>();
    }
    int nbytes = bytes.length;
    ByteBuffer buffer = ByteBuffer.allocate(nbytes).order(ByteOrder.nativeOrder());
    buffer.position(0);
    for (int i = 0; i < nbytes; i++) {
      buffer.put(bytes[i]);
    }
    return VCFInfoFmtDecoder.decodeInfoFmtBytes(buffer);
  }
}
