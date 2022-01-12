package io.tiledb.vcf;

import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.log4j.Logger;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class Util {
  private static Logger logger = Logger.getLogger(Util.class);

  public static int longToInt(long num) throws ArithmeticException {

    if (num > Integer.MAX_VALUE)
      throw new ArithmeticException(
          String.format("Value %d exceeds the maximum integer value.", num));

    return (int) num;
  }

  /**
   * Returns the record size in bytes. Current implementation always returns 8, but it is intended
   * for future use.
   *
   * @param clazz The input ValueVector class
   * @return The size in bytes
   */
  public static long getDefaultRecordByteCount(Class<? extends ValueVector> clazz) {
    if (BaseVariableWidthVector.class.isAssignableFrom(clazz)) return 8;
    else if (BaseValueVector.class.isAssignableFrom(clazz)) return 8;

    logger.warn(
        "Did not found size of the class with name: "
            + clazz.getCanonicalName()
            + " returning 8 as the record size");

    return 8;
  }

  /**
   * Translates a CaseInsensitiveStringMap to VCFDataSourceOptions
   *
   * @param options The table options
   * @return
   */
  public static VCFDataSourceOptions asVCFOptions(CaseInsensitiveStringMap options) {
    return new VCFDataSourceOptions(new DataSourceOptions(options));
  }
}
