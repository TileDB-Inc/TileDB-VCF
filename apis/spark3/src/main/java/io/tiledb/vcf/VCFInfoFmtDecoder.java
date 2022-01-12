package io.tiledb.vcf;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/** Helper class for decoding info/fmt byte arrays. */
public class VCFInfoFmtDecoder {

  /** htslib flag type tag. specifies no values */
  private static final int BCF_HT_FLAG = 0;
  /** htslib integer type tag. */
  private static final int BCF_HT_INT = 1;
  /** htslib float type tag. */
  private static final int BCF_HT_REAL = 2;
  /** htslib string type tag. */
  private static final int BCF_HT_STR = 3;

  /**
   * Given a the binary byte array of encoded INFO/FMT fields, return a mapping of field name to
   * string representation of the field value.
   *
   * @param infoFmtBytes Byte array of encoded INFO/FMT fields values.
   * @return Map of field name to field value.
   */
  public static Map<String, String> decodeInfoFmtBytes(ByteBuffer infoFmtBytes) {
    Map<String, String> result = new HashMap<>();
    StringBuilder name = new StringBuilder();
    StringBuilder values = new StringBuilder();
    infoFmtBytes.position(0);

    int numValues = infoFmtBytes.getInt();
    while (infoFmtBytes.hasRemaining()) {
      name.setLength(0);
      values.setLength(0);

      // Get name of field
      while (true) {
        char c = (char) infoFmtBytes.get();
        if (c == '\0') {
          break;
        } else {
          name.append(c);
        }
      }

      // Get stringified values.
      int type = infoFmtBytes.getInt();
      int nvalues = infoFmtBytes.getInt();

      if (type == BCF_HT_FLAG) {
        // For flag fields, `nvalues` is 0, or empty.  we just duplicate the name for the value
        values.append(name.toString());
        break;
      }

      for (int i = 0; i < nvalues; i++) {
        switch (type) {
          case BCF_HT_INT:
            values.append(infoFmtBytes.getInt());
            break;
          case BCF_HT_REAL:
            values.append(infoFmtBytes.getFloat());
            break;
          case BCF_HT_STR:
            byte[] dst = new byte[nvalues + 1];
            infoFmtBytes.get(dst, 0, nvalues);
            // Find the first NULL character.
            int nullPos = 0;
            dst[nvalues] = '\0';
            for (; nullPos < dst.length && dst[nullPos] != '\0'; nullPos++) ;
            // Convert to String
            values.append(new String(dst, 0, nullPos));
            break;
          default:
            throw new UnsupportedOperationException(
                String.format("Unimplemented INFO/FMT field type %d", type));
        }

        if (type == BCF_HT_STR) {
          // For string fields, `nvalues` is the number of characters, so we always break after one
          // iteration.
          break;
        }

        if (i < nvalues - 1) {
          values.append(',');
        }
      }
      result.put(name.toString(), values.toString());
    }
    return result;
  }
}
