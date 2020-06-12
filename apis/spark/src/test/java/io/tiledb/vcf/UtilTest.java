package io.tiledb.vcf;

import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.junit.Assert;
import org.junit.Test;

public class UtilTest {

  @Test(expected = ArithmeticException.class)
  public void testLongToInt() {
    Util.longToInt(Integer.MAX_VALUE + 1L);
  }

  @Test
  public void getDefaultRecordByteCountTest() throws Exception {

    Assert.assertEquals(8, Util.getDefaultRecordByteCount(BaseValueVector.class));
    Assert.assertEquals(8, Util.getDefaultRecordByteCount(BaseVariableWidthVector.class));
  }
}
