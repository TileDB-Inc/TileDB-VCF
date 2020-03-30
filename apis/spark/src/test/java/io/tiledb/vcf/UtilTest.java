package io.tiledb.vcf;

import org.junit.Test;

public class UtilTest {

    @Test(expected = ArithmeticException.class)
    public void testLongToInt() {
        Util.longToInt(Integer.MAX_VALUE+1L);
    }
}
