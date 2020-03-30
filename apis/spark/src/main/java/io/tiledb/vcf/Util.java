package io.tiledb.vcf;

import java.util.Optional;

public class Util {
    public static int longToInt(long num) throws ArithmeticException {

        if (num > Integer.MAX_VALUE)
            throw new ArithmeticException(String.format("Value %d exceeds the maximum integer value.", num));

        return (int)num;
    }
}
