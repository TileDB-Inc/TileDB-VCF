# The LibVCFNative bindings are produced with `javah`.

## To build the LibVCFNative header run:
```
javac -cp .:commons-io-2.14.0.jar  *.java -h dstara  
```

This will generate io_tiledb_libvcfnative_LibVCFNative.h as a separate file.

## Format the new header file:
```
clang-format -i io_tiledb_libvcfnative_LibVCFNative.h
```

## Replace the old header file with the new:
```
mv io_tiledb_libvcfnative_LibVCFNative.h LibVCFNative.h 
```

## It is safe to delete the class files now:
```
rm io/tiledb/libvcfnative/*.class
```







