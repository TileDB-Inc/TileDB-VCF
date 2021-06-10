The LibVCFNative bindings are produced with `javah`.

To build the binding you must first compile the java file to a class file:

```
cd api/spark/src/main/java/io/tiledb/libvcfnative
javac LibVCFNative.java
```

Next you can use `javah` to rebuild the LibVCFNative header:
```
# Navigate back to top level spark src directory
cd ../../../
javah -v -cp $PWD -o io/tiledb/libvcfnative/LibVCFNative.h io.tiledb.libvcfnative.LibVCFNative
```

It is safe to delete the class file now:
```
rm io/tiledb/libvcfnative/LibVCFNative.class
```
