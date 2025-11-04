The LibVCFNative bindings are produced with `javah`.

To build the binding you must first compile the java file to a header file by using the ```io.commons``` external library
```
javac -cp .:commons-io-2.14.0.jar *.java -h .;
```

Next, replace the old header
```
mv io_tiledb_libvcfnative_LibVCFNative.h LibVCFNative.h;
```

Finally, remove all ```.class``` files 

```
rm *.class;
```

This process can be done autmatically using the `genearteJNI.sh` script.
