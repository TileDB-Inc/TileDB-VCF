#!/bin/bash
mv ../libraries/* .

mkdir lib

for arch in $(ls | grep .tar.gz)
do
tar -xf $arch
done

# OSX
mv *.dylib lib

# Linux
mv *.so* lib

# Spark2
cd ./apis/spark

./gradlew assemble
./gradlew shadowJar

mkdir $BUILD_BINARIESDIRECTORY/jars
cp ./build/libs/*.jar $BUILD_BINARIESDIRECTORY/jars

# Spark3
cd ../..

cd ./apis/spark3

./gradlew assemble
./gradlew shadowJar

mkdir $BUILD_BINARIESDIRECTORY/jars
cp ./build/libs/*.jar $BUILD_BINARIESDIRECTORY/jars
