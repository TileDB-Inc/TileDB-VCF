## Implementation
Our main abstraction is called `TileDBHailVCFReader`. The source file can be found at the following link, which extends 
the [`MatrixHybridReader`](https://github.com/TileDB-Inc/hail/blob/victorgiannakouris/tiledbvcf-hail-0.2.64/hail/src/main/scala/is/hail/expr/ir/MatrixIR.scala#L114) class. Below, the class hierarchy is described.

- [TileDBHailVCFReader](https://github.com/TileDB-Inc/TileDB-VCF/blob/victorgiannakouris/ch4753/make-a-pull-request-for-integrating-tiledb/apis/spark/src/main/scala/TileDBVCFHailReader.scala) (class)
   - [MatrixHybridReader](https://github.com/TileDB-Inc/hail/blob/victorgiannakouris/tiledbvcf-hail-0.2.64/hail/src/main/scala/is/hail/expr/ir/MatrixIR.scala#L114) (abstract class)
     - [TableReader](https://github.com/TileDB-Inc/hail/blob/victorgiannakouris/tiledbvcf-hail-0.2.64/hail/src/main/scala/is/hail/expr/ir/TableIR.scala#L406) (interface)

The main method that we need to implement, is the `apply()` method of the [`TableReader`](https://github.com/TileDB-Inc/hail/blob/victorgiannakouris/tiledbvcf-hail-0.2.64/hail/src/main/scala/is/hail/expr/ir/TableIR.scala#L409) interface. 
Given a `TableReader` instance, this is the method that Hail invokes in order to pass as input a `TableRead` instance 
and then return the corresponding `TableValue`. The `TableRead`, contains the VCF file and its requested schema 
(i.e. which INFO/FORMAT fields are requested).

### The `appy()` method
The `apply()` method is the most important part of our implementation, as it takes as input a `TableRead`, does the
required transformations according to the input requested schema. Based on the input `TableRead`, we extract and store
the requested row type in the `rowType` variable.

Next, we load the input TileDB-VCF array using TileDB-VCF Spark. We load the input VCF TileDB array into the `df` 
variable as a Spark Dataframe. Next, we transform the Dataframe's column names of all the format (`fmt_*`) attributes
using aliases (`withColumnRenamed()` method). For example, the `fmt_DP` attribute becomes `DP`, the `fmt_PL` becomes
`PL` and so on. We do that in order to match Hail's attribute names and project the only requested attributes included
in the input `TableRead`.

#### Reading the basic attributes (locus, alleles)
Next, we read the basic TileDB attributes, namely `contig`, `posStart` and `alleles`. We read those three in order to
construct the locus (a `(contig, postStart)` pair) alleles fields in the Matirx Table.


## Python End-to-End Example
In this section, we explain how we can run an end-to-end example and load a TileDB VCF array into Hail, using Hail’s Python framework.

1. Switch to Java 8, e.g.:<br>
   `export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_201.jdk/Contents/Home/`

2. First, we need to clone our Hail fork, and specifically the `victorgiannakouris/hail-0.2.64-maven-local` and do the following:<br>
   1. `git clone -b victorgiannakouris/hail-0.2.64-maven-local https://github.com/TileDB-Inc/hail.git`
   2. `cd hail/hail`
   3. `./gradlew clean assemble && ./gradlew publishToMavenLocal` 

3. Clone the TileDB-VCF repo and install the dependencies locally:
    1. `git clone -b victorgiannakouris/ch4753/make-a-pull-request-for-integrating-tiledb https://github.com/TileDB-Inc/TileDB-VCF.git`
    2. `cd TileDB-VCF/apis/spark`
    3.  `./gradlew clean assemble && ./gradlew publishToMavenLocal`

4. Inside the Hail repo, checkout the `victorgiannakouris/tiledbvcf-hail-0.2.64` and do the following:<br>
   1. `git checkout victorgiannakouris/tiledbvcf-hail-0.2.64`
   2. `cd hail`
   5. `virtualenv ./python/.venv -p /usr/local/bin/python3.7`
   6.  `source ./python/.venv/bin/activate`
   7. `make install HAIL_COMPILE_NATIVES=1` 
   8. `cp -f build/libs/hail-all-main-spark.jar ./python/hail/backend/hail-all-spark.jar`
   9. `cd python`
   10. `touch README.md` 
   11. `python setup.py install`
   
### Running the Jupyter Notebook
After executing successfully all the above steps, the following are required:
1. In the `hail/hail/python` directory (assuming you are already in the virtual environment) create dedicated
IPython kernel for hail as follows:
   1. `pip install ipykernel`
   2. `python -m ipykernel install --name=hail_kernel`