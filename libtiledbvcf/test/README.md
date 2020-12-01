# Unit Test Data

Documentation for input data files used in our unit tests and instructions to regenerate them.

## `E001_15_coreMarks_dense_filtered.bed.gz`

The original bedfile contains results the [Roadmap Epigenomics Project](http://www.roadmapepigenomics.org)'s chromatin state learning model for ES-I3 cell line. The bedfile was filtered to include only *Enhancer* regions on chromosomes 1, 2, and 3.

```
make bedfile
```
