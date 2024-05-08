package io.tiledb.vcf;

import io.tiledb.java.api.*;
import io.tiledb.libvcfnative.VCFBedFile;
import io.tiledb.libvcfnative.VCFReader;
import io.tiledb.util.CredentialProviderUtils;
import java.net.URI;
import java.util.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.log4j.Logger;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.reader.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class VCFDataSourceReader
    implements DataSourceReader,
        SupportsPushDownRequiredColumns,
        SupportsScanColumnarBatch,
        SupportsPushDownFilters {

  static Logger log = Logger.getLogger(VCFDataSourceReader.class.getName());

  private URI uri;
  private VCFDataSourceOptions options;
  private VCFSparkSchema schema;
  private List<String> pushedSampleNames;
  private List<Filter> pushedFilters;

  public VCFDataSourceReader(URI uri, VCFDataSourceOptions options) {
    this.uri = uri;
    this.options = options;
    this.schema = new VCFSparkSchema(uri, options);
    this.pushedSampleNames = new ArrayList<>();
    this.pushedFilters = new ArrayList<>();
  }

  @Override
  public void pruneColumns(StructType pushDownSchema) {
    log.trace("Pushdown columns: " + pushDownSchema);
    this.schema.setPushDownSchema(pushDownSchema);
  }

  @Override
  public StructType readSchema() {
    StructType readSchema = schema.getSparkSchema();
    log.trace("Read schema: + " + readSchema);
    return readSchema;
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    List<Filter> nonPushedFilters = new ArrayList<>();
    pushedFilters.clear();
    pushedSampleNames.clear();
    for (Filter f : filters) {
      boolean pushed = false;
      if (f instanceof EqualTo) {
        if (((EqualTo) f).attribute().equals("sampleName")) {
          if (options.getSamples().isPresent() || options.getSampleURI().isPresent()) {
            throw new UnsupportedOperationException(
                "Cannot have a sampleName in where clause while also having samples or sampleFile in options list");
          }
          String value = (String) ((EqualTo) f).value();
          pushedSampleNames.add(value);
          pushed = true;
        }
      }

      if (pushed) {
        pushedFilters.add(f);
      } else {
        nonPushedFilters.add(f);
      }
    }

    Filter[] arr = new Filter[nonPushedFilters.size()];
    nonPushedFilters.toArray(arr);
    return arr;
  }

  @Override
  public Filter[] pushedFilters() {
    Filter[] arr = new Filter[pushedFilters.size()];
    pushedFilters.toArray(arr);
    return arr;
  }

  @Override
  public boolean enableBatchRead() {
    // always read in batch mode
    return true;
  }

  @Override
  public List<InputPartition<ColumnarBatch>> planBatchInputPartitions() {
    HashSet<String> dedupSamples = new HashSet<>(pushedSampleNames);
    Optional<String[]> optionSamples = options.getSamples();
    if (optionSamples.isPresent()) {
      if (!pushedSampleNames.isEmpty()) {
        // TODO: we can override one or the other here, it's just not clear what the right choice
        // is.
        throw new RuntimeException(
            "Cannot combine 'samples' DF option with 'where sampleName' filtering");
      }
      dedupSamples.addAll(Arrays.asList(optionSamples.get()));
    }

    // Check for range/sample indexes
    Optional<Integer> optionRangePartitionIndex = options.getRangePartitionIndex();
    Optional<Integer> optionSamplePartitionIndex = options.getSamplePartitionIndex();

    // Get number of range partitions (if specified)
    List<String> samples = new ArrayList<>(dedupSamples);
    Optional<Integer> optionRangePartitions = options.getRangePartitions();
    int numRangePartitions = 1;
    if (optionRangePartitions.isPresent()) {
      numRangePartitions = optionRangePartitions.get();
      if (numRangePartitions < 1) {
        throw new RuntimeException("Invalid number of range partitions: " + numRangePartitions);
      }
    }

    // Get number of sample partitions (if specified)
    Optional<Integer> optionSamplePartitions = options.getSamplePartitions();
    int numSamplePartitions = 1;
    if (optionSamplePartitions.isPresent()) {
      numSamplePartitions = optionSamplePartitions.get();
      if (numSamplePartitions < 1) {
        throw new RuntimeException("Invalid number of sample partitions: " + numSamplePartitions);
      }
    }

    // Try to add additional partitions if partitions is also specified
    Optional<Integer> optionPartitions = options.getPartitions();
    if (optionPartitions.isPresent()) {
      int numPartitions = optionPartitions.get();
      if (numPartitions < 1) {
        throw new RuntimeException("Invalid number of partitions: " + numPartitions);
      }
      if (optionRangePartitions.isPresent() && optionSamplePartitions.isPresent()) {
        if ((numRangePartitions * numSamplePartitions) != numPartitions) {
          throw new RuntimeException(
              "Cannot partition domain given "
                  + numRangePartitions
                  + " range partitions, "
                  + numSamplePartitions
                  + " sample partitions and "
                  + numPartitions
                  + " domain partitions");
        }
      } else if (optionRangePartitions.isPresent()) {
        if (numPartitions % numRangePartitions != 0) {
          throw new RuntimeException(
              "Cannot partition domain given "
                  + numRangePartitions
                  + " range partitions and "
                  + numPartitions
                  + " domain partitions");
        }
        numSamplePartitions = numPartitions / numRangePartitions;
      } else if (optionSamplePartitions.isPresent()) {
        if (numPartitions % numSamplePartitions != 0) {
          throw new RuntimeException(
              "Cannot partition domain given "
                  + numRangePartitions
                  + " range partitions and "
                  + numPartitions
                  + " domain partitions");
        }
        numRangePartitions = numPartitions / numSamplePartitions;
      } else {
        // only domain partitions specified, default to partitioning by ranges as that is the most
        // performant default given an unknown number of samples and ranges
        numRangePartitions = numPartitions;
      }
    }

    List<InputPartition<ColumnarBatch>> inputPartitions =
        new ArrayList<>(numRangePartitions * numSamplePartitions);

    // Initial variables
    int ranges_start = 0;
    int samples_start = 0;
    int ranges_end = numRangePartitions;
    int samples_end = numSamplePartitions;
    // If the user sets both the optionRangePartitionIndex and optionSamplePartitionIndex we only
    // have a single partition
    if (optionRangePartitionIndex.isPresent() && optionSamplePartitionIndex.isPresent()) {
      inputPartitions.add(
          new VCFInputPartition(
              uri,
              schema,
              options,
              samples,
              new VCFPartitionInfo(optionRangePartitionIndex.get(), numRangePartitions, null),
              new VCFPartitionInfo(optionSamplePartitionIndex.get(), numSamplePartitions, null)));
      // Exit early, its only 1 partition
      return inputPartitions;
    } else if (optionRangePartitionIndex.isPresent()) {
      ranges_start = optionRangePartitionIndex.get();
      ranges_end = optionRangePartitionIndex.get() + 1;
    } else if (optionSamplePartitionIndex.isPresent()) {
      samples_start = optionSamplePartitionIndex.get();
      samples_end = optionSamplePartitionIndex.get() + 1;
    }

    // Create Spark input partitions
    List<List<String>> regions = null;
    if (options.getNewPartitionMethod().orElse(false)) {

      // Compute regions from bed array or bed file
      Optional<URI> bedArrayURI = options.getBedArrayURI();
      Optional<URI> bedURI = options.getBedURI();
      if (bedArrayURI.isPresent()) {
        regions = computeRegionPartitionsFromBedArray(numRangePartitions, bedArrayURI.get());
      } else if (bedURI.isPresent()) {
        regions = computeRegionPartitionsFromBedFile(numRangePartitions);
      } else {
        throw new RuntimeException(
            "Can't use new_partition_method without setting bed_file or bed_array");
      }

      numRangePartitions = regions.size();
      ranges_end = regions.size();
      log.info("New partition method has yielded " + numRangePartitions + " range partitions");
    }

    for (int r = ranges_start; r < ranges_end; r++) {
      List<String> local_regions = null;
      if (regions != null) {
        local_regions = regions.get(r);
        // Skip empty region list
        // TODO: find out why this happens?
        if (local_regions.size() == 0) {
          log.warn(String.format("range %d of %d: local_regions.size() == 0", r, ranges_end));
          continue;
        }
      }
      for (int s = samples_start; s < samples_end; s++) {
        inputPartitions.add(
            new VCFInputPartition(
                uri,
                schema,
                options,
                samples,
                new VCFPartitionInfo(r, numRangePartitions, local_regions),
                new VCFPartitionInfo(s, numSamplePartitions, null)));
      }
    }

    return inputPartitions;
  }

  List<List<String>> computeRegionPartitionsFromBedArray(
      int desiredNumRangePartitions, URI arrayURI) {

    // Read bed array

    try {
      Map<String, List<String>> mapOfRegions = new HashMap<>();
      int counter = 0;

      Context ctx = new Context();
      Array bedArray = new Array(ctx, arrayURI.toString(), QueryType.TILEDB_READ);

      String CONTIG = "alias contig";
      String START = "alias start";
      String END = "alias end";

      String[] keys = new String[] {CONTIG, START, END};

      NativeArray contigAliasNA = bedArray.getMetadata(CONTIG, Datatype.TILEDB_STRING_ASCII);
      NativeArray startAliasNA = bedArray.getMetadata(START, Datatype.TILEDB_STRING_ASCII);
      NativeArray endAliasNA = bedArray.getMetadata(END, Datatype.TILEDB_STRING_ASCII);

      String contigAlias = new String((byte[]) contigAliasNA.toJavaArray());
      String startAlias = new String((byte[]) startAliasNA.toJavaArray());
      String endAlias = new String((byte[]) endAliasNA.toJavaArray());

      Query query = new Query(bedArray);
      query.setLayout(Layout.TILEDB_UNORDERED);

      Pair<Long, Long> estSize = query.getEstResultSizeVar(ctx, contigAlias);

      // todo unsafe casting needs to be addressed in the java api
      // Prepare buffers
      query.setDataBuffer(
          contigAlias,
          new NativeArray(ctx, estSize.getSecond().intValue(), Datatype.TILEDB_STRING_ASCII));
      query.setOffsetsBuffer(
          contigAlias, new NativeArray(ctx, estSize.getFirst().intValue(), Datatype.TILEDB_UINT64));
      query.setDataBuffer(
          startAlias, new NativeArray(ctx, estSize.getFirst().intValue(), Datatype.TILEDB_UINT64));
      query.setDataBuffer(
          endAlias, new NativeArray(ctx, estSize.getFirst().intValue(), Datatype.TILEDB_UINT64));

      do {
        query.submit();
        // get buffers
        long[] contigOffsets = (long[]) query.getVarBuffer(contigAlias);
        byte[] contigData = (byte[]) query.getBuffer(contigAlias);

        String[] contigs = io.tiledb.java.api.Util.bytesToStrings(contigOffsets, contigData);
        long[] start = (long[]) query.getBuffer(startAlias);
        long[] end = (long[]) query.getBuffer(endAlias);

        if (!(contigs.length == start.length && start.length == end.length)) {
          throw new RuntimeException("There was an error reading the bed array");
        }

        // Put regions in map
        for (int i = 0; i < contigs.length; i++) {
          String shortContig = contigs[i].replace("chr", "");
          String region = shortContig + ":" + start[i] + "-" + end[i] + ":" + counter;
          counter++;

          // Check if the key exists in the map
          if (mapOfRegions.containsKey(shortContig)) {
            // If the key exists, append the region string to the existing list
            mapOfRegions.get(shortContig).add(region);
          } else {
            // If the key doesn't exist, create a new list with the region string
            List<String> newList = new ArrayList<>();
            newList.add(region);
            mapOfRegions.put(shortContig, newList);
          }
        }
      } while (query.getQueryStatus() == QueryStatus.TILEDB_INCOMPLETE);

      List<List<String>> res = new LinkedList<>(mapOfRegions.values());

      sortRegions(res, desiredNumRangePartitions);

      return res;

    } catch (TileDBError err) {
      throw new RuntimeException(err);
    }
  }

  List<List<String>> computeRegionPartitionsFromBedFile(int desiredNumRangePartitions) {
    Optional<URI> bedURI = options.getBedURI();
    Optional<URI> bedArrayURI = options.getBedArrayURI();
    if (!bedURI.isPresent() && !bedArrayURI.isPresent()) {
      throw new RuntimeException(
          "Can't use new_partition_method without setting bed_file or bed_array");
    }

    log.info("Init VCFReader for partition calculation");
    String uriString = options.getDatasetURI().get().toString();

    Optional<String> credentialsCsv =
        options
            .getCredentialsProvider()
            .map(CredentialProviderUtils::buildConfigMap)
            .flatMap(VCFDataSourceOptions::getConfigCSV);

    Optional<String> configCsv =
        VCFDataSourceOptions.combineCsvOptions(options.getConfigCSV(), credentialsCsv);

    String[] samples = new String[] {};
    VCFReader vcfReader = new VCFReader(uriString, samples, options.getSampleURI(), configCsv);

    VCFBedFile bedFile = new VCFBedFile(vcfReader, bedURI.get().toString());

    Map<String, List<String>> mapOfRegions = bedFile.getContigRegionStrings();
    List<List<String>> res = new LinkedList<>(mapOfRegions.values());

    sortRegions(res, desiredNumRangePartitions);

    bedFile.close();
    vcfReader.close();

    return res;
  }

  private void sortRegions(List<List<String>> res, int desiredNumRangePartitions) {
    // Sort the region list by size of regions in contig, largest first
    res.sort(Comparator.comparingInt(List<String>::size).reversed());

    // Keep splitting the largest region lists until we have the desired minimum number of range
    // Partitions, we stop if the large region has a size of 10 or less
    while (res.size() < desiredNumRangePartitions && res.get(0).size() >= 10) {
      List<String> top = res.remove(0);
      List<String> first = new LinkedList<>(top.subList(0, top.size() / 2));
      List<String> second = new LinkedList<>(top.subList(top.size() / 2, top.size()));
      res.add(first);
      res.add(second);

      // Sort the region list by size of regions in contig
      res.sort(Comparator.comparingInt(List::size));
      Collections.reverse(res);
    }
  }
}
