package io.tiledb.libvcfnative;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class VCFBedFile implements AutoCloseable {
  private long bedFilePtr;
  private final String bedFileURI;

  private final Map<String, List<Region>> contigRegions;

  private long totalRegions;

  public static class Region {
    public String regionStr;
    public String regionContig;
    public long regionStart;
    public long regionEnd;

    public Region(String regionStr, String regionContig, long regionStart, long regionEnd) {
      this.regionStr = regionStr;
      this.regionContig = regionContig;
      this.regionStart = regionStart;
      this.regionEnd = regionEnd;
    }
  }

  public VCFBedFile(VCFReader reader, String bedFileURI) {
    long[] bedFilePtrArray = new long[1];
    int rc = LibVCFNative.tiledb_vcf_bed_file_alloc(bedFilePtrArray);
    if (rc != 0 || bedFilePtrArray[0] == 0) {
      throw new RuntimeException("Error allocating bed file object");
    }

    bedFilePtr = bedFilePtrArray[0];

    rc = LibVCFNative.tiledb_vcf_bed_file_parse(reader.ptr(), bedFilePtr, bedFileURI);
    if (rc != 0) {
      String msg = getLastErrorMessage();
      throw new RuntimeException("Error parsing bed file: " + msg);
    }
    this.bedFileURI = bedFileURI;

    // Set total count
    long[] totalCount = new long[1];
    rc = LibVCFNative.tiledb_vcf_bed_file_get_total_region_count(bedFilePtr, totalCount);
    if (rc != 0) {
      String msg = getLastErrorMessage();
      throw new RuntimeException("Error getting bed file region count: " + msg);
    }
    totalRegions = totalCount[0];

    // Build contig list
    contigRegions = new HashMap<>();

    long[] contigCount = new long[1];
    rc = LibVCFNative.tiledb_vcf_bed_file_get_contig_count(bedFilePtr, contigCount);
    if (rc != 0) {
      String msg = getLastErrorMessage();
      throw new RuntimeException("Error getting bed file contig count: " + msg);
    }

    long[] contigRegionCount = new long[1];
    byte[] regionStrBytes = new byte[1024];
    byte[] regionContigBytes = new byte[1024];
    long[] regionStart = new long[1];
    long[] regionEnd = new long[1];
    for (long contigIndex = 0; contigIndex < contigCount[0]; contigIndex++) {
      LibVCFNative.tiledb_vcf_bed_file_get_contig_region_count(
          bedFilePtr, contigIndex, contigRegionCount);
      String regionContig = "";

      // We use linked list because ArrayList are 32bit
      List<Region> regionList = new LinkedList<>();
      for (long regionIndex = 0; regionIndex < contigRegionCount[0]; regionIndex++) {
        rc =
            LibVCFNative.tiledb_vcf_bed_file_get_contig_region(
                bedFilePtr,
                contigIndex,
                regionIndex,
                regionStrBytes,
                regionContigBytes,
                regionStart,
                regionEnd);
        if (rc != 0) {
          String msg = getLastErrorMessage();
          throw new RuntimeException("Error listing bed file regions: " + msg);
        }

        int j;
        for (j = 0; j < regionStrBytes.length && regionStrBytes[j] != 0; j++) {}
        String regionStr = new String(regionStrBytes, 0, j);

        j = 0;
        for (j = 0; j < regionContigBytes.length && regionContigBytes[j] != 0; j++) {}
        regionContig = new String(regionContigBytes, 0, j);

        regionList.add(new Region(regionStr, regionContig, regionStart[0], regionEnd[0]));
      }
      contigRegions.put(regionContig, regionList);
    }
  }

  public long getTotalRegions() {
    return totalRegions;
  }

  public long getContigCount() {
    return contigRegions.size();
  }

  public Map<String, List<Region>> getContigRegions() {
    return contigRegions;
  }

  public Map<String, List<String>> getContigRegionStrings() {
    Map<String, List<String>> res = new HashMap<>();
    for (Map.Entry<String, List<Region>> regions : contigRegions.entrySet()) {
      // We use linked list because ArrayList are 32bit
      List<String> l = new LinkedList<>();
      for (Region region : regions.getValue()) {
        l.add(region.regionStr);
      }

      res.put(regions.getKey(), l);
    }

    return res;
  }

  private String getLastErrorMessage() {
    String msg = LibVCFNative.tiledb_vcf_bed_file_get_last_error_message(bedFilePtr);
    if (msg == null) {
      return "";
    }
    return msg;
  }

  @Override
  public void close() {
    if (bedFilePtr != 0L) {
      LibVCFNative.tiledb_vcf_bed_file_free(bedFilePtr);
    }
    bedFilePtr = 0L;
  }
}
