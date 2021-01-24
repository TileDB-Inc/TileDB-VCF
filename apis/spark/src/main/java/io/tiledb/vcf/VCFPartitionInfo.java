package io.tiledb.vcf;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** Convenience class holding information about a VCF reader partition. */
public class VCFPartitionInfo implements Serializable {
  private final int index;
  private final int numPartitions;
  private final List<String> regions;

  public VCFPartitionInfo(int index, int numPartitions, List<String> regions) {
    this.index = index;
    this.numPartitions = numPartitions;
    if (regions != null) {
      this.regions = regions;
    } else {
      this.regions = new ArrayList<>();
    }
  }

  /**
   * The partition index
   *
   * @return index
   */
  public int getIndex() {
    return index;
  }

  /**
   * The total number of partitions
   *
   * @return total number of partitions
   */
  public int getNumPartitions() {
    return numPartitions;
  }

  /**
   * List of regions for partition
   *
   * @return list of regions for partition
   */
  public List<String> getRegions() {
    return regions;
  }
}
