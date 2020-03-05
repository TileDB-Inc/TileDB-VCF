package io.tiledb.vcf;

import java.io.Serializable;

/** Convenience class holding information about a VCF reader partition. */
public class VCFPartitionInfo implements Serializable {
  private int index;
  private int numPartitions;

  public VCFPartitionInfo(int index, int numPartitions) {
    this.index = index;
    this.numPartitions = numPartitions;
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
}
