import warnings
from collections import namedtuple
from typing import List

import pandas as pd
import pyarrow as pa

from . import libtiledbvcf

try:
    import tiledb.cloud

    can_use_cloud = True
except Exception:
    can_use_cloud = False

try:
    import tiledb

    can_use_embedded = True
except Exception:
    can_use_embebbed = False

DEFAULT_ATTRS = [
    "sample_name",
    "contig",
    "pos_start",
    "alleles",
    "fmt_GT",
]

ReadConfig = namedtuple(
    "ReadConfig",
    [
        # Max number of records (rows) to read.
        "limit",
        # Region partition tuple (idx, num_partitions)
        "region_partition",
        # Samples partition tuple (idx, num_partitions)
        "sample_partition",
        # Whether or not to sort the regions to be read (default True)
        "sort_regions",
        # Memory budget (MB) for buffer and internal allocations (default 2048MB)
        "memory_budget_mb",
        # List of strings of format 'option=value'
        "tiledb_config",
        # Percentage of memory to dedicate to TileDB Query Buffers (default: 25)
        "buffer_percentage",
        # Percentage of memory to dedicate to TileDB Tile Cache (default: 10)
        "tiledb_tile_cache_percentage",
    ],
)
"""
Config settings for a TileDB-VCF dataset.

Attributes
----------
limit : int
    Max number of records (rows) to read
region_partition : tuple
    Region partition tuple (idx, num_partitions)
sample_partition : tuple
    Samples partition tuple (idx, num_partitions)
sort_regions : bool
    Whether or not to sort the regions to be read, default True
memory_budget_mb : int
    Memory budget (MB) for buffer and internal allocations, default 2048MB
tiledb_config : List[str]
    List of strings of format 'option=value'
buffer_percentage : int
    Percentage of memory to dedicate to TileDB Query Buffers, default 25
tiledb_tile_cache_percentage : int
    Percentage of memory to dedicate to TileDB Tile Cache, default 10
"""
ReadConfig.__new__.__defaults__ = (None,) * 8  # len(ReadConfig._fields)


def config_logging(level: str = "fatal", log_file: str = ""):
    """
    Configure tiledbvcf logging.

    Parameters
    ----------
    level
        Log level from (fatal|error|warn|info|debug|trace)
    log_file
        Log file path.
    """
    if level not in ["fatal", "error", "warn", "info", "debug", "trace"]:
        raise Exception(f"Unsupported log level: {level}")

    libtiledbvcf.config_logging(level, log_file)


class Dataset(object):
    """
    A class that provides read/write access to a TileDB-VCF dataset.

    Parameters
    ----------
    uri
        URI of the dataset.
    mode
        Mode of operation ('r'|'w')
    cfg
        TileDB-VCF configuration.
    stats
        Enable internal TileDB statistics.
    verbose
        Enable verbose output.
    tiledb_config
        TileDB configuration, alternative to `cfg.tiledb_config`.
    """

    def __init__(
        self,
        uri: str,
        mode: str = "r",
        cfg: ReadConfig = None,
        stats: bool = False,
        verbose: bool = False,
        tiledb_config: dict = None,
    ):
        if cfg and tiledb_config:
            raise Exception("Cannot specify both cfg and tiledb_config")

        if tiledb_config:
            cfg = ReadConfig(tiledb_config=tiledb_config)

        self.uri = uri
        self.mode = mode
        self.cfg = cfg
        if self.mode == "r":
            self.reader = libtiledbvcf.Reader()
            self.reader.set_verbose(verbose)
            self._set_read_cfg(cfg)
            self.reader.init(uri)
            self.reader.set_tiledb_stats_enabled(stats)
        elif self.mode == "w":
            self.writer = libtiledbvcf.Writer()
            self.writer.set_verbose(verbose)
            self._set_write_cfg(cfg)
            self.writer.init(uri)
            self.writer.set_tiledb_stats_enabled(stats)
        else:
            raise Exception("Unsupported dataset mode {}".format(mode))

    def _set_read_cfg(self, cfg):
        if cfg is None:
            return
        if cfg.limit is not None:
            self.reader.set_max_num_records(cfg.limit)
        if cfg.region_partition is not None:
            self.reader.set_region_partition(*cfg.region_partition)
        if cfg.sample_partition is not None:
            self.reader.set_sample_partition(*cfg.sample_partition)
        if cfg.sort_regions is not None:
            self.reader.set_sort_regions(cfg.sort_regions)
        if cfg.memory_budget_mb is not None:
            self.reader.set_memory_budget(cfg.memory_budget_mb)
        if cfg.buffer_percentage is not None:
            self.reader.set_buffer_percentage(cfg.buffer_percentage)
        if cfg.tiledb_tile_cache_percentage is not None:
            self.reader.set_tiledb_tile_cache_percentage(
                cfg.tiledb_tile_cache_percentage
            )
        if cfg.tiledb_config is not None:
            tiledb_config_list = list()
            if isinstance(cfg.tiledb_config, list):
                tiledb_config_list = cfg.tiledb_config
            # Support dictionaries and tiledb.Config objects also
            elif isinstance(cfg.tiledb_config, dict):
                for key in cfg.tiledb_config:
                    if cfg.tiledb_config[key] != "":
                        tiledb_config_list.append(
                            "{}={}".format(key, cfg.tiledb_config[key])
                        )
            else:
                try:
                    import tiledb

                    if isinstance(cfg.tiledb_config, tiledb.Config):
                        for key in cfg.tiledb_config:
                            if cfg.tiledb_config[key] != "":
                                tiledb_config_list.append(
                                    "{}={}".format(key, cfg.tiledb_config[key])
                                )
                except ImportError:
                    pass
            self.reader.set_tiledb_config(",".join(tiledb_config_list))

    def _set_write_cfg(self, cfg):
        if cfg is None:
            return
        if cfg.tiledb_config is not None:
            tiledb_config_list = list()
            if isinstance(cfg.tiledb_config, list):
                tiledb_config_list = cfg.tiledb_config
            # Support dictionaries and tiledb.Config objects also
            elif isinstance(cfg.tiledb_config, dict):
                for key in cfg.tiledb_config:
                    if cfg.tiledb_config[key] != "":
                        tiledb_config_list.append(
                            "{}={}".format(key, cfg.tiledb_config[key])
                        )
            else:
                try:
                    import tiledb

                    if isinstance(cfg.tiledb_config, tiledb.Config):
                        for key in cfg.tiledb_config:
                            if cfg.tiledb_config[key] != "":
                                tiledb_config_list.append(
                                    "{}={}".format(key, cfg.tiledb_config[key])
                                )
                except ImportError:
                    pass
            self.writer.set_tiledb_config(",".join(tiledb_config_list))

    def read_arrow(
        self,
        attrs: List[str] = DEFAULT_ATTRS,
        samples: (str, List[str]) = None,
        regions: (str, List[str]) = None,
        samples_file: str = None,
        bed_file: str = None,
        skip_check_samples: bool = False,
        set_af_filter: str = "",
        scan_all_samples: bool = False,
        enable_progress_estimation: bool = False,
    ) -> pa.Table:
        """
        Read data from the dataset into a PyArrow Table.

        For large queries, a call to `read_arrow()` may not be able to fit all
        results in memory. In that case, the returned table will contain as
        many results as possible, and in order to retrieve the rest of the
        results, use the `continue_read()` function.

        Parameters
        ----------
        attrs
            List of attribute names to be read.
        samples
            Sample names to be read.
        regions
            Genomic regions to be read.
        samples_file
            URI of file containing sample names to be read, one per line.
        bed_file
            URI of a BED file of genomic regions to be read.
        skip_check_samples
            Skip checking if the samples in `samples_file` exist in the dataset.
        set_af_filter
            Filter variants by internal allele frequency. For example, to include
            variants with AF > 0.1, set this to ">0.1".
        scan_all_samples
            Scan all samples when computing internal allele frequency.
        enable_progress_estimation
            **DEPRECATED** - This parameter will be removed in a future release.

        Returns
        -------
        :
            Query results as a PyArrow Table.
        """
        if self.mode != "r":
            raise Exception("Dataset not open in read mode")

        if isinstance(regions, str):
            regions = [regions]
        if isinstance(samples, str):
            samples = [samples]

        self.reader.reset()
        self.reader.set_export_to_disk(False)
        self._set_samples(samples, samples_file)

        regions = "" if regions is None else regions
        self.reader.set_regions(",".join(regions))
        self.reader.set_attributes(attrs)
        self.reader.set_check_samples_exist(not skip_check_samples)
        self.reader.set_af_filter(set_af_filter)
        self.reader.set_scan_all_samples(scan_all_samples)
        self.reader.set_enable_progress_estimation(enable_progress_estimation)

        if bed_file is not None:
            self.reader.set_bed_file(bed_file)

        return self.continue_read_arrow()

    def read_variant_stats(
        self,
        region: str = None,
    ) -> pd.DataFrame:
        """
        Read variant stats from the dataset into a Pandas DataFrame

        Parameters
        ----------
        region
            Genomic region to be queried.
        """
        if self.mode != "r":
            raise Exception("Dataset not open in read mode")
        self.reader.set_regions(region)
        return self.reader.get_variant_stats_results()

    def read_allele_count(
        self,
        region: str = None,
    ) -> pd.DataFrame:
        """
        Read allele count from the dataset into a Pandas DataFrame

        Parameters
        ----------
        region
            Genomic region to be queried.
        """
        if self.mode != "r":
            raise Exception("Dataset not open in read mode")
        self.reader.set_regions(region)
        return self.reader.get_allele_count_results()

    def read(
        self,
        attrs: List[str] = DEFAULT_ATTRS,
        samples: (str, List[str]) = None,
        regions: (str, List[str]) = None,
        samples_file: str = None,
        bed_file: str = None,
        skip_check_samples: bool = False,
        set_af_filter: str = "",
        scan_all_samples: bool = False,
        enable_progress_estimation: bool = False,
    ) -> pd.DataFrame:
        """
        Read data from the dataset into a Pandas DataFrame.

        For large datasets, a call to `read()` may not be able to fit all
        results in memory. In that case, the returned table will contain as
        many results as possible, and in order to retrieve the rest of the
        results, use the `continue_read()` function.

        You can also use the Python generator version, `read_iter()`.

        Parameters
        ----------
        attrs
            List of attribute names to be read.
        samples
            Sample names to be read.
        regions
            Genomic regions to be read.
        samples_file
            URI of file containing sample names to be read, one per line.
        bed_file
            URI of a BED file of genomic regions to be read.
        skip_check_samples
            Skip checking if the samples in `samples_file` exist in the dataset.
        set_af_filter
            Filter variants by internal allele frequency. For example, to include
            variants with AF > 0.1, set this to ">0.1".
        enable_progress_estimation
            **DEPRECATED** - This parameter will be removed in a future release.

        Returns
        -------
        :
            Query results as a Pandas DataFrame.
        """
        if self.mode != "r":
            raise Exception("Dataset not open in read mode")

        if isinstance(regions, str):
            regions = [regions]
        if isinstance(samples, str):
            samples = [samples]

        self.reader.reset()
        self.reader.set_export_to_disk(False)
        self._set_samples(samples, samples_file)

        regions = "" if regions is None else regions
        self.reader.set_regions(",".join(regions))
        self.reader.set_attributes(attrs)
        self.reader.set_check_samples_exist(not skip_check_samples)
        self.reader.set_af_filter(set_af_filter)
        self.reader.set_scan_all_samples(scan_all_samples)
        self.reader.set_enable_progress_estimation(enable_progress_estimation)

        if bed_file is not None:
            self.reader.set_bed_file(bed_file)

        return self.continue_read()

    def export(
        self,
        samples: (str, List[str]) = None,
        regions: (str, List[str]) = None,
        samples_file: str = None,
        bed_file: str = None,
        skip_check_samples: bool = False,
        enable_progress_estimation: bool = False,
        merge: bool = False,
        output_format: str = "z",
        output_path: str = "",
        output_dir: str = ".",
    ):
        """
        Exports data to multiple VCF files or a combined VCF file.

        Parameters
        ----------
        samples
            Sample names to be read.
        regions
            Genomic regions to be read.
        samples_file
            URI of file containing sample names to be read, one per line.
        bed_file
            URI of a BED file of genomic regions to be read.
        skip_check_samples
            Skip checking if the samples in `samples_file` exist in the dataset.
        set_af_filter
            Filter variants by internal allele frequency. For example, to include
            variants with AF > 0.1, set this to ">0.1".
        scan_all_samples
            Scan all samples when computing internal allele frequency.
        enable_progress_estimation
            **DEPRECATED** - This parameter will be removed in a future release.
        merge
            Merge samples to create a combined VCF file.
        output_format
            Export file format: 'b': bcf (compressed), 'u': bcf, 'z':vcf.gz, 'v': vcf.
        output_path
            Combined VCF output file.
        output_dir
            Directory used for local output of exported samples.
        """
        if self.mode != "r":
            raise Exception("Dataset not open in read mode")

        if isinstance(regions, str):
            regions = [regions]
        if isinstance(samples, str):
            samples = [samples]

        self.reader.reset()
        self.reader.set_export_to_disk(True)
        self._set_samples(samples, samples_file)

        regions = "" if regions is None else regions
        self.reader.set_regions(",".join(regions))
        self.reader.set_check_samples_exist(not skip_check_samples)
        self.reader.set_enable_progress_estimation(enable_progress_estimation)
        self.reader.set_merge(merge)
        self.reader.set_output_format(output_format)
        self.reader.set_output_path(output_path)
        self.reader.set_output_dir(output_dir)

        if merge and not output_path:
            raise Exception("output_path required when merge=True")

        if bed_file is not None:
            self.reader.set_bed_file(bed_file)

        self.reader.read()
        if not self.read_completed():
            raise Exception("Unexpected read status during export.")

    def read_iter(
        self,
        attrs: List[str] = DEFAULT_ATTRS,
        samples: (str, List[str]) = None,
        regions: (str, List[str]) = None,
        samples_file: str = None,
        bed_file: str = None,
    ):
        """
        Iterator version of `read()`.

        Parameters
        ----------
        attrs
            List of attribute names to be read.
        samples
            Sample names to be read.
        regions
            Genomic regions to be read.
        samples_file
            URI of file containing sample names to be read, one per line.
        bed_file
            URI of a BED file of genomic regions to be read.
        """
        if self.mode != "r":
            raise Exception("Dataset not open in read mode")

        if isinstance(regions, str):
            regions = [regions]
        if isinstance(samples, str):
            samples = [samples]

        if not self.read_completed():
            yield self.read(attrs, samples, regions, samples_file, bed_file)
        while not self.read_completed():
            yield self.continue_read()

    def continue_read(self, release_buffers: bool = True) -> pd.DataFrame:
        """
        Continue an incomplete read.

        Parameters
        ----------
        release_buffers
            Release the buffers after reading.

        Returns
        -------
        :
            The next batch of data as a Pandas DataFrame.
        """
        table = self.continue_read_arrow(release_buffers=release_buffers)

        return table.to_pandas()

    def continue_read_arrow(self, release_buffers: bool = True) -> pa.Table:
        """
        Continue an incomplete read.

        Parameters
        ----------
        release_buffers
            Release the buffers after reading.

        Returns
        -------
        :
            The next batch of data as a PyArrow Table.
        """
        if self.mode != "r":
            raise Exception("Dataset not open in read mode")

        self.reader.read(release_buffers)
        try:
            table = self.reader.get_results_arrow()
        except:
            # Return an empty table
            table = pa.Table.from_pandas(pd.DataFrame())
        return table

    def read_completed(self) -> bool:
        """
        Returns true if the previous read operation was complete.
        A read is considered complete if the resulting dataframe contained
        all results.

        Returns
        -------
            True if the previous read operation was complete.
        """
        if self.mode != "r":
            raise Exception("Dataset not open in read mode")
        return self.reader.completed()

    def count(
        self,
        samples: (str, List[str]) = None,
        regions: (str, List[str]) = None,
    ) -> int:
        """
        Count records in the dataset.

        Parameters
        ----------
        samples
            Sample names to include in the count.
        regions
            Genomic regions to include in the count.

        Returns
        -------
        :
            Number of intersecting records in the dataset.
        """
        if self.mode != "r":
            raise Exception("Dataset not open in read mode")

        if isinstance(regions, str):
            regions = [regions]
        if isinstance(samples, str):
            samples = [samples]

        self.reader.reset()
        self.reader.set_export_to_disk(False)

        samples = "" if samples is None else samples
        regions = "" if regions is None else regions
        self.reader.set_samples(",".join(samples))
        self.reader.set_regions(",".join(regions))

        self.reader.read()
        if not self.read_completed():
            raise Exception("Unexpected read status during count.")

        return self.reader.result_num_records()

    def create_dataset(
        self,
        extra_attrs: str = None,
        vcf_attrs: str = None,
        tile_capacity: int = 10000,
        anchor_gap: int = 1000,
        checksum_type: str = "sha256",
        allow_duplicates: bool = True,
        enable_allele_count: bool = True,
        enable_variant_stats: bool = True,
        enable_sample_stats: bool = True,
        compress_sample_dim: bool = True,
        compression_level: int = 4,
        variant_stats_version: int = 2,
    ):
        """
        Create a new dataset.

        Parameters
        ----------
        extra_attrs
            CSV list of extra attributes to materialize from fmt and info fields.
        vcf_attrs
            URI of VCF file with all fmt and info fields to materialize in the dataset.
        tile_capacity
            Tile capacity to use for the array schema.
        anchor_gap
            Length of gaps between inserted anchor records in bases.
        checksum_type
            Optional checksum type for the dataset, "sha256" or "md5".
        allow_duplicates
            Allow records with duplicate start positions to be written to the array.
        enable_allele_count
            Enable the allele count ingestion task.
        enable_variant_stats
            Enable the variant stats ingestion task.
        enable_sample_stats
            Enable the sample stats ingestion task.
        compress_sample_dim
            Enable compression on the sample dimension.
        compression_level
            Compression level for zstd compression.
        variant_stats_version
            Version of the variant stats array.
        """
        if self.mode != "w":
            raise Exception("Dataset not open in write mode")

        if extra_attrs is not None and vcf_attrs is not None:
            raise Exception("Cannot provide both extra_attrs and vcf_attrs.")

        if extra_attrs is not None:
            self.writer.set_extra_attributes(",".join(extra_attrs))

        if vcf_attrs is not None:
            self.writer.set_vcf_attributes(vcf_attrs)

        if tile_capacity is not None:
            self.writer.set_tile_capacity(tile_capacity)

        if anchor_gap is not None:
            self.writer.set_anchor_gap(anchor_gap)

        if checksum_type is not None:
            if checksum_type.lower() not in ["sha256", "md5"]:
                raise Exception("Invalid checksum_type, must be 'sha256' or 'md5'.")
            checksum_type = checksum_type.lower()
            self.writer.set_checksum(checksum_type)

        self.writer.set_allow_duplicates(allow_duplicates)

        if enable_allele_count is not None:
            self.writer.set_enable_allele_count(enable_allele_count)

        if enable_variant_stats is not None:
            self.writer.set_enable_variant_stats(enable_variant_stats)

        if enable_sample_stats is not None:
            self.writer.set_enable_sample_stats(enable_sample_stats)

        if compress_sample_dim is not None:
            self.writer.set_compress_sample_dim(compress_sample_dim)

        if compression_level is not None:
            self.writer.set_compression_level(compression_level)

        if variant_stats_version is not None:
            self.writer.set_variant_stats_version(variant_stats_version)

        # This call throws an exception if the dataset already exists.
        self.writer.create_dataset()

    def delete_dataset(self):
        is_cloud_uri = self.uri.startswith("tiledb:")
        if is_cloud_uri:
            if can_use_cloud:
                tiledb.cloud.asset.delete(self.uri)
            else:
                raise Exception(
                    "Deleting this dataset requires the tiledb.cloud package"
                )
        else:
            if can_use_embedded:
                with tiledb.scope_ctx(self.cfg):
                    with tiledb.Group(self.uri, "m") as g:
                        g.delete(recursive=True)
            else:
                raise Exception("Deleting this dataset requires the tiledb package")

    def ingest_samples(
        self,
        sample_uris: List[str] = None,
        threads: int = None,
        total_memory_budget_mb: int = None,
        total_memory_percentage: float = None,
        ratio_tiledb_memory: float = None,
        max_tiledb_memory_mb: int = None,
        input_record_buffer_mb: int = None,
        avg_vcf_record_size: int = None,
        ratio_task_size: float = None,
        ratio_output_flush: float = None,
        scratch_space_path: str = None,
        scratch_space_size: int = None,
        sample_batch_size: int = None,
        resume: bool = False,
        contig_fragment_merging: bool = True,
        contigs_to_keep_separate: List[str] = None,
        contigs_to_allow_merging: List[str] = None,
        contig_mode: str = "all",
        thread_task_size: int = None,
        memory_budget_mb: int = None,
        record_limit: int = None,
    ):
        """
        Ingest VCF files into the dataset.

        Parameters
        ----------
        sample_uris
            List of sample URIs to ingest.
        threads
            Set the number of threads used for ingestion.
        total_memory_budget_mb
            Total memory budget for ingestion (MiB).
        total_memory_percentage
            Percentage of total system memory used for ingestion
            (overrides 'total_memory_budget_mb').
        ratio_tiledb_memory
            Ratio of memory budget allocated to `TileDB::sm.mem.total_budget`.
        max_tiledb_memory_mb
            Maximum memory allocated to TileDB::sm.mem.total_budget (MiB).
        input_record_buffer_mb
            Size of input record buffer for each sample file (MiB).
        avg_vcf_record_size
            Average VCF record size (bytes).
        ratio_task_size
            Ratio of worker task size to computed task size.
        ratio_output_flush
            Ratio of output buffer capacity that triggers a flush to TileDB.
        scratch_space_path
            Directory used for local storage of downloaded remote samples.
        scratch_space_size
            Amount of local storage that can be used for downloading
            remote samples (MB).
        sample_batch_size
            Number of samples per batch for ingestion (default 10).
        resume
            Whether to check and attempt to resume a partial completed ingestion.
        contig_fragment_merging
            Whether to enable merging of contigs into fragments. This
            overrides the contigs-to-keep-separate/contigs-to-allow-
            merging options. Generally contig fragment merging is good,
            this is a performance optimization to reduce the prefixes on
            a s3/azure/gcs bucket when there is a large number of pseudo
            contigs which are small in size.
        contigs_to_keep_separate
            List of contigs that should not be merged into combined
            fragments. The default list includes all standard human
            chromosomes in both UCSC (e.g., chr1) and Ensembl (e.g., 1)
            formats.
        contigs_to_allow_merging
            List of contigs that should be allowed to be merged into
            combined fragments.
        contig_mode
            Select which contigs are ingested: 'all', 'separate', or 'merged'.
        thread_task_size
            **DEPRECATED** - This parameter will be removed in a future release.
        memory_budget_mb
            **DEPRECATED** - This parameter will be removed in a future release.
        record_limit
            **DEPRECATED** - This parameter will be removed in a future release.
        """

        if self.mode != "w":
            raise Exception("Dataset not open in write mode")

        if sample_uris is None:
            return

        if threads is not None:
            self.writer.set_num_threads(threads)

        if total_memory_budget_mb is not None:
            self.writer.set_total_memory_budget_mb(total_memory_budget_mb)

        if total_memory_percentage is not None:
            self.writer.set_total_memory_percentage(total_memory_percentage)

        if ratio_tiledb_memory is not None:
            self.writer.set_ratio_tiledb_memory(ratio_tiledb_memory)

        if max_tiledb_memory_mb is not None:
            self.writer.set_max_tiledb_memory_mb(max_tiledb_memory_mb)

        if input_record_buffer_mb is not None:
            self.writer.set_input_record_buffer_mb(input_record_buffer_mb)

        if avg_vcf_record_size is not None:
            self.writer.set_avg_vcf_record_size(avg_vcf_record_size)

        if ratio_task_size is not None:
            self.writer.set_ratio_task_size(ratio_task_size)

        if ratio_output_flush is not None:
            self.writer.set_ratio_output_flush(ratio_output_flush)

        if thread_task_size is not None:
            self.writer.set_thread_task_size(thread_task_size)

        if memory_budget_mb is not None:
            self.writer.set_memory_budget(memory_budget_mb)

        if scratch_space_path is not None and scratch_space_size is not None:
            self.writer.set_scratch_space(scratch_space_path, scratch_space_size)
        elif scratch_space_path is not None or scratch_space_size is not None:
            raise Exception(
                "Must set both scratch_space_path and scratch_space_size to use scratch space"
            )

        if record_limit is not None:
            self.writer.set_max_num_records(record_limit)

        if sample_batch_size is not None:
            self.writer.set_sample_batch_size(sample_batch_size)

        # set whether to attempt partial sample ingestion resumption
        self.writer.set_resume(resume)

        self.writer.set_contig_fragment_merging(contig_fragment_merging)

        if contigs_to_keep_separate is not None:
            if not isinstance(contigs_to_keep_separate, list):
                raise Exception("contigs_to_keep_separate must be a list")

            self.writer.set_contigs_to_keep_separate(contigs_to_keep_separate)

        if contigs_to_allow_merging is not None:
            if not isinstance(contigs_to_allow_merging, list):
                raise Exception("contigs_to_allow_merging must be a list")

            self.writer.set_contigs_to_allow_merging(contigs_to_allow_merging)

        if contig_mode is not None:
            contig_mode_map = {"all": 0, "separate": 1, "merged": 2}
            if contig_mode not in contig_mode_map:
                raise Exception("contig_mode must be 'all', 'separate' or 'merged'")

            self.writer.set_contig_mode(contig_mode_map[contig_mode])

        self.writer.set_samples(",".join(sample_uris))

        # Only v2 and v3 datasets need registration
        if self.schema_version() < 4:
            self.writer.register_samples()
        self.writer.ingest_samples()

    def tiledb_stats(self) -> str:
        """
        Get TileDB stats as a string.

        Returns
        -------
        :
            TileDB stats as a string.
        """
        if self.mode == "r":
            if not self.reader.get_tiledb_stats_enabled:
                raise Exception("TileDB read stats not enabled")
            return self.reader.get_tiledb_stats()

        if self.mode == "w":
            if not self.writer.get_tiledb_stats_enabled:
                raise Exception("TileDB write stats not enabled")
            return self.writer.get_tiledb_stats()

    def schema_version(self) -> int:
        """
        Get the VCF schema version of the dataset.

        Returns
        -------
        :
            VCF schema version of the dataset.
        """
        if self.mode != "r":
            return self.writer.get_schema_version()
        return self.reader.get_schema_version()

    def sample_count(self) -> int:
        """
        Get the number of samples in the dataset.

        Returns
        -------
        :
            Number of samples in the dataset.
        """
        if self.mode != "r":
            raise Exception("Samples can only be retrieved for reader")
        return self.reader.get_sample_count()

    def samples(self) -> list:
        """
        Get the list of samples in the dataset.

        Returns
        -------
        :
            List of samples in the dataset.
        """
        if self.mode != "r":
            raise Exception("Sample names can only be retrieved for reader")
        return self.reader.get_sample_names()

    def attributes(self, attr_type="all") -> list:
        """
        Return a list of queryable attributes available in the VCF dataset.

        Parameters
        ----------
        attr_type : str
            The subset of attributes to retrieve; "info" or "fmt" will
            only retrieve attributes ingested from the VCF INFO and
            FORMAT fields, respectively, "builtin" retrieves the static
            attributes defined in TileDB-VCF's schema, "all" (the
            default) returns all queryable attributes.

        Returns
        -------
        :
            A list of attribute names.
        """

        if self.mode != "r":
            raise Exception("Attributes can only be retrieved in read mode")

        attr_types = ("all", "info", "fmt", "builtin")
        if attr_type not in attr_types:
            raise ValueError("Invalid attribute type. Must be one of: %s" % attr_types)

        # combined attributes with type object
        comb_attrs = ("info", "fmt")

        if attr_type == "info":
            return self.reader.get_info_attributes()
        elif attr_type == "fmt":
            return self.reader.get_fmt_attributes()
        elif attr_type == "builtin":
            return self.reader.get_materialized_attributes()
        else:
            return self.reader.get_queryable_attributes()

    def _set_samples(self, samples=None, samples_file=None):
        if samples is not None and samples_file is not None:
            raise TypeError(
                "Argument 'samples' not allowed with 'samples_file'. "
                "Only one of these two arguments can be passed at a time."
            )
        elif samples is not None:
            self.reader.set_samples(",".join(samples))
        elif samples_file is not None:
            self.reader.set_samples("")
            self.reader.set_samples_file(samples_file)
        else:
            self.reader.set_samples("")

    def version(self) -> str:
        """
        Return the TileDB-VCF version used to create the dataset.

        Returns
        -------
        :
            The TileDB-VCF version.
        """
        if self.mode == "r":
            return self.reader.version()

        return self.writer.version()

    def map_dask(self, *args, **kwargs):
        """
        Maps a function on a Dask dataframe obtained by reading from the dataset.

        May be more efficient in some cases than read_dask() followed by a regular
        Dask map operation.

        The remaining parameters are the same as the read_dask() function.

        :return: Dask DataFrame with results
        """

        # Delay importing dask until it is needed
        from . import dask_functions

        return dask_functions.map_dask(self, *args, **kwargs)

    def read_dask(self, *args, **kwargs):
        """
        Reads data from a TileDB-VCF into a Dask DataFrame.

        Partitioning proceeds by a straightforward block distribution, parameterized
        by the total number of partitions and the particular partition index that
        a particular read operation is responsible for.

        Both region and sample partitioning can be used together.

        The remaining parameters are the same as the normal read() function.

        :param int region_partition: Number of partitions over regions
        :param int sample_partition: Number of partitions over samples
        :param int limit_partitions: Maximum number of partitions to read (for testing/debugging)

        :return: Dask DataFrame with results
        """

        # Delay importing dask until it is needed
        from . import dask_functions

        return dask_functions.read_dask(self, *args, **kwargs)


class TileDBVCFDataset(Dataset):
    """
    A class that provides read/write access to a TileDB-VCF dataset.

    **DEPRECATED** - This class will be removed in a future release, use `Dataset` instead.

    Parameters
    ----------
    uri
        URI of the dataset.
    mode
        Mode of operation ('r'|'w')
    cfg
        TileDB-VCF configuration.
    stats
        Enable internal TileDB statistics.
    verbose
        Enable verbose output.
    """

    def __init__(self, uri, mode="r", cfg=None, stats=False, verbose=False):
        warnings.warn(
            "TileDBVCFDataset is deprecated, use Dataset instead", DeprecationWarning
        )
        super().__init__(uri, mode, cfg, stats, verbose)
