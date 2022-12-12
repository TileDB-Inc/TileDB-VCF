import pandas as pd
import pyarrow as pa
import sys
import warnings

from collections import namedtuple
from . import libtiledbvcf

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
ReadConfig.__new__.__defaults__ = (None,) * 8  # len(ReadConfig._fields)


def config_logging(level="fatal", log_file=""):
    """Configure tiledbvcf logging.

    :param str level: log level "fatal,error,warn,info,debug,trace"
    :param str log_file: log message file
    """

    if level not in ["fatal", "error", "warn", "info", "debug", "trace"]:
        raise Exception(f"Unsupported log level: {level}")

    libtiledbvcf.config_logging(level, log_file)


class Dataset(object):
    """A handle on a TileDB-VCF dataset."""

    def __init__(self, uri, mode="r", cfg=None, stats=False, verbose=False):
        """Initializes a TileDB-VCF dataset for interaction.

        :param uri: URI of TileDB-VCF dataset
        :param mode: Mode of operation.
        :type mode: 'r' or 'w'
        :param cfg: TileDB VCF configuration (optional)
        :param stats: Enable internal TileDB statistics (default False)
        :param verbose: Enable TileDB-VCF verbose output (default False)
        """
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
        attrs,
        samples=None,
        regions=None,
        samples_file=None,
        bed_file=None,
        skip_check_samples=False,
        set_af_filter="",
        enable_progress_estimation=False,
    ):
        """Reads data from a TileDB-VCF dataset into Arrow table.

        For large datasets, a call to `read()` may not be able to fit all
        results in memory. In that case, the returned table will contain as
        many results as possible, and in order to retrieve the rest of the
        results, use the `continue_read()` function.

        You can also use the Python generator version, `read_iter()`.

        :param list of str attrs: List of attribute names to be read.
        :param list of str samples: CSV list of sample names to be read.
        :param list of str regions: CSV list of genomic regions to be read.
        :param str samples_file: URI of file containing sample names to be read,
            one per line.
        :param str bed_file: URI of a BED file of genomic regions to be read.
        :param bool skip_check_samples: Should checking the samples requested exist in the array
        :param bool enable_progress_estimation: Should we skip estimating the progress in verbose mode? Estimating progress can have performance or memory impacts in some cases.
        :return: Pandas DataFrame or PyArrow Array containing results.
        """
        if self.mode != "r":
            raise Exception("Dataset not open in read mode")

        self.reader.reset()
        self.reader.set_export_to_disk(False)
        self._set_samples(samples, samples_file)

        regions = "" if regions is None else regions
        self.reader.set_regions(",".join(regions))
        self.reader.set_attributes(attrs)
        self.reader.set_check_samples_exist(not skip_check_samples)
        self.reader.set_af_filter(set_af_filter)
        self.reader.set_enable_progress_estimation(enable_progress_estimation)

        if bed_file is not None:
            self.reader.set_bed_file(bed_file)

        return self.continue_read_arrow()

    def read(
        self,
        attrs,
        samples=None,
        regions=None,
        samples_file=None,
        bed_file=None,
        skip_check_samples=False,
        set_af_filter="",
        enable_progress_estimation=False,
    ):
        """Reads data from a TileDB-VCF dataset into Pandas dataframe.

        For large datasets, a call to `read()` may not be able to fit all
        results in memory. In that case, the returned table will contain as
        many results as possible, and in order to retrieve the rest of the
        results, use the `continue_read()` function.

        You can also use the Python generator version, `read_iter()`.

        :param list of str attrs: List of attribute names to be read.
        :param list of str samples: CSV list of sample names to be read.
        :param list of str regions: CSV list of genomic regions to be read.
        :param str samples_file: URI of file containing sample names to be read,
            one per line.
        :param str bed_file: URI of a BED file of genomic regions to be read.
        :param bool skip_check_samples: Should checking the samples requested exist in the array
        :param bool enable_progress_estimation: Should we skip estimating the progress in verbose mode? Estimating progress can have performance or memory impacts in some cases.
        :return: Pandas DataFrame or PyArrow Array containing results.
        """
        if self.mode != "r":
            raise Exception("Dataset not open in read mode")

        self.reader.reset()
        self.reader.set_export_to_disk(False)
        self._set_samples(samples, samples_file)

        regions = "" if regions is None else regions
        self.reader.set_regions(",".join(regions))
        self.reader.set_attributes(attrs)
        self.reader.set_check_samples_exist(not skip_check_samples)
        self.reader.set_af_filter(set_af_filter)
        self.reader.set_enable_progress_estimation(enable_progress_estimation)

        if bed_file is not None:
            self.reader.set_bed_file(bed_file)

        return self.continue_read()

    def export(
        self,
        samples=None,
        regions=None,
        samples_file=None,
        bed_file=None,
        skip_check_samples=False,
        enable_progress_estimation=False,
        merge=False,
        output_format="z",
        output_path="",
        output_dir=".",
    ):
        """Exports data from a TileDB-VCF dataset to multiple VCF files or a combined VCF file.

        :param list of str samples: CSV list of sample names to be read.
        :param list of str regions: CSV list of genomic regions to be read.
        :param str samples_file: URI of file containing sample names to be read,
            one per line.
        :param str bed_file: URI of a BED file of genomic regions to be read.
        :param bool skip_check_samples: Should checking the samples requested exist in the array
        :param bool enable_progress_estimation: Should we skip estimating the progress in verbose mode? Estimating progress can have performance or memory impacts in some cases.
        :param bool merge: Merge samples to create a combined VCF file.
        :param str output_format: Export file format: 'b': bcf (compressed), 'u': bcf, 'z': vcf.gz, 'v': vcf
        :param str output_path: Combined VCF output file.
        :param str output_dir: Directory used for local output of exported samples.
        """
        if self.mode != "r":
            raise Exception("Dataset not open in read mode")

        self.reader.reset()
        self.reader.set_export_to_disk(True)
        self._set_samples(samples, samples_file)

        regions = "" if regions is None else regions
        self.reader.set_regions(",".join(regions))
        # self.reader.set_attributes(attrs)
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
        self, attrs, samples=None, regions=None, samples_file=None, bed_file=None
    ):
        if self.mode != "r":
            raise Exception("Dataset not open in read mode")

        if not self.read_completed():
            yield self.read(attrs, samples, regions, samples_file, bed_file)
        while not self.read_completed():
            yield self.continue_read()

    def continue_read(self, release_buffers=True):
        """
        Continue an incomplete read
        :return: Pandas DataFrame
        """
        table = self.continue_read_arrow(release_buffers=release_buffers)

        return table.to_pandas()

    def continue_read_arrow(self, release_buffers=True):
        """
        Continue an incomplete read
        :return: PyArrow Table
        """
        if self.mode != "r":
            raise Exception("Dataset not open in read mode")

        self.reader.read(release_buffers)
        try:
            table = self.reader.get_results_arrow()
        except:
            table = pa.Table.from_pandas(pd.DataFrame())
        return table

    def read_completed(self):
        """Returns true if the previous read operation was complete.
        A read is considered complete if the resulting dataframe contained
        all results."""
        if self.mode != "r":
            raise Exception("Dataset not open in read mode")
        return self.reader.completed()

    def count(self, samples=None, regions=None):
        """Counts data in a TileDB-VCF dataset.

        :param list of str samples: CSV list of sample names to include in
            the count.
        :param list of str regions: CSV list of genomic regions include in
            the count
        :return: Number of intersecting records in the dataset
        """
        if self.mode != "r":
            raise Exception("Dataset not open in read mode")
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
        extra_attrs=None,
        vcf_attrs=None,
        tile_capacity=None,
        anchor_gap=None,
        checksum_type=None,
        allow_duplicates=True,
        enable_allele_count=False,
        enable_variant_stats=False,
    ):
        """Create a new dataset

        :param list of str extra_attrs: CSV list of extra attributes to
            materialize from fmt and info fields.
        :param str vcf_attrs: URI of VCF file with all fmt and info fields
            to materialize in the dataset.
        :param int tile_capacity: Tile capacity to use for the array schema
            (default = 10000).
        :param int anchor_gap: Length of gaps between inserted anchor records in
            bases (default = 1000).
        :param str checksum_type: Optional override checksum type for creating
            new dataset valid values are sha256, md5 or none.
        :param bool allow_duplicates: Allow records with duplicate start
            positions to be written to the array.
        :param bool enable_allele_count: Enable the allele count ingestion task.
        :param bool enable_variant_stats: Enable the variant stats ingestion task.
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
            checksum_type = checksum_type.lower()
            self.writer.set_checksum(checksum_type)

        self.writer.set_allow_duplicates(allow_duplicates)

        if enable_allele_count is not None:
            self.writer.set_enable_allele_count(enable_allele_count)

        if enable_variant_stats is not None:
            self.writer.set_enable_variant_stats(enable_variant_stats)

        # Create is a no-op if the dataset already exists.
        # TODO: Inform user if dataset already exists?
        self.writer.create_dataset()

    def ingest_samples(
        self,
        sample_uris=None,
        threads=None,
        total_memory_budget_mb=None,
        total_memory_percentage=None,
        ratio_tiledb_memory=None,
        max_tiledb_memory_mb=None,
        input_record_buffer_mb=None,
        avg_vcf_record_size=None,
        ratio_task_size=None,
        ratio_output_flush=None,
        scratch_space_path=None,
        scratch_space_size=None,
        sample_batch_size=None,
        resume=False,
        contig_fragment_merging=True,
        contigs_to_keep_separate=None,
        contigs_to_allow_merging=None,
        contig_mode="all",
        thread_task_size=None,
        memory_budget_mb=None,
        record_limit=None,
    ):
        """Ingest samples

        :param list of str sample_uris: CSV list of sample names to include in
            the count.
        :param int threads: Set the number of threads used for ingestion.
        :param int total_memory_budget_mb: Total memory budget for ingestion (MiB)
        :param float total_memory_percentage: Percentage of total system memory used for ingestion (overrides 'total_memory_budget_mb')
        :param float ratio_tiledb_memory: Ratio of memory budget allocated to TileDB::sm.mem.total_budget
        :param int max_tiledb_memory_mb: Maximum memory allocated to TileDB::sm.mem.total_budget (MiB)
        :param int input_record_buffer_mb: Size of input record buffer for each sample file (MiB)
        :param int avg_vcf_record_size: Average VCF record size (bytes)
        :param float ratio_task_size: Ratio of worker task size to computed task size
        :param float ratio_output_flush: Ratio of output buffer capacity that triggers a flush to TileDB
        :param str scratch_space_path: Directory used for local storage of
            downloaded remote samples.
        :param int scratch_space_size: Amount of local storage that can be used
            for downloading remote samples (MB).
        :param int sample_batch_size: Number of samples per batch for ingestion (default 10).
        :param bool resume: Whether to check and attempt to resume a partial completed ingestion
        :param bool contig_fragment_merging: Whether to enable merging of contigs into fragments. This overrides the contigs-to-keep-separate/contigs-to-allow-mering options. Generally contig fragment merging is good, this is a performance optimization to reduce the prefixes on a s3/azure/gcs bucket when there is a large number of pseduo contigs which are small in size.
        :param list contigs_to_keep_separate: List of contigs that should not be merged into combined fragments. The default list includes all standard human chromosomes in both UCSC (e.g., chr1) and Ensembl (e.g., 1) formats.
        :param list contigs_to_allow_merging: List of contigs that should be allowed to be merged into combined fragments.
        :param list contig_mode: Select which contigs are ingested: 'all', 'separate', or 'merged' contigs (default 'all').

        :param int thread_task_size: Set the max length (# columns) of an
            ingestion task. Affects load balancing of ingestion work across
            threads, and total memory consumption. (Legacy option)
        :param int memory_budget_mb: Set the max size (MB) of TileDB buffers before flushing
            (Legacy option)
        :param int record_limit: Limit the number of VCF records read into memory
            per file (Legacy option)
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

    def tiledb_stats(self):
        if self.mode == "r":
            if not self.reader.get_tiledb_stats_enabled:
                raise Exception("TileDB read stats not enabled")
            return self.reader.get_tiledb_stats()

        if self.mode == "w":
            if not self.writer.get_tiledb_stats_enabled:
                raise Exception("TileDB write stats not enabled")
            return self.writer.get_tiledb_stats()

    def schema_version(self):
        """Retrieve the VCF dataset's schema version"""
        if self.mode != "r":
            return self.writer.get_schema_version()
        return self.reader.get_schema_version()

    def sample_count(self):
        if self.mode != "r":
            raise Exception("Samples can only be retrieved for reader")
        return self.reader.get_sample_count()

    def samples(self):
        """Retrieve list of sample names registered in the VCF dataset"""
        if self.mode != "r":
            raise Exception("Sample names can only be retrieved for reader")
        return self.reader.get_sample_names()

    def attributes(self, attr_type="all"):
        """List queryable attributes available in the VCF dataset

        :param str type: The subset of attributes to retrieve; "info" or "fmt"
            will only retrieve attributes ingested from the VCF INFO and FORMAT
            fields, respectively, "builtin" retrieves the static attributes
            defined in TileDB-VCF's schema, "all" (the default) returns all
            queryable attributes
        :returns: a list of strings representing the attribute names
        """

        if self.mode != "r":
            raise Exception("Attributes can only be retrieved in read mode")

        attr_types = ("all", "info", "fmt", "builtin")
        if attr_type not in attr_types:
            raise ValueError("Invalid attribute type. Must be one of: %s" % attr_types)

        # combined attributes with type object
        comb_attrs = ("info", "fmt")

        if attr_type == "info":
            return self._info_attrs()
        elif attr_type == "fmt":
            return self._fmt_attrs()
        elif attr_type == "builtin":
            return self._materialized_attrs()
        else:
            return self._queryable_attrs()

    def _queryable_attrs(self):
        return self.reader.get_queryable_attributes()

    def _fmt_attrs(self):
        return self.reader.get_fmt_attributes()

    def _info_attrs(self):
        return self.reader.get_info_attributes()

    def _materialized_attrs(self):
        return self.reader.get_materialized_attributes()

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

    def version(self):
        if self.mode == "r":
            return self.reader.version()

        return self.writer.version()


class TileDBVCFDataset(Dataset):
    """A handle on a TileDB-VCF dataset."""

    def __init__(self, uri, mode="r", cfg=None, stats=False, verbose=False):
        """Initializes a TileDB-VCF dataset for interaction.

        :param uri: URI of TileDB-VCF dataset
        :param mode: Mode of operation.
        :type mode: 'r' or 'w'
        :param cfg: TileDB VCF configuration (optional)
        :param stats: Enable or disable TileDB stats (optional)
        :param verbose: Enable or disable TileDB VCF verbose output (optional)
        """
        warnings.warn(
            "TileDBVCFDataset is deprecated, use Dataset instead", DeprecationWarning
        )
        super().__init__(uri, mode, cfg, stats, verbose)
