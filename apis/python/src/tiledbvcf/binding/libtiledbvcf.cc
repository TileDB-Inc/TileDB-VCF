#include <pybind11/pybind11.h>
#include <pybind11/stl_bind.h>
#include <tiledbvcf/tiledbvcf.h>

#include <map>
#include <stdexcept>

#include "reader.h"
#include "writer.h"

namespace py = pybind11;
using namespace tiledbvcfpy;

PYBIND11_MODULE(libtiledbvcf, m) {
  m.doc() = "Python binding of the TileDB-VCF library C API.";

  m.def("config_logging", &config_logging);

  py::class_<Reader>(m, "Reader")
      .def(py::init())
      .def("init", &Reader::init)
      .def("reset", &Reader::reset)
      .def("set_samples", &Reader::set_samples)
      .def("set_samples_file", &Reader::set_samples_file)
      .def("set_regions", &Reader::set_regions)
      .def("set_bed_file", &Reader::set_bed_file)
      .def("set_sort_regions", &Reader::set_sort_regions)
      .def("set_region_partition", &Reader::set_region_partition)
      .def("set_sample_partition", &Reader::set_sample_partition)
      .def("set_memory_budget", &Reader::set_memory_budget)
      .def("set_max_num_records", &Reader::set_max_num_records)
      .def("set_tiledb_config", &Reader::set_tiledb_config)
      .def("set_attributes", &Reader::set_attributes)
      .def("set_tiledb_stats_enabled", &Reader::set_tiledb_stats_enabled)
      .def("set_verbose", &Reader::set_verbose)
      .def("set_export_to_disk", &Reader::set_export_to_disk)
      .def("set_merge", &Reader::set_merge)
      .def("set_output_format", &Reader::set_output_format)
      .def("set_output_path", &Reader::set_output_path)
      .def("set_output_dir", &Reader::set_output_dir)
      .def("set_af_filter", &Reader::set_af_filter)
      .def("read", &Reader::read, py::arg("release_buffs") = true)
      .def("get_results_arrow", &Reader::get_results_arrow)
      .def("completed", &Reader::completed)
      .def("result_num_records", &Reader::result_num_records)
      .def("get_tiledb_stats_enabled", &Reader::get_tiledb_stats_enabled)
      .def("get_tiledb_stats", &Reader::get_tiledb_stats)
      .def("get_schema_version", &Reader::get_schema_version)
      .def("get_fmt_attributes", &Reader::get_fmt_attributes)
      .def("get_info_attributes", &Reader::get_info_attributes)
      .def("get_queryable_attributes", &Reader::get_queryable_attributes)
      .def("get_materialized_attributes", &Reader::get_materialized_attributes)
      .def("get_sample_count", &Reader::get_sample_count)
      .def("get_sample_names", &Reader::get_sample_names)
      .def("set_buffer_percentage", &Reader::set_buffer_percentage)
      .def(
          "set_tiledb_tile_cache_percentage",
          &Reader::set_tiledb_tile_cache_percentage)
      .def("set_check_samples_exist", &Reader::set_check_samples_exist)
      .def("version", &Reader::version)
      .def(
          "set_enable_progress_estimation",
          &Reader::set_enable_progress_estimation)
      .def("set_debug_print_vcf_regions", &Reader::set_debug_print_vcf_regions)
      .def("set_debug_print_sample_list", &Reader::set_debug_print_sample_list)
      .def(
          "set_debug_print_tiledb_query_ranges",
          &Reader::set_debug_print_tiledb_query_ranges);

  py::class_<Writer>(m, "Writer")
      .def(py::init())
      .def("init", &Writer::init)
      .def("set_tiledb_stats_enabled", &Writer::set_tiledb_stats_enabled)
      .def("set_samples", &Writer::set_samples)
      .def("set_extra_attributes", &Writer::set_extra_attributes)
      .def("set_vcf_attributes", &Writer::set_vcf_attributes)
      .def("set_checksum", &Writer::set_checksum)
      .def("set_allow_duplicates", &Writer::set_allow_duplicates)
      .def("set_tile_capacity", &Writer::set_tile_capacity)
      .def("set_anchor_gap", &Writer::set_anchor_gap)
      .def("set_num_threads", &Writer::set_num_threads)
      .def("set_total_memory_budget_mb", &Writer::set_total_memory_budget_mb)
      .def("set_total_memory_percentage", &Writer::set_total_memory_percentage)
      .def("set_ratio_tiledb_memory", &Writer::set_ratio_tiledb_memory)
      .def("set_max_tiledb_memory_mb", &Writer::set_max_tiledb_memory_mb)
      .def("set_input_record_buffer_mb", &Writer::set_input_record_buffer_mb)
      .def("set_avg_vcf_record_size", &Writer::set_avg_vcf_record_size)
      .def("set_ratio_task_size", &Writer::set_ratio_task_size)
      .def("set_ratio_output_flush", &Writer::set_ratio_output_flush)
      .def("set_thread_task_size", &Writer::set_thread_task_size)
      .def("set_memory_budget", &Writer::set_memory_budget)
      .def("set_scratch_space", &Writer::set_scratch_space)
      .def("set_max_num_records", &Writer::set_max_num_records)
      .def("create_dataset", &Writer::create_dataset)
      .def("register_samples", &Writer::register_samples)
      .def("set_verbose", &Writer::set_verbose)
      .def("ingest_samples", &Writer::ingest_samples)
      .def("get_schema_version", &Writer::get_schema_version)
      .def("set_tiledb_config", &Writer::set_tiledb_config)
      .def("set_sample_batch_size", &Writer::set_sample_batch_size)
      .def("get_tiledb_stats_enabled", &Writer::get_tiledb_stats_enabled)
      .def("get_tiledb_stats", &Writer::get_tiledb_stats)
      .def("version", &Writer::version)
      .def("set_resume", &Writer::set_resume)
      .def("set_contig_fragment_merging", &Writer::set_contig_fragment_merging)
      .def(
          "set_contigs_to_keep_separate", &Writer::set_contigs_to_keep_separate)
      .def(
          "set_contigs_to_allow_merging", &Writer::set_contigs_to_allow_merging)
      .def("set_contig_mode", &Writer::set_contig_mode)
      .def("set_enable_allele_count", &Writer::set_enable_allele_count)
      .def("set_enable_variant_stats", &Writer::set_enable_variant_stats);
}
