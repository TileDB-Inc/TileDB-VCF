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
      .def("read", &Reader::read)
      .def("get_buffers", &Reader::get_buffers)
      .def("get_results_arrow", &Reader::get_results_arrow)
      .def("completed", &Reader::completed)
      .def("result_num_records", &Reader::result_num_records)
      .def("get_tiledb_stats_enabled", &Reader::get_tiledb_stats_enabled)
      .def("get_tiledb_stats", &Reader::get_tiledb_stats)
      .def("get_fmt_attribute_count", &Reader::get_fmt_attribute_count)
      .def("get_fmt_attribute_name", &Reader::get_fmt_attribute_name)
      .def("get_info_attribute_count", &Reader::get_info_attribute_count)
      .def("get_info_attribute_name", &Reader::get_info_attribute_name);

  py::class_<Writer>(m, "Writer")
      .def(py::init())
      .def("init", &Writer::init)
      .def("set_samples", &Writer::set_samples)
      .def("set_extra_attributes", &Writer::set_extra_attributes)
      .def("set_checksum", &Writer::set_checksum)
      .def("set_allow_duplicates", &Writer::set_allow_duplicates)
      .def("set_scratch_space", &Writer::set_scratch_space)
      .def("create_dataset", &Writer::create_dataset)
      .def("register_samples", &Writer::register_samples)
      .def("set_verbose", &Writer::set_verbose)
      .def("ingest_samples", &Writer::ingest_samples);
}
