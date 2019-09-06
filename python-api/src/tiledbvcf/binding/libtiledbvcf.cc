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
      .def("set_regions", &Reader::set_regions)
      .def("set_attributes", &Reader::set_attributes)
      .def("set_buffer_alloc_size", &Reader::set_buffer_alloc_size)
      .def("read", &Reader::read)
      .def("get_results", &Reader::get_buffers)
      .def("result_num_records", &Reader::result_num_records);

  py::class_<Writer>(m, "Writer")
      .def(py::init())
      .def("init", &Writer::init)
      .def("set_samples", &Writer::set_samples)
      .def("set_extra_attributes", &Writer::set_extra_attributes)
      .def("create_dataset", &Writer::create_dataset)
      .def("register_samples", &Writer::register_samples)
      .def("ingest_samples", &Writer::ingest_samples);
}