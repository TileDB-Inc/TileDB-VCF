#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>
#include "stats/arrow_adapter.h"
#include "stats/column_buffer.h"

namespace tiledb::vcf::buffer {

namespace py = pybind11;
using namespace py::literals;

/**
 * @brief Convert ColumnBuffer to Arrow array.
 *
 * @param column_buffer ColumnBuffer
 * @return py::object Arrow array
 */
py::object to_array(std::shared_ptr<ColumnBuffer> column_buffer) {
  auto pa = py::module::import("pyarrow");
  auto pa_array_import = pa.attr("Array").attr("_import_from_c");

  auto [array, schema] = ArrowAdapter::to_arrow(column_buffer);
  return pa_array_import(py::capsule(array.get()), py::capsule(schema.get()));
}

/**
 * @brief Convert ArrayBuffers to Arrow table.
 *
 * @param cbs ArrayBuffers
 * @return py::object
 */
py::object to_table(std::shared_ptr<ArrayBuffers> array_buffers) {
  auto pa = py::module::import("pyarrow");
  auto pa_table_from_arrays = pa.attr("Table").attr("from_arrays");

  py::list names;
  py::list arrays;

  for (auto& name : array_buffers->names()) {
    auto column = array_buffers->at(name);
    names.append(name);
    arrays.append(to_array(column));
  }

  return pa_table_from_arrays(arrays, names);
}
}  // namespace tiledb::vcf::buffer
