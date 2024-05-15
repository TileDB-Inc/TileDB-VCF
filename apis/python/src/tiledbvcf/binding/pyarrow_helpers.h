/**
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#ifndef TILEDB_VCF_PYARROW_HELPERS_H
#define TILEDB_VCF_PYARROW_HELPERS_H

#include <pybind11/pybind11.h>
#include "stats/array_buffers.h"
#include "stats/arrow_adapter.h"

namespace tiledbvcfpy {

namespace py = pybind11;
using namespace py::literals;
using namespace tiledb::vcf;

/**
 * @brief Convert ArrayBuffers to Arrow table.
 *
 * @param buffers ArrayBuffers
 * @return py::object Arrow table
 */
py::object _buffer_to_table(std::shared_ptr<ArrayBuffers> buffers) {
  auto pa = py::module::import("pyarrow");
  auto pa_table_from_arrays = pa.attr("Table").attr("from_arrays");
  auto pa_array_import = pa.attr("Array").attr("_import_from_c");
  auto pa_schema_import = pa.attr("Schema").attr("_import_from_c");

  py::list array_list;
  py::list names;

  // Process each column buffer in the expected order
  for (auto& name : buffers->names()) {
    auto column = buffers->at(name);
    auto [pa_array, pa_schema] = ArrowAdapter::to_arrow(column);
    auto array = pa_array_import(
        py::capsule(pa_array.get()), py::capsule(pa_schema.get()));
    array_list.append(array);
    names.append(name);
  }

  return pa_table_from_arrays(array_list, names);
}

std::optional<py::object> to_table(
    std::optional<std::shared_ptr<ArrayBuffers>> buffers) {
  if (buffers.has_value()) {
    return _buffer_to_table(*buffers);
  }

  return std::nullopt;
}

}  // namespace tiledbvcfpy

#endif
