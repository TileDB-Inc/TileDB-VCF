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

#include <pybind11/pybind11.h>

#include "pyarrow_helpers.h"
#include "stats/sample_stats.h"

namespace tiledbvcfpy {

namespace py = pybind11;
using namespace py::literals;
using namespace tiledb::vcf;

void load_sample_stats(py::module& m) {
  m.def(
      "sample_qc",
      [](const std::string& dataset_uri,
         const std::vector<std::string>& samples,
         const std::map<std::string, std::string>& config)
          -> std::optional<py::object> {
        auto buffers = SampleStats::sample_qc(dataset_uri, samples, config);
        return to_table(buffers);
      },
      "dataset_uri"_a,
      "samples"_a,
      "config"_a);
}

}  // namespace tiledbvcfpy
