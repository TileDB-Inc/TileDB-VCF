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
#include <pybind11/stl.h>
#include <tiledbvcf.h>

#include "stats/carrow.h"
#include "vcf_arrow.h"

namespace tiledbvcfpy {

struct ArrowBuffer {
  ArrowBuffer(std::shared_ptr<BufferInfo> buffer)
      : buffer_(buffer){};

  std::shared_ptr<BufferInfo> buffer_;
};

static void release_schema(struct ArrowSchema* schema) {
  // Free children
  for (int i = 0; i < schema->n_children; i++) {
    struct ArrowSchema* child = schema->children[i];
    if (child->release != nullptr) {
      child->release(child);
    }
    free(child);
  }
  free(schema->children);

  // Mark released
  schema->release = nullptr;
}

static void release_array(struct ArrowArray* array) {
  if (array->private_data != nullptr) {
    // Delete the ArrowBuffer, which was allocated with new.
    // If the ArrowBuffer.buffer_ shared_ptr is the last reference
    // to the BufferInfo, then the BufferInfo buffers will be deleted.
    auto arrow_buffer = static_cast<ArrowBuffer*>(array->private_data);
    delete arrow_buffer;
  }

  // Free children
  for (int i = 0; i < array->n_children; i++) {
    struct ArrowArray* child = array->children[i];
    if (child->release != nullptr) {
      child->release(child);
    }
    free(child);
  }
  free(array->children);

  // Free buffers array. The buffers themselves are owned by the BufferInfo
  // and will be freed when the BufferInfo is deleted.
  free(array->buffers);

  // Mark released
  array->release = nullptr;
}

static std::pair<ArrowSchema*, ArrowArray*> arrow_data_array(
    std::shared_ptr<BufferInfo> buffer,
    uint64_t length,
    std::vector<std::byte>& data,
    const std::vector<uint8_t>& bitmap = {}) {
  auto schema = (struct ArrowSchema*)malloc(sizeof(struct ArrowSchema));
  schema->format = buffer->arrow_datatype().c_str();
  schema->name = buffer->name().c_str();
  schema->metadata = nullptr;
  schema->flags = 0;
  schema->n_children = 0;
  schema->children = nullptr;
  schema->dictionary = nullptr;
  schema->release = &release_schema;
  schema->private_data = nullptr;

  auto array = (struct ArrowArray*)malloc(sizeof(struct ArrowArray));
  array->length = length;
  array->null_count = 0;
  array->offset = 0;
  array->n_buffers = 2;
  array->n_children = 0;
  array->buffers = nullptr;
  array->children = nullptr;
  array->dictionary = nullptr;
  array->release = &release_array;
  array->private_data = (void*)new ArrowBuffer(buffer);

  array->buffers = (const void**)malloc(array->n_buffers * sizeof(void*));
  array->buffers[0] = nullptr;      // validity
  array->buffers[1] = data.data();  // data

  if (bitmap.capacity()) {
    schema->flags |= ARROW_FLAG_NULLABLE;
    array->null_count = -1;
    array->buffers[0] = bitmap.data();
  }

  return std::make_pair(schema, array);
}

static std::pair<ArrowSchema*, ArrowArray*> arrow_string_array(
    std::shared_ptr<BufferInfo> buffer,
    uint64_t length,
    std::vector<int32_t>& offsets,
    std::vector<std::byte>& data,
    const std::vector<uint8_t>& bitmap = {}) {
  auto schema = (struct ArrowSchema*)malloc(sizeof(struct ArrowSchema));

  schema->format = "u";  // string array with 32 bit offsets
  schema->name = buffer->name().c_str();
  schema->metadata = nullptr;
  schema->flags = 0;
  schema->n_children = 0;
  schema->children = nullptr;
  schema->dictionary = nullptr;
  schema->release = &release_schema;
  schema->private_data = nullptr;

  auto array = (struct ArrowArray*)malloc(sizeof(struct ArrowArray));
  array->length = length;
  array->null_count = 0;
  array->offset = 0;
  array->n_buffers = 3;
  array->n_children = 0;
  array->buffers = nullptr;
  array->children = nullptr;
  array->dictionary = nullptr;
  array->release = &release_array;
  array->private_data = (void*)new ArrowBuffer(buffer);

  array->buffers = (const void**)malloc(3 * sizeof(void*));
  if (bitmap.capacity()) {
    schema->flags |= ARROW_FLAG_NULLABLE;
    array->null_count = -1;
    array->buffers[0] = bitmap.data();
  } else {
    array->buffers[0] = nullptr;  // validity
  }
  //  Edge case: empty results should still have a single 0 offset
  if (!length && !offsets.size()) {
    offsets.push_back(0);
  }
  array->buffers[1] = offsets.data();  // offsets
  array->buffers[2] = data.data();     // data

  return std::make_pair(schema, array);
}

static std::pair<ArrowSchema*, ArrowArray*> arrow_list_array(
    std::shared_ptr<BufferInfo> buffer,
    uint64_t length,
    std::vector<int32_t>& value_offsets,
    ArrowSchema* values_schema,
    ArrowArray* values_array,
    const std::vector<uint8_t>& bitmap = {}) {
  auto schema = (struct ArrowSchema*)malloc(sizeof(struct ArrowSchema));

  schema->format = "+l";  // list with 32 bit offsets
  schema->name = "list";
  schema->metadata = nullptr;
  schema->flags = 0;
  schema->n_children = 1;
  schema->children = (struct ArrowSchema**)malloc(sizeof(struct ArrowSchema*));
  schema->children[0] = values_schema;
  schema->dictionary = nullptr;
  schema->release = &release_schema;
  schema->private_data = nullptr;

  auto array = (struct ArrowArray*)malloc(sizeof(struct ArrowArray));
  array->length = length;
  array->null_count = 0;
  array->offset = 0;
  array->n_buffers = 2;
  array->n_children = 1;
  array->buffers = nullptr;
  array->children = (struct ArrowArray**)malloc(sizeof(struct ArrowArray*));
  array->children[0] = values_array;
  array->dictionary = nullptr;
  array->release = &release_array;
  array->private_data = nullptr;  // Buffers will be deleted by the child array

  array->buffers = (const void**)malloc(array->n_buffers * sizeof(void*));
  array->buffers[0] = nullptr;  // validity
  // Edge case: empty results should still have a single 0 offset
  if (!length && !value_offsets.size()) {
    value_offsets.push_back(0);
  }
  array->buffers[1] = value_offsets.data();  // data

  if (bitmap.capacity()) {
    schema->flags |= ARROW_FLAG_NULLABLE;
    array->null_count = -1;
    array->buffers[0] = bitmap.data();
  }

  return std::make_pair(schema, array);
}

void build_arrow_array(
    std::shared_ptr<BufferInfo> buffer,
    uint64_t num_rows,
    uint64_t num_offsets,
    uint64_t num_data_elements) {
  bool var_len = buffer->offsets().capacity() > 0;
  bool list = buffer->list_offsets().capacity() > 0;

  ArrowSchema* array_schema = nullptr;
  ArrowArray* array_array = nullptr;

  if (list) {
    if (var_len) {
      // List of lists of primitives
      auto [values_schema, values_array] =
          arrow_data_array(buffer, num_data_elements, buffer->data());

      // Edge case: only subtract if there's offsets to avoid underflow
      if (num_offsets) {
        num_offsets -= 1;
      }
      std::tie(array_schema, array_array) = arrow_list_array(
          buffer, num_offsets, buffer->offsets(), values_schema, values_array);
    } else {
      std::tie(array_schema, array_array) =
          arrow_data_array(buffer, num_data_elements, buffer->data());
    }
    auto [arrow_schema, arrow_array] = arrow_list_array(
        buffer,
        num_rows,
        buffer->list_offsets(),
        array_schema,
        array_array,
        buffer->bitmap());
    buffer->set_arrow_array(arrow_schema, arrow_array);
  } else if (var_len) {
    auto [values_schema, values_array] =
        arrow_data_array(buffer, num_data_elements, buffer->data());
    auto [arrow_schema, arrow_array] = arrow_list_array(
        buffer,
        num_rows,
        buffer->offsets(),
        values_schema,
        values_array,
        buffer->bitmap());
    buffer->set_arrow_array(arrow_schema, arrow_array);
  } else {
    // fixed length
    auto [arrow_schema, arrow_array] = arrow_data_array(
        buffer, num_data_elements, buffer->data(), buffer->bitmap());
    buffer->set_arrow_array(arrow_schema, arrow_array);
  }
}

void build_arrow_array_from_buffer(
    std::shared_ptr<BufferInfo> buffer,
    uint64_t num_rows,
    uint64_t num_offsets,
    uint64_t num_data_elements) {
  if (buffer->datatype() == TILEDB_VCF_CHAR) {
    if (buffer->list_offsets().capacity() > 0) {
      // Edge case: only subtract if there's offsets to avoid underflow
      if (num_offsets) {
        num_offsets -= 1;
      }
      // Array of strings
      auto [values_schema, values_array] = arrow_string_array(
          buffer, num_offsets, buffer->offsets(), buffer->data());

      // Array of lists of strings
      auto [arrow_schema, arrow_array] = arrow_list_array(
          buffer,
          num_rows,
          buffer->list_offsets(),
          values_schema,
          values_array,
          buffer->bitmap());
      buffer->set_arrow_array(arrow_schema, arrow_array);
    } else {
      // Array of strings
      auto [arrow_schema, arrow_array] = arrow_string_array(
          buffer,
          num_rows,
          buffer->offsets(),
          buffer->data(),
          buffer->bitmap());
      buffer->set_arrow_array(arrow_schema, arrow_array);
    }
  } else {
    // Array of primitives
    build_arrow_array(buffer, num_rows, num_offsets, num_data_elements);
  }
}

py::object buffers_to_table(std::vector<std::shared_ptr<BufferInfo>>& buffers) {
  auto pa = py::module::import("pyarrow");
  auto pa_table_from_arrays = pa.attr("Table").attr("from_arrays");
  auto pa_array_import = pa.attr("Array").attr("_import_from_c");
  auto pa_schema_import = pa.attr("Schema").attr("_import_from_c");

  py::list array_list;
  py::list names;

  for (auto& buffer : buffers) {
    auto pa_array = buffer->array();
    auto pa_schema = buffer->schema();
    auto array = pa_array_import(py::capsule(pa_array), py::capsule(pa_schema));
    array_list.append(array);
    names.append(buffer->name());
  }

  return pa_table_from_arrays(array_list, names);
}

}  // namespace tiledbvcfpy
