/**
 * @file   arrow_adapter.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 *
 * @section DESCRIPTION
 *
 * This file defines the ArrowAdapter class.
 */

#include "arrow_adapter.h"
#include "../utils/logger_public.h"
#include "column_buffer.h"

namespace tiledb::vcf {

using namespace tiledb;

void ArrowAdapter::release_schema(struct ArrowSchema* schema) {
  schema->release = nullptr;

  for (int i = 0; i < schema->n_children; ++i) {
    struct ArrowSchema* child = schema->children[i];
    if (schema->name != nullptr) {
      free((void*)schema->name);
      schema->name = nullptr;
    }
    if (child->release != NULL) {
      child->release(child);
    }
    free(child);
  }
  free(schema->children);

  struct ArrowSchema* dict = schema->dictionary;
  if (dict != nullptr) {
    if (dict->format != nullptr) {
      free((void*)dict->format);
      dict->format = nullptr;
    }
    if (dict->release != nullptr) {
      delete dict;
      dict = nullptr;
    }
  }

  LOG_TRACE("[ArrowAdapter] release_schema");
}

void ArrowAdapter::release_array(struct ArrowArray* array) {
  auto arrow_buffer = static_cast<ArrowBuffer*>(array->private_data);

  LOG_TRACE(fmt::format(
      "[ArrowAdapter] release_array {} use_count={}",
      arrow_buffer->buffer_->name(),
      arrow_buffer->buffer_.use_count()));

  // Delete the ArrowBuffer, which was allocated with new.
  // If the ArrowBuffer.buffer_ shared_ptr is the last reference to the
  // underlying ColumnBuffer, the ColumnBuffer will be deleted.
  delete arrow_buffer;

  if (array->buffers != nullptr) {
    delete[] array->buffers;
  }

  struct ArrowArray* dict = array->dictionary;
  if (dict != nullptr) {
    if (dict->buffers != nullptr) {
      free(dict->buffers);
      dict->buffers = nullptr;
    }
    if (dict->release != nullptr) {
      delete dict;
      dict = nullptr;
    }
  }

  array->release = nullptr;
}

std::unique_ptr<ArrowSchema> ArrowAdapter::arrow_schema_from_tiledb_array(
    std::shared_ptr<Context> ctx, std::shared_ptr<Array> tiledb_array) {
  auto tiledb_schema = tiledb_array->schema();
  auto ndim = tiledb_schema.domain().ndim();
  auto nattr = tiledb_schema.attribute_num();

  std::unique_ptr<ArrowSchema> arrow_schema = std::make_unique<ArrowSchema>();
  arrow_schema->format = "+s";
  arrow_schema->n_children = ndim + nattr;
  arrow_schema->release = &ArrowAdapter::release_schema;
  arrow_schema->children = new ArrowSchema*[arrow_schema->n_children];

  ArrowSchema* child = nullptr;

  for (uint32_t i = 0; i < ndim; ++i) {
    auto dim = tiledb_schema.domain().dimension(i);
    child = arrow_schema->children[i] = new ArrowSchema;
    child->format = ArrowAdapter::to_arrow_format(dim.type()).data();
    child->name = strdup(dim.name().c_str());
    child->metadata = nullptr;
    child->flags = 0;
    child->n_children = 0;
    child->dictionary = nullptr;
    child->children = nullptr;
    child->release = &ArrowAdapter::release_schema;
  }

  for (uint32_t i = 0; i < nattr; ++i) {
    auto attr = tiledb_schema.attribute(i);
    child = arrow_schema->children[ndim + i] = new ArrowSchema;
    child->format = ArrowAdapter::to_arrow_format(attr.type()).data();
    child->name = strdup(attr.name().c_str());
    child->metadata = nullptr;
    child->flags = attr.nullable() ? ARROW_FLAG_NULLABLE : 0;
    child->n_children = 0;
    child->children = nullptr;
    child->dictionary = nullptr;
    child->release = &ArrowAdapter::release_schema;
  }

  return arrow_schema;
}

std::pair<std::unique_ptr<ArrowArray>, std::unique_ptr<ArrowSchema>>
ArrowAdapter::to_arrow(std::shared_ptr<ColumnBuffer> column) {
  std::unique_ptr<ArrowSchema> schema = std::make_unique<ArrowSchema>();
  std::unique_ptr<ArrowArray> array = std::make_unique<ArrowArray>();

  schema->format = to_arrow_format(column->type()).data();
  schema->name = column->name().data();
  schema->metadata = nullptr;
  schema->flags = 0;
  schema->n_children = 0;
  schema->children = nullptr;
  schema->dictionary = nullptr;
  schema->release = &release_schema;
  schema->private_data = nullptr;

  int n_buffers = column->is_var() ? 3 : 2;

  // Create an ArrowBuffer to manage the lifetime of `column`.
  // - `arrow_buffer` holds a shared_ptr to `column`, which
  // increments
  //   the use count and keeps the ColumnBuffer data alive.
  // - When the arrow array is released, `array->release()` is
  // called with
  //   `arrow_buffer` in `private_data`. `arrow_buffer` is
  //   deleted, which decrements the the `column` use count. When
  //   the `column` use count reaches 0, the ColumnBuffer data
  //   will be deleted.
  auto arrow_buffer = new ArrowBuffer(column);

  array->length = column->size();
  array->null_count = 0;
  array->offset = 0;
  array->n_buffers = n_buffers;
  array->n_children = 0;
  array->buffers = nullptr;
  array->children = nullptr;
  array->dictionary = nullptr;
  array->release = &release_array;
  array->private_data = (void*)arrow_buffer;

  LOG_TRACE(fmt::format(
      "[ArrowAdapter] create array name='{}' use_count={}",
      column->name(),
      column.use_count()));

  array->buffers = new const void*[n_buffers];
  assert(array->buffers != nullptr);
  array->buffers[0] = nullptr;                                   // validity
  array->buffers[n_buffers - 1] = column->data<void*>().data();  // data
  if (n_buffers == 3) {
    array->buffers[1] = column->offsets().data();  // offsets
  }

  if (column->is_nullable()) {
    schema->flags |= ARROW_FLAG_NULLABLE;

    // Count nulls
    for (auto v : column->validity()) {
      array->null_count += v == 0;
    }

    // Convert validity bytemap to a bitmap in place
    column->validity_to_bitmap();
    array->buffers[0] = column->validity().data();
  }

  // Workaround to cast TILEDB_BOOL from uint8 to 1-bit Arrow boolean
  if (column->type() == TILEDB_BOOL) {
    column->data_to_bitmap();
  }

  return std::pair(std::move(array), std::move(schema));
}

std::string_view ArrowAdapter::to_arrow_format(
    tiledb_datatype_t datatype, bool use_large) {
  switch (datatype) {
    case TILEDB_STRING_ASCII:
    case TILEDB_STRING_UTF8:
      return use_large ? "U" : "u";  // large because TileDB
                                     // uses 64bit offsets
    case TILEDB_CHAR:
    case TILEDB_BLOB:
      return use_large ? "Z" : "z";  // large because TileDB
                                     // uses 64bit offsets
    case TILEDB_BOOL:
      return "b";
    case TILEDB_INT32:
      return "i";
    case TILEDB_INT64:
      return "l";
    case TILEDB_FLOAT32:
      return "f";
    case TILEDB_FLOAT64:
      return "g";
    case TILEDB_INT8:
      return "c";
    case TILEDB_UINT8:
      return "C";
    case TILEDB_INT16:
      return "s";
    case TILEDB_UINT16:
      return "S";
    case TILEDB_UINT32:
      return "I";
    case TILEDB_UINT64:
      return "L";
    case TILEDB_TIME_SEC:
      return "tts";
    case TILEDB_TIME_MS:
      return "ttm";
    case TILEDB_TIME_US:
      return "ttu";
    case TILEDB_TIME_NS:
      return "ttn";
    case TILEDB_DATETIME_SEC:
      return "tss:";
    case TILEDB_DATETIME_MS:
      return "tsm:";
    case TILEDB_DATETIME_US:
      return "tsu:";
    case TILEDB_DATETIME_NS:
      return "tsn:";
    default:
      break;
  }
  throw std::runtime_error(fmt::format(
      "ArrowAdapter: Unsupported TileDB datatype: {} ",
      tiledb::impl::type_to_str(datatype)));
}

}  // namespace tiledb::vcf
