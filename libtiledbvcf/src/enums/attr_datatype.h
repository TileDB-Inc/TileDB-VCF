/**
 * @file attr_datatype.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2019 TileDB, Inc.
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
 */

#ifndef TILEDB_VCF_ATTR_DATATYPE_H
#define TILEDB_VCF_ATTR_DATATYPE_H

#include <stdexcept>
#include <string>

namespace tiledb {
namespace vcf {

/** Defines the attribute datatype enum. */
enum class AttrDatatype : uint8_t {
#define TILEDB_VCF_ATTR_DATATYPE_ENUM(id) id
#include "c_api/tiledbvcf_enum.h"
#undef TILEDB_VCF_ATTR_DATATYPE_ENUM
};

/** Returns the string representation of the input attribute datatype. */
inline std::string attr_datatype_str(AttrDatatype attr_datatype) {
  switch (attr_datatype) {
    case AttrDatatype::CHAR:
      return "CHAR";
    case AttrDatatype::UINT8:
      return "UINT8";
    case AttrDatatype::INT32:
      return "INT32";
    case AttrDatatype::FLOAT32:
      return "FLOAT32";
    default:
      throw std::runtime_error("Error converting AttrDatatype to string.");
  }
}

/** Returns the attribute datatype given a string representation. */
inline void attr_datatype_enum(
    const std::string& attr_datatype_str, AttrDatatype* attr_datatype) {
  if (attr_datatype_str == "CHAR")
    *attr_datatype = AttrDatatype::CHAR;
  else if (attr_datatype_str == "UINT8")
    *attr_datatype = AttrDatatype::UINT8;
  else if (attr_datatype_str == "INT32")
    *attr_datatype = AttrDatatype::INT32;
  else if (attr_datatype_str == "FLOAT32")
    *attr_datatype = AttrDatatype::FLOAT32;
  else
    throw std::runtime_error("Error converting string AttrDatatype to enum.");
}

/** Returns the byte size of the input attribute datatype. */
inline uint64_t attr_datatype_size(AttrDatatype attr_datatype) {
  switch (attr_datatype) {
    case AttrDatatype::CHAR:
      return sizeof(char);
    case AttrDatatype::UINT8:
      return sizeof(uint8_t);
    case AttrDatatype::INT32:
      return sizeof(int32_t);
    case AttrDatatype::FLOAT32:
      return sizeof(float);
    default:
      throw std::runtime_error("Error converting AttrDatatype to size.");
  }
}

inline std::ostream& operator<<(
    std::ostream& os, const AttrDatatype& datatype) {
  os << attr_datatype_str(datatype);
  return os;
}

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_ATTR_DATATYPE_H
