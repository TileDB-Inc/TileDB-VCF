/**
 * @file read_status.h
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

#ifndef TILEDB_VCF_READ_STATUS_H
#define TILEDB_VCF_READ_STATUS_H

#include <string>

namespace tiledb {
namespace vcf {

/** Defines the read status. */
enum class ReadStatus : uint8_t {
#define TILEDB_VCF_READ_STATUS_ENUM(id) id
#include "c_api/tiledbvcf_enum.h"
#undef TILEDB_VCF_READ_STATUS_ENUM
};

/** Returns the string representation of the input read status type. */
inline std::string read_status_str(ReadStatus read_status) {
  switch (read_status) {
    case ReadStatus::FAILED:
      return "FAILED";
    case ReadStatus::COMPLETED:
      return "COMPLETED";
    case ReadStatus::INCOMPLETE:
      return "INCOMPLETE";
    case ReadStatus::UNINITIALIZED:
      return "UNINITIALIZED";
    default:
      throw std::runtime_error("Error converting ReadStatus to string.");
  }
}

/** Returns the read status given a string representation. */
inline void read_status_enum(
    const std::string& read_status_str, ReadStatus* read_status) {
  if (read_status_str == "FAILED")
    *read_status = ReadStatus::FAILED;
  else if (read_status_str == "COMPLETED")
    *read_status = ReadStatus::COMPLETED;
  else if (read_status_str == "INCOMPLETE")
    *read_status = ReadStatus::INCOMPLETE;
  else if (read_status_str == "UNINITIALIZED")
    *read_status = ReadStatus::UNINITIALIZED;
  else
    throw std::runtime_error("Error converting string ReadStatus to enum.");
}

inline std::ostream& operator<<(std::ostream& os, const ReadStatus& stat) {
  os << read_status_str(stat);
  return os;
}

}  // namespace vcf
}  // namespace tiledb

#endif  // TILEDB_VCF_READ_STATUS_H
