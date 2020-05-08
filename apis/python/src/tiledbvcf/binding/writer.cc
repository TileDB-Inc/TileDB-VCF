/**
 * @file   writer.cc
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
 */

#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <tiledbvcf/tiledbvcf.h>

#include <map>
#include <set>

#include "writer.h"

namespace py = pybind11;

namespace {
void check_error(tiledb_vcf_writer_t* writer, int32_t rc) {
  if (rc != TILEDB_VCF_OK) {
    std::string msg =
        "TileDB-VCF-Py: Error getting tiledb_vcf_error_t error message.";
    tiledb_vcf_error_t* err = nullptr;
    const char* c_msg = nullptr;
    if (tiledb_vcf_writer_get_last_error(writer, &err) == TILEDB_VCF_OK &&
        tiledb_vcf_error_get_message(err, &c_msg) == TILEDB_VCF_OK) {
      msg = std::string(c_msg);
    }
    throw std::runtime_error(msg);
  }
}
}  // namespace

namespace tiledbvcfpy {

Writer::Writer()
    : ptr(nullptr, deleter) {
  tiledb_vcf_writer_t* r;
  if (tiledb_vcf_writer_alloc(&r) != TILEDB_VCF_OK)
    throw std::runtime_error(
        "TileDB-VCF-Py: Failed to allocate tiledb_vcf_writer_t instance.");
  ptr.reset(r);
}

void Writer::init(const std::string& dataset_uri) {
  auto writer = ptr.get();
  check_error(writer, tiledb_vcf_writer_init(writer, dataset_uri.c_str()));
}

void Writer::set_samples(const std::string& samples) {
  auto writer = ptr.get();
  check_error(writer, tiledb_vcf_writer_set_samples(writer, samples.c_str()));
}

void Writer::set_extra_attributes(const std::string& attributes) {
  auto writer = ptr.get();
  check_error(
      writer,
      tiledb_vcf_writer_set_extra_attributes(writer, attributes.c_str()));
}

void Writer::set_checksum(const std::string& checksum) {
  auto writer = ptr.get();
  tiledb_vcf_checksum_type_t checksum_type = TILEDB_VCF_CHECKSUM_SHA256;

  if (checksum == "md5")
    checksum_type = TILEDB_VCF_CHECKSUM_MD5;
  else if (checksum == "sha256")
    checksum_type = TILEDB_VCF_CHECKSUM_SHA256;
  else if (checksum == "none")
    checksum_type = TILEDB_VCF_CHECKSUM_NONE;

  check_error(writer, tiledb_vcf_writer_set_checksum_type(writer, checksum_type));
}

void Writer::set_allow_duplicates(const bool &allow_duplicates) {
  auto writer = ptr.get();
  check_error(writer, tiledb_vcf_writer_set_allow_duplicates(writer, allow_duplicates));
}

void Writer::create_dataset() {
  auto writer = ptr.get();
  check_error(writer, tiledb_vcf_writer_create_dataset(writer));
}

void Writer::register_samples() {
  auto writer = ptr.get();
  check_error(writer, tiledb_vcf_writer_register(writer));
}

void Writer::ingest_samples() {
  auto writer = ptr.get();
  check_error(writer, tiledb_vcf_writer_store(writer));
}

void Writer::deleter(tiledb_vcf_writer_t* w) {
  tiledb_vcf_writer_free(&w);
}

}  // namespace tiledbvcfpy
