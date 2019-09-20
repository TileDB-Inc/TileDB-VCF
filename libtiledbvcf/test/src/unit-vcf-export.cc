/**
 * @file   unit-vcf-export.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2019 TileDB Inc.
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
 * Tests for VCF export.
 */

#include "catch.hpp"

#include "dataset/tiledbvcfdataset.h"
#include "read/reader.h"
#include "utils/constants.h"
#include "write/writer.h"

#include <cstring>
#include <fstream>
#include <iostream>

using namespace tiledb::vcf;

static const std::string input_dir = TILEDB_VCF_TEST_INPUT_DIR;

namespace {

/** Helper struct representing a user buffer pair. */
struct UserBuffer {
  template <typename T>
  const T* data() const {
    return reinterpret_cast<const T*>(data_.data());
  }

  template <typename T>
  T* data() {
    return reinterpret_cast<T*>(data_.data());
  }

  const std::vector<int32_t>& offsets() const {
    return offsets_;
  }

  std::vector<int32_t>& offsets() {
    return offsets_;
  }

  const std::vector<uint8_t>& bitmap() const {
    return bitmap_;
  }

  std::vector<uint8_t>& bitmap() {
    return bitmap_;
  }

  void resize(size_t nbytes) {
    data_.resize(nbytes);
  }

  size_t size() const {
    return data_.size();
  }

  std::vector<char> data_;
  std::vector<int32_t> offsets_;
  std::vector<uint8_t> bitmap_;
};

template <typename T>
void check_result(
    const Reader& reader,
    const std::string& attr,
    const UserBuffer& buffer,
    const std::vector<T>& expected) {
  int64_t num_offsets, num_data_elements, num_data_bytes;
  reader.result_size(attr, &num_offsets, &num_data_elements, &num_data_bytes);

  unsigned nrec = expected.size();
  REQUIRE(num_offsets == 0);
  REQUIRE(num_data_elements == (int64_t)nrec);
  REQUIRE(num_data_bytes == (int64_t)(nrec * sizeof(T)));
  std::vector<T> actual;
  for (unsigned i = 0; i < nrec; i++)
    actual.push_back(*(buffer.data<T>() + i));

  REQUIRE(actual == expected);
}

template <typename T>
void check_var_result(
    const Reader& reader,
    const std::string& attr,
    const UserBuffer& buffer,
    const std::vector<T>& expected) {
  int64_t num_offsets, num_data_elements, num_data_bytes;
  reader.result_size(attr, &num_offsets, &num_data_elements, &num_data_bytes);

  std::vector<T> actual;
  for (unsigned i = 0; i < num_offsets - 1; i++) {
    int32_t offset = buffer.offsets()[i];
    int32_t next_offset = buffer.offsets()[i + 1];
    int32_t len = next_offset - offset;
    const T* p = buffer.data<T>() + offset;

    if (!buffer.bitmap().empty()) {
      bool is_null = (buffer.bitmap()[i / 8] & (uint8_t(1) << (i % 8))) == 0;
      if (is_null)
        REQUIRE(len == 0);
    }

    unsigned nvals = len;
    for (unsigned j = 0; j < nvals; j++) {
      T t = p[j];
      actual.push_back(t);
    }
  }

  for (unsigned i = 0; i < actual.size(); i++)
    REQUIRE(actual[i] == expected[i]);
}

void check_string_result(
    const Reader& reader,
    const std::string& attr,
    const UserBuffer& buffer,
    const std::vector<std::string>& expected) {
  int64_t num_offsets, num_data_elements, num_data_bytes;
  reader.result_size(attr, &num_offsets, &num_data_elements, &num_data_bytes);

  unsigned nrec = expected.size();
  REQUIRE(num_offsets == (int64_t)nrec + 1);
  std::vector<std::string> actual;
  for (unsigned i = 0; i < nrec; i++) {
    auto len = buffer.offsets()[i + 1] - buffer.offsets()[i];
    std::string s(buffer.data<char>() + buffer.offsets()[i], len);
    actual.push_back(s);
  }
  REQUIRE(actual == expected);
}
}  // namespace

TEST_CASE("TileDB-VCF: Test export", "[tiledbvcf][export]") {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  std::string dataset_uri = "test_dataset";
  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);

  std::string output_dir = "test_dataset_out";
  if (vfs.is_dir(output_dir))
    vfs.remove_dir(output_dir);

  CreationParams create_args;
  create_args.uri = dataset_uri;
  create_args.tile_capacity = 10000;
  create_args.extra_attributes = {"fmt_GT", "fmt_PL", "fmt_MIN_DP"};
  TileDBVCFDataset::create(create_args);

  // Register two samples
  {
    TileDBVCFDataset ds;
    ds.open(dataset_uri);
    RegistrationParams args;
    args.sample_uris = {input_dir + "/small.bcf", input_dir + "/small2.bcf"};
    ds.register_samples(args);
  }

  // Ingest the samples
  {
    Writer writer;
    IngestionParams params;
    params.uri = dataset_uri;
    params.sample_uris = {input_dir + "/small.bcf", input_dir + "/small2.bcf"};
    writer.set_all_params(params);
    writer.ingest_samples();
  }

  // Query a few regions
  {
    Reader reader;

    // Allocate some buffers to receive data
    UserBuffer sample_name, contig, pos, end, gt, pl, dp, min_dp;
    sample_name.resize(1024);
    sample_name.offsets().resize(100);
    contig.resize(1024);
    contig.offsets().resize(100);
    pos.resize(1024);
    end.resize(1024);
    gt.resize(1024);
    gt.offsets().resize(100);
    pl.resize(1024);
    pl.offsets().resize(100);
    dp.resize(1024);
    dp.offsets().resize(100);
    min_dp.resize(1024);
    min_dp.offsets().resize(100);

    // Set buffers on the reader
    reader.set_buffer(
        "sample_name",
        sample_name.offsets().data(),
        sample_name.offsets().size(),
        sample_name.data<void>(),
        sample_name.size());
    reader.set_buffer(
        "contig",
        contig.offsets().data(),
        contig.offsets().size(),
        contig.data<void>(),
        contig.size());
    reader.set_buffer("pos_start", nullptr, 0, pos.data<void>(), pos.size());
    reader.set_buffer("pos_end", nullptr, 0, end.data<void>(), end.size());
    reader.set_buffer(
        "fmt_GT",
        gt.offsets().data(),
        gt.offsets().size(),
        gt.data<void>(),
        gt.size());
    reader.set_buffer(
        "fmt_PL",
        pl.offsets().data(),
        pl.offsets().size(),
        pl.data<void>(),
        pl.size());
    reader.set_buffer(
        "fmt_DP",
        dp.offsets().data(),
        dp.offsets().size(),
        dp.data<void>(),
        dp.size());
    reader.set_buffer(
        "fmt_MIN_DP",
        min_dp.offsets().data(),
        min_dp.offsets().size(),
        min_dp.data<void>(),
        min_dp.size());

    ExportParams params;
    params.uri = dataset_uri;
    params.output_dir = output_dir;
    params.sample_names = {"HG00280", "HG01762"};
    params.regions = {"1:12700-13400", "1:17000-17400"};
    reader.set_all_params(params);
    reader.open_dataset(dataset_uri);
    reader.read();
    REQUIRE(reader.read_status() == ReadStatus::COMPLETED);
    REQUIRE(reader.num_records_exported() == 7);
    check_string_result(
        reader,
        "sample_name",
        sample_name,
        {"HG01762",
         "HG00280",
         "HG00280",
         "HG01762",
         "HG00280",
         "HG00280",
         "HG00280"});
    check_string_result(
        reader, "contig", contig, {"1", "1", "1", "1", "1", "1", "1"});
    check_result<uint32_t>(
        reader,
        "pos_start",
        pos,
        {12546, 12546, 13354, 13354, 13375, 13396, 17319});
    check_result<uint32_t>(
        reader,
        "pos_end",
        end,
        {12771, 12771, 13374, 13389, 13395, 13413, 17479});
    check_var_result<int32_t>(
        reader, "fmt_GT", gt, {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
    check_var_result<int32_t>(
        reader, "fmt_PL", pl, {0,   0, 0, 0,  0, 0, 0,  24, 360, 0, 66,
                               990, 0, 6, 90, 0, 3, 32, 0,  0,   0});
    check_var_result<int32_t>(reader, "fmt_DP", dp, {0, 0, 15, 64, 6, 2, 0});
    check_var_result<int32_t>(
        reader, "fmt_MIN_DP", min_dp, {0, 0, 14, 30, 3, 1, 0});
  }

  // Check count operation
  {
    Reader reader;
    ExportParams params;
    params.uri = dataset_uri;
    params.output_dir = output_dir;
    params.sample_names = {"HG00280", "HG01762"};
    params.regions = {"1:12700-13400", "1:17000-17400"};
    reader.set_all_params(params);
    reader.open_dataset(dataset_uri);
    reader.read();
    REQUIRE(reader.read_status() == ReadStatus::COMPLETED);
    REQUIRE(reader.num_records_exported() == 7);
  }

  // Check some field datatypes
  {
    Reader reader;
    reader.open_dataset(dataset_uri);
    AttrDatatype dtype;
    bool var_len, nullable;
    REQUIRE_THROWS(
        reader.attribute_datatype("abc", &dtype, &var_len, &nullable));
    REQUIRE_THROWS(
        reader.attribute_datatype("info_abc", &dtype, &var_len, &nullable));
    REQUIRE_THROWS(
        reader.attribute_datatype("fmt_gt", &dtype, &var_len, &nullable));

    reader.attribute_datatype("sample_name", &dtype, &var_len, &nullable);
    REQUIRE(dtype == AttrDatatype::CHAR);
    REQUIRE(var_len);
    REQUIRE(!nullable);
    reader.attribute_datatype("contig", &dtype, &var_len, &nullable);
    REQUIRE(dtype == AttrDatatype::CHAR);
    REQUIRE(var_len);
    REQUIRE(!nullable);
    reader.attribute_datatype("query_bed_start", &dtype, &var_len, &nullable);
    REQUIRE(dtype == AttrDatatype::INT32);
    REQUIRE(!var_len);
    REQUIRE(!nullable);
    reader.attribute_datatype("pos_end", &dtype, &var_len, &nullable);
    REQUIRE(dtype == AttrDatatype::INT32);
    REQUIRE(!var_len);
    REQUIRE(!nullable);
    reader.attribute_datatype("info", &dtype, &var_len, &nullable);
    REQUIRE(dtype == AttrDatatype::UINT8);
    REQUIRE(var_len);
    REQUIRE(nullable);

    reader.attribute_datatype("fmt_GT", &dtype, &var_len, &nullable);
    REQUIRE(dtype == AttrDatatype::INT32);
    REQUIRE(var_len);
    REQUIRE(nullable);

    reader.attribute_datatype("fmt_GT", &dtype, &var_len, &nullable);
    REQUIRE(dtype == AttrDatatype::INT32);
    REQUIRE(var_len);
    REQUIRE(nullable);
    reader.attribute_datatype("fmt_AD", &dtype, &var_len, &nullable);
    REQUIRE(dtype == AttrDatatype::INT32);
    REQUIRE(var_len);
    REQUIRE(nullable);

    reader.attribute_datatype("info_BaseQRankSum", &dtype, &var_len, &nullable);
    REQUIRE(dtype == AttrDatatype::FLOAT32);
    REQUIRE(var_len);
    REQUIRE(nullable);
    reader.attribute_datatype("info_DS", &dtype, &var_len, &nullable);
    REQUIRE(dtype == AttrDatatype::INT32);
    REQUIRE(var_len);
    REQUIRE(nullable);
  }

  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);
  if (vfs.is_dir(output_dir))
    vfs.remove_dir(output_dir);
}

TEST_CASE("TileDB-VCF: Test get buffers", "[tiledbvcf][export]") {
  Reader reader;

  // Allocate some buffers to receive data
  UserBuffer sample_name, contig, pos, end, gt, pl, dp, min_dp;
  sample_name.resize(1024);
  sample_name.offsets().resize(100);
  contig.resize(1024);
  contig.offsets().resize(100);
  pos.resize(1024);
  end.resize(1024);
  gt.resize(1024);
  gt.offsets().resize(100);
  pl.resize(1024);
  pl.offsets().resize(100);
  dp.resize(1024);
  dp.offsets().resize(100);
  min_dp.resize(1024);
  min_dp.offsets().resize(100);

  // Set buffers on the reader
  reader.set_buffer(
      "sample_name",
      sample_name.offsets().data(),
      sample_name.offsets().size(),
      sample_name.data<void>(),
      sample_name.size());
  reader.set_buffer(
      "contig",
      contig.offsets().data(),
      contig.offsets().size(),
      contig.data<void>(),
      contig.size());
  reader.set_buffer("pos_start", nullptr, 0, pos.data<void>(), pos.size());
  reader.set_buffer("pos_end", nullptr, 0, end.data<void>(), end.size());
  reader.set_buffer(
      "fmt_GT",
      gt.offsets().data(),
      gt.offsets().size(),
      gt.data<void>(),
      gt.size());
  reader.set_buffer(
      "fmt_PL",
      pl.offsets().data(),
      pl.offsets().size(),
      pl.data<void>(),
      pl.size());
  reader.set_buffer(
      "fmt_DP",
      dp.offsets().data(),
      dp.offsets().size(),
      dp.data<void>(),
      dp.size());
  reader.set_buffer(
      "fmt_MIN_DP",
      min_dp.offsets().data(),
      min_dp.offsets().size(),
      min_dp.data<void>(),
      min_dp.size());

  // Check buffer accessors
  int32_t num_buffers_set = 0;
  reader.num_buffers(&num_buffers_set);
  REQUIRE(num_buffers_set == 8);
  const char* name;
  int32_t* offs;
  int64_t offs_size;
  void* data;
  int64_t data_size;

  reader.get_buffer(0, &name, &offs, &offs_size, &data, &data_size);
  REQUIRE(name == std::string("sample_name"));
  REQUIRE(offs == sample_name.offsets().data());
  REQUIRE(offs_size == sample_name.offsets().size());
  REQUIRE(data == sample_name.data<void>());
  REQUIRE(data_size == sample_name.size());

  reader.get_buffer(7, &name, &offs, &offs_size, &data, &data_size);
  REQUIRE(name == std::string("fmt_MIN_DP"));
  REQUIRE(offs == min_dp.offsets().data());
  REQUIRE(offs_size == min_dp.offsets().size());
  REQUIRE(data == min_dp.data<void>());
  REQUIRE(data_size == min_dp.size());

  REQUIRE_THROWS(
      reader.get_buffer(8, &name, &offs, &offs_size, &data, &data_size));
}

TEST_CASE(
    "TileDB-VCF: Test export from buffered ingest", "[tiledbvcf][export]") {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  std::string dataset_uri = "test_dataset";
  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);

  CreationParams create_args;
  create_args.uri = dataset_uri;
  create_args.tile_capacity = 10000;
  TileDBVCFDataset::create(create_args);

  // Register
  {
    TileDBVCFDataset ds;
    ds.open(dataset_uri);
    RegistrationParams args;
    args.sample_uris = {input_dir + "/small.bcf", input_dir + "/small2.bcf"};
    ds.register_samples(args);
  }

  // Ingest
  {
    Writer writer;
    IngestionParams params;
    params.uri = dataset_uri;
    params.sample_uris = {input_dir + "/small.bcf", input_dir + "/small2.bcf"};
    params.max_record_buffer_size = 1;
    writer.set_all_params(params);
    writer.ingest_samples();
  }

  // Check count operation
  {
    Reader reader;
    ExportParams params;
    params.uri = dataset_uri;
    params.sample_names = {"HG00280", "HG01762"};
    params.regions = {"1:12700-13400", "1:17000-17400"};
    reader.set_all_params(params);
    reader.open_dataset(dataset_uri);
    reader.read();
    REQUIRE(reader.read_status() == ReadStatus::COMPLETED);
    REQUIRE(reader.num_records_exported() == 7);
  }

  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);
}

TEST_CASE("TileDB-VCF: Test export all regions", "[tiledbvcf][export]") {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  std::string dataset_uri = "test_dataset";
  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);

  std::string output_dir = "test_dataset_out";
  if (vfs.is_dir(output_dir))
    vfs.remove_dir(output_dir);

  CreationParams create_args;
  create_args.uri = dataset_uri;
  create_args.tile_capacity = 10000;
  TileDBVCFDataset::create(create_args);

  // Register two samples
  {
    TileDBVCFDataset ds;
    ds.open(dataset_uri);
    RegistrationParams args;
    args.sample_uris = {input_dir + "/small.bcf", input_dir + "/small2.bcf"};
    ds.register_samples(args);
  }

  // Ingest the samples
  {
    Writer writer;
    IngestionParams params;
    params.uri = dataset_uri;
    params.sample_uris = {input_dir + "/small.bcf", input_dir + "/small2.bcf"};
    writer.set_all_params(params);
    writer.ingest_samples();
  }

  // Query a few regions
  {
    Reader reader;
    UserBuffer end;
    end.resize(1024);
    reader.set_buffer("pos_end", nullptr, 0, end.data<void>(), end.size());

    ExportParams params;
    params.uri = dataset_uri;
    params.output_dir = output_dir;
    params.sample_names = {"HG00280", "HG01762"};
    reader.set_all_params(params);
    reader.open_dataset(dataset_uri);
    reader.read();
    REQUIRE(reader.read_status() == ReadStatus::COMPLETED);
    REQUIRE(reader.num_records_exported() == 14);
    check_result<uint32_t>(
        reader,
        "pos_end",
        end,
        {12277,
         12277,
         12771,
         12771,
         13374,
         13389,
         13395,
         13413,
         13451,
         13519,
         13544,
         13689,
         17479,
         17486});
  }

  // Check count operation
  {
    Reader reader;
    ExportParams params;
    params.uri = dataset_uri;
    params.output_dir = output_dir;
    params.sample_names = {"HG00280", "HG01762"};
    params.regions = {"1:12700-13400", "1:17000-17400"};
    reader.set_all_params(params);
    reader.open_dataset(dataset_uri);
    reader.read();
    REQUIRE(reader.read_status() == ReadStatus::COMPLETED);
    REQUIRE(reader.num_records_exported() == 7);
  }

  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);
  if (vfs.is_dir(output_dir))
    vfs.remove_dir(output_dir);
}

TEST_CASE("TileDB-VCF: Test export multiple times", "[tiledbvcf][export]") {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  std::string dataset_uri = "test_dataset";
  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);

  std::string output_dir = "test_dataset_out";
  if (vfs.is_dir(output_dir))
    vfs.remove_dir(output_dir);

  CreationParams create_args;
  create_args.uri = dataset_uri;
  create_args.tile_capacity = 10000;
  TileDBVCFDataset::create(create_args);

  // Register two samples
  {
    TileDBVCFDataset ds;
    ds.open(dataset_uri);
    RegistrationParams args;
    args.sample_uris = {input_dir + "/small.bcf", input_dir + "/small2.bcf"};
    ds.register_samples(args);
  }

  // Ingest the samples
  {
    Writer writer;
    IngestionParams params;
    params.uri = dataset_uri;
    params.sample_uris = {input_dir + "/small.bcf", input_dir + "/small2.bcf"};
    writer.set_all_params(params);
    writer.ingest_samples();
  }

  // Query a few regions
  {
    Reader reader;
    UserBuffer end;
    end.resize(1024);
    reader.set_buffer("pos_end", nullptr, 0, end.data<void>(), end.size());

    ExportParams params;
    params.uri = dataset_uri;
    params.output_dir = output_dir;
    params.sample_names = {"HG00280", "HG01762"};
    reader.set_all_params(params);
    reader.open_dataset(dataset_uri);
    reader.read();
    REQUIRE(reader.read_status() == ReadStatus::COMPLETED);
    REQUIRE(reader.num_records_exported() == 14);
    check_result<uint32_t>(
        reader,
        "pos_end",
        end,
        {12277,
         12277,
         12771,
         12771,
         13374,
         13389,
         13395,
         13413,
         13451,
         13519,
         13544,
         13689,
         17479,
         17486});

    // Reset the reader and read again.
    reader.reset();
    REQUIRE(reader.read_status() == ReadStatus::UNINITIALIZED);
    REQUIRE(reader.num_records_exported() == 0);
    reader.read();
    REQUIRE(reader.read_status() == ReadStatus::COMPLETED);
    REQUIRE(reader.num_records_exported() == 14);
    check_result<uint32_t>(
        reader,
        "pos_end",
        end,
        {12277,
         12277,
         12771,
         12771,
         13374,
         13389,
         13395,
         13413,
         13451,
         13519,
         13544,
         13689,
         17479,
         17486});

    // Reset the reader and read a different region.
    reader.reset();
    reader.set_regions("1:12700-13400");
    REQUIRE(reader.read_status() == ReadStatus::UNINITIALIZED);
    REQUIRE(reader.num_records_exported() == 0);
    reader.read();
    REQUIRE(reader.read_status() == ReadStatus::COMPLETED);
    REQUIRE(reader.num_records_exported() == 6);
    check_result<uint32_t>(
        reader, "pos_end", end, {12771, 12771, 13374, 13389, 13395, 13413});
  }

  // Check count operation
  {
    Reader reader;
    ExportParams params;
    params.uri = dataset_uri;
    params.output_dir = output_dir;
    params.sample_names = {"HG00280", "HG01762"};
    params.regions = {"1:12700-13400", "1:17000-17400"};
    reader.set_all_params(params);
    reader.open_dataset(dataset_uri);
    reader.read();
    REQUIRE(reader.read_status() == ReadStatus::COMPLETED);
    REQUIRE(reader.num_records_exported() == 7);
  }

  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);
  if (vfs.is_dir(output_dir))
    vfs.remove_dir(output_dir);
}

TEST_CASE("TileDB-VCF: Test export to BCF", "[tiledbvcf][export]") {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  std::string dataset_uri = "test_dataset";
  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);

  std::string output_dir = "test_dataset_out";
  if (vfs.is_dir(output_dir))
    vfs.remove_dir(output_dir);
  vfs.create_dir(output_dir);

  CreationParams create_args;
  create_args.uri = dataset_uri;
  create_args.tile_capacity = 10000;
  TileDBVCFDataset::create(create_args);

  // Register two samples
  {
    TileDBVCFDataset ds;
    ds.open(dataset_uri);
    RegistrationParams args;
    args.sample_uris = {input_dir + "/small.bcf", input_dir + "/small2.bcf"};
    ds.register_samples(args);
  }

  // Ingest the samples
  {
    Writer writer;
    IngestionParams params;
    params.uri = dataset_uri;
    params.sample_uris = {input_dir + "/small.bcf", input_dir + "/small2.bcf"};
    writer.set_all_params(params);
    writer.ingest_samples();
  }

  // Query a few regions
  {
    Reader reader;
    ExportParams params;
    params.uri = dataset_uri;
    params.output_dir = output_dir;
    params.sample_names = {"HG00280", "HG01762"};
    params.regions = {"1:12700-13400", "1:17000-17400"};
    params.export_to_disk = true;
    reader.set_all_params(params);
    reader.open_dataset(dataset_uri);
    reader.read();
    REQUIRE(reader.read_status() == ReadStatus::COMPLETED);
    REQUIRE(reader.num_records_exported() == 7);
  }

  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);
  if (vfs.is_dir(output_dir))
    vfs.remove_dir(output_dir);
}

TEST_CASE("TileDB-VCF: Test export to TSV", "[tiledbvcf][export]") {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  std::string dataset_uri = "test_dataset";
  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);

  std::string output_dir = "test_dataset_out";
  if (vfs.is_dir(output_dir))
    vfs.remove_dir(output_dir);
  vfs.create_dir(output_dir);

  CreationParams create_args;
  create_args.uri = dataset_uri;
  create_args.tile_capacity = 10000;
  TileDBVCFDataset::create(create_args);

  // Register two samples
  {
    TileDBVCFDataset ds;
    ds.open(dataset_uri);
    RegistrationParams args;
    args.sample_uris = {input_dir + "/small.bcf", input_dir + "/small2.bcf"};
    ds.register_samples(args);
  }

  // Ingest the samples
  {
    Writer writer;
    IngestionParams params;
    params.uri = dataset_uri;
    params.sample_uris = {input_dir + "/small.bcf", input_dir + "/small2.bcf"};
    writer.set_all_params(params);
    writer.ingest_samples();
  }

  // Query a few regions
  {
    Reader reader;
    ExportParams params;
    params.uri = dataset_uri;
    params.output_dir = output_dir;
    params.sample_names = {"HG00280", "HG01762"};
    params.regions = {"1:12700-13400", "1:17000-17400"};
    params.export_to_disk = true;
    params.format = ExportFormat::TSV;
    params.tsv_output_path = "out.tsv";
    reader.set_all_params(params);
    reader.open_dataset(dataset_uri);
    reader.read();
    REQUIRE(reader.read_status() == ReadStatus::COMPLETED);
    REQUIRE(reader.num_records_exported() == 7);
  }

  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);
  if (vfs.is_dir(output_dir))
    vfs.remove_dir(output_dir);
}

TEST_CASE(
    "TileDB-VCF: Test export sample partitioning", "[tiledbvcf][export]") {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  std::string dataset_uri = "test_dataset";
  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);

  std::string output_dir = "test_dataset_out";
  if (vfs.is_dir(output_dir))
    vfs.remove_dir(output_dir);

  CreationParams create_args;
  create_args.uri = dataset_uri;
  create_args.tile_capacity = 10000;
  TileDBVCFDataset::create(create_args);

  // Register two samples
  {
    TileDBVCFDataset ds;
    ds.open(dataset_uri);
    RegistrationParams args;
    args.sample_uris = {input_dir + "/small.bcf", input_dir + "/small2.bcf"};
    ds.register_samples(args);
  }

  // Ingest the samples
  {
    Writer writer;
    IngestionParams params;
    params.uri = dataset_uri;
    params.sample_uris = {input_dir + "/small.bcf", input_dir + "/small2.bcf"};
    writer.set_all_params(params);
    writer.ingest_samples();
  }

  // Query
  {
    std::unique_ptr<Reader> reader(new Reader);
    ExportParams params;
    params.uri = dataset_uri;
    params.output_dir = output_dir;
    params.sample_names = {"HG00280", "HG01762"};
    reader->set_all_params(params);
    reader->open_dataset(dataset_uri);
    reader->read();
    REQUIRE(reader->read_status() == ReadStatus::COMPLETED);
    REQUIRE(reader->num_records_exported() == 14);

    reader.reset(new Reader);
    params.sample_partitioning.partition_index = 0;
    params.sample_partitioning.num_partitions = 2;
    reader->set_all_params(params);
    reader->open_dataset(dataset_uri);
    reader->read();
    REQUIRE(reader->read_status() == ReadStatus::COMPLETED);
    REQUIRE(reader->num_records_exported() == 3);

    reader.reset(new Reader);
    params.sample_partitioning.partition_index = 1;
    params.sample_partitioning.num_partitions = 2;
    reader->set_all_params(params);
    reader->open_dataset(dataset_uri);
    reader->read();
    REQUIRE(reader->read_status() == ReadStatus::COMPLETED);
    REQUIRE(reader->num_records_exported() == 11);
  }

  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);
  if (vfs.is_dir(output_dir))
    vfs.remove_dir(output_dir);
}

TEST_CASE(
    "TileDB-VCF: Test export region partitioning", "[tiledbvcf][export]") {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  std::string dataset_uri = "test_dataset";
  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);

  std::string output_dir = "test_dataset_out";
  if (vfs.is_dir(output_dir))
    vfs.remove_dir(output_dir);

  CreationParams create_args;
  create_args.uri = dataset_uri;
  create_args.tile_capacity = 10000;
  TileDBVCFDataset::create(create_args);

  // Register two samples
  {
    TileDBVCFDataset ds;
    ds.open(dataset_uri);
    RegistrationParams args;
    args.sample_uris = {input_dir + "/small.bcf", input_dir + "/small2.bcf"};
    ds.register_samples(args);
  }

  // Ingest the samples
  {
    Writer writer;
    IngestionParams params;
    params.uri = dataset_uri;
    params.sample_uris = {input_dir + "/small.bcf", input_dir + "/small2.bcf"};
    writer.set_all_params(params);
    writer.ingest_samples();
  }

  // Query
  {
    std::unique_ptr<Reader> reader(new Reader);
    ExportParams params;
    params.uri = dataset_uri;
    params.output_dir = output_dir;
    params.sample_names = {"HG01762", "HG00280"};
    params.regions = {"1:12000-13500", "1:17000-18000"};
    reader->set_all_params(params);
    reader->open_dataset(dataset_uri);
    reader->read();
    REQUIRE(reader->read_status() == ReadStatus::COMPLETED);
    REQUIRE(reader->num_records_exported() == 12);

    reader.reset(new Reader);
    params.region_partitioning.partition_index = 0;
    params.region_partitioning.num_partitions = 2;
    reader->set_all_params(params);
    reader->open_dataset(dataset_uri);
    reader->read();
    REQUIRE(reader->read_status() == ReadStatus::COMPLETED);
    REQUIRE(reader->num_records_exported() == 10);

    reader.reset(new Reader);
    params.region_partitioning.partition_index = 1;
    params.region_partitioning.num_partitions = 2;
    reader->set_all_params(params);
    reader->open_dataset(dataset_uri);
    reader->read();
    REQUIRE(reader->read_status() == ReadStatus::COMPLETED);
    REQUIRE(reader->num_records_exported() == 2);

    // Test adding sample partitioning as well.
    reader.reset(new Reader);
    params.sample_partitioning.partition_index = 1;
    params.sample_partitioning.num_partitions = 2;
    params.region_partitioning.partition_index = 0;
    params.region_partitioning.num_partitions = 2;
    reader->set_all_params(params);
    reader->open_dataset(dataset_uri);
    reader->read();
    REQUIRE(reader->read_status() == ReadStatus::COMPLETED);
    REQUIRE(reader->num_records_exported() == 7);

    reader.reset(new Reader);
    params.sample_partitioning.partition_index = 1;
    params.sample_partitioning.num_partitions = 2;
    params.region_partitioning.partition_index = 1;
    params.region_partitioning.num_partitions = 2;
    reader->set_all_params(params);
    reader->open_dataset(dataset_uri);
    reader->read();
    REQUIRE(reader->read_status() == ReadStatus::COMPLETED);
    REQUIRE(reader->num_records_exported() == 2);
  }

  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);
  if (vfs.is_dir(output_dir))
    vfs.remove_dir(output_dir);
}

TEST_CASE("TileDB-VCF: Test export limit records", "[tiledbvcf][export]") {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  std::string dataset_uri = "test_dataset";
  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);

  std::string output_dir = "test_dataset_out";
  if (vfs.is_dir(output_dir))
    vfs.remove_dir(output_dir);

  CreationParams create_args;
  create_args.uri = dataset_uri;
  create_args.tile_capacity = 10000;
  TileDBVCFDataset::create(create_args);

  // Register two samples
  {
    TileDBVCFDataset ds;
    ds.open(dataset_uri);
    RegistrationParams args;
    args.sample_uris = {input_dir + "/small.bcf", input_dir + "/small2.bcf"};
    ds.register_samples(args);
  }

  // Ingest the samples
  {
    Writer writer;
    IngestionParams params;
    params.uri = dataset_uri;
    params.sample_uris = {input_dir + "/small.bcf", input_dir + "/small2.bcf"};
    writer.set_all_params(params);
    writer.ingest_samples();
  }

  // Query
  {
    std::unique_ptr<Reader> reader(new Reader);
    ExportParams params;
    params.uri = dataset_uri;
    params.output_dir = output_dir;
    params.sample_names = {"HG01762", "HG00280"};
    params.regions = {"1:12000-13500", "1:17000-18000"};
    params.max_num_records = 5;
    reader->set_all_params(params);
    reader->open_dataset(dataset_uri);
    reader->read();
    REQUIRE(reader->read_status() == ReadStatus::COMPLETED);
    REQUIRE(reader->num_records_exported() == 5);

    reader.reset(new Reader);
    params.max_num_records = 100;
    reader->set_all_params(params);
    reader->open_dataset(dataset_uri);
    reader->read();
    REQUIRE(reader->read_status() == ReadStatus::COMPLETED);
    REQUIRE(reader->num_records_exported() == 12);
  }

  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);
  if (vfs.is_dir(output_dir))
    vfs.remove_dir(output_dir);
}

TEST_CASE("TileDB-VCF: Test export incomplete queries", "[tiledbvcf][export]") {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  std::string dataset_uri = "test_dataset";
  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);

  std::string output_dir = "test_dataset_out";
  if (vfs.is_dir(output_dir))
    vfs.remove_dir(output_dir);

  CreationParams create_args;
  create_args.uri = dataset_uri;
  create_args.tile_capacity = 10000;
  TileDBVCFDataset::create(create_args);

  // Register two samples
  {
    TileDBVCFDataset ds;
    ds.open(dataset_uri);
    RegistrationParams args;
    args.sample_uris = {input_dir + "/small.bcf", input_dir + "/small2.bcf"};
    ds.register_samples(args);
  }

  // Ingest the samples
  {
    Writer writer;
    IngestionParams params;
    params.uri = dataset_uri;
    params.sample_uris = {input_dir + "/small.bcf", input_dir + "/small2.bcf"};
    writer.set_all_params(params);
    writer.ingest_samples();
  }

  // Query a few regions
  {
    Reader reader;

    // Allocate some buffers to receive data
    UserBuffer sample_name, contig, pos, end;
    sample_name.resize(25);
    sample_name.offsets().resize(100);
    contig.resize(1024);
    contig.offsets().resize(100);
    pos.resize(1024);
    end.resize(1024);

    // Set buffers on the reader
    reader.set_buffer(
        "sample_name",
        sample_name.offsets().data(),
        sample_name.offsets().size(),
        sample_name.data<void>(),
        sample_name.size());
    reader.set_buffer(
        "contig",
        contig.offsets().data(),
        contig.offsets().size(),
        contig.data<void>(),
        contig.size());
    reader.set_buffer("pos_start", nullptr, 0, pos.data<void>(), pos.size());
    reader.set_buffer("pos_end", nullptr, 0, end.data<void>(), end.size());

    ExportParams params;
    params.uri = dataset_uri;
    params.output_dir = output_dir;
    params.sample_names = {"HG00280", "HG01762"};
    params.regions = {"1:12700-13400", "1:17000-17400"};
    reader.set_all_params(params);
    reader.open_dataset(dataset_uri);
    reader.read();
    REQUIRE(reader.read_status() == ReadStatus::INCOMPLETE);
    REQUIRE(reader.num_records_exported() == 3);
    check_string_result(
        reader, "sample_name", sample_name, {"HG01762", "HG00280", "HG00280"});
    check_string_result(reader, "contig", contig, {"1", "1", "1"});
    check_result<uint32_t>(reader, "pos_start", pos, {12546, 12546, 13354});
    check_result<uint32_t>(reader, "pos_end", end, {12771, 12771, 13374});

    reader.read();
    REQUIRE(reader.read_status() == ReadStatus::INCOMPLETE);
    REQUIRE(reader.num_records_exported() == 3);
    check_string_result(
        reader, "sample_name", sample_name, {"HG01762", "HG00280", "HG00280"});
    check_string_result(reader, "contig", contig, {"1", "1", "1"});
    check_result<uint32_t>(reader, "pos_start", pos, {13354, 13375, 13396});
    check_result<uint32_t>(reader, "pos_end", end, {13389, 13395, 13413});

    reader.read();
    REQUIRE(reader.read_status() == ReadStatus::COMPLETED);
    REQUIRE(reader.num_records_exported() == 1);
    check_string_result(reader, "sample_name", sample_name, {"HG00280"});
    check_string_result(reader, "contig", contig, {"1"});
    check_result<uint32_t>(reader, "pos_start", pos, {17319});
    check_result<uint32_t>(reader, "pos_end", end, {17479});
  }

  // Test both types of incomplete queries (TileDB as well).
  {
    Reader reader;

    // Allocate some buffers to receive data
    UserBuffer sample_name, contig, pos, end;
    sample_name.resize(25);
    sample_name.offsets().resize(100);
    contig.resize(1024);
    contig.offsets().resize(100);
    pos.resize(1024);
    end.resize(1024);

    // Set buffers on the reader
    reader.set_buffer(
        "sample_name",
        sample_name.offsets().data(),
        sample_name.offsets().size(),
        sample_name.data<void>(),
        sample_name.size());
    reader.set_buffer(
        "contig",
        contig.offsets().data(),
        contig.offsets().size(),
        contig.data<void>(),
        contig.size());
    reader.set_buffer("pos_start", nullptr, 0, pos.data<void>(), pos.size());
    reader.set_buffer("pos_end", nullptr, 0, end.data<void>(), end.size());

    ExportParams params;
    params.uri = dataset_uri;
    params.output_dir = output_dir;
    params.sample_names = {"HG00280", "HG01762"};
    params.regions = {"1:12700-13400", "1:17000-17400"};
    params.attribute_buffer_size_mb = 0;  // Use undocumented "0MB" alloc.
    reader.set_all_params(params);
    reader.open_dataset(dataset_uri);
    reader.read();
    REQUIRE(reader.read_status() == ReadStatus::INCOMPLETE);
    REQUIRE(reader.num_records_exported() == 3);
    check_string_result(
        reader, "sample_name", sample_name, {"HG01762", "HG00280", "HG00280"});
    check_string_result(reader, "contig", contig, {"1", "1", "1"});
    check_result<uint32_t>(reader, "pos_start", pos, {12546, 12546, 13354});
    check_result<uint32_t>(reader, "pos_end", end, {12771, 12771, 13374});

    reader.read();
    REQUIRE(reader.read_status() == ReadStatus::INCOMPLETE);
    REQUIRE(reader.num_records_exported() == 3);
    check_string_result(
        reader, "sample_name", sample_name, {"HG01762", "HG00280", "HG00280"});
    check_string_result(reader, "contig", contig, {"1", "1", "1"});
    check_result<uint32_t>(reader, "pos_start", pos, {13354, 13375, 13396});
    check_result<uint32_t>(reader, "pos_end", end, {13389, 13395, 13413});

    reader.read();
    REQUIRE(reader.read_status() == ReadStatus::COMPLETED);
    REQUIRE(reader.num_records_exported() == 1);
    check_string_result(reader, "sample_name", sample_name, {"HG00280"});
    check_string_result(reader, "contig", contig, {"1"});
    check_result<uint32_t>(reader, "pos_start", pos, {17319});
    check_result<uint32_t>(reader, "pos_end", end, {17479});
  }

  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);
  if (vfs.is_dir(output_dir))
    vfs.remove_dir(output_dir);
}

TEST_CASE("TileDB-VCF: Test export 100", "[tiledbvcf][export]") {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  std::string dataset_uri = "test_dataset";
  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);

  CreationParams create_args;
  create_args.uri = dataset_uri;
  create_args.tile_capacity = 10000;
  // Smaller anchor gap because the synthetic records are not very long.
  create_args.anchor_gap = 1000;
  TileDBVCFDataset::create(create_args);

  // Register the samples in batches
  std::vector<std::string> all_samples;
  for (unsigned i = 0; i < 10; i++) {
    RegistrationParams args;
    for (unsigned j = 1; j <= 10; j++) {
      unsigned idx = i * 10 + j;
      std::string uri =
          input_dir + "/random_synthetic/G" + std::to_string(idx) + ".bcf";
      args.sample_uris.push_back(uri);
      all_samples.push_back(uri);
    }

    TileDBVCFDataset ds;
    ds.open(dataset_uri);
    ds.register_samples(args);
  }

  // Ingest
  Writer writer;
  IngestionParams params;
  params.uri = dataset_uri;
  params.sample_uris = all_samples;
  params.verbose = true;
  writer.set_all_params(params);
  writer.ingest_samples();

  // Perform a couple of count queries. Note: bash command to get correct
  // counts is e.g.: for f in test/inputs/random_synthetic/G{1,2,59}.bcf; do
  // bcftools view -H -r 1:12700-13400,1:17000-17400 $f; done | wc -l
  std::unique_ptr<Reader> reader(new Reader);
  ExportParams read_params;
  read_params.uri = dataset_uri;
  read_params.sample_names = {"G1", "G2", "G59"};
  read_params.regions = {"1:12700-13400",
                         "1:17000-17400",
                         "2:1234-12340",
                         "14:50000-100000",
                         "14:100000-200000"};
  reader->set_all_params(read_params);
  reader->open_dataset(dataset_uri);
  reader->read();
  REQUIRE(reader->read_status() == ReadStatus::COMPLETED);
  REQUIRE(reader->num_records_exported() == 18);

  // Check with unsorted samples and regions
  reader.reset(new Reader);
  read_params.sample_names = {"G59", "G1", "G2"};
  read_params.regions = {"1:12700-13400",
                         "14:50000-100000",
                         "2:1234-12340",
                         "1:17000-17400",
                         "14:100000-200000"};
  reader->set_all_params(read_params);
  reader->open_dataset(dataset_uri);
  reader->read();
  REQUIRE(reader->read_status() == ReadStatus::COMPLETED);
  REQUIRE(reader->num_records_exported() == 18);

  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);
}

TEST_CASE("TileDB-VCF: Test export 100 using BED", "[tiledbvcf][export]") {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  std::string dataset_uri = "test_dataset";
  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);

  CreationParams create_args;
  create_args.uri = dataset_uri;
  create_args.tile_capacity = 10000;
  TileDBVCFDataset::create(create_args);

  // Register the samples in batches
  std::vector<std::string> all_samples;
  for (unsigned i = 0; i < 10; i++) {
    RegistrationParams args;
    for (unsigned j = 1; j <= 10; j++) {
      unsigned idx = i * 10 + j;
      std::string uri =
          input_dir + "/random_synthetic/G" + std::to_string(idx) + ".bcf";
      args.sample_uris.push_back(uri);
      all_samples.push_back(uri);
    }

    TileDBVCFDataset ds;
    ds.open(dataset_uri);
    ds.register_samples(args);
  }

  // Ingest
  Writer writer;
  IngestionParams params;
  params.uri = dataset_uri;
  params.sample_uris = all_samples;
  params.verbose = true;
  writer.set_all_params(params);
  writer.ingest_samples();

  const std::string bed_path = "test.bed";
  std::ofstream os(bed_path);
  os << "1\t10000\t200000\n";
  os << "3\t307486\t307487\n";
  os << "7\t10000\t200000\n";
  os << "7\t200000\t200001\n";
  os.close();

  Reader reader;
  ExportParams read_params;
  read_params.uri = dataset_uri;
  read_params.sample_names = {"G1", "G2", "G59"};
  read_params.regions_file_uri = bed_path;
  reader.set_all_params(read_params);
  reader.open_dataset(dataset_uri);
  reader.read();
  REQUIRE(reader.read_status() == ReadStatus::COMPLETED);
  REQUIRE(reader.num_records_exported() == 28);

  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);
  if (vfs.is_file(bed_path))
    vfs.remove_file(bed_path);
}

TEST_CASE("TileDB-VCF: Test export with nulls", "[tiledbvcf][export]") {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  std::string dataset_uri = "test_dataset";
  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);

  std::string output_dir = "test_dataset_out";
  if (vfs.is_dir(output_dir))
    vfs.remove_dir(output_dir);

  CreationParams create_args;
  create_args.uri = dataset_uri;
  create_args.tile_capacity = 10000;
  create_args.extra_attributes = {"info_BaseQRankSum", "info_DP", "fmt_DP"};
  TileDBVCFDataset::create(create_args);

  // Register two samples
  {
    TileDBVCFDataset ds;
    ds.open(dataset_uri);
    RegistrationParams args;
    args.sample_uris = {input_dir + "/small3.bcf", input_dir + "/small.bcf"};
    ds.register_samples(args);
  }

  // Ingest the samples
  {
    Writer writer;
    IngestionParams params;
    params.uri = dataset_uri;
    params.sample_uris = {input_dir + "/small3.bcf", input_dir + "/small.bcf"};
    writer.set_all_params(params);
    writer.ingest_samples();
  }

  // Query a few regions
  {
    Reader reader;

    // Allocate some buffers to receive data
    UserBuffer sample_name, pos, end, baseq, info_dp, fmt_dp;
    sample_name.resize(3 * 1024);
    sample_name.offsets().resize(3 * 100);
    sample_name.bitmap().resize(3 * 100);
    pos.resize(3 * 1024);
    end.resize(3 * 1024);
    baseq.resize(3 * 1024);
    baseq.offsets().resize(3 * 100);
    baseq.bitmap().resize(3 * 100);
    info_dp.resize(3 * 1024);
    info_dp.offsets().resize(3 * 100);
    info_dp.bitmap().resize(3 * 100);
    fmt_dp.resize(3 * 1024);
    fmt_dp.offsets().resize(3 * 100);
    fmt_dp.bitmap().resize(3 * 100);

    // Set buffers on the reader
    reader.set_buffer(
        "sample_name",
        sample_name.offsets().data(),
        sample_name.offsets().size(),
        sample_name.data<void>(),
        sample_name.size());
    REQUIRE_THROWS(reader.set_validity_bitmap(
        "sample_name",
        sample_name.bitmap().data(),
        sample_name.bitmap().size()));
    reader.set_buffer("pos_start", nullptr, 0, pos.data<void>(), pos.size());
    reader.set_buffer("pos_end", nullptr, 0, end.data<void>(), end.size());
    REQUIRE_THROWS(reader.set_validity_bitmap(
        "pos_end", sample_name.bitmap().data(), sample_name.bitmap().size()));
    reader.set_buffer(
        "info_BaseQRankSum",
        baseq.offsets().data(),
        baseq.offsets().size(),
        baseq.data<void>(),
        baseq.size());
    reader.set_validity_bitmap(
        "info_BaseQRankSum", baseq.bitmap().data(), baseq.bitmap().size());
    reader.set_buffer(
        "info_DP",
        info_dp.offsets().data(),
        info_dp.offsets().size(),
        info_dp.data<void>(),
        info_dp.size());
    reader.set_validity_bitmap(
        "info_DP", info_dp.bitmap().data(), info_dp.bitmap().size());
    reader.set_buffer(
        "fmt_DP",
        fmt_dp.offsets().data(),
        fmt_dp.offsets().size(),
        fmt_dp.data<void>(),
        fmt_dp.size());
    reader.set_validity_bitmap(
        "fmt_DP", fmt_dp.bitmap().data(), fmt_dp.bitmap().size());

    ExportParams params;
    params.uri = dataset_uri;
    params.output_dir = output_dir;
    params.sample_names = {"HG00280", "HG01762"};
    params.regions = {"1:12700-13400", "1:69500-69800"};
    reader.set_all_params(params);
    reader.open_dataset(dataset_uri);
    reader.read();
    REQUIRE(reader.read_status() == ReadStatus::COMPLETED);
    REQUIRE(reader.num_records_exported() == 12);
    check_string_result(
        reader,
        "sample_name",
        sample_name,
        {"HG00280",
         "HG01762",
         "HG00280",
         "HG01762",
         "HG00280",
         "HG00280",
         "HG00280",
         "HG00280",
         "HG00280",
         "HG00280",
         "HG00280",
         "HG00280"});
    check_result<uint32_t>(
        reader,
        "pos_start",
        pos,
        {12546,
         12546,
         13354,
         13354,
         13375,
         13396,
         69371,
         69511,
         69512,
         69761,
         69762,
         69771});
    check_result<uint32_t>(
        reader,
        "pos_end",
        end,
        {12771,
         12771,
         13374,
         13389,
         13395,
         13413,
         69510,
         69511,
         69760,
         69761,
         69770,
         69834});

    check_var_result<float>(
        reader, "info_BaseQRankSum", baseq, {-0.787f, 1.97f});
    check_var_result<int32_t>(reader, "info_DP", info_dp, {89, 24});
    check_var_result<int32_t>(
        reader,
        "fmt_DP",
        fmt_dp,
        {0, 0, 15, 64, 6, 2, 180, 88, 97, 24, 23, 21});
  }

  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);
  if (vfs.is_dir(output_dir))
    vfs.remove_dir(output_dir);
}
