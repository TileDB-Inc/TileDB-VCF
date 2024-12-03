/**
 * @file   unit-vcf-time-travel.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2019-2021 TileDB Inc.
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
 * Tests for VCF time travel.
 */

#include "catch.hpp"

#include "dataset/tiledbvcfdataset.h"
#include "read/reader.h"
#include "utils/logger_public.h"
#include "write/writer.h"

#include <cstring>
#include <fstream>
#include <iostream>

using namespace tiledb::vcf;

static const std::string input_dir = TILEDB_VCF_TEST_INPUT_DIR;

std::string now_ms_str() {
  return std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count());
}

TEST_CASE("TileDB-VCF: Test time travel", "[tiledbvcf][time-travel]") {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  std::string dataset_uri = "test_dataset";
  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);

  CreationParams create_args;
  create_args.uri = dataset_uri;
  create_args.tile_capacity = 10000;
  create_args.allow_duplicates = false;
  TileDBVCFDataset::create(create_args);

  auto t0 = now_ms_str();

  // Ingest
  {
    LOG_TRACE("Ingest at t0");
    Writer writer;
    IngestionParams params;
    params.uri = dataset_uri;
    params.sample_uris = {input_dir + "/small.vcf.gz"};
    params.max_record_buffer_size = 1;
    writer.set_all_params(params);
    writer.ingest_samples();
  }

  auto t1 = now_ms_str();

  // Ingest
  {
    LOG_TRACE("Ingest at t1");
    Writer writer;
    IngestionParams params;
    params.uri = dataset_uri;
    params.sample_uris = {input_dir + "/small2.vcf.gz"};
    params.max_record_buffer_size = 1;
    writer.set_all_params(params);
    writer.ingest_samples();
  }

  auto t2 = now_ms_str();

  // Ingest
  {
    LOG_TRACE("Ingest at t2");
    Writer writer;
    IngestionParams params;
    params.uri = dataset_uri;
    params.sample_uris = {input_dir + "/dupeEndPos.vcf.gz"};
    params.max_record_buffer_size = 1;
    writer.set_all_params(params);
    writer.ingest_samples();
  }

  auto t3 = now_ms_str();

  // Check sample count ingested between (t0, MAX)
  {
    LOG_TRACE("Check count");
    Reader reader;
    ExportParams params;
    params.uri = dataset_uri;
    params.tiledb_config.push_back(fmt::format("vcf.start_timestamp={}", t0));
    reader.set_all_params(params);
    reader.open_dataset(dataset_uri);
    reader.read();
    REQUIRE(reader.read_status() == ReadStatus::COMPLETED);
    int sample_count;
    reader.sample_count(&sample_count);
    LOG_TRACE("sample count = {}", sample_count);
    REQUIRE(sample_count == 3);
  }

  // Check sample count ingested between (t1, MAX)
  {
    LOG_TRACE("Check count");
    Reader reader;
    ExportParams params;
    params.uri = dataset_uri;
    params.tiledb_config.push_back(fmt::format("vcf.start_timestamp={}", t1));
    reader.set_all_params(params);
    reader.open_dataset(dataset_uri);
    reader.read();
    REQUIRE(reader.read_status() == ReadStatus::COMPLETED);
    int sample_count;
    reader.sample_count(&sample_count);
    LOG_TRACE("sample count = {}", sample_count);
    REQUIRE(sample_count == 2);
  }

  // Check sample count ingested between (t2, MAX)
  {
    LOG_TRACE("Check count");
    Reader reader;
    ExportParams params;
    params.uri = dataset_uri;
    params.tiledb_config.push_back(fmt::format("vcf.start_timestamp={}", t2));
    reader.set_all_params(params);
    reader.open_dataset(dataset_uri);
    reader.read();
    REQUIRE(reader.read_status() == ReadStatus::COMPLETED);
    int sample_count;
    reader.sample_count(&sample_count);
    LOG_TRACE("sample count = {}", sample_count);
    REQUIRE(sample_count == 1);
  }

  // Check sample count ingested between (t3, MAX)
  {
    LOG_TRACE("Check count");
    Reader reader;
    ExportParams params;
    params.uri = dataset_uri;
    params.tiledb_config.push_back(fmt::format("vcf.start_timestamp={}", t3));
    reader.set_all_params(params);
    reader.open_dataset(dataset_uri);
    reader.read();
    REQUIRE(reader.read_status() == ReadStatus::COMPLETED);
    int sample_count;
    reader.sample_count(&sample_count);
    LOG_TRACE("sample count = {}", sample_count);
    REQUIRE(sample_count == 0);
  }

  // Check sample count ingested between (0, t0)
  {
    LOG_TRACE("Check count");
    Reader reader;
    ExportParams params;
    params.uri = dataset_uri;
    params.tiledb_config.push_back(fmt::format("vcf.end_timestamp={}", t0));
    reader.set_all_params(params);
    reader.open_dataset(dataset_uri);
    reader.read();
    REQUIRE(reader.read_status() == ReadStatus::COMPLETED);
    int sample_count;
    reader.sample_count(&sample_count);
    LOG_TRACE("sample count = {}", sample_count);
    REQUIRE(sample_count == 0);
  }

  // Check sample count ingested between (0, t1)
  {
    LOG_TRACE("Check count");
    Reader reader;
    ExportParams params;
    params.uri = dataset_uri;
    params.tiledb_config.push_back(fmt::format("vcf.end_timestamp={}", t1));
    reader.set_all_params(params);
    reader.open_dataset(dataset_uri);
    reader.read();
    REQUIRE(reader.read_status() == ReadStatus::COMPLETED);
    int sample_count;
    reader.sample_count(&sample_count);
    LOG_TRACE("sample count = {}", sample_count);
    REQUIRE(sample_count == 1);
  }

  // Check sample count ingested between (0, t2)
  {
    LOG_TRACE("Check count");
    Reader reader;
    ExportParams params;
    params.uri = dataset_uri;
    params.tiledb_config.push_back(fmt::format("vcf.end_timestamp={}", t2));
    reader.set_all_params(params);
    reader.open_dataset(dataset_uri);
    reader.read();
    REQUIRE(reader.read_status() == ReadStatus::COMPLETED);
    int sample_count;
    reader.sample_count(&sample_count);
    LOG_TRACE("sample count = {}", sample_count);
    REQUIRE(sample_count == 2);
  }

  // Check sample count ingested between (0, t3)
  {
    LOG_TRACE("Check count");
    Reader reader;
    ExportParams params;
    params.uri = dataset_uri;
    params.tiledb_config.push_back(fmt::format("vcf.end_timestamp={}", t3));
    reader.set_all_params(params);
    reader.open_dataset(dataset_uri);
    reader.read();
    REQUIRE(reader.read_status() == ReadStatus::COMPLETED);
    int sample_count;
    reader.sample_count(&sample_count);
    LOG_TRACE("sample count = {}", sample_count);
    REQUIRE(sample_count == 3);
  }

  // Check sample count ingested between (t2, t1)
  {
    LOG_TRACE("Check count");
    Reader reader;
    ExportParams params;
    params.uri = dataset_uri;
    params.tiledb_config.push_back(fmt::format("vcf.start_timestamp={}", t2));
    params.tiledb_config.push_back(fmt::format("vcf.end_timestamp={}", t1));
    reader.set_all_params(params);
    reader.open_dataset(dataset_uri);
    reader.read();
    REQUIRE(reader.read_status() == ReadStatus::COMPLETED);
    int sample_count;
    reader.sample_count(&sample_count);
    LOG_TRACE("sample count = {}", sample_count);
    REQUIRE(sample_count == 0);
  }

  // Check sample count ingested between (t1, t2)
  {
    LOG_TRACE("Check count");
    Reader reader;
    ExportParams params;
    params.uri = dataset_uri;
    params.tiledb_config.push_back(fmt::format("vcf.start_timestamp={}", t1));
    params.tiledb_config.push_back(fmt::format("vcf.end_timestamp={}", t2));
    reader.set_all_params(params);
    reader.open_dataset(dataset_uri);
    reader.read();
    REQUIRE(reader.read_status() == ReadStatus::COMPLETED);
    int sample_count;
    reader.sample_count(&sample_count);
    LOG_TRACE("sample count = {}", sample_count);
    REQUIRE(sample_count == 1);
  }

  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);
}
