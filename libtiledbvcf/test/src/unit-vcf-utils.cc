/**
 * @file   unit-vcf-utils.cc
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
 * Tests for VCF export.
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

TEST_CASE(
    "TileDB-VCF: Test consolidate and vacuum fragment metadata and fragments",
    "[tiledbvcf][utils]") {
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

  // Ingest
  {
    LOG_TRACE("Ingest");
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
    LOG_TRACE("Check count");
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

  // Ingest a second time to double the fragments
  {
    LOG_TRACE("Ingest again");
    Writer writer;
    IngestionParams params;
    params.uri = dataset_uri;
    params.sample_uris = {input_dir + "/small.bcf", input_dir + "/small2.bcf"};
    params.max_record_buffer_size = 1;
    writer.set_all_params(params);
    writer.ingest_samples();
  }

  // Check count operation to make sure results are the same as before.
  // Duplicates are disabled so this should be true
  {
    LOG_TRACE("Check count");
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

  // Consolidate fragment metadata
  {
    LOG_TRACE("Consolidate fragment metadata");
    TileDBVCFDataset dataset(std::make_shared<tiledb::Context>(ctx));
    dataset.open(dataset_uri);
    UtilsParams params;
    params.uri = dataset_uri;
    dataset.consolidate_fragment_metadata(params);
  }

  // Check count operation after consolidating fragment metadata
  {
    LOG_TRACE("Check count");
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

  // Consolidate fragments
  {
    LOG_TRACE("Consolidate fragments");
    TileDBVCFDataset dataset(std::make_shared<tiledb::Context>(ctx));
    dataset.open(dataset_uri);
    UtilsParams params;
    params.uri = dataset_uri;
    dataset.consolidate_fragments(params);
  }

  // Check count operation after consolidating fragments
  {
    LOG_TRACE("Check count");
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

  // Vacuum fragment metadata
  {
    LOG_TRACE("Vacuum fragment metadata");
    TileDBVCFDataset dataset(std::make_shared<tiledb::Context>(ctx));
    dataset.open(dataset_uri);
    UtilsParams params;
    params.uri = dataset_uri;
    dataset.vacuum_fragment_metadata(params);
  }

  // Check count operation after consolidating fragment metadata
  {
    LOG_TRACE("Check count");
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

  // Vacuum fragments
  {
    LOG_TRACE("Vacuum fragments");
    TileDBVCFDataset dataset(std::make_shared<tiledb::Context>(ctx));
    dataset.open(dataset_uri);
    UtilsParams params;
    params.uri = dataset_uri;
    dataset.vacuum_fragments(params);
  }

  // Check count operation after consolidating fragments
  {
    LOG_TRACE("Check count");
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

TEST_CASE(
    "TileDB-VCF: Test consolidate and vacuum commits", "[tiledbvcf][utils]") {
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

  // Ingest
  {
    LOG_TRACE("Ingest");
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
    LOG_TRACE("Check count");
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

  // Ingest a second time to double the fragments
  {
    LOG_TRACE("Ingest again");
    Writer writer;
    IngestionParams params;
    params.uri = dataset_uri;
    params.sample_uris = {input_dir + "/small.bcf", input_dir + "/small2.bcf"};
    params.max_record_buffer_size = 1;
    writer.set_all_params(params);
    writer.ingest_samples();
  }

  // Check count operation to make sure results are the same as before.
  // Duplicates are disabled so this should be true
  {
    LOG_TRACE("Check count");
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

  // Consolidate commits
  {
    LOG_TRACE("Consolidate commits");
    TileDBVCFDataset dataset(std::make_shared<tiledb::Context>(ctx));
    dataset.open(dataset_uri);
    UtilsParams params;
    params.uri = dataset_uri;
    dataset.consolidate_commits(params);
  }

  // Check count operation after consolidating commits
  {
    LOG_TRACE("Check count");
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

  // Vacuum commits
  {
    LOG_TRACE("Vacuum commits");
    TileDBVCFDataset dataset(std::make_shared<tiledb::Context>(ctx));
    dataset.open(dataset_uri);
    UtilsParams params;
    params.uri = dataset_uri;
    dataset.vacuum_commits(params);
  }

  // Check count operation after vacuuming commits
  {
    LOG_TRACE("Check count");
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
