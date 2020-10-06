/**
 * @file   unit-vcf-store.cc
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
 * Tests for VCF ingestion.
 */

#include "catch.hpp"

#include "dataset/tiledbvcfdataset.h"
#include "write/writer.h"

#include <cstring>
#include <fstream>
#include <iostream>

using namespace tiledb::vcf;

static const std::string input_dir = TILEDB_VCF_TEST_INPUT_DIR;

TEST_CASE("TileDB-VCF: Test create", "[tiledbvcf][ingest]") {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  std::string dataset_uri = "test_dataset";
  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);

  CreationParams args;
  args.uri = dataset_uri;
  args.tile_capacity = 123;
  args.extra_attributes = {"a1", "a2"};
  REQUIRE_THROWS(TileDBVCFDataset::create(args));

  args.extra_attributes = {"info_a1", "fmt_a2"};
  TileDBVCFDataset::create(args);
  REQUIRE_NOTHROW(TileDBVCFDataset::create(args));

  TileDBVCFDataset ds;
  ds.open(dataset_uri);
  REQUIRE(ds.metadata().tile_capacity == 123);
  REQUIRE(ds.metadata().free_sample_id == 0);
  REQUIRE(ds.metadata().anchor_gap == 1000);
  REQUIRE(ds.metadata().row_tile_extent == 10);
  REQUIRE(
      ds.metadata().extra_attributes ==
      std::vector<std::string>{"info_a1", "fmt_a2"});

  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);
}

TEST_CASE("TileDB-VCF: Test register", "[tiledbvcf][ingest]") {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  std::string dataset_uri = "test_dataset";
  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);

  CreationParams create_args;
  create_args.uri = dataset_uri;
  create_args.tile_capacity = 10000;
  TileDBVCFDataset::create(create_args);

  // Register a sample
  {
    TileDBVCFDataset ds;
    ds.open(dataset_uri);
    REQUIRE_THROWS(ds.register_samples({}));
    RegistrationParams args;
    args.sample_uris = {input_dir + "/small.bcf"};
    ds.register_samples(args);
  }

  // Reopen the dataset and check the metadata.
  {
    TileDBVCFDataset ds;
    ds.open(dataset_uri);
    REQUIRE(ds.metadata().all_samples == std::vector<std::string>{"HG01762"});
    REQUIRE(ds.metadata().free_sample_id == 1);
    REQUIRE(ds.metadata().sample_ids.at("HG01762") == 0);
    REQUIRE(ds.metadata().sample_names.at(0) == "HG01762");
    REQUIRE(ds.metadata().contig_offsets.at("1") == 0);
    REQUIRE(ds.metadata().contig_offsets.at("2") == 249250621);
    REQUIRE(ds.metadata().contig_offsets.at("3") == 249250621 + 243199373);

    auto hdrs = ds.fetch_vcf_headers(ctx, {{"HG01762", 0}});
    REQUIRE(hdrs.size() == 1);
    REQUIRE(bcf_hdr_nsamples(hdrs.at(0)) == 1);
    REQUIRE(hdrs.at(0)->samples[0] == std::string("HG01762"));

    REQUIRE(ds.fmt_field_type("GQ") == BCF_HT_INT);
    REQUIRE(ds.info_field_type("BaseQRankSum") == BCF_HT_REAL);
  }

  // Register a second sample
  {
    TileDBVCFDataset ds;
    ds.open(dataset_uri);
    RegistrationParams args;
    args.sample_uris = {input_dir + "/small2.bcf"};
    ds.register_samples(args);
  }

  // Check updated metadata
  {
    TileDBVCFDataset ds;
    ds.open(dataset_uri);
    REQUIRE(
        ds.metadata().all_samples ==
        std::vector<std::string>{"HG01762", "HG00280"});
    REQUIRE(ds.metadata().free_sample_id == 2);
    REQUIRE(ds.metadata().sample_ids.at("HG01762") == 0);
    REQUIRE(ds.metadata().sample_ids.at("HG00280") == 1);
    REQUIRE(ds.metadata().sample_names.at(0) == "HG01762");
    REQUIRE(ds.metadata().sample_names.at(1) == "HG00280");
    REQUIRE(ds.metadata().contig_offsets.at("1") == 0);
    REQUIRE(ds.metadata().contig_offsets.at("2") == 249250621);
    REQUIRE(ds.metadata().contig_offsets.at("3") == 249250621 + 243199373);

    auto hdrs = ds.fetch_vcf_headers(ctx, {{"HG01762", 0}, {"HG00280", 1}});
    REQUIRE(hdrs.size() == 2);
    REQUIRE(bcf_hdr_nsamples(hdrs.at(0)) == 1);
    REQUIRE(hdrs.at(0)->samples[0] == std::string("HG01762"));
    REQUIRE(bcf_hdr_nsamples(hdrs.at(1)) == 1);
    REQUIRE(hdrs.at(1)->samples[0] == std::string("HG00280"));
  }

  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);
}

TEST_CASE("TileDB-VCF: Test register 100", "[tiledbvcf][ingest]") {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  std::string dataset_uri = "test_dataset";
  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);

  CreationParams create_args;
  create_args.uri = dataset_uri;
  create_args.tile_capacity = 10000;
  TileDBVCFDataset::create(create_args);

  SECTION("- List all") {
    // Register the samples
    RegistrationParams args;
    for (unsigned i = 1; i <= 100; i++) {
      std::string uri =
          input_dir + "/random_synthetic/G" + std::to_string(i) + ".bcf";
      args.sample_uris.push_back(uri);
    }

    TileDBVCFDataset ds;
    ds.open(dataset_uri);
    ds.register_samples(args);
  }

  SECTION("- Sample list file") {
    std::string samples_file = "dataset-samples.txt";
    std::ofstream os(samples_file.c_str(), std::ios::out | std::ios::trunc);
    for (unsigned i = 1; i <= 100; i++) {
      std::string uri =
          input_dir + "/random_synthetic/G" + std::to_string(i) + ".bcf";
      os << uri << "\n";
    }
    os.flush();
    os.close();

    // Register the samples
    RegistrationParams args;
    args.sample_uris_file = samples_file;
    TileDBVCFDataset ds;
    ds.open(dataset_uri);
    ds.register_samples(args);
  }

  SECTION("- Sample list file with explicit indices") {
    std::string samples_file = "dataset-samples.txt";
    std::ofstream os(samples_file.c_str(), std::ios::out | std::ios::trunc);
    for (unsigned i = 1; i <= 100; i++) {
      std::string uri =
          input_dir + "/random_synthetic/G" + std::to_string(i) + ".bcf";
      os << uri << "\t" << (uri + ".csi") << "\n";
    }
    os.flush();
    os.close();

    // Register the samples
    RegistrationParams args;
    args.sample_uris_file = samples_file;
    TileDBVCFDataset ds;
    ds.open(dataset_uri);
    ds.register_samples(args);
  }

  SECTION("- Incremental") {
    for (unsigned i = 0; i < 10; i++) {
      RegistrationParams args;
      for (unsigned j = 1; j <= 10; j++) {
        unsigned idx = i * 10 + j;
        std::string uri =
            input_dir + "/random_synthetic/G" + std::to_string(idx) + ".bcf";
        args.sample_uris.push_back(uri);
      }

      TileDBVCFDataset ds;
      ds.open(dataset_uri);
      ds.register_samples(args);
    }
  }

  // Reopen the dataset and check some of the metadata.
  {
    TileDBVCFDataset ds;
    ds.open(dataset_uri);
    REQUIRE(ds.metadata().all_samples.size() == 100);
    REQUIRE(ds.metadata().all_samples[42] == "G43");
    REQUIRE(ds.metadata().free_sample_id == 100);
    REQUIRE(ds.metadata().sample_ids.at("G17") == 16);
    REQUIRE(ds.metadata().sample_names.at(16) == "G17");

    auto hdrs =
        ds.fetch_vcf_headers(ctx, {{"G10", 9}, {"G11", 10}, {"G12", 11}});
    REQUIRE(hdrs.size() == 3);
    REQUIRE(bcf_hdr_nsamples(hdrs.at(0)) == 1);
    std::vector<std::string> samples = {
        hdrs.at(9)->samples[0],
        hdrs.at(10)->samples[0],
        hdrs.at(11)->samples[0]};
    std::vector<std::string> expected_samples = {"G10", "G11", "G12"};
    REQUIRE_THAT(expected_samples, Catch::Matchers::UnorderedEquals(samples));
  }

  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);
}

TEST_CASE("TileDB-VCF: Test ingest", "[tiledbvcf][ingest]") {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  std::string dataset_uri = "test_dataset";
  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);

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

  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);
}

TEST_CASE("TileDB-VCF: Test ingest larger", "[tiledbvcf][ingest]") {
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
    args.sample_uris = {input_dir + "/small3.bcf"};
    ds.register_samples(args);
  }

  // Ingest
  {
    Writer writer;
    IngestionParams params;
    params.uri = dataset_uri;
    params.sample_uris = {input_dir + "/small3.bcf"};
    writer.set_all_params(params);
    writer.ingest_samples();
  }

  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);
}

TEST_CASE("TileDB-VCF: Test ingest .vcf.gz", "[tiledbvcf][ingest]") {
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
    args.sample_uris = {input_dir + "/small.vcf.gz"};
    ds.register_samples(args);
  }

  // Ingest
  {
    Writer writer;
    IngestionParams params;
    params.uri = dataset_uri;
    params.sample_uris = {input_dir + "/small.vcf.gz"};
    writer.set_all_params(params);
    writer.ingest_samples();
  }

  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);
}

TEST_CASE("TileDB-VCF: Test ingest with attributes", "[tiledbvcf][ingest]") {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  std::string dataset_uri = "test_dataset";
  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);

  CreationParams create_args;
  create_args.uri = dataset_uri;
  create_args.tile_capacity = 10000;
  create_args.extra_attributes = {"DP", "MLEAC", "MLEAF", "MQ", "AD", "GQ"};
  REQUIRE_THROWS(TileDBVCFDataset::create(create_args));
  create_args.extra_attributes = {
      "fmt_DP", "info_MLEAC", "info_MLEAF", "info_MQ", "fmt_AD", "info_GQ"};
  TileDBVCFDataset::create(create_args);

  // Register
  {
    TileDBVCFDataset ds;
    ds.open(dataset_uri);
    RegistrationParams args;
    args.sample_uris = {input_dir + "/small3.bcf"};
    ds.register_samples(args);
  }

  // Ingest
  {
    Writer writer;
    IngestionParams params;
    params.uri = dataset_uri;
    params.sample_uris = {input_dir + "/small3.bcf"};
    writer.set_all_params(params);
    writer.ingest_samples();
  }

  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);
}

TEST_CASE("TileDB-VCF: Test ingest 100", "[tiledbvcf][ingest]") {
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

  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);
}

TEST_CASE("TileDB-VCF: Write to existing V2 array", "[tiledbvcf][ingest][v2]") {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  std::string dataset_uri = "test_dataset";
  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);

  // Copy an array created with V2 to our test dataset location.
  const std::string dataset_uri_src = input_dir + "/arrays/v2/ingested_1sample";

  // This is obviously a system-specific directory copy but
  // is OK for now because we only support Linux and run CI on Ubuntu.
  const std::string cp_cmd = "cp -r " + dataset_uri_src + " " + dataset_uri;
  FILE* const pipe = popen(cp_cmd.c_str(), "r");
  REQUIRE(pipe != nullptr);
  pclose(pipe);

  // Open the existing array.
  TileDBVCFDataset ds;
  ds.open(dataset_uri);

  // Register a new sample.
  RegistrationParams args;
  const std::string sample_uri = input_dir + "/small.bcf";
  args.sample_uris.push_back(sample_uri);
  ds.register_samples(args);

  // Ingest the new sample.
  Writer writer;
  IngestionParams params;
  params.uri = dataset_uri;
  params.sample_uris = {sample_uri};
  params.verbose = true;
  writer.set_all_params(params);
  writer.ingest_samples();

  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);
}