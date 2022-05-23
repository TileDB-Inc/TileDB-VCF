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
#include <regex>

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

  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);

  args.extra_attributes = {"info_a1", "fmt_a2"};
  REQUIRE_NOTHROW(TileDBVCFDataset::create(args));

  TileDBVCFDataset ds(std::make_shared<tiledb::Context>(ctx));
  ds.open(dataset_uri);
  REQUIRE(ds.metadata().tile_capacity == 123);
  REQUIRE(ds.metadata().anchor_gap == 1000);
  REQUIRE(ds.metadata().ingestion_sample_batch_size == 10);
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

  // Ingest the samples
  {
    Writer writer;
    IngestionParams params;
    params.uri = dataset_uri;
    params.sample_uris = {input_dir + "/small.bcf"};
    writer.set_all_params(params);
    writer.ingest_samples();
  }

  // Reopen the dataset and check the metadata.
  {
    TileDBVCFDataset ds(std::make_shared<tiledb::Context>(ctx));
    ds.open(dataset_uri);
    if (ds.metadata().version == tiledb::vcf::TileDBVCFDataset::Version::V4)
      ds.load_sample_names_v4();
    REQUIRE(ds.metadata().all_samples == std::vector<std::string>{"HG01762"});
    REQUIRE(ds.metadata().sample_ids.count("HG01762") == 1);
    REQUIRE_THAT(
        ds.metadata().sample_names_,
        Catch::Matchers::VectorContains(std::string("HG01762")));

    auto hdrs =
        ds.fetch_vcf_headers_v4({{"HG01762", 0}}, nullptr, false, false);
    REQUIRE(hdrs.size() == 1);
    REQUIRE(bcf_hdr_nsamples(hdrs.at(0)) == 1);
    REQUIRE(hdrs.at(0)->samples[0] == std::string("HG01762"));

    REQUIRE(ds.fmt_field_type("GQ", hdrs.at(0).get()) == BCF_HT_INT);
    REQUIRE(
        ds.info_field_type("BaseQRankSum", hdrs.at(0).get()) == BCF_HT_REAL);
  }

  // Ingest the samples
  {
    Writer writer;
    IngestionParams params;
    params.uri = dataset_uri;
    params.sample_uris = {input_dir + "/small2.bcf"};
    writer.set_all_params(params);
    writer.ingest_samples();
  }

  // Check updated metadata
  {
    TileDBVCFDataset ds(std::make_shared<tiledb::Context>(ctx));
    ds.open(dataset_uri);
    if (ds.metadata().version == tiledb::vcf::TileDBVCFDataset::Version::V4)
      ds.load_sample_names_v4();
    REQUIRE_THAT(
        ds.metadata().all_samples,
        Catch::Matchers::UnorderedEquals(
            std::vector<std::string>{"HG01762", "HG00280"}));
    REQUIRE(ds.metadata().sample_ids.count("HG01762") == 1);
    REQUIRE(ds.metadata().sample_ids.count("HG00280") == 1);
    std::vector<std::string> samples = {"HG01762", "HG00280"};
    REQUIRE_THAT(
        ds.metadata().sample_names_, Catch::Matchers::Contains(samples));

    auto hdrs = ds.fetch_vcf_headers_v4(
        {{"HG01762", 0}, {"HG00280", 1}}, nullptr, false, false);
    REQUIRE(hdrs.size() == 2);
    std::vector<std::string> expected_samples = {"HG01762", "HG00280"};
    std::vector<std::string> result_samples = {
        hdrs.at(1)->samples[0], hdrs.at(0)->samples[0]};
    REQUIRE_THAT(
        expected_samples, Catch::Matchers::UnorderedEquals(result_samples));
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

TEST_CASE("TileDB-VCF: Test ingest annotation VCF", "[tiledbvcf][ingest]") {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  std::string dataset_uri = "test_dataset";
  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);

  CreationParams create_args;
  create_args.uri = dataset_uri;
  TileDBVCFDataset::create(create_args);

  // Ingest bad vcf (throws)
  {
    Writer writer;
    IngestionParams params;
    params.uri = dataset_uri;
    params.sample_uris = {
        input_dir + "/E001_15_coreMarks_dense_filtered.bed.gz"};
    writer.set_all_params(params);
    REQUIRE_THROWS(writer.ingest_samples());
  }

  // Ingest vcf without sample name
  // TODO: enable this test
  /*
  {
    Writer writer;
    IngestionParams params;
    params.uri = dataset_uri;
    params.sample_uris = {input_dir + "/no-sample.bcf"};
    writer.set_all_params(params);
    writer.ingest_samples();
  }
  */

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
    for (unsigned j = 1; j <= 10; j++) {
      unsigned idx = i * 10 + j;
      std::string uri =
          input_dir + "/random_synthetic/G" + std::to_string(idx) + ".bcf";
      all_samples.push_back(uri);
    }
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
  TileDBVCFDataset ds(std::make_shared<tiledb::Context>(ctx));
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

TEST_CASE("TileDB-VCF: Test Resume", "[tiledbvcf][ingest]") {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  std::string dataset_uri = "test_dataset_resume";
  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);

  CreationParams create_args;
  create_args.uri = dataset_uri;
  create_args.tile_capacity = 10000;
  TileDBVCFDataset::create(create_args);

  // Ingest the sample
  {
    Writer writer;
    IngestionParams params;
    params.uri = dataset_uri;
    params.sample_uris = {input_dir + "/v2-DjrIAzkP-downsampled.vcf.gz"};
    params.resume_sample_partial_ingestion = true;
    params.contig_fragment_merging = false;
    writer.set_all_params(params);
    writer.ingest_samples();
  }

  // Check that there were 42 fragments created
  // Then remove the last fragment
  {
    tiledb::FragmentInfo fragmentInfo(ctx, dataset_uri + "/data");
    fragmentInfo.load();

    REQUIRE(fragmentInfo.fragment_num() == 44);

    // Get the last fragment
    std::string uri = fragmentInfo.fragment_uri(41);
    vfs.remove_dir(uri);
    uri = std::regex_replace(uri, std::regex("__fragments"), "__commits");
    vfs.remove_file(uri + ".wrt");
  }

  // Ingest the sample again, this should add only the missing data
  {
    Writer writer;
    IngestionParams params;
    params.uri = dataset_uri;
    params.sample_uris = {input_dir + "/v2-DjrIAzkP-downsampled.vcf.gz"};
    params.resume_sample_partial_ingestion = true;
    params.contig_fragment_merging = false;
    writer.set_all_params(params);
    writer.ingest_samples();
  }

  // Check that there are only 44 fragments created
  // Then remove the middle fragments (21-23)
  {
    tiledb::FragmentInfo fragmentInfo(ctx, dataset_uri + "/data");
    fragmentInfo.load();

    REQUIRE(fragmentInfo.fragment_num() == 44);

    // Remove fragment 21
    std::string uri = fragmentInfo.fragment_uri(21);
    vfs.remove_dir(uri);
    uri = std::regex_replace(uri, std::regex("__fragments"), "__commits");
    vfs.remove_file(uri + ".wrt");

    // Remove fragment 22
    uri = fragmentInfo.fragment_uri(22);
    vfs.remove_dir(uri);
    uri = std::regex_replace(uri, std::regex("__fragments"), "__commits");
    vfs.remove_file(uri + ".wrt");

    // Remove fragment 23
    uri = fragmentInfo.fragment_uri(23);
    vfs.remove_dir(uri);
    uri = std::regex_replace(uri, std::regex("__fragments"), "__commits");
    vfs.remove_file(uri + ".wrt");
  }

  // Ingest the sample again, this should add only the missing data
  {
    Writer writer;
    IngestionParams params;
    params.uri = dataset_uri;
    params.sample_uris = {input_dir + "/v2-DjrIAzkP-downsampled.vcf.gz"};
    params.resume_sample_partial_ingestion = true;
    params.contig_fragment_merging = false;
    writer.set_all_params(params);
    writer.ingest_samples();
  }

  // Check that there are only 44 fragments created
  {
    tiledb::FragmentInfo fragmentInfo(ctx, dataset_uri + "/data");
    fragmentInfo.load();

    REQUIRE(fragmentInfo.fragment_num() == 44);
  }

  // Ingest the sample one last time, this should result in no fragment changes
  {
    Writer writer;
    IngestionParams params;
    params.uri = dataset_uri;
    params.sample_uris = {input_dir + "/v2-DjrIAzkP-downsampled.vcf.gz"};
    params.resume_sample_partial_ingestion = true;
    params.contig_fragment_merging = false;
    writer.set_all_params(params);
    writer.ingest_samples();
  }

  // Check that there are only 44 fragments created
  {
    tiledb::FragmentInfo fragmentInfo(ctx, dataset_uri + "/data");
    fragmentInfo.load();

    REQUIRE(fragmentInfo.fragment_num() == 44);
  }

  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);
}

TEST_CASE("TileDB-VCF: Test Resume Disabled", "[tiledbvcf][ingest]") {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  std::string dataset_uri = "test_dataset_resume_disabled";
  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);

  CreationParams create_args;
  create_args.uri = dataset_uri;
  create_args.tile_capacity = 10000;
  TileDBVCFDataset::create(create_args);

  // Ingest the sample
  {
    Writer writer;
    IngestionParams params;
    params.uri = dataset_uri;
    params.sample_uris = {input_dir + "/v2-DjrIAzkP-downsampled.vcf.gz"};
    params.resume_sample_partial_ingestion = false;
    params.contig_fragment_merging = false;
    writer.set_all_params(params);
    writer.ingest_samples();
  }

  // Check that there were 44 fragments created
  {
    tiledb::FragmentInfo fragmentInfo(ctx, dataset_uri + "/data");
    fragmentInfo.load();

    REQUIRE(fragmentInfo.fragment_num() == 44);
  }

  // Ingest the sample a second time, we should get 88 fragments
  {
    Writer writer;
    IngestionParams params;
    params.uri = dataset_uri;
    params.sample_uris = {input_dir + "/v2-DjrIAzkP-downsampled.vcf.gz"};
    params.resume_sample_partial_ingestion = false;
    params.contig_fragment_merging = false;
    writer.set_all_params(params);
    writer.ingest_samples();
  }

  // Check that there were 88 fragments created
  {
    tiledb::FragmentInfo fragmentInfo(ctx, dataset_uri + "/data");
    fragmentInfo.load();

    REQUIRE(fragmentInfo.fragment_num() == 88);
  }

  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);
}

TEST_CASE("TileDB-VCF: Test Merging Contigs Defaults", "[tiledbvcf][ingest]") {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  std::string dataset_uri = "test_dataset_merge_configs";
  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);

  CreationParams create_args;
  create_args.uri = dataset_uri;
  create_args.tile_capacity = 10000;
  TileDBVCFDataset::create(create_args);

  // Ingest the sample
  {
    Writer writer;
    IngestionParams params;
    params.uri = dataset_uri;
    params.sample_uris = {input_dir + "/v2-DjrIAzkP-downsampled.vcf.gz"};
    params.resume_sample_partial_ingestion = false;
    params.contig_fragment_merging = true;
    writer.set_all_params(params);
    writer.ingest_samples();
  }

  // Check that there were 38 fragments created
  {
    tiledb::FragmentInfo fragmentInfo(ctx, dataset_uri + "/data");
    fragmentInfo.load();

    REQUIRE(fragmentInfo.fragment_num() == 38);
  }

  // Ingest the sample a second time, we should get 76 fragments
  {
    Writer writer;
    IngestionParams params;
    params.uri = dataset_uri;
    params.sample_uris = {input_dir + "/v2-DjrIAzkP-downsampled.vcf.gz"};
    params.resume_sample_partial_ingestion = false;
    params.contig_fragment_merging = true;
    writer.set_all_params(params);
    writer.ingest_samples();
  }

  // Check that there were 76 fragments created
  {
    tiledb::FragmentInfo fragmentInfo(ctx, dataset_uri + "/data");
    fragmentInfo.load();

    REQUIRE(fragmentInfo.fragment_num() == 76);
  }

  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);
}

TEST_CASE("TileDB-VCF: Test Resume Contig Merge", "[tiledbvcf][ingest]") {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  std::string dataset_uri = "test_dataset_resume";
  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);

  CreationParams create_args;
  create_args.uri = dataset_uri;
  create_args.tile_capacity = 10000;
  TileDBVCFDataset::create(create_args);

  // Ingest the sample
  {
    Writer writer;
    IngestionParams params;
    params.uri = dataset_uri;
    params.sample_uris = {input_dir + "/v2-DjrIAzkP-downsampled.vcf.gz"};
    params.resume_sample_partial_ingestion = true;
    params.contig_fragment_merging = true;
    params.thread_task_size = 500000000;
    params.verbose = true;
    writer.set_all_params(params);
    writer.ingest_samples();
  }

  // Check that there were 38 fragments created
  // Then remove the last fragment
  {
    tiledb::FragmentInfo fragmentInfo(ctx, dataset_uri + "/data");
    fragmentInfo.load();

    REQUIRE(fragmentInfo.fragment_num() == 38);

    // Get the last fragment
    std::string uri = fragmentInfo.fragment_uri(37);
    vfs.remove_dir(uri);
    uri = std::regex_replace(uri, std::regex("__fragments"), "__commits");
    vfs.remove_file(uri + ".wrt");
  }

  // Ingest the sample again, this should add only the missing data
  {
    Writer writer;
    IngestionParams params;
    params.uri = dataset_uri;
    params.sample_uris = {input_dir + "/v2-DjrIAzkP-downsampled.vcf.gz"};
    params.resume_sample_partial_ingestion = true;
    params.contig_fragment_merging = true;
    params.verbose = true;
    params.thread_task_size = 500000000;
    writer.set_all_params(params);
    writer.ingest_samples();
  }

  // Check that there are only 38 fragments created
  // Then remove the middle fragments (17-19)
  {
    tiledb::FragmentInfo fragmentInfo(ctx, dataset_uri + "/data");
    fragmentInfo.load();

    REQUIRE(fragmentInfo.fragment_num() == 38);

    // Remove fragment 17
    std::string uri = fragmentInfo.fragment_uri(17);
    vfs.remove_dir(uri);
    uri = std::regex_replace(uri, std::regex("__fragments"), "__commits");
    vfs.remove_file(uri + ".wrt");

    // Remove fragment 18
    uri = fragmentInfo.fragment_uri(18);
    vfs.remove_dir(uri);
    uri = std::regex_replace(uri, std::regex("__fragments"), "__commits");
    vfs.remove_file(uri + ".wrt");

    // Remove fragment 19
    uri = fragmentInfo.fragment_uri(19);
    vfs.remove_dir(uri);
    uri = std::regex_replace(uri, std::regex("__fragments"), "__commits");
    vfs.remove_file(uri + ".wrt");
  }

  // Ingest the sample again, this should add only the missing data
  {
    Writer writer;
    IngestionParams params;
    params.uri = dataset_uri;
    params.sample_uris = {input_dir + "/v2-DjrIAzkP-downsampled.vcf.gz"};
    params.resume_sample_partial_ingestion = true;
    params.contig_fragment_merging = true;
    writer.set_all_params(params);
    writer.ingest_samples();
  }

  // Check that there are only 38 fragments created
  {
    tiledb::FragmentInfo fragmentInfo(ctx, dataset_uri + "/data");
    fragmentInfo.load();

    REQUIRE(fragmentInfo.fragment_num() == 38);
  }

  // Ingest the sample one last time, this should result in no fragment changes
  {
    Writer writer;
    IngestionParams params;
    params.uri = dataset_uri;
    params.sample_uris = {input_dir + "/v2-DjrIAzkP-downsampled.vcf.gz"};
    params.resume_sample_partial_ingestion = true;
    params.contig_fragment_merging = true;
    writer.set_all_params(params);
    writer.ingest_samples();
  }

  // Check that there are only 38 fragments created
  {
    tiledb::FragmentInfo fragmentInfo(ctx, dataset_uri + "/data");
    fragmentInfo.load();

    REQUIRE(fragmentInfo.fragment_num() == 38);
  }

  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);
}
