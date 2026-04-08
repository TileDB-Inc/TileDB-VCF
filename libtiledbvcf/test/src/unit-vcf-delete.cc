/**
 * @file   unit-vcf-delete.cc
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
 * Tests for VCF sample delete.
 */

#include "catch.hpp"

#include "dataset/tiledbvcfdataset.h"
#include "read/reader.h"
#include "stats/allele_count.h"
#include "stats/variant_stats.h"
#include "utils/logger_public.h"
#include "write/writer.h"

#include <algorithm>
#include <cstring>
#include <fstream>
#include <iostream>

using namespace tiledb::vcf;

static const std::string input_dir = TILEDB_VCF_TEST_INPUT_DIR;

template <typename T>
auto sum(
    const Context& ctx, const std::string& uri, const std::string& column) {
  // Open the array and create a query
  auto array = std::shared_ptr<Array>(new Array(ctx, uri, TILEDB_READ));
  Query query(ctx, *array);

  // Add aggregate for sum on the default channel.
  QueryChannel default_channel = QueryExperimental::get_default_channel(query);
  ChannelOperation operation =
      QueryExperimental::create_unary_aggregate<SumOperator>(query, column);
  default_channel.apply_aggregate("Sum", operation);

  // Set layout and buffer.
  std::vector<T> sum(1);
  query.set_layout(TILEDB_UNORDERED).set_data_buffer("Sum", sum);

  // Submit the query and close the array.
  query.submit();
  array->close();

  return sum[0];
}

TEST_CASE("TileDB-VCF: Test delete stats", "[tiledbvcf][delete]") {
  // LOG_CONFIG("debug");

  auto enable_ac = GENERATE(false, true);
  auto enable_vs = GENERATE(false, true);
  auto enable_ss = GENERATE(false, true);

  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  std::string dataset_uri = "test_dataset";
  std::string sample_name = "stats-test";

  if (vfs.is_dir(dataset_uri)) {
    vfs.remove_dir(dataset_uri);
  }

  // Create and enable stats arrays
  {
    CreationParams create_args;
    create_args.uri = dataset_uri;
    create_args.tile_capacity = 10000;
    create_args.allow_duplicates = false;
    create_args.enable_allele_count = enable_ac;
    create_args.enable_variant_stats = enable_vs;
    create_args.enable_sample_stats = enable_ss;
    TileDBVCFDataset::create(create_args);
  }

  // Ingest
  {
    Writer writer;
    IngestionParams params;
    params.uri = dataset_uri;
    params.sample_uris = {input_dir + "/stats-test.vcf.gz"};
    writer.set_all_params(params);
    writer.ingest_samples();
  }

  // Check sample is present
  {
    TileDBVCFDataset dataset(std::make_shared<Context>(ctx));
    dataset.open(dataset_uri);
    REQUIRE(!dataset.sample_names().empty());
  }

  // Check allele counts
  if (enable_ac) {
    std::string array_uri = dataset_uri + "/allele_count";
    REQUIRE(sum<int64_t>(ctx, array_uri, "count") == 246);
  }

  // Check variant stats
  if (enable_vs) {
    std::string array_uri = dataset_uri + "/variant_stats";
    REQUIRE(sum<int64_t>(ctx, array_uri, "ac") == 492);
    REQUIRE(sum<int64_t>(ctx, array_uri, "n_hom") == 163);
  }

  // Check sample stats
  if (enable_ss) {
    std::string array_uri = dataset_uri + "/sample_stats";
    REQUIRE(sum<uint64_t>(ctx, array_uri, "n_records") == 246);
  }

  // Delete
  {
    Config cfg;
    TileDBVCFDataset dataset(cfg);
    dataset.delete_samples(dataset_uri, {sample_name});
  }

  // Check sample is deleted
  {
    auto ctx = std::make_shared<Context>();
    TileDBVCFDataset dataset(ctx);
    dataset.open(dataset_uri);
    REQUIRE(dataset.sample_names().empty());
  }

  // Check allele counts
  if (enable_ac) {
    std::string array_uri = dataset_uri + "/allele_count";
    REQUIRE(sum<int64_t>(ctx, array_uri, "count") == 0);
  }

  // Check variant stats
  if (enable_vs) {
    std::string array_uri = dataset_uri + "/variant_stats";
    REQUIRE(sum<int64_t>(ctx, array_uri, "ac") == 0);
    REQUIRE(sum<int64_t>(ctx, array_uri, "n_hom") == 0);
  }

  // Check sample stats
  if (enable_ss) {
    std::string array_uri = dataset_uri + "/sample_stats";
    REQUIRE(sum<uint64_t>(ctx, array_uri, "n_records") == 0);
  }

  if (vfs.is_dir(dataset_uri)) {
    vfs.remove_dir(dataset_uri);
  }
}

TEST_CASE("TileDB-VCF: Test multi sample delete", "[tiledbvcf][delete]") {
  // LOG_CONFIG("debug");

  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  std::string dataset_uri = "test_dataset";
  std::vector<std::string> sample_names;
  std::vector<std::string> sample_uris;
  std::string last_sample;

  if (vfs.is_dir(dataset_uri)) {
    vfs.remove_dir(dataset_uri);
  }

  // Create dataset
  {
    CreationParams create_args;
    create_args.uri = dataset_uri;
    create_args.tile_capacity = 10000;
    create_args.allow_duplicates = false;
    TileDBVCFDataset::create(create_args);
  }

  // Ingest
  {
    for (unsigned i = 1; i <= 10; i++) {
      std::string name = "G" + std::to_string(i);
      std::string uri = input_dir + "/random_synthetic/" + name + ".bcf";
      sample_names.push_back(name);
      sample_uris.push_back(uri);
    }

    Writer writer;
    IngestionParams params;
    params.uri = dataset_uri;
    params.sample_uris = sample_uris;
    writer.set_all_params(params);
    writer.ingest_samples();
  }

  // Check samples are present
  {
    TileDBVCFDataset dataset(std::make_shared<Context>(ctx));
    dataset.open(dataset_uri);
    REQUIRE(dataset.sample_names().size() == 10);
  }

  // Delete all but one sample
  {
    last_sample = sample_names.back();
    sample_names.pop_back();
    Config cfg;
    TileDBVCFDataset dataset(cfg);
    dataset.delete_samples(dataset_uri, sample_names);
  }

  // Check the samples were deleted
  {
    auto ctx = std::make_shared<Context>();
    TileDBVCFDataset dataset(ctx);
    dataset.open(dataset_uri);
    REQUIRE(dataset.sample_names().size() == 1);
    std::string sample_str(dataset.sample_names().at(0).data());
    REQUIRE(sample_str == last_sample);
  }

  if (vfs.is_dir(dataset_uri)) {
    vfs.remove_dir(dataset_uri);
  }
}

TEST_CASE(
    "TileDB-VCF: Test delete with skip_aggregate_stats",
    "[tiledbvcf][delete]") {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  std::string dataset_uri = "test_dataset";
  std::string sample_name = "stats-test";

  if (vfs.is_dir(dataset_uri)) {
    vfs.remove_dir(dataset_uri);
  }

  // Create dataset with all stats arrays enabled
  {
    CreationParams create_args;
    create_args.uri = dataset_uri;
    create_args.tile_capacity = 10000;
    create_args.allow_duplicates = false;
    create_args.enable_allele_count = true;
    create_args.enable_variant_stats = true;
    create_args.enable_sample_stats = true;
    TileDBVCFDataset::create(create_args);
  }

  // Ingest
  {
    Writer writer;
    IngestionParams params;
    params.uri = dataset_uri;
    params.sample_uris = {input_dir + "/stats-test.vcf.gz"};
    writer.set_all_params(params);
    writer.ingest_samples();
  }

  // Verify pre-delete stat values
  REQUIRE(sum<int64_t>(ctx, dataset_uri + "/allele_count", "count") == 246);
  REQUIRE(sum<int64_t>(ctx, dataset_uri + "/variant_stats", "ac") == 492);
  REQUIRE(sum<int64_t>(ctx, dataset_uri + "/variant_stats", "n_hom") == 163);
  REQUIRE(
      sum<uint64_t>(ctx, dataset_uri + "/sample_stats", "n_records") == 246);

  // Delete with skip_aggregate_stats=true
  {
    Config cfg;
    TileDBVCFDataset dataset(cfg);
    dataset.delete_samples(dataset_uri, {sample_name}, {}, true);
  }

  // Verify sample is deleted
  {
    auto ctx2 = std::make_shared<Context>();
    TileDBVCFDataset dataset(ctx2);
    dataset.open(dataset_uri);
    REQUIRE(dataset.sample_names().empty());
  }

  // allele_count and variant_stats should be unchanged (stats were skipped)
  REQUIRE(sum<int64_t>(ctx, dataset_uri + "/allele_count", "count") == 246);
  REQUIRE(sum<int64_t>(ctx, dataset_uri + "/variant_stats", "ac") == 492);
  REQUIRE(sum<int64_t>(ctx, dataset_uri + "/variant_stats", "n_hom") == 163);

  // sample_stats should be zeroed (always runs regardless of flag)
  REQUIRE(sum<uint64_t>(ctx, dataset_uri + "/sample_stats", "n_records") == 0);

  // Verify skipped_delete_samples metadata on allele_count
  {
    auto ctx_ptr = std::make_shared<tiledb::Context>();
    tiledb::Group group(*ctx_ptr, dataset_uri, TILEDB_READ);
    auto skipped = AlleleCount::get_skipped_delete_samples(ctx_ptr, group);
    REQUIRE(skipped.size() == 1);
    REQUIRE(skipped[0] == sample_name);
  }

  // Verify skipped_delete_samples metadata on variant_stats
  {
    auto ctx_ptr = std::make_shared<tiledb::Context>();
    tiledb::Group group(*ctx_ptr, dataset_uri, TILEDB_READ);
    auto skipped = VariantStats::get_skipped_delete_samples(ctx_ptr, group);
    REQUIRE(skipped.size() == 1);
    REQUIRE(skipped[0] == sample_name);
  }

  if (vfs.is_dir(dataset_uri)) {
    vfs.remove_dir(dataset_uri);
  }
}

TEST_CASE(
    "TileDB-VCF: Test skip_aggregate_stats metadata accumulates",
    "[tiledbvcf][delete]") {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  std::string dataset_uri = "test_dataset";

  if (vfs.is_dir(dataset_uri)) {
    vfs.remove_dir(dataset_uri);
  }

  // Create dataset with allele_count and variant_stats enabled
  {
    CreationParams create_args;
    create_args.uri = dataset_uri;
    create_args.tile_capacity = 10000;
    create_args.allow_duplicates = false;
    create_args.enable_allele_count = true;
    create_args.enable_variant_stats = true;
    TileDBVCFDataset::create(create_args);
  }

  // Ingest G1, G2, G3
  {
    Writer writer;
    IngestionParams params;
    params.uri = dataset_uri;
    for (int i = 1; i <= 3; i++) {
      params.sample_uris.push_back(
          input_dir + "/random_synthetic/G" + std::to_string(i) + ".bcf");
    }
    writer.set_all_params(params);
    writer.ingest_samples();
  }

  // Delete G1 with skip_aggregate_stats=true
  {
    Config cfg;
    TileDBVCFDataset dataset(cfg);
    dataset.delete_samples(dataset_uri, {"G1"}, {}, true);
  }

  // Verify metadata records G1 on both arrays
  {
    auto ctx_ptr = std::make_shared<tiledb::Context>();
    tiledb::Group group(*ctx_ptr, dataset_uri, TILEDB_READ);
    auto skipped = AlleleCount::get_skipped_delete_samples(ctx_ptr, group);
    REQUIRE(skipped.size() == 1);
    REQUIRE(skipped[0] == "G1");
  }
  {
    auto ctx_ptr = std::make_shared<tiledb::Context>();
    tiledb::Group group(*ctx_ptr, dataset_uri, TILEDB_READ);
    auto skipped = VariantStats::get_skipped_delete_samples(ctx_ptr, group);
    REQUIRE(skipped.size() == 1);
    REQUIRE(skipped[0] == "G1");
  }

  // Delete G2 with skip_aggregate_stats=true
  {
    Config cfg;
    TileDBVCFDataset dataset(cfg);
    dataset.delete_samples(dataset_uri, {"G2"}, {}, true);
  }

  // Verify metadata records both G1 and G2 on both arrays
  {
    auto ctx_ptr = std::make_shared<tiledb::Context>();
    tiledb::Group group(*ctx_ptr, dataset_uri, TILEDB_READ);
    auto skipped = AlleleCount::get_skipped_delete_samples(ctx_ptr, group);
    REQUIRE(skipped.size() == 2);
    REQUIRE(std::find(skipped.begin(), skipped.end(), "G1") != skipped.end());
    REQUIRE(std::find(skipped.begin(), skipped.end(), "G2") != skipped.end());
  }
  {
    auto ctx_ptr = std::make_shared<tiledb::Context>();
    tiledb::Group group(*ctx_ptr, dataset_uri, TILEDB_READ);
    auto skipped = VariantStats::get_skipped_delete_samples(ctx_ptr, group);
    REQUIRE(skipped.size() == 2);
    REQUIRE(std::find(skipped.begin(), skipped.end(), "G1") != skipped.end());
    REQUIRE(std::find(skipped.begin(), skipped.end(), "G2") != skipped.end());
  }

  // Verify G3 still present, G1 and G2 are gone
  {
    auto ctx2 = std::make_shared<Context>();
    TileDBVCFDataset dataset(ctx2);
    dataset.open(dataset_uri);
    REQUIRE(dataset.sample_names().size() == 1);
    std::string remaining(dataset.sample_names().at(0).data());
    REQUIRE(remaining == "G3");
  }

  if (vfs.is_dir(dataset_uri)) {
    vfs.remove_dir(dataset_uri);
  }
}
