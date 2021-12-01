/**
 * @file   unit-c-api-writer.cc
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
 */

#include "c_api/tiledbvcf.h"
#include "catch.hpp"
#include "dataset/tiledbvcfdataset.h"

#include <cstring>
#include <iostream>

static const std::string INPUT_DIR =
    TILEDB_VCF_TEST_INPUT_DIR + std::string("/");

TEST_CASE("C API: Writer allocation", "[capi][writer]") {
  tiledb_vcf_writer_t* writer = nullptr;
  REQUIRE(tiledb_vcf_writer_alloc(&writer) == TILEDB_VCF_OK);
  REQUIRE(writer != nullptr);
  tiledb_vcf_writer_free(&writer);
  REQUIRE(writer == nullptr);
}

TEST_CASE("C API: Writer create default", "[capi][writer]") {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  std::string dataset_uri = "test_dataset";
  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);

  tiledb_vcf_writer_t* writer = nullptr;
  REQUIRE(tiledb_vcf_writer_alloc(&writer) == TILEDB_VCF_OK);
  REQUIRE(tiledb_vcf_writer_init(writer, dataset_uri.c_str()) == TILEDB_VCF_OK);

  REQUIRE(tiledb_vcf_writer_create_dataset(writer) == TILEDB_VCF_OK);

  tiledb::vcf::TileDBVCFDataset ds(std::make_shared<tiledb::Context>(ctx));
  REQUIRE_NOTHROW(ds.open(dataset_uri));

  tiledb_vcf_writer_free(&writer);
  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);
}

TEST_CASE("C API: Writer create md5 checksum", "[capi][writer]") {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  std::string dataset_uri = "test_dataset";
  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);

  tiledb_vcf_writer_t* writer = nullptr;
  REQUIRE(tiledb_vcf_writer_alloc(&writer) == TILEDB_VCF_OK);
  REQUIRE(tiledb_vcf_writer_init(writer, dataset_uri.c_str()) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_writer_set_checksum_type(writer, TILEDB_VCF_CHECKSUM_MD5) ==
      TILEDB_VCF_OK);

  REQUIRE(tiledb_vcf_writer_create_dataset(writer) == TILEDB_VCF_OK);

  tiledb::vcf::TileDBVCFDataset ds(std::make_shared<tiledb::Context>(ctx));
  REQUIRE_NOTHROW(ds.open(dataset_uri));

  tiledb_vcf_writer_free(&writer);
  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);
}

TEST_CASE("C API: Writer create no checksum", "[capi][writer]") {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  std::string dataset_uri = "test_dataset";
  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);

  tiledb_vcf_writer_t* writer = nullptr;
  REQUIRE(tiledb_vcf_writer_alloc(&writer) == TILEDB_VCF_OK);
  REQUIRE(tiledb_vcf_writer_init(writer, dataset_uri.c_str()) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_writer_set_checksum_type(writer, TILEDB_VCF_CHECKSUM_NONE) ==
      TILEDB_VCF_OK);

  REQUIRE(tiledb_vcf_writer_create_dataset(writer) == TILEDB_VCF_OK);

  tiledb::vcf::TileDBVCFDataset ds(std::make_shared<tiledb::Context>(ctx));
  REQUIRE_NOTHROW(ds.open(dataset_uri));

  tiledb_vcf_writer_free(&writer);
  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);
}

TEST_CASE("C API: Writer store", "[capi][writer]") {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  std::string dataset_uri = "test_dataset";
  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);

  tiledb_vcf_writer_t* writer = nullptr;
  REQUIRE(tiledb_vcf_writer_alloc(&writer) == TILEDB_VCF_OK);
  REQUIRE(tiledb_vcf_writer_init(writer, dataset_uri.c_str()) == TILEDB_VCF_OK);
  REQUIRE(tiledb_vcf_writer_create_dataset(writer) == TILEDB_VCF_OK);

  std::string samples =
      INPUT_DIR + "small.bcf" + "," + INPUT_DIR + "small2.bcf";
  REQUIRE(
      tiledb_vcf_writer_set_samples(writer, samples.c_str()) == TILEDB_VCF_OK);

  REQUIRE(tiledb_vcf_writer_store(writer) == TILEDB_VCF_OK);

  tiledb::vcf::TileDBVCFDataset ds(std::make_shared<tiledb::Context>(ctx));
  REQUIRE_NOTHROW(ds.open(dataset_uri));
  if (ds.metadata().version == tiledb::vcf::TileDBVCFDataset::Version::V4)
    ds.load_sample_names_v4();
  REQUIRE_THAT(
      ds.metadata().sample_names_,
      Catch::Matchers::UnorderedEquals(
          std::vector<std::string>{"HG01762", "HG00280"}));

  tiledb_vcf_writer_free(&writer);
  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);
}

TEST_CASE("C API: Writer with extra attributes", "[capi][writer]") {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  std::string dataset_uri = "test_dataset";
  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);

  tiledb_vcf_writer_t* writer = nullptr;
  REQUIRE(tiledb_vcf_writer_alloc(&writer) == TILEDB_VCF_OK);
  REQUIRE(tiledb_vcf_writer_init(writer, dataset_uri.c_str()) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_writer_set_extra_attributes(writer, "info_GT,fmt_MIN_DP") ==
      TILEDB_VCF_OK);
  REQUIRE(tiledb_vcf_writer_create_dataset(writer) == TILEDB_VCF_OK);

  std::string samples =
      INPUT_DIR + "small.bcf" + "," + INPUT_DIR + "small2.bcf";
  REQUIRE(
      tiledb_vcf_writer_set_samples(writer, samples.c_str()) == TILEDB_VCF_OK);
  REQUIRE(tiledb_vcf_writer_store(writer) == TILEDB_VCF_OK);

  tiledb::vcf::TileDBVCFDataset ds(std::make_shared<tiledb::Context>(ctx));
  REQUIRE_NOTHROW(ds.open(dataset_uri));
  REQUIRE(
      ds.metadata().extra_attributes ==
      std::vector<std::string>{"info_GT", "fmt_MIN_DP"});

  tiledb_vcf_writer_free(&writer);
  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);
}

TEST_CASE("C API: Writer with vcf attributes", "[capi][writer]") {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  std::string dataset_uri = "test_dataset";
  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);

  std::string vcf_uri = INPUT_DIR + "small.vcf";

  tiledb_vcf_writer_t* writer = nullptr;
  REQUIRE(tiledb_vcf_writer_alloc(&writer) == TILEDB_VCF_OK);
  REQUIRE(tiledb_vcf_writer_init(writer, dataset_uri.c_str()) == TILEDB_VCF_OK);
  REQUIRE(
      tiledb_vcf_writer_set_vcf_attributes(writer, vcf_uri.c_str()) ==
      TILEDB_VCF_OK);
  REQUIRE(tiledb_vcf_writer_create_dataset(writer) == TILEDB_VCF_OK);

  std::string samples =
      INPUT_DIR + "small.bcf" + "," + INPUT_DIR + "small2.bcf";
  REQUIRE(
      tiledb_vcf_writer_set_samples(writer, samples.c_str()) == TILEDB_VCF_OK);
  REQUIRE(tiledb_vcf_writer_store(writer) == TILEDB_VCF_OK);

  tiledb::vcf::TileDBVCFDataset ds(std::make_shared<tiledb::Context>(ctx));
  REQUIRE_NOTHROW(ds.open(dataset_uri));
  REQUIRE(
      ds.metadata().extra_attributes == std::vector<std::string>{
                                            "fmt_AD",
                                            "fmt_DP",
                                            "fmt_GQ",
                                            "fmt_GT",
                                            "fmt_MIN_DP",
                                            "fmt_PL",
                                            "fmt_SB",
                                            "info_BaseQRankSum",
                                            "info_ClippingRankSum",
                                            "info_DP",
                                            "info_DS",
                                            "info_END",
                                            "info_HaplotypeScore",
                                            "info_InbreedingCoeff",
                                            "info_MLEAC",
                                            "info_MLEAF",
                                            "info_MQ",
                                            "info_MQ0",
                                            "info_MQRankSum",
                                            "info_ReadPosRankSum"});

  tiledb_vcf_writer_free(&writer);
  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);
}

TEST_CASE(
    "C API: Writer store with overlapping records",
    "[capi][writer][overlapping]") {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  std::string dataset_uri = "test_dataset";
  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);

  tiledb_vcf_writer_t* writer = nullptr;
  REQUIRE(tiledb_vcf_writer_alloc(&writer) == TILEDB_VCF_OK);
  REQUIRE(tiledb_vcf_writer_init(writer, dataset_uri.c_str()) == TILEDB_VCF_OK);
  REQUIRE(tiledb_vcf_writer_create_dataset(writer) == TILEDB_VCF_OK);

  std::string samples = INPUT_DIR + "overlapping.bcf";
  REQUIRE(
      tiledb_vcf_writer_set_samples(writer, samples.c_str()) == TILEDB_VCF_OK);

  REQUIRE(tiledb_vcf_writer_store(writer) == TILEDB_VCF_OK);

  tiledb::vcf::TileDBVCFDataset ds(std::make_shared<tiledb::Context>(ctx));
  REQUIRE_NOTHROW(ds.open(dataset_uri));
  if (ds.metadata().version == tiledb::vcf::TileDBVCFDataset::Version::V4)
    ds.load_sample_names_v4();
  REQUIRE(ds.metadata().sample_names_ == std::vector<std::string>{"HG00096"});

  tiledb_vcf_writer_free(&writer);
  if (vfs.is_dir(dataset_uri))
    vfs.remove_dir(dataset_uri);
}