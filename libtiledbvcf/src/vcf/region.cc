/**
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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

#include <htslib/hts.h>
#include <algorithm>
#include <future>
#include <regex>

#include "htslib_plugin/hfile_tiledb_vfs.h"
#include "utils/logger_public.h"
#include "utils/utils.h"
#include "vcf/region.h"
#include "vcf_utils.h"

namespace tiledb {
namespace vcf {

Region::Region()
    : min(0)
    , max(0)
    , seq_offset(std::numeric_limits<uint32_t>::max() - 1)
    , line(0) {
  region_str = to_str();
}

Region::Region(const std::string& seq, uint32_t min, uint32_t max, int32_t line)
    : seq_name(seq)
    , min(min)
    , max(max)
    , seq_offset(std::numeric_limits<uint32_t>::max() - 1)
    , line(line) {
  region_str = to_str();
}

Region::Region(const std::string& str) {
  auto r = parse_region(str);
  seq_name = r.seq_name;
  min = r.min;
  max = r.max;
  seq_offset = std::numeric_limits<uint32_t>::max() - 1;
  line = r.line;

  // if line was not provided, convert region from 1-indexed, inclusive to
  // 0-indexed, inclusive
  if (line == -1) {
    assert(min > 0 && max > 0);
    min -= 1;
    max -= 1;
    region_str = to_str();
  }
}

std::string Region::to_str() const {
  std::string result = seq_name + ':' + std::to_string(min) + '-' +
                       std::to_string(max) + ":" + std::to_string(line);
  return result;
}

Region Region::parse_region(const std::string& region_str) {
  if (region_str.empty())
    return {"", 0, 0};

  Region result;
  std::vector<std::string> region_split = utils::split(region_str, ':');

  if (region_split.size() == 1) {
    region_split.push_back("0-0");
    region_split.push_back("-1");  // add missing bed line number
  } else if (region_split.size() == 2) {
    region_split.push_back("-1");  // add missing bed line number
  } else if (region_split.size() != 3) {
    throw std::invalid_argument(
        "Error parsing region string '" + region_str + "'; invalid format.");
  }

  result.seq_name = region_split[0];

  // Strip commas
  region_split[1].erase(
      std::remove(region_split[1].begin(), region_split[1].end(), ','),
      region_split[1].end());

  // Range
  auto range_split = utils::split(region_split[1], '-');
  if (range_split.size() != 2)
    throw std::invalid_argument(
        "Error parsing region string; invalid region format, should be "
        "CHR:XX,XXX-YY,YYY\n\t" +
        region_str);

  try {
    result.min = std::stoul(range_split[0]);
    result.max = std::stoul(range_split[1]);
  } catch (std::exception& e) {
    throw std::invalid_argument(
        "Error parsing region string '" + region_str + "'");
  }

  if (result.min > result.max)
    throw std::invalid_argument(
        "Invalid region " + region_str + ", min > max (" +
        std::to_string(result.min) + " > " + std::to_string(result.max) +
        ") with contig " + result.seq_name + ".");

  // bed line number
  try {
    result.line = std::stol(region_split[2]);
  } catch (std::exception& e) {
    throw std::invalid_argument(
        "Error parsing region string '" + region_str + "'");
  }

  result.region_str = region_str;
  return result;
}

void Region::parse_bed_file_htslib(
    const std::string& bed_file_uri, std::list<Region>* result) {
  // htslib requires bed file names end in ".bed", ".bed.gz", or ".bed.bgz"
  // (case-sensitive)
  if (!std::regex_match(bed_file_uri, std::regex(".*\\.bed(\\.gz|\\.bgz)?$"))) {
    LOG_ERROR(
        "BED file uri must end in '.bed', '.bed.gz', or '.bed.bgz': {}",
        bed_file_uri);
    throw std::invalid_argument(
        "BED file uri must end in '.bed', '.bed.gz', or '.bed.bgz': " +
        bed_file_uri);
  }

  // htslib is very chatty as it will try (and fail) to find all possible index
  // files resulting in a lot of output In the htslib vfs plugin we set the
  // errors on opening a non-existent file to warning to avoid this
  const auto old_log_level = hts_get_log_level();
  hts_set_log_level(HTS_LOG_ERROR);
  std::string path =
      std::string(HFILE_TILEDB_VFS_SCHEME) + "://" + bed_file_uri;

  // If the user passes a http or ftp let htslib deal with it directly
  if (bed_file_uri.substr(0, 6) == "ftp://" ||
      bed_file_uri.substr(0, 7) == "http://" ||
      bed_file_uri.substr(0, 8) == "https://")
    path = bed_file_uri;
  // 0, 1, -2 come from bcf_sr_set_regions, these are suppose to be ignored when
  // reading from a file though
  SafeRegionFh regions_file(
      bcf_sr_regions_init(path.c_str(), 1, 0, 1, -2), bcf_sr_regions_destroy);

  if (regions_file == nullptr)
    throw std::runtime_error("Error parsing BED file: " + bed_file_uri);

  // If there is an index file process in parallel
  if (regions_file->tbx) {
    std::vector<std::future<std::list<Region>>> futures;
    // Loop over the sequences in the index file and launch a task for each one
    std::vector<SafeRegionFh> open_files;
    for (int i = 0; i < regions_file->nseqs; i++) {
      open_files.emplace_back(SafeRegionFh(
          bcf_sr_regions_init(path.c_str(), 1, 0, 1, -2),
          bcf_sr_regions_destroy));
    }
    for (int i = 0; i < regions_file->nseqs; i++) {
      const char* chr = regions_file->seq_names[i];
      auto open_file = std::move(open_files[i]);
      TRY_CATCH_THROW(futures.push_back(std::async(
          std::launch::async,
          [chr](SafeRegionFh file) {
            return parse_bed_file_htslib_section(std::move(file), chr);
          },
          std::move(open_file))));
    }

    // Add results to final linked list, this will block on the future
    // Order is preserved by always adding to the end of the list
    for (auto& res : futures) {
      if (!res.valid())
        throw std::runtime_error("Parsing of BED file failed");
      // Set bedfile region parsing timeout to 5 minutes
      // TODO: make this a config option
      std::future_status status;
      TRY_CATCH_THROW(status = res.wait_for(std::chrono::seconds(300)));
      if (status != std::future_status::ready)
        throw std::runtime_error("Parsing of BED file timed out");
      std::list<Region> res_list;
      TRY_CATCH_THROW(res_list = res.get());
      // Adjust the line number based on where the contig started in the bed
      // file Since we loop over them in order we can just use the current
      // result size
      for (auto& region : res_list) {
        region.line += result->size();
        region.region_str = region.to_str();
      }

      result->splice(result->end(), res_list);
    }
    LOG_TRACE(fmt::format(
        std::locale(""),
        "BED file: parsed {:L} regions total",
        result->size()));
  } else {
    // If there is no index file just loop over the entire file
    uint32_t line = 0;
    while (!bcf_sr_regions_next(regions_file.get())) {
      // regions from htslib are 0-indexed, inclusive (no mod required)
      result->emplace_back(
          regions_file->seq_names[regions_file->iseq],
          regions_file->start,
          regions_file->end,
          line);
      ++line;
    }
    LOG_TRACE(fmt::format(
        std::locale(""),
        "Unindexed BED file: parsed {:L} regions total",
        line));
  }

  // reset htslib log level
  hts_set_log_level(old_log_level);
}

// std::mutex shared_mtx_parse;
std::list<Region> Region::parse_bed_file_htslib_section(
    SafeRegionFh regions_file, const char* chr) {
  std::list<Region> result;

  if (regions_file == nullptr)
    throw std::runtime_error(
        "Error parsing BED file for chromosome " + std::string(chr));

  // Seek to the chromosome region of the BED file via the index
  if (bcf_sr_regions_seek(regions_file.get(), chr) != 0)
    throw std::runtime_error(
        "Error parsing BED file seek to chromosome " + std::string(chr) +
        " failed");

  uint32_t line = 0;
  while (!bcf_sr_regions_next(regions_file.get())) {
    // regions from htslib are 0-indexed, inclusive (no mod required)
    result.emplace_back(
        regions_file->seq_names[regions_file->iseq],
        regions_file->start,
        regions_file->end,
        line);
    ++line;
  }

  LOG_TRACE(fmt::format(
      std::locale(""),
      "BED file: parsed {:L} regions in contig {}",
      line,
      chr));

  return result;
}

void Region::sort(
    const std::map<std::string, uint32_t>& contig_offsets,
    std::vector<Region>* regions) {
  std::sort(
      regions->begin(),
      regions->end(),
      [&contig_offsets](const Region& a, const Region& b) {
        auto it_a = contig_offsets.find(a.seq_name);
        if (it_a == contig_offsets.end())
          throw std::runtime_error(
              "Error sorting regions list; no contig offset found for '" +
              a.seq_name + "'.");
        auto it_b = contig_offsets.find(b.seq_name);
        if (it_b == contig_offsets.end())
          throw std::runtime_error(
              "Error sorting regions list; no contig offset found for '" +
              b.seq_name + "'.");
        const uint32_t global_min_a = it_a->second + a.min;
        const uint32_t global_min_b = it_b->second + b.min;
        return global_min_a < global_min_b;
      });
}

}  // namespace vcf
}  // namespace tiledb
