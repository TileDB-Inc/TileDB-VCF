/**
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020 TileDB, Inc.
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

#ifndef __UNIT_HELPERS
#define __UNIT_HELPERS

#include <numeric>
#include <sstream>
#include <utility>
#include <vector>
#include "catch.hpp"

template <typename T, typename Compare>
std::vector<std::size_t> sort_permutation(
    const std::vector<T>& vec, const Compare& compare) {
  std::vector<std::size_t> p(vec.size());
  std::iota(p.begin(), p.end(), 0);
  std::sort(p.begin(), p.end(), [&](std::size_t i, std::size_t j) {
    return compare(vec[i], vec[j]);
  });
  return p;
}

template <typename T>
std::vector<T> apply_permutation(
    const std::vector<T>& vec, const std::vector<std::size_t>& p) {
  std::vector<T> sorted_vec(vec.size());
  std::transform(p.begin(), p.end(), sorted_vec.begin(), [&](std::size_t i) {
    return vec[i];
  });
  return sorted_vec;
}

struct record {
  std::string sample;
  uint32_t start_pos = 0;
  uint32_t end_pos = 0;
  uint32_t query_bed_start = 0;
  uint32_t query_bed_end = 0;
  uint32_t query_bed_line = 0;
  std::string contig;
  std::vector<std::string> alleles;
  std::vector<std::string> filters;
  bool filter_valid;
  std::string info;
  std::string fmt;
  std::vector<int32_t> fmt_GT;
  int fmt_DP = 0;
  std::vector<int> fmt_PL;

  explicit record(
      std::string sample = "",
      uint32_t start_pos = 0,
      uint32_t end_pos = 0,
      uint32_t query_bed_start = 0,
      uint32_t query_bed_end = 0,
      std::string contig = "",
      std::vector<std::string> alleles = {},
      std::vector<std::string> filters = {},
      bool filter_valid = false,
      std::string info = "",
      std::string fmt = "",
      std::vector<int32_t> fmt_GT = {},
      int fmt_DP = 0,
      std::vector<int> fmt_PL = {},
      uint32_t query_bed_line = 0)
      : sample(std::move(sample))
      , start_pos(start_pos)
      , end_pos(end_pos)
      , query_bed_start(query_bed_start)
      , query_bed_end(query_bed_end)
      , query_bed_line(query_bed_line)
      , contig(std::move(contig))
      , alleles(std::move(alleles))
      , filters(std::move(filters))
      , filter_valid(filter_valid)
      , info(std::move(info))
      , fmt(std::move(fmt))
      , fmt_GT(std::move(fmt_GT))
      , fmt_DP(fmt_DP)
      , fmt_PL(std::move(fmt_PL)) {
  }

  bool operator==(const record& b) const {
    if (sample != b.sample)
      return false;

    if (start_pos != b.start_pos)
      return false;

    if (end_pos != b.end_pos)
      return false;

    if (query_bed_start != b.query_bed_start)
      return false;

    if (query_bed_end != b.query_bed_end)
      return false;

    if (query_bed_line != b.query_bed_line)
      return false;

    if (contig != b.contig)
      return false;

    if (info != b.info)
      return false;

    if (fmt != b.fmt)
      return false;

    if (fmt_DP != b.fmt_DP)
      return false;

    if (filter_valid != b.filter_valid)
      return false;

    if (alleles.size() != b.alleles.size())
      return false;

    if (filters.size() != b.filters.size())
      return false;

    if (fmt_GT.size() != b.fmt_GT.size())
      return false;

    if (fmt_PL.size() != b.fmt_PL.size())
      return false;

    for (size_t i = 0; i < alleles.size(); i++) {
      if (alleles[i] != b.alleles[i])
        return false;
    }

    for (size_t i = 0; i < filters.size(); i++) {
      if (filters[i] != b.filters[i])
        return false;
    }

    for (size_t i = 0; i < fmt_GT.size(); i++) {
      if (fmt_GT[i] != b.fmt_GT[i])
        return false;
    }

    for (size_t i = 0; i < fmt_PL.size(); i++) {
      if (fmt_PL[i] != b.fmt_PL[i])
        return false;
    }

    return true;
  }

  bool operator!=(const record& b) const {
    return !(*this == b);
  }

  friend std::ostream& operator<<(std::ostream& out, const record& r);

  std::string describe() const {
    std::stringstream out;
    out << this->sample << "-" << this->contig << "-" << this->start_pos << "-"
        << this->end_pos << "-" << this->query_bed_start << "-"
        << this->query_bed_end << "-" << this->query_bed_line << "-";
    for (size_t i = 0; i < this->alleles.size(); i++) {
      out << this->alleles[i];
      if (i < this->alleles.size() - 1)
        out << ",";
    }
    out << "-";
    for (size_t i = 0; i < this->filters.size(); i++) {
      out << this->filters[i];
      if (i < this->filters.size() - 1)
        out << ",";
    }
    return out.str();
  }
};

std::ostream& operator<<(std::ostream& out, const record& r) {
  out << r.describe();
  return out;
}

template <typename T>
std::vector<T> var_value(
    std::vector<T> data, std::vector<int32_t> offsets, uint64_t index) {
  // Arrow compatible buffers have one final offset
  uint64_t start = offsets[index];
  uint64_t end = offsets[index + 1];
  return std::vector<T>(data.begin() + start, data.begin() + end);
}

template <typename T>
std::vector<std::vector<T>> var_list_value(
    std::vector<T> data,
    std::vector<int32_t> offsets,
    std::vector<int32_t> list_offsets,
    uint64_t index) {
  // Arrow compatible buffers have one final offset
  uint64_t start = offsets[index];
  uint64_t end = offsets[index + 1];
  return std::vector<T>(data.begin() + start, data.begin() + end);
}

std::vector<std::string> var_list_value(
    std::vector<char> data,
    std::vector<int32_t> offsets,
    std::vector<int32_t> list_offsets,
    uint64_t index) {
  std::vector<std::string> ret;
  // Arrow compatible buffers have one final offset
  uint32_t list_offset_start = list_offsets[index];
  uint32_t list_offset_end = list_offsets[index + 1];

  for (uint32_t i = list_offset_start; i < list_offset_end; i++) {
    uint32_t start = offsets[i];
    uint32_t end = offsets[i + 1];
    ret.emplace_back(data.begin() + start, data.begin() + end);
  }

  return ret;
}

#endif