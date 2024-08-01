/**
 * @file   normalize.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * This file defines class Logger, declared in logger.h, and the public logging
 * functions, declared in logger_public.h.
 */

#include <utils/normalize.h>

static inline std::string& get_allele(std::pair<std::string, bool>& allele) {
  return allele.first;
}

static inline std::string& get_allele(std::string& allele) {
  return allele;
}

static inline std::string& ref_allele(
    std::vector<std::pair<std::string, bool>>& alleles) {
  return alleles.back().first;
}

static inline std::string& ref_allele(std::vector<std::string>& alleles) {
  return alleles[0];
}

template <typename T>
static inline bool is_ref(std::string& allele, std::vector<T>& alleles) {
  return &allele == &ref_allele(alleles);
}

namespace tiledb::vcf {
template <typename T>
void normalize(std::vector<T>& alleles) {
  if (alleles.size() == 0) {
    return;
  }
  for (T& allele_wrapper : alleles) {
    size_t min_length = UINT64_MAX;
    min_length = std::min(min_length, get_allele(allele_wrapper).length());
    size_t i = 1;
    std::string& allele = get_allele(allele_wrapper);
    std::string& ref = ref_allele(alleles);
    if (!is_ref(allele, alleles) &&
        allele[allele.length() - i] == ref[ref.length() - i]) {
      i++;
    }
    i--;
    allele.resize(allele.size() - i);
  }
}

template void normalize(std::vector<std::string>&);
template void normalize(std::vector<std::pair<std::string, bool>>&);
}  // namespace tiledb::vcf
