/*
 * This file (md5.h) and the corresponding md5.cc file are licensed under the
 * Unlicense license and are therefore released into the public domain.
 *
 * This is an a adaptation of the MD5 reference implementation from
 * https://github.com/Zunawe/md5-c. It is derived from the RSA Data Security,
 * Inc. MD5 Message-Digest Algorithm and modified slightly to be functionally
 * identical but condensed into control structures.
 */

#ifndef TILEDB_EXTERNAL_MD5_H
#define TILEDB_EXTERNAL_MD5_H

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>

namespace md5 {

typedef struct {
  uint64_t size;       // Size of input in bytes
  uint32_t buffer[4];  // Current accumulation of hash
  uint8_t input[64];   // Input to be used in the next step
  uint8_t digest[16];  // Result of algorithm
} MD5Context;

/*
 * Initialize an md5 context
 */
void md5_init(MD5Context* ctx);

/*
 * Add some amount of input to the context
 *
 * If the input fills out a block of 512 bits, apply the algorithm (md5_step)
 * and save the result in the buffer. Also updates the overall size.
 */
void md5_update(MD5Context* ctx, const uint8_t* input, size_t input_len);

/*
 * Pad the current input to get to 448 bytes, append the size in bits to the
 * very end, and save the result of the final iteration into digest.
 */
void md5_finalize(MD5Context* ctx);

/*
 * Step on 512 bits of input with the main MD5 algorithm.
 */
void md5_step(uint32_t* buffer, uint32_t* input);

/*
 * Functions that run the algorithm on the provided input and put the digest
 * into result. result should be able to store 16 bytes.
 */
void md5_string(const char* input, uint8_t* result);

/*
 * Functions that run the algorithm on the contents of the provided file and
 * put the digest into result. result should be able to store 16 bytes.
 */
void md5_file(FILE* file, uint8_t* result);

/*
 * Converts an md5 array into a C string.
 */
void md5_to_hex(const uint8_t* input, char* output);

/*
 * Converts an md5 array into a string.
 */
std::string md5_to_hex(const uint8_t* input);

}  // namespace md5

#endif
