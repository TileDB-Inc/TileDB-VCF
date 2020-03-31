/*
 * The MIT License
 *
 * @copyright Copyright (c) 2019 TileDB, Inc.
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

#ifdef TILEDB_VCF_READ_STATUS_ENUM
/** Query failed */
TILEDB_VCF_READ_STATUS_ENUM(FAILED) = 0,
    /** Query completed (all data has been read) */
    TILEDB_VCF_READ_STATUS_ENUM(COMPLETED) = 1,
    /** Query completed (but not all data has been read) */
    TILEDB_VCF_READ_STATUS_ENUM(INCOMPLETE) = 2,
    /** Query not initialized.  */
    TILEDB_VCF_READ_STATUS_ENUM(UNINITIALIZED) = 3,
#endif

#ifdef TILEDB_VCF_ATTR_DATATYPE_ENUM
    /** Character */
    TILEDB_VCF_ATTR_DATATYPE_ENUM(CHAR) = 0,
    /** 8-bit unsigned integer */
    TILEDB_VCF_ATTR_DATATYPE_ENUM(UINT8) = 1,
    /** Signed 32-bit integer */
    TILEDB_VCF_ATTR_DATATYPE_ENUM(INT32) = 2,
    /** 32-bit floating-point  */
    TILEDB_VCF_ATTR_DATATYPE_ENUM(FLOAT32) = 3,
#endif

#ifdef TILEDB_VCF_CHECKSUM_TYPE_ENUM
    /** No-op filter */
    TILEDB_VCF_CHECKSUM_TYPE_ENUM(CHECKSUM_NONE) = 0,
    /** MD5 checksum filter. */
    TILEDB_VCF_CHECKSUM_TYPE_ENUM(CHECKSUM_MD5) = 12,
    /** SHA256 checksum filter. */
    TILEDB_VCF_CHECKSUM_TYPE_ENUM(CHECKSUM_SHA256) = 13,
#endif