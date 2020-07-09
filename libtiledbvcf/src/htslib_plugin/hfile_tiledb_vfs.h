/**
 * @section LICENSE
 *
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

#ifndef TILEDB_VCF_HFILE_TILEDB_VFS_H
#define TILEDB_VCF_HFILE_TILEDB_VFS_H

#ifdef __cplusplus
extern "C" {
#endif

#define HFILE_TILEDB_VFS_SCHEME "vfs"

#include <tiledb/tiledb.h>
#include "hfile_internal.h"

typedef struct {
  hFILE base;
  tiledb_ctx_t* ctx;
  int8_t owns_ctx;
  tiledb_vfs_t* vfs;
  tiledb_vfs_fh_t* vfs_fh;
  uint64_t size;
  uint64_t offset;
  tiledb_vfs_mode_t mode;
  //  char *uri;
} hFILE_tiledb_vfs;

// global config used to ensure user TileDB config params are applied to htslib
extern tiledb_config_t* hfile_tiledb_vfs_config;
// global context used to allow a single global context
extern tiledb_ctx_t* hfile_tiledb_vfs_ctx;

/**
 * Open a URI for htslib
 * @param uri to open
 * @param modestr mode char ('a', 'r', 'w')
 * @return hFILE pointer or NULL if error
 */
hFILE* hopen_tiledb_vfs(const char* uri, const char* modestr);

/**
 * Read specified bytes into buffer from opened URI
 * @param fpv hFILE_tiledb_vfs struct
 * @param buffer to read into
 * @param nbytes size read
 * @return number of bytes read or -1 on error
 */
ssize_t tiledb_vfs_hfile_read(hFILE* fpv, void* buffer, size_t nbytes);

/**
 * Write specified bytes into file from buffer
 * @param fpv hFILE_tiledb_vfs struct
 * @param buffer to write from
 * @param nbytes size write
 * @return number of bytes written or -1 on error
 */
ssize_t tiledb_vfs_hfile_write(hFILE* fpv, const void* buffer, size_t nbytes);

/**
 * Move the offset pointer to specified position
 * @param fpv hFILE_tiledb_vfs struct
 * @param offset position to increment
 * @param whence where to increment from
 * @return new offset or -1 on error
 */
off_t tiledb_vfs_hfile_seek(hFILE* fpv, off_t offset, int whence);

/**
 * Flush a file
 * @param fpv hFILE_tiledb_vfs struct
 * @return 0 on success or -1 on errror
 */
int tiledb_vfs_hfile_flush(hFILE* fpv);

/**
 * Close an opened file
 * @param fpv hFILE_tiledb_vfs struct
 * @return 0 on success or -1 on error
 */
int tiledb_vfs_hfile_close(hFILE* fpv);

/**
 * Init function for plugin
 *
 * Normally htslib will load plugins by dlopening libraries in the specified
 * plugin directory. This function is normally called after the dlopen.
 *
 * Since we are compiling the plugin as part of libtiledbvcf, we use a helper
 * function, to call this function. See utils::init_htslib()
 *
 * @param self
 * @return 0 on success or -1 on error
 */
int hfile_plugin_init(struct hFILE_plugin* self);

// htslib handler for plugin struct
extern const struct hFILE_scheme_handler tiledb_vfs_handler;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_VCF_HFILE_TILEDB_VFS_H