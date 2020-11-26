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

#include "hfile_tiledb_vfs.h"
#include <errno.h>
#include <htslib/hts_log.h>
#include <stdio.h>
#include <stdlib.h>

tiledb_config_t* hfile_tiledb_vfs_config = NULL;
tiledb_ctx_t* hfile_tiledb_vfs_ctx = NULL;

ssize_t tiledb_vfs_hfile_read(hFILE* fpv, void* buffer, size_t nbytes) {
  hFILE_tiledb_vfs* fp = (hFILE_tiledb_vfs*)fpv;

  // Make sure the file is in read mode
  if (fp->mode != TILEDB_VFS_READ) {
    const char* mode_str;
    tiledb_vfs_mode_to_str(fp->mode, &mode_str);
    hts_log_error("Can't read as file is opened in %s mode", mode_str);
    return 0;
  }

  // Don't read more than whats left in the file
  if (nbytes + fp->offset > fp->size)
    nbytes = fp->size - fp->offset;

  // If we're at the end return 0 indicating EOF
  if (nbytes == 0)
    return 0;

  int32_t rc = tiledb_vfs_read(fp->ctx, fp->vfs_fh, fp->offset, buffer, nbytes);
  if (rc != TILEDB_OK) {
    tiledb_error_t* error;
    tiledb_ctx_get_last_error(fp->ctx, &error);
    const char* msg;
    tiledb_error_message(error, &msg);
    hts_log_error("%s\n", msg);

    if (error != NULL)
      tiledb_error_free(&error);
    return -1;
  }
  fp->offset += nbytes;
  return nbytes;
}

ssize_t tiledb_vfs_hfile_write(hFILE* fpv, const void* buffer, size_t nbytes) {
  hFILE_tiledb_vfs* fp = (hFILE_tiledb_vfs*)fpv;
  int32_t rc = tiledb_vfs_write(fp->ctx, fp->vfs_fh, buffer, nbytes);
  if (rc != TILEDB_OK) {
    tiledb_error_t* error;
    tiledb_ctx_get_last_error(fp->ctx, &error);
    const char* msg;
    tiledb_error_message(error, &msg);
    hts_log_error("%s\n", msg);

    if (error != NULL)
      tiledb_error_free(&error);
    return -1;
  }
  return nbytes;
}

off_t tiledb_vfs_hfile_seek(hFILE* fpv, off_t offset, int whence) {
  hFILE_tiledb_vfs* fp = (hFILE_tiledb_vfs*)fpv;

  size_t absoffset = (offset >= 0) ? offset : -offset;
  size_t origin;

  switch (whence) {
    case SEEK_SET:
      origin = 0;
      break;
    case SEEK_CUR:
      origin = fp->offset;
      break;
    case SEEK_END:
      origin = fp->size;
      break;
    default:
      errno = EINVAL;
      return -1;
  }

  if ((offset < 0 && absoffset > origin) ||
      (offset >= 0 && absoffset > fp->size - origin)) {
    errno = EINVAL;
    return -1;
  }

  fp->offset = origin + offset;
  return fp->offset;
}

int tiledb_vfs_hfile_flush(hFILE* fpv) {
  hFILE_tiledb_vfs* fp = (hFILE_tiledb_vfs*)fpv;
  int32_t rc = tiledb_vfs_sync(fp->ctx, fp->vfs_fh);
  if (rc != TILEDB_OK) {
    tiledb_error_t* error;
    tiledb_ctx_get_last_error(fp->ctx, &error);
    const char* msg;
    tiledb_error_message(error, &msg);
    hts_log_error("%s\n", msg);

    if (error != NULL)
      tiledb_error_free(&error);
    return -1;
  }
  return 0;
}

int tiledb_vfs_hfile_close(hFILE* fpv) {
  hFILE_tiledb_vfs* fp = (hFILE_tiledb_vfs*)fpv;
  if (fp->vfs_fh != NULL) {
    int32_t closed = 0;
    tiledb_vfs_fh_is_closed(fp->ctx, fp->vfs_fh, &closed);
    if (closed == 0)
      tiledb_vfs_close(fp->ctx, fp->vfs_fh);
    tiledb_vfs_fh_free(&fp->vfs_fh);
  }
  if (fp->vfs != NULL)
    tiledb_vfs_free(&fp->vfs);
  if (fp->ctx != NULL && fp->owns_ctx)
    tiledb_ctx_free(&fp->ctx);

  return 0;
}

static const struct hFILE_backend htslib_vfs_backend = {
    tiledb_vfs_hfile_read,
    tiledb_vfs_hfile_write,
    tiledb_vfs_hfile_seek,
    tiledb_vfs_hfile_flush,
    tiledb_vfs_hfile_close};

hFILE* hopen_tiledb_vfs(const char* uri, const char* modestr) {
  int rc = 0;

  // Initialize hFILE_tiledb_vfs
  hFILE_tiledb_vfs* fp =
      (hFILE_tiledb_vfs*)hfile_init(sizeof(hFILE_tiledb_vfs), modestr, 0);
  if (fp == NULL) {
    printf("error in fp == NULL");
    return NULL;
  }
  fp->offset = 0;
  fp->size = 0;
  fp->owns_ctx = 0;
  fp->ctx = NULL;
  fp->vfs = NULL;
  fp->vfs_fh = NULL;

  // Convert the mode string to vfs mode enum
  fp->mode = TILEDB_VFS_READ;
  if (strncmp(modestr, "r", 1) == 0)
    fp->mode = TILEDB_VFS_READ;
  else if (strncmp(modestr, "w", 1) == 0)
    fp->mode = TILEDB_VFS_WRITE;
  else if (strncmp(modestr, "a", 1) == 0)
    fp->mode = TILEDB_VFS_APPEND;

  // Attempt to initialize the config from the global set config
  tiledb_config_t* config = hfile_tiledb_vfs_config;

  tiledb_error_t* error = NULL;
  int8_t owns_config = 0;
  if (config == NULL) {
    owns_config = 1;
    rc = tiledb_config_alloc(&config, &error);
    if (rc != TILEDB_OK) {
      const char* msg;
      tiledb_error_message(error, &msg);
      hts_log_error("uri: %s, error: %s\n", uri, msg);

      if (fp != NULL)
        hfile_destroy((hFILE*)fp);
      return NULL;
    }
  }

  // Attempt to initialize the context from the global set context
  tiledb_ctx_t* context = hfile_tiledb_vfs_ctx;
  if (context == NULL) {
    rc = tiledb_ctx_alloc(config, &context);
    fp->owns_ctx = 1;
    // If we create the config free it
    if (owns_config == 1 && config != NULL)
      tiledb_config_free(&config);

    if (rc != TILEDB_OK) {
      hts_log_error("Error : creating tiledb for hopen context\n");

      if (fp != NULL)
        hfile_destroy((hFILE*)fp);
      return NULL;
    }
  }

  fp->ctx = context;
  tiledb_vfs_alloc(context, NULL, &fp->vfs);
  // move the URI pointer pasted the first scheme and ://
  const char* uri_fixed = uri + strlen(HFILE_TILEDB_VFS_SCHEME) + 3;

  int32_t is_file = 0;
  rc = tiledb_vfs_is_file(fp->ctx, fp->vfs, uri_fixed, &is_file);
  if (rc != TILEDB_OK || is_file != 1) {
    if (rc != TILEDB_OK) {
      tiledb_ctx_get_last_error(fp->ctx, &error);
      const char* msg;
      tiledb_error_message(error, &msg);
      // Specifically log this as an info because htslib will often try to open
      // a file just to see if it exists if we log it as an error we get many
      // false positives
      hts_log_info("uri: %s, error: %s\n", uri, msg);
    }

    if (fp->vfs_fh != NULL) {
      int32_t closed = 0;
      tiledb_vfs_fh_is_closed(fp->ctx, fp->vfs_fh, &closed);
      if (closed == 0)
        tiledb_vfs_close(fp->ctx, fp->vfs_fh);
      tiledb_vfs_fh_free(&fp->vfs_fh);
    }
    if (fp->vfs != NULL)
      tiledb_vfs_free(&fp->vfs);
    if (fp->ctx != NULL && fp->owns_ctx)
      tiledb_ctx_free(&fp->ctx);

    if (fp != NULL)
      hfile_destroy((hFILE*)fp);
    return NULL;
  }

  rc = tiledb_vfs_open(fp->ctx, fp->vfs, uri_fixed, fp->mode, &fp->vfs_fh);
  if (rc != TILEDB_OK) {
    tiledb_ctx_get_last_error(fp->ctx, &error);
    const char* msg;
    tiledb_error_message(error, &msg);
    hts_log_error("uri: %s, error: %s\n", uri, msg);

    if (fp->vfs_fh != NULL) {
      int32_t closed = 0;
      tiledb_vfs_fh_is_closed(fp->ctx, fp->vfs_fh, &closed);
      if (closed == 0)
        tiledb_vfs_close(fp->ctx, fp->vfs_fh);
      tiledb_vfs_fh_free(&fp->vfs_fh);
    }
    if (fp->vfs != NULL)
      tiledb_vfs_free(&fp->vfs);
    if (fp->ctx != NULL && fp->owns_ctx)
      tiledb_ctx_free(&fp->ctx);

    if (fp != NULL)
      hfile_destroy((hFILE*)fp);
    return NULL;
  }

  if (fp->mode == TILEDB_VFS_READ || fp->mode == TILEDB_VFS_APPEND) {
    rc = tiledb_vfs_file_size(fp->ctx, fp->vfs, uri_fixed, &fp->size);
    if (rc != TILEDB_OK) {
      tiledb_ctx_get_last_error(fp->ctx, &error);
      const char* msg;
      tiledb_error_message(error, &msg);
      hts_log_error("uri: %s, error: %s\n", uri, msg);

      int32_t closed = 0;
      tiledb_vfs_fh_is_closed(fp->ctx, fp->vfs_fh, &closed);
      if (closed == 0)
        tiledb_vfs_close(fp->ctx, fp->vfs_fh);
      if (fp->vfs_fh != NULL)
        tiledb_vfs_fh_free(&fp->vfs_fh);
      if (fp->vfs != NULL)
        tiledb_vfs_free(&fp->vfs);
      if (fp->ctx != NULL && fp->owns_ctx)
        tiledb_ctx_free(&fp->ctx);
      if (fp != NULL)
        hfile_destroy((hFILE*)fp);
      return NULL;
    }

    if (fp->mode == TILEDB_VFS_APPEND)
      fp->offset = fp->size;
  }

  fp->base.backend = &htslib_vfs_backend;
  return &fp->base;
}

void hFILE_tiledb_vfs_destroy() {
  // Deinit for plugin, lets cleanup global variables
  if (hfile_tiledb_vfs_ctx != NULL)
    tiledb_ctx_free(&hfile_tiledb_vfs_ctx);
  if (hfile_tiledb_vfs_config != NULL)
    tiledb_config_free(&hfile_tiledb_vfs_config);
}

/* If we want to support different files with some being marked remote (so they
are downloaded and cached) we'd need a something like the function below. Seth
originally tried to use this to work around htslib trying to `stat` index files.
However htslib only checks the parent bed file for if remote or not. For now it
was decided to keep things simple and mark vfs as `hfile_always_remote`. int
hFILE_tiledb_vfs_is_remote(const char* fn) {
  // For index files, if we set it to local it first tries to stat from sys
  // so we have to pretend index files are remote

  char* csi_ext = ".csi";
  char* bai_ext = ".bai";
  char* tbi_ext = ".tbi";
  char* crai_ext = ".crai";
  size_t len = strlen(fn);
  size_t ext_start_pos = 0;
  for (size_t i = len - 1; i > 0; --i) {
    if (fn[i] == '.') {
      ext_start_pos = i;
      break;
    }
  }
  printf("hFILE_tiledb_vfs_is_remote: checking %s\n", fn+ext_start_pos);
  if (strcmp(fn + ext_start_pos, csi_ext) == 0 ||
      strcmp(fn + ext_start_pos, bai_ext) == 0 ||
      strcmp(fn + ext_start_pos, tbi_ext) == 0 ||
      strcmp(fn + ext_start_pos, crai_ext) == 0)
    return 1;
  // Pretend VFS is local

  printf("hFILE_tiledb_vfs_is_remote: returning 0 for local\n");
  return 0;
}*/

const struct hFILE_scheme_handler tiledb_vfs_handler = {
    hopen_tiledb_vfs, hfile_always_remote, "tiledb_vfs", 10};

int hfile_plugin_init(struct hFILE_plugin* self) {
  self->name = "tiledb_vfs";
  hfile_add_scheme_handler(HFILE_TILEDB_VFS_SCHEME, &tiledb_vfs_handler);
  self->destroy = &hFILE_tiledb_vfs_destroy;
  return 0;
}
