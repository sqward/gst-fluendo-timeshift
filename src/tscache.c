/* GStreamer MPEG TS Time Shifting
 * Copyright (C) 2011 Fluendo S.A. <support@fluendo.com>
 *               2013 YouView TV Ltd. <krzysztof.konopko@youview.com>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the
 * Free Software Foundation, Inc., 51 Franklin St, Fifth Floor,
 * Boston, MA 02110-1301, USA.
 */

#define _GNU_SOURCE

#ifdef HAVE_CONFIG
#include "config.h"
#endif

#include "tscache.h"

#include <stdio.h>
#include <glib/gstdio.h>
#include <string.h>

/* For sync_file_range */
#include <sys/types.h>
#include <unistd.h>
#include <sys/mman.h>
#include <fcntl.h>

GST_DEBUG_CATEGORY_EXTERN (ts_flow);
#define GST_CAT_DEFAULT (ts_flow)

#define DEBUG 0
#define DEBUG_RINGBUFFER 0

#define INVALID_OFFSET ((guint64) -1)

/*
 * Page cache management
 *
 * The timeshift cache takes care to avoid taking up too much of the linux
 * page cache as it has fairly predictible read/write behaviour.  We can say:
 *
 *  * Once a page has been read it is not likely to be read again.
 *  * Once a page has been written it will not be written again until the
 *    timeshifter wraps
 *
 * We can use sync_file_range and posix_fadvise to tell the kernel to drop the
 * pages from cache.  We always do this for pages that have just been read.
 * We also wish to do this for pages that have been written but are not going
 * to be read for a while (e.g. while timeshifting) while still giving the
 * kernel some time to write the pages out to disk before we block waiting for
 * it to do so.  We don't want to ask the kernel to drop a page from cache if
 * it's just about to be read.
 *
 * So we define two values:
 *
 *  * PAGE_SYNC_TIME_BYTES - how long do we want to give the kernel to write
 *    newly written pages to disk before we block until pages are successfully
 *    written.
 *  * READ_KEEP_PAGE_BYTES - Don't throw pages away if the read head is going
 *    to be needing them.
 *
 * Diagram:
 *
 *         r−−−−>                         Legend
 *                  <−−−−−−−−−−−w
 * --------#--------#############······   r−−−−> - read head plus line showing
 *                                                 READ_KEEP_PAGE_SLOTS
 *              r−−−−>                    <−−−−w - write head plus line showing
 *                  <−−−−−−−−−−−w                  PAGE_SYNC_TIME_SLOTS
 * -------------#################······   ······ - Unwritten slots
 *                                        ###### - Pages possibly in page cache
 *                    r−−−−>              ------ - Written pages not in page
 *                  <−−−−−−−−−−−w                  cache
 * -----------------#############······
 *
 *                           r−−−−>
 *                  <−−−−−−−−−−−w
 * -----------------#############······
 *
 * Note: there's still the possiblity of some data being left in page cache
 * in the specific case when you seek while in the state indicated in the
 * second diagram above.  It is not worth the additonal complexity to the
 * seeking code to "fix" this as it it unlikely and nothing too bad happens
 * even if it occurs.
 */

#define PAGE_SYNC_TIME_BYTES (640 * 1024)       /* 640kB */
#define READ_KEEP_PAGE_BYTES (320 * 1024)       /* 320kB */
#define PAGE_SIZE (4 * 1024)                    /* 4KB*/
/* GstTSCache */

struct _GstTSCache
{
  volatile gint refcount;

  GMutex lock;

  gboolean flushing;

  gboolean need_discont;

  int output_buffer_size;
  int write_fd;
  int read_fd;

  /* All of these are in stream offset, not offset into timeshift file.  The
   * latter are derived quantities. */
  off64_t write_head;
  off64_t read_head;
  
  off64_t buffer_size;
};

#define GST_CACHE_LOCK(cache) G_STMT_START {                                \
  g_mutex_lock (&cache->lock);                                              \
} G_STMT_END

#define GST_CACHE_UNLOCK(cache) G_STMT_START {                              \
  g_mutex_unlock (&cache->lock);                                            \
} G_STMT_END

static off64_t
stream_to_file_offset(GstTSCache * cache, off64_t stream )
{
  return stream % cache->buffer_size;
}

static inline off64_t get_recycle_head(GstTSCache * cache)
{
  return MAX( cache->write_head-cache->buffer_size,0);
}

typedef struct Range_
{
  off64_t offset;
  off64_t size;
} Range;

static Range
page_align(Range x)
{
  off64_t begin = x.offset, end = x.offset + x.size;
  begin = begin - begin % PAGE_SIZE;
  end = end - end % PAGE_SIZE;
  x.offset = begin;
  x.size = end - begin;
  return x;
}

static gboolean
range_overlaps(Range a, Range b)
{
  return ((a.offset + a.size) > b.offset) && ((b.offset + b.size) > a.offset);
}

static void
range_consume (Range *r, off64_t bytes)
{
  g_assert  (r);
  r->offset += bytes;
  if (bytes <= r->size)
    r->size -= bytes;
  else
    r->size = 0;
}

static Range
next_contiguous_range (GstTSCache *cache, Range r)
{
  r.offset = stream_to_file_offset (cache, r.offset);
  /* If we're going to wrap break up our writes: */
  if (r.offset + r.size > cache->buffer_size)
    r.size = cache->buffer_size - r.offset;

  return r;
}

static Range
consume_contiguous_range (GstTSCache *cache, Range *r)
{
  Range out = next_contiguous_range (cache, *r);
  range_consume (r, out.size);
  return out;
}

static inline GstFlowReturn
buffer_write_locked (GstTSCache * cache, guint8 * data, guint size)
{
  Range for_writing = {cache->write_head, size};
  Range for_writeout = {cache->write_head, size};
  Range for_cache_force_out = {cache->write_head, size};
  Range for_cache_dropping = {cache->write_head, size};
  Range r;

  off64_t new_recycle_head = MAX(cache->write_head + size - cache->buffer_size, 0);
  if (cache->read_head < new_recycle_head) {
    GST_WARNING ("no space in the buffer!");
    return GST_FLOW_ERROR;
  }

  while ((r = next_contiguous_range(cache, for_writing)).size > 0) {
    ssize_t bytes_written = 0;
    int write_errno;

    /* we've wrapped */
    if (r.offset == 0) {
      GST_INFO("Timeshifter: wrapping");
      if (lseek (cache->write_fd, 0, SEEK_SET) == -1) {
        GST_WARNING ("Timeshift buffer seek failed: %s", strerror (errno));
        return GST_FLOW_ERROR;
      }
    }

    GST_CACHE_UNLOCK (cache);
    bytes_written = write(cache->write_fd, data, r.size);
    write_errno = errno;
    GST_CACHE_LOCK (cache);

    if (bytes_written < 0) {
      if (errno == EAGAIN)
        continue;
      else {
        GST_WARNING ("Timeshift buffer write failed: %s", strerror (errno));
        return GST_FLOW_ERROR;
      }
    } else {
      range_consume(&for_writing, bytes_written);
      data += bytes_written;
      cache->write_head += bytes_written;
    }
  }

  while ((r = consume_contiguous_range(cache, &for_writeout)).size > 0) {
    r = page_align(r);
    if (r.size > 0) {
      g_warn_if_fail (sync_file_range (cache->write_fd, r.offset,
        r.size, SYNC_FILE_RANGE_WRITE) == 0);
    }
  }

  for_cache_force_out.offset -= PAGE_SYNC_TIME_BYTES;
  if (for_cache_force_out.offset < 0) {
    range_consume (&for_cache_force_out, -for_cache_force_out.offset);
  }
  for_cache_dropping = for_cache_force_out;
  while ((r = consume_contiguous_range(cache, &for_cache_force_out)).size > 0) {
    r = page_align(r);
    if (r.size > 0) {
      /* Make sure the pages from a while ago have been written out by now. */
      /* once slots has been removed it's a bit tricky to know anything */
      g_warn_if_fail (sync_file_range (cache->write_fd,
              r.offset, r.size,
              SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WRITE |
              SYNC_FILE_RANGE_WAIT_AFTER) == 0);
    }
  }

  /* And drop them from the cache unless they're going to be needed by
   * the read head soon.  e.g. if
   * (read head < writeout_idx < read_head + READ_KEEP_PAGE_SLOTS) */
  Range read_ahead_region = {cache->read_head, READ_KEEP_PAGE_BYTES};
  if (!range_overlaps(read_ahead_region, for_cache_dropping)) {
    while ((r = consume_contiguous_range(cache, &for_cache_dropping)).size > 0) {
      r = page_align(r);
      if (r.size > 0) {
        g_warn_if_fail (posix_fadvise64 (cache->write_fd,
                r.offset, r.size,
                POSIX_FADV_DONTNEED) == 0);
      }
    }
  }

  return GST_FLOW_OK;
}

/* Slot Buffer */

typedef struct BufferContext_
{
  GstTSCache *cache;
  gpointer data;
  Range file_range;
} BufferContext;

static void
cleanup_buffer (gpointer * data)
{
  BufferContext *ctx = (BufferContext *) data;
  GstTSCache * cache = ctx->cache;

  /* Avoid trashing caches.  This will fail if the pages haven't hit disk yet
   * but that's OK as in that case the write head will take care of it. */
  Range r = page_align(ctx->file_range);
  if (r.size > 0) {
    g_warn_if_fail (posix_fadvise64 (cache->write_fd,
            r.offset, r.size,
            POSIX_FADV_DONTNEED) == 0);
  }
  g_free (ctx->data);
  g_slice_free (BufferContext, ctx);
}

static GstBuffer *
buffer_new_locked (GstTSCache * cache)
{
  BufferContext *ctx;
  off64_t ro = stream_to_file_offset(cache, cache->read_head);
  gint64 read_bytes = 0;
  /* Make sure we can produce buffer with satisfactory size. 
   * this is important because small buffers can cause performance
   * issues especially on embedded systems
   */
  if ( cache->read_head + cache->output_buffer_size >= cache->write_head ) {
    return NULL;
  }

  /* On buffer wrap a tiny buffer could be generated because only a portion
   * of the buffer would be linear in the file. So as a simplification only that
   * bit of a buffer is taken. In reality this would rearly happen or not happen
   * at all so such a code simplification is desired.
   */

  size_t out_size = 
    ( ro + cache->output_buffer_size <= cache->buffer_size ) ?
      cache->output_buffer_size : cache->buffer_size - ro;

  if ( out_size == 0 ) {
    out_size = cache->output_buffer_size;
    ro = 0;
  }

  ctx = g_slice_new (BufferContext);
  if ( !ctx ){
    GST_ERROR ("g_slice_new (BufferContext) failed");    
  }

  ctx->data = g_malloc( out_size );

  GST_CACHE_UNLOCK (cache);

  if (lseek (cache->read_fd, ro, SEEK_SET) == -1) {
    GST_WARNING ("Timeshift buffer seek failed: %s", strerror (errno));
    GST_CACHE_LOCK (cache);
    return NULL;
  }

  read_bytes = read( cache->read_fd, ctx->data, out_size );
  GST_CACHE_LOCK (cache);

  if ( read_bytes < 0 ) {
    GST_ERROR ("couldn't read from the ringbuffer (read) failed %s", 
      strerror (errno));
  }

  ctx->cache = cache;
  ctx->file_range.offset = ro;
  ctx->file_range.size = read_bytes;

  GstBuffer *buffer =
      gst_buffer_new_wrapped_full (GST_MEMORY_FLAG_READONLY, ctx->data,
      cache->output_buffer_size, 0, out_size, ctx,
      (gpointer) cleanup_buffer);

  GST_BUFFER_OFFSET (buffer) = cache->read_head;
  GST_BUFFER_OFFSET_END (buffer) = cache->read_head + cache->output_buffer_size;

  cache->read_head += out_size;

  return buffer;
}


static inline void
gst_ts_cache_flush (GstTSCache * cache)
{
  GST_CACHE_LOCK (cache);
  cache->write_head = 0;
  cache->read_head = 0;
  cache->need_discont = TRUE;
  GST_CACHE_UNLOCK (cache);
}

static int
reopen (int fd, int flags)
{
  char *filename;
  int newfd, errno_tmp;

  filename = g_strdup_printf ("/proc/%u/fd/%i", (unsigned) getpid (), fd);
  newfd = open (filename, flags);
  errno_tmp = errno;
  g_free (filename);
  errno = errno_tmp;
  return newfd;
}

/**
 * gst_ts_cache_new:
 * @size: cache size
 *
 * Create a new cache instance.  File pointed to by fd will be used as the
 * timeshift cache backing file.
 *
 * Returns: a new #GstTSCache
 *
 */
GstTSCache *
gst_ts_cache_new (int fd, int output_buffers_size)
{
  GstTSCache *cache;
  off64_t size = 0;
  int wr_fd, rd_fd = -1;
  struct stat stat_buf;

  wr_fd = reopen (fd, O_WRONLY | O_CLOEXEC);
  rd_fd = reopen (fd, O_RDONLY | O_CLOEXEC);
  if (wr_fd == -1 || rd_fd == -1) {
    GST_ERROR ("Failed reopening fd %i: %s", fd, strerror (errno));
    goto errout;
  }
  if (fstat (fd, &stat_buf) != 0) {
    GST_ERROR ("Failed to stat fd %i: %s", fd, strerror (errno));
    goto errout;
  }
  size = stat_buf.st_size;

  cache = g_new (GstTSCache, 1);

  cache->output_buffer_size = output_buffers_size;
  cache->refcount = 1;

  g_mutex_init (&cache->lock);

  cache->write_head = 0;
  cache->read_head = 0;
  cache->write_fd = wr_fd;
  cache->read_fd = rd_fd;

  cache->buffer_size = size;
  gst_ts_cache_flush (cache);

  return cache;
errout:
  close (wr_fd);
  close (rd_fd);
  return NULL;
}

/**
 * gst_ts_cache_ref:
 * @cache: a #GstTSCache
 *
 * Increase the refcount of @cache.
 *
 */
GstTSCache *
gst_ts_cache_ref (GstTSCache * cache)
{
  g_return_val_if_fail (cache != NULL, NULL);

  g_atomic_int_inc (&cache->refcount);

  return cache;
}

static void
gst_ts_cache_free (GstTSCache * cache)
{
  close (cache->write_fd);
  cache->write_fd = -1;
  close (cache->read_fd);
  cache->read_fd = -1;

  g_mutex_clear (&cache->lock);

  g_free (cache);
}

/**
 * gst_ts_cache_unref:
 * @cache: a #GstTSCache
 *
 * Unref @cache and free the resources when the refcount reaches 0.
 *
 */
void
gst_ts_cache_unref (GstTSCache * cache)
{
  g_return_if_fail (cache != NULL);

  if (g_atomic_int_dec_and_test (&cache->refcount))
    gst_ts_cache_free (cache);
}

/**
 * gst_ts_cache_pop:
 * @cache: a #GstTSCache
 *
 * Get the head of the cache.
 *
 * Returns: the head buffer of @cache or NULL when the cache is empty.
 *
 */

GstBuffer *
gst_ts_cache_pop (GstTSCache * cache, gboolean drain)
{
  GstBuffer *buffer = NULL;
  GST_CACHE_LOCK (cache);

  buffer = buffer_new_locked (cache);
  if ( buffer && cache->need_discont ) {
    GST_BUFFER_FLAG_SET (buffer, GST_BUFFER_FLAG_DISCONT);
    cache->need_discont = FALSE;
  }
  GST_CACHE_UNLOCK (cache);
  return buffer;
}

/**
 * gst_ts_cache_push:
 * @cache: a #GstTSCache
 * @data: pointer to the data to be inserted in the cache
 * @size: size in bytes of provided data
 *
 * Cache the @buffer and takes ownership of the it.
 *
 */
gboolean
gst_ts_cache_push (GstTSCache * cache, guint8 * data, gsize size)
{
  GstFlowReturn write_result;
  GST_CACHE_LOCK (cache);
  write_result = buffer_write_locked (cache, data, size );
  if (write_result != GST_FLOW_OK) {
    /* error or flushing */
    GST_CACHE_UNLOCK (cache);
    return FALSE;
  }
  GST_CACHE_UNLOCK (cache);
  return TRUE;
}

/**
 * gst_ts_cache_has_offset:
 * @cache: a #GstTSCache
 * @offset: byte offset to check if is in the cache.
 *
 * Checks if an specified offset can be found on the cache.
 *
 */
gboolean
gst_ts_cache_has_offset (GstTSCache * cache, guint64 offset)
{
  gboolean ret;
  g_return_val_if_fail (cache != NULL, FALSE);

  GST_CACHE_LOCK (cache);

  ret = (offset >= stream_to_file_offset( cache, get_recycle_head(cache) ) 
          && offset < stream_to_file_offset( cache, cache->write_head ));
  GST_CACHE_UNLOCK (cache);
  return ret;
}

/**
 * gst_ts_cache_get_total_bytes_received:
 * @cache: a #GstTSCache
 *
 * Returns the total number of bytes which have been written into the cache
 * equivalent to the "duration" of the cache in bytes.
 */
guint64
gst_ts_cache_get_total_bytes_received (GstTSCache * cache)
{
  guint64 offset;
  GST_CACHE_LOCK (cache);
  offset = cache->write_head - get_recycle_head(cache);
  GST_CACHE_UNLOCK (cache);
  return offset;
}

static guint64 
clamp( guint64 min, guint64 max, guint64 val)
{
  return MAX( MIN( val, max ), min );
}

/**
 * gst_ts_cache_seek:
 * @cache: a #GstTSCache
 * @offset: byte offset where the cache have to be repositioned.
 *
 * Reconfigures the cache to read from the closest location to the specified
 * offset.
 *
 */
gboolean
gst_ts_cache_seek (GstTSCache * cache, guint64 offset)
{
  g_return_val_if_fail (cache != NULL, FALSE);

  GST_DEBUG ("seeking for offset: %" G_GUINT64_FORMAT, offset);

  GST_CACHE_LOCK (cache);

  cache->read_head = clamp ( get_recycle_head(cache), 
                        cache->write_head, get_recycle_head(cache) + offset );

  GST_CACHE_UNLOCK (cache);
  return TRUE;
}

/**
 * gst_ts_cache_is_empty:
 * @cache: a #GstTSCache
 *
 * Return TRUE if cache is empty.
 *
 */
gboolean
gst_ts_cache_is_empty (GstTSCache * cache)
{
  gboolean is_empty = TRUE;
  g_return_val_if_fail (cache != NULL, TRUE);
  GST_CACHE_LOCK (cache);
  is_empty = ((cache->write_head-cache->read_head ) <= cache->output_buffer_size);
  GST_CACHE_UNLOCK (cache);
  return is_empty;
}

/**
 * gst_ts_cache_fullness:
 * @cache: a #GstTSCache
 *
 * Return # of bytes remaining in the ringbuffer.
 *
 */
guint64
gst_ts_cache_fullness (GstTSCache * cache)
{
  g_return_val_if_fail (cache != NULL, 0);

  if (gst_ts_cache_is_empty (cache)) {
    return 0;
  } else {
    guint64 remaining = 0;
    GST_CACHE_LOCK (cache);
    remaining =  cache->buffer_size - (cache->write_head - cache->read_head);
    GST_CACHE_UNLOCK (cache);
    return remaining;
  }
}
/* TODO: I don't understand what this could be used for and what the outputs
  are. */
void
gst_ts_cache_buffered_range (GstTSCache * cache, guint64 * begin, guint64 * end)
{
  g_return_if_fail (cache != NULL);

  GST_CACHE_LOCK (cache);
  if (end) {
    *end = stream_to_file_offset(cache, cache->write_head ) ;
  }

  if (begin) {
    *begin = stream_to_file_offset( cache, get_recycle_head(cache) ) ;
  }
  GST_CACHE_UNLOCK (cache);
}
