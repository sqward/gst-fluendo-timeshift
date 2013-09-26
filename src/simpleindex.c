/* GStreamer MPEG TS Time Shifting
 * Copyright (C) 2013 YouView LTD
 * Written by Mariusz Buras <mariusz.buras@youview.com>
 *
 * simpleindex.c: collects asosciations of timestamps and offsets
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

#include "simpleindex.h"

#include <stdio.h>
#include <glib/gstdio.h>
#include <string.h>

/* For sync_file_range */
#include <sys/types.h>
#include <unistd.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <assert.h>
#include <sys/stat.h>

SimpleIndex* simpleindex_new()
{
  SimpleIndex* index = g_new (SimpleIndex, 1);
  index->refcount = 1;
  index->index_fd = -1;
  index->page_size = sysconf(_SC_PAGE_SIZE);
  return index;
}

void simple_index_set_fd( SimpleIndex* index, int fd )
{
  index->index_fd = fd;
}

typedef struct __attribute__ ((__packed__))
{
  guint64 time;
  guint64 offset;
} IndexEntry;

void simple_indexer_add_entry ( SimpleIndex* index, guint64 time, guint64 offset )
{
  if ( index->index_fd != -1 ) {
    IndexEntry entry = { time, offset };
    if ( sizeof(IndexEntry)
          != write( index->index_fd, &entry, sizeof(IndexEntry))) {
      GST_WARNING ("Timeshift buffer write failed: %s", strerror (errno));
    }
  }
}

static inline gint64
rounddown_offset ( guint64 offset )
{
  return offset - offset % sizeof(IndexEntry);
}

static IndexEntry* 
map_entries ( SimpleIndex* index, guint64 *index_file_size ) 
{
  IndexEntry* entries;
  struct stat stat_buf;

  if (fstat (index->index_fd, &stat_buf) != 0) {
    GST_ERROR ("Failed to stat fd %i: %s", index->index_fd, strerror (errno));
    return 0; /* not sure about it */
  }

  *index_file_size = rounddown_offset(stat_buf.st_size);

  if ( *index_file_size == 0 )
    return 0;

  entries = (IndexEntry*) mmap ( NULL, *index_file_size, 
                                PROT_READ, MAP_PRIVATE, index->index_fd, 0 );

  if ( (void *) entries == MAP_FAILED ) {
    GST_ERROR ("failed to mmap fd %i: %s", index->index_fd, strerror (errno));
    return 0; /* not sure about it */
  }
  
  return entries;
}

static inline void
unmap_entries ( IndexEntry* entries, guint64 map_entries_size )
{
  munmap ( (void*)entries, map_entries_size );
}

gint64 find_value ( IndexEntry* entries, gint low, gint high, 
  gint64 (*val_func)(IndexEntry*,gint64*), gint64 val )
{
  gint64 ret_val = -1;
  guint64 midpoint = 0;
  
  while ( high > low ) {
    midpoint = (low + high) / 2;
    if ( val_func(&entries[ midpoint ],0) < val ) {
      low = midpoint + 1;
    } else {
      high = midpoint;
    }
  }
  /* This is aproximate lookup so we pickup the nearest result */
  val_func(&entries[ midpoint ], &ret_val);
  return ret_val;
}

gint64 select_offset( IndexEntry* entry, gint64* assoc )
{
  if ( assoc )
    *assoc = entry->time;
  return entry->offset;
}

gint64 select_time( IndexEntry* entry, gint64* assoc )
{
  if ( assoc )
    *assoc = entry->offset;
  return entry->time;
}

static gint64 
simple_indexer_search ( SimpleIndex* index, gint64 val, 
    gint64 (*val_func)(IndexEntry*) )
{
  gint low = 0, high = 0;
  guint64 index_file_size, association = 0;
  IndexEntry* entries = map_entries( index, &index_file_size );

  if ( !entries )
    goto err_unmapped;

  high = ( index_file_size / sizeof(IndexEntry) ) - 1;
  
  if ( high < 0 )
    goto err;
  
  association = find_value ( entries, low, high, val_func, val );
  
  unmap_entries( entries, index_file_size );
  return association;
err:
  unmap_entries( entries, index_file_size );
err_unmapped:
  return -1;
}

gint64 simple_indexer_search_offset ( SimpleIndex* index, gint64 time )
{
  gint64 val = simple_indexer_search ( index, time, select_time );
  fprintf(stderr,"by_offset(%llu): %llu\n",time, val);
  return val;
}

gint64 simple_indexer_search_time ( SimpleIndex* index, gint64 offset )
{
  gint64 val = simple_indexer_search ( index, offset, select_offset );
  fprintf(stderr,"by_time(%llu): %llu\n",offset, val);
  return val;
}

/**
 * gst_ts_simpleindex_ref:
 * @index: a #SimpleCache
 *
 * Increase the refcount of @simpleindex.
 *
 */
SimpleIndex *
gst_ts_simpleindex_ref (SimpleIndex* index)
{
  g_return_val_if_fail (index != NULL, NULL);
  g_atomic_int_inc (&index->refcount);
  return index;
}

static void
gst_ts_simpleindex_free (SimpleIndex* index)
{
  close (index->index_fd);
  index->index_fd = -1;
  g_free (index);
}

/**
 * gst_ts_simpleindex_unref:
 * @index: a #SimpleCache
 *
 * Unref @simpleindex and free the resources when the refcount reaches 0.
 *
 */
void
gst_ts_simpleindex_unref (SimpleIndex* index)
{
  g_return_if_fail (index != NULL);
  if (g_atomic_int_dec_and_test (&index->refcount))
    gst_ts_simpleindex_free (index);
}
