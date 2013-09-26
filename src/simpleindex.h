/* GStreamer MPEG TS Time Shifting
 * Copyright (C) 2013 YouView LTD
 * Written by Mariusz Buras <mariusz.buras@youview.com>
 *
 * simpleindex.h: collects asosciations of timestamps and offsets
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
#ifndef __TS_SIMPLEINDEX_H__
#define __TS_SIMPLEINDEX_H__

#include <gst/gst.h>

typedef struct _SimpleIndex SimpleIndex;

struct _SimpleIndex
{
    volatile gint refcount;

    /* private */
    int index_fd;
    size_t page_size;
};
GType gst_ts_simpleindex_get_type (void);
SimpleIndex* simpleindex_new( );
void simple_index_set_fd( SimpleIndex* index, int fd );
void simple_index_add_entry ( SimpleIndex* index, guint64 time, guint64 offset );
gint64 simple_indexer_search_offset ( SimpleIndex* index, gint64 time );
gint64 simple_indexer_search_time ( SimpleIndex* index, gint64 offset );
SimpleIndex *gst_ts_simpleindex_ref (SimpleIndex * cache);
void gst_ts_simpleindex_unref (SimpleIndex * cache);

#endif /* __TS_INDEX_H__ */