/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.index.sasi.memory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.index.sasi.disk.IndexEntry;
import org.apache.cassandra.index.sasi.disk.TokenTreeBuilder;
import org.apache.cassandra.index.sasi.utils.AbstractIterator;
import org.apache.cassandra.index.sasi.utils.CombinedValue;
import org.apache.cassandra.index.sasi.utils.RangeIterator;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

public class KeyRangeIterator extends RangeIterator<Long, IndexEntry>
{
    private final PeekingIterator<Map.Entry<DecoratedKey, ConcurrentSkipListSet<Clustering>>> iterator;

    public KeyRangeIterator(ConcurrentSkipListMap<DecoratedKey, ConcurrentSkipListSet<Clustering>> rows)
    {
        super((Long) rows.firstEntry().getKey().getToken().getTokenValue(),
              (Long) rows.lastEntry().getKey().getToken().getTokenValue(), rows.size());
        this.iterator = Iterators.peekingIterator(rows.entrySet().iterator());
    }

    protected IndexEntry computeNext()
    {
        return iterator.hasNext() ? new InMemoryIndexEntry(iterator.next()) : endOfData();
    }

    protected void performSkipTo(Long nextToken)
    {
        while (iterator.hasNext())
        {
            DecoratedKey key = iterator.peek().getKey();
            if (Long.compare((long) key.getToken().getTokenValue(), nextToken) >= 0)
                break;

            // consume smaller key
            iterator.next();
        }
    }

    public void close() throws IOException
    {}

    private static class InMemoryIndexEntry extends IndexEntry
    {
        private final NavigableMap<DecoratedKey, NavigableSet<Clustering>> rows;

        public InMemoryIndexEntry(final Map.Entry<DecoratedKey, ConcurrentSkipListSet<Clustering>> rows)
        {
            super((long) rows.getKey().getToken().getTokenValue());
            this.rows = new TreeMap<DecoratedKey, NavigableSet<Clustering>>(DecoratedKey.comparator) {{
                put(rows.getKey(), new TreeSet<>(rows.getValue()));
            }};
        }

        public TokenTreeBuilder.Entries getOffsets()
        {
            // for an in-memory index entry we don't have/know of any on-disk offsets
            throw new UnsupportedOperationException();
        }

        public void merge(CombinedValue<Long> other)
        {
            if (!(other instanceof IndexEntry))
                return;

            IndexEntry o = (IndexEntry) other;
            assert o.get().equals(token);

            o.forEach(this::mergeEntryData);
        }

        public Iterator<EntryData> iterator()
        {
            return Iterators.transform(rows.entrySet().iterator(), e -> new EntryData(e.getKey(), e.getValue()));
        }

        protected void mergeEntryData(EntryData ed)
        {
            NavigableSet<Clustering> clusterings = rows.putIfAbsent(ed.key, ed.clusterings());
            if (clusterings != null)
                clusterings.addAll(ed.clusterings());
        }
    }
}