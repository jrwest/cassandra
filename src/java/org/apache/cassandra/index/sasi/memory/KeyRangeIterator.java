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
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;

import com.carrotsearch.hppc.ObjectSet;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.index.sasi.disk.IndexEntry;
import org.apache.cassandra.index.sasi.disk.TokenTreeBuilder;
import org.apache.cassandra.index.sasi.utils.AbstractIterator;
import org.apache.cassandra.index.sasi.utils.CombinedValue;
import org.apache.cassandra.index.sasi.utils.RangeIterator;

import com.carrotsearch.hppc.LongOpenHashSet;
import com.carrotsearch.hppc.LongSet;
import com.google.common.collect.PeekingIterator;

public class KeyRangeIterator extends RangeIterator<Long, IndexEntry>
{
    private final DKIterator iterator;

    public KeyRangeIterator(ConcurrentSkipListSet<DecoratedKey> keys)
    {
        super((Long) keys.first().getToken().getTokenValue(), (Long) keys.last().getToken().getTokenValue(), keys.size());
        this.iterator = new DKIterator(keys.iterator());
    }

    protected IndexEntry computeNext()
    {
        return iterator.hasNext() ? new InMemoryIndexEntry(iterator.next()) : endOfData();
    }

    protected void performSkipTo(Long nextToken)
    {
        while (iterator.hasNext())
        {
            DecoratedKey key = iterator.peek();
            if (Long.compare((long) key.getToken().getTokenValue(), nextToken) >= 0)
                break;

            // consume smaller key
            iterator.next();
        }
    }

    public void close() throws IOException
    {}

    private static class DKIterator extends AbstractIterator<DecoratedKey> implements PeekingIterator<DecoratedKey>
    {
        private final Iterator<DecoratedKey> keys;

        public DKIterator(Iterator<DecoratedKey> keys)
        {
            this.keys = keys;
        }

        protected DecoratedKey computeNext()
        {
            return keys.hasNext() ? keys.next() : endOfData();
        }
    }

    private static class InMemoryIndexEntry extends IndexEntry
    {
        private final SortedSet<DecoratedKey> keys;

        public InMemoryIndexEntry(final DecoratedKey key)
        {
            super((long) key.getToken().getTokenValue());

            keys = new TreeSet<DecoratedKey>(DecoratedKey.comparator)
            {{
                add(key);
            }};
        }

        public ObjectSet<TokenTreeBuilder.Entry> getOffsets()
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

            if (o instanceof InMemoryIndexEntry)
            {
                keys.addAll(((InMemoryIndexEntry) o).keys);
            }
            else
            {
                for (DecoratedKey key : o)
                    keys.add(key);
            }
        }

        public Iterator<DecoratedKey> iterator()
        {
            return keys.iterator();
        }
    }
}