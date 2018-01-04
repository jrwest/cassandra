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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;

import com.carrotsearch.hppc.ObjectSet;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.sasi.disk.IndexedRow;
import org.apache.cassandra.index.sasi.disk.Token;
import org.apache.cassandra.index.sasi.disk.TokenTreeEntry;
import org.apache.cassandra.index.sasi.utils.AbstractIterator;
import org.apache.cassandra.index.sasi.utils.CombinedValue;
import org.apache.cassandra.index.sasi.utils.RangeIterator;

import com.google.common.collect.PeekingIterator;

public class KeyRangeIterator extends RangeIterator<Long, Token>
{
    private final DKIterator iterator;

    public KeyRangeIterator(ConcurrentSkipListSet<IndexedRow> keys)
    {
        super((Long) keys.first().partitionKey().getToken().getTokenValue(),
              (Long) keys.last().partitionKey().getToken().getTokenValue(), keys.size());
        this.iterator = new DKIterator(keys.iterator());
    }

    protected Token computeNext()
    {
        return iterator.hasNext() ? new DKToken(iterator.next()) : endOfData();
    }

    protected void performSkipTo(Long nextToken)
    {
        while (iterator.hasNext())
        {
            IndexedRow row = iterator.peek();
            if (Long.compare((long) row.partitionKey().getToken().getTokenValue(), nextToken) >= 0)
                break;

            // consume smaller key
            iterator.next();
        }
    }

    public void close() throws IOException
    {}

    private static class DKIterator extends AbstractIterator<IndexedRow> implements PeekingIterator<IndexedRow>
    {
        private final Iterator<IndexedRow> rows;

        public DKIterator(Iterator<IndexedRow> keys)
        {
            this.rows = keys;
        }

        protected IndexedRow computeNext()
        {
            return rows.hasNext() ? rows.next() : endOfData();
        }
    }

    private static class DKToken extends Token
    {
        private final SortedSet<IndexedRow> keys;

        public DKToken(final IndexedRow row)
        {
            super((long) row.partitionKey().getToken().getTokenValue());

            keys = new TreeSet<IndexedRow>(IndexedRow.COMPARATOR)
            {{
                add(row);
            }};
        }

        public ObjectSet<TokenTreeEntry> getEntries()
        {
            throw new UnsupportedOperationException();
            /*Set<TokenTreeEntry> offsets = new TokenTreeEntrySet(1);
            for (DecoratedKey key : keys)
                offsets.add(new TokenTreeEntry.PartitionOnly((long) key.getToken().getTokenValue()));

            return offsets;*/
        }

        public void merge(CombinedValue<Long> other)
        {
            if (!(other instanceof Token))
                return;

            Token o = (Token) other;
            assert o.get().equals(token);

            if (o instanceof DKToken)
            {
                keys.addAll(((DKToken) o).keys);
            }
            else
            {
                for (IndexedRow key : o)
                    keys.add(key);
            }
        }

        public Iterator<IndexedRow> iterator()
        {
            return keys.iterator();
        }
    }
}