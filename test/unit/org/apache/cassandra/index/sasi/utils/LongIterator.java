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
package org.apache.cassandra.index.sasi.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.carrotsearch.hppc.LongOpenHashSet;
import com.carrotsearch.hppc.LongSet;
import com.carrotsearch.hppc.ObjectOpenHashSet;
import com.carrotsearch.hppc.ObjectSet;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.index.sasi.disk.IndexEntry;
import org.apache.cassandra.index.sasi.disk.TokenTreeBuilder;

public class LongIterator extends RangeIterator<Long, IndexEntry>
{
    private final List<LongIndexEntry> tokens;
    private int currentIdx = 0;

    public LongIterator(long[] tokens)
    {
        super(tokens.length == 0 ? null : tokens[0], tokens.length == 0 ? null : tokens[tokens.length - 1], tokens.length);
        this.tokens = new ArrayList<>(tokens.length);
        for (long token : tokens)
            this.tokens.add(new LongIndexEntry(token));
    }

    @Override
    protected IndexEntry computeNext()
    {
        if (currentIdx >= tokens.size())
            return endOfData();

        return tokens.get(currentIdx++);
    }

    @Override
    protected void performSkipTo(Long nextToken)
    {
        for (int i = currentIdx == 0 ? 0 : currentIdx - 1; i < tokens.size(); i++)
        {
            LongIndexEntry token = tokens.get(i);
            if (token.get().compareTo(nextToken) >= 0)
            {
                currentIdx = i;
                break;
            }
        }
    }

    @Override
    public void close() throws IOException
    {}

    public static class LongIndexEntry extends IndexEntry
    {
        public LongIndexEntry(long token)
        {
            super(token);
        }

        @Override
        public void merge(CombinedValue<Long> other)
        {
            // no-op
        }

        @Override
        public TokenTreeBuilder.Entries getOffsets()
        {
            // dont have any offsets because we dont have knowledge of any
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterator<EntryData> iterator()
        {
            return Collections.emptyIterator();
        }
    }

    public static List<Long> convert(RangeIterator<Long, IndexEntry> tokens)
    {
        List<Long> results = new ArrayList<>();
        while (tokens.hasNext())
            results.add(tokens.next().get());

        return results;
    }

    public static List<Long> convert(final long... nums)
    {
        return new ArrayList<Long>(nums.length)
        {{
                for (long n : nums)
                    add(n);
        }};
    }
}
