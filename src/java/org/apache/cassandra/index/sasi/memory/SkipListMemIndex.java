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

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.index.sasi.conf.ColumnIndex;
import org.apache.cassandra.index.sasi.disk.IndexEntry;
import org.apache.cassandra.index.sasi.plan.Expression;
import org.apache.cassandra.index.sasi.utils.RangeUnionIterator;
import org.apache.cassandra.index.sasi.utils.RangeIterator;
import org.apache.cassandra.db.marshal.AbstractType;

public class SkipListMemIndex extends MemIndex
{
    public static final int CSLM_OVERHEAD = 128; // average overhead of CSLM

    private final ConcurrentSkipListMap<ByteBuffer,
                                       ConcurrentSkipListMap<DecoratedKey, ConcurrentSkipListSet<Clustering>>> index;

    public SkipListMemIndex(AbstractType<?> keyValidator, ColumnIndex columnIndex)
    {
        super(keyValidator, columnIndex);
        index = new ConcurrentSkipListMap<>(columnIndex.getValidator());
    }

    public long add(DecoratedKey key, Clustering clustering, ByteBuffer value)
    {
        long overhead = CSLM_OVERHEAD; // DKs are shared
        ConcurrentSkipListMap<DecoratedKey, ConcurrentSkipListSet<Clustering>> entry = index.get(value);

        if (entry == null)
        {
            ConcurrentSkipListMap<DecoratedKey, ConcurrentSkipListSet<Clustering>> newEntry = new ConcurrentSkipListMap<>(DecoratedKey.comparator);
            entry = index.putIfAbsent(value, newEntry);
            if (entry == null)
            {
                overhead += CSLM_OVERHEAD + value.remaining();
                entry = newEntry;
            }
        }

        ConcurrentSkipListSet<Clustering> clusterings = entry.get(key);
        if (clusterings == null)
        {
            ConcurrentSkipListSet<Clustering> newClusterings = new ConcurrentSkipListSet<>(columnIndex.clusteringComparator());
            clusterings = entry.putIfAbsent(key, newClusterings);
            if (clusterings == null)
            {
                overhead += CSLM_OVERHEAD;
                clusterings = newClusterings;
            }
        }

        clusterings.add(clustering);

        return overhead;
    }

    public RangeIterator<Long, IndexEntry> search(Expression expression)
    {
        ByteBuffer min = expression.lower == null ? null : expression.lower.value;
        ByteBuffer max = expression.upper == null ? null : expression.upper.value;

        SortedMap<ByteBuffer, ConcurrentSkipListMap<DecoratedKey, ConcurrentSkipListSet<Clustering>>> search;

        if (min == null && max == null)
        {
            throw new IllegalArgumentException();
        }
        if (min != null && max != null)
        {
            search = index.subMap(min, expression.lower.inclusive, max, expression.upper.inclusive);
        }
        else if (min == null)
        {
            search = index.headMap(max, expression.upper.inclusive);
        }
        else
        {
            search = index.tailMap(min, expression.lower.inclusive);
        }

        RangeUnionIterator.Builder<Long, IndexEntry> builder = RangeUnionIterator.builder();
        search.values().stream()
                       .filter(keys -> !keys.isEmpty())
                       .forEach(keys -> builder.add(new KeyRangeIterator(keys)));

        return builder.build();
    }
}
