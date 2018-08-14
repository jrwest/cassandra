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
package org.apache.cassandra.index.sasi.disk;

import java.util.Comparator;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import com.google.common.primitives.Longs;

import com.carrotsearch.hppc.ObjectSet;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.index.sasi.utils.CombinedValue;

import com.carrotsearch.hppc.LongSet;
import org.apache.cassandra.schema.TableMetadata;

public abstract class IndexEntry implements CombinedValue<Long>, Iterable<IndexEntry.EntryData>
{
    protected final long token;

    public IndexEntry(long token)
    {
        this.token = token;
    }

    public Long get()
    {
        return token;
    }

    public abstract TokenTreeBuilder.Entries getOffsets();

    public int compareTo(CombinedValue<Long> o)
    {
        return Longs.compare(token, ((IndexEntry) o).token);
    }

    public static class EntryData
    {
        public static Comparator<EntryData> comparator = new Comparator<EntryData>()
        {
            public int compare(EntryData o1, EntryData o2)
            {
                return DecoratedKey.comparator.compare(o1.key, o2.key);
            }
        };

        public final DecoratedKey key;
        private NavigableSet<Clustering> clusterings;

        public EntryData(DecoratedKey key, NavigableSet<Clustering> clusterings)
        {
            this.key = key;
            this.clusterings = clusterings;
        }

        public SinglePartitionReadCommand readCommand(TableMetadata metadata,
                                                      PartitionRangeReadCommand command)
        {
            ClusteringIndexFilter filter = readsWholePartition() ? new ClusteringIndexSliceFilter(Slices.ALL, false)
                                                                 : new ClusteringIndexNamesFilter(selectableClusterings(command), false);

            return SinglePartitionReadCommand.create(metadata,
                                                     command.nowInSec(),
                                                     command.columnFilter(),
                                                     command.rowFilter(),
                                                     command.limits(),
                                                     key,
                                                     filter);
        }

        public NavigableSet<Clustering> clusterings()
        {
            return clusterings;
        }

        public void addClusterings(EntryData other)
        {
            // if one side doesn't have clusterings we have to read the whole partition
            if (clusterings == null)
                return;
            else if (other.clusterings == null)
                clusterings = null;
            else
                clusterings.addAll(other.clusterings);
        }

        private boolean readsWholePartition()
        {
            return clusterings == null ||
                   clusterings.stream().anyMatch(c -> c.kind() == ClusteringPrefix.Kind.STATIC_CLUSTERING);
        }

        private NavigableSet<Clustering> selectableClusterings(PartitionRangeReadCommand command)
        {
            return clusterings.stream()
                              .filter(c -> command.selectsClustering(key, c))
                              .collect(() -> new TreeSet<>(clusterings.comparator()),
                                       (s,c) -> s.add(c),
                                       (s1,s2) -> s1.addAll(s2));
        }
    }
}
