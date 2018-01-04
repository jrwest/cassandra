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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyclustering ownership.  The ASF licenses this file
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

// TODO (jwest): move out of this package along with KeyFetcher?
package org.apache.cassandra.index.sasi.disk;

import java.util.Arrays;
import java.util.Comparator;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.HashCodeBuilder;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Primary key of the found row, a combination of the Partition Key
 * and clustering that belongs to the row.
 */
public class IndexedRow implements Comparable<IndexedRow>
{

    private final DecoratedKey partitionKey;
    private final Clustering clustering;
    private final ClusteringComparator comparator;

    public IndexedRow(DecoratedKey primaryKey, Clustering clustering, ClusteringComparator comparator)
    {
        this.partitionKey = primaryKey;
        this.clustering = clustering;
        this.comparator = comparator;
    }

    public DecoratedKey partitionKey()
    {
        return partitionKey;
    }

    public Clustering clustering()
    {
        return clustering;
    }

    public ClusteringComparator comparator()
    {
        return comparator;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexedRow indexedRow = (IndexedRow) o;

        if (partitionKey != null ? !partitionKey.equals(indexedRow.partitionKey) : indexedRow.partitionKey != null)
            return false;

        return clustering != null ? comparator.compare(clustering, indexedRow.clustering) == 0 : indexedRow.clustering == null;
    }

    public int hashCode()
    {
        return new HashCodeBuilder().append(partitionKey).append(clustering).toHashCode();
    }

    public int compareTo(IndexedRow other)
    {
        int cmp = this.partitionKey.compareTo(other.partitionKey);
        if (cmp == 0 && clustering != null)
        {
            // Both clustering and rows should match
            if (clustering.kind() == ClusteringPrefix.Kind.STATIC_CLUSTERING || other.clustering.kind() == ClusteringPrefix.Kind.STATIC_CLUSTERING)
                return 0;

            return comparator.compare(this.clustering, other.clustering);
        }
        else
        {
            return cmp;
        }
    }

    public static IndexedRowComparator COMPARATOR = new IndexedRowComparator();

    public String toString(TableMetadata metadata)
    {
        return String.format("IndexedRow: { pk : %s, clustering: %s}",
                             metadata.partitionKeyType.getString(partitionKey.getKey()),
                             clustering.toString(metadata));
    }

    @Override
    public String toString()
    {
        return String.format("IndexedRow: { pk : %s, clustering: %s}",
                             ByteBufferUtil.bytesToHex(partitionKey.getKey()),
                             String.join(",", Arrays.stream(clustering.getRawValues()).map(ByteBufferUtil::bytesToHex).collect(Collectors.toList())));
    }

    private static class IndexedRowComparator implements Comparator<IndexedRow>
    {
        public int compare(IndexedRow o1, IndexedRow o2)
        {
            return o1.compareTo(o2);
        }
    }

}
