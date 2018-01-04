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

import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.LongSet;
import com.carrotsearch.hppc.cursors.LongObjectCursor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.Pair;

/**
 * A data entry in the token tree. Contains the offset
 * of a partition key in the index file and either the cluster values
 * or the offsets of the rows in the data file.
 */
public interface TokenTreeEntry
{

    /**
     * @return an offset in an sstable index file pointing
     * to the start of a RowIndexEntry
     */
    public long getPartitionOffset();

    /**
     * Merge with another entry
     */
    public void merge(TokenTreeEntry other);

    /**
     * Return an iterator over the rows stored in the entry
     */
    public Iterator<Pair<Long, Clustering>> rowIterator();

    public abstract static class AbstractEntry implements TokenTreeEntry {

        protected final long partitionOffset;

        public AbstractEntry(final long partitionOffset)
        {
            this.partitionOffset = partitionOffset;
        }

        public long getPartitionOffset()
        {
            return partitionOffset;
        }

        public Iterator<Pair<Long, Clustering>> rowIterator()
        {
            return Collections.emptyIterator();
        }

        public final int hashCode()
        {
            return new HashCodeBuilder(17, 37)
                   .append(partitionOffset)
                   .toHashCode();
        }

    }

    public static class PartitionOnly extends AbstractEntry
    {
        public PartitionOnly(final long partitionOffset)
        {
            super(partitionOffset);
        }

        public void merge(TokenTreeEntry other)
        {
            // nothing to do here because we assume that these
            // must be for the same partition offset
            assert other instanceof PartitionOnly;
            assert other.getPartitionOffset() == getPartitionOffset();
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            PartitionOnly that = (PartitionOnly) o;

            return new EqualsBuilder()
                   .append(partitionOffset, that.getPartitionOffset())
                   .isEquals();
        }

    }

    public static class PartitionWithStaticRow extends AbstractEntry
    {

        public PartitionWithStaticRow(final long partitionOffset)
        {
            super(partitionOffset);
        }

        public void merge(TokenTreeEntry other)
        {
            // nothing to do here because we assume that these
            // must be for the same partition offset
            assert other instanceof PartitionWithStaticRow;
            assert other.getPartitionOffset() == getPartitionOffset();
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            PartitionWithStaticRow that = (PartitionWithStaticRow) o;

            return new EqualsBuilder()
                   .append(partitionOffset, that.getPartitionOffset())
                   .isEquals();
        }
    }

    public static class PartitionAndClusterings extends AbstractEntry
    {

        private final LongObjectOpenHashMap<Clustering> rows;

        public PartitionAndClusterings(final long partitionOffset)
        {
            super(partitionOffset);
            this.rows = new LongObjectOpenHashMap<>();
        }

        public PartitionAndClusterings(final long partitionOffset, final Clustering rowKey, final long rowOffset)
        {
            super(partitionOffset);
            this.rows = new LongObjectOpenHashMap<Clustering>()
            {{
                put(rowOffset, rowKey);
            }};
        }

        public long getPartitionOffset()
        {
            return partitionOffset;
        }

        // TODO (jwest): be able to merge with PartitionsAndOffsets
        public void merge(TokenTreeEntry otherEntry)
        {
            assert otherEntry instanceof PartitionAndClusterings;

            PartitionAndClusterings other = (PartitionAndClusterings) otherEntry;
            for (LongObjectCursor<Clustering> l : other.rows)
                // TODO (jwest): is put if absent correct here? do we need to throw if the values are not equal? how to handle one missing/one existing?
                rows.putIfAbsent(l.key, l.value);
        }

        public Iterator<Pair<Long, Clustering>> rowIterator()
        {
            final Iterator<LongObjectCursor<Clustering>> inner = rows.iterator();
            return new AbstractIterator<Pair<Long, Clustering>>()
            {
                protected Pair<Long, Clustering> computeNext()
                {
                    if (!inner.hasNext())
                        return endOfData();

                    LongObjectCursor<Clustering> row = inner.next();
                    return Pair.create(row.key, row.value);
                }
            };
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            PartitionAndClusterings that = (PartitionAndClusterings) o;

            return new EqualsBuilder()
                   .append(partitionOffset, that.getPartitionOffset())
                   .append(rows, that.rows)
                   .isEquals();
        }

    }
}