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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.Iterators;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.carrotsearch.hppc.ObjectOpenHashSet;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.index.sasi.utils.MappedBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.Pair;

import com.carrotsearch.hppc.ObjectSet;

public interface TokenTreeBuilder extends Iterable<Pair<Long, TokenTreeBuilder.Entries>>
{
    final int BLOCK_BYTES = 4096;
    final int BLOCK_HEADER_BYTES = 64;
    final int BLOCK_ENTRY_BYTES = 2 * Long.BYTES;
    final int OVERFLOW_TRAILER_BYTES = 64;
    final int OVERFLOW_ENTRY_BYTES = Long.BYTES;
    final int OVERFLOW_TRAILER_CAPACITY = OVERFLOW_TRAILER_BYTES / OVERFLOW_ENTRY_BYTES;
    final int TOKENS_PER_BLOCK = (BLOCK_BYTES - BLOCK_HEADER_BYTES - OVERFLOW_TRAILER_BYTES) / BLOCK_ENTRY_BYTES;
    final long MAX_OFFSET = (1L << 47) - 1; // 48 bits for (signed) offset
    final byte LAST_LEAF_SHIFT = 1;
    final byte SHARED_HEADER_BYTES = 19;
    final byte ENTRY_TYPE_MASK = 0x0F;
    final byte ENTRY_DATA_SHIFT = 4;
    final byte ENTRY_DATA_MASK = 0x10;
    final short ENTRY_IS_PACKABLE = 0;
    final short ENTRY_NOT_PACKABLE = 1;
    final short ENTRY_UNPACKED_SIZE = Long.BYTES;
    final long ENTRY_UNPACKED_ZERO_SENTINEL = Long.MIN_VALUE;
    final int ENTRY_STATIC_CLUSTERING_SENTINEL = Integer.MIN_VALUE;
    final int ENTRY_MAX_CLUSTERING_SIZE = BLOCK_BYTES;

    // note: ordinal positions are used here, do not change order
    enum EntryType
    {
        SIMPLE, FACTORED, PACKED, OVERFLOW;

        public static EntryType of(int ordinal)
        {
            if (ordinal == SIMPLE.ordinal())
                return SIMPLE;

            if (ordinal == FACTORED.ordinal())
                return FACTORED;

            if (ordinal == PACKED.ordinal())
                return PACKED;

            if (ordinal == OVERFLOW.ordinal())
                return OVERFLOW;

            throw new IllegalArgumentException("Unknown ordinal: " + ordinal);
        }
    }

    ClusteringComparator clusteringComparator();
    void add(Long token, long partitionKeyPosition, Clustering clustering);
    void add(TokenTreeBuilder ttb);

    boolean isEmpty();
    long getTokenCount();

    TokenTreeBuilder finish();

    int serializedSize();
    void write(DataOutputPlus out) throws IOException;

    public static class Entries implements Iterable<Entry>
    {
        private final ClusteringComparator clusteringComparator;
        private final ObjectSet<Entry> entries;
        private boolean isPackable = true;
        private int serializedSize  = 0;

        public Entries(ClusteringComparator clusteringComparator)
        {
            this(new ObjectOpenHashSet<>(2), clusteringComparator);
        }

        public Entries(ObjectSet<Entry> es, ClusteringComparator comparator)
        {
            entries = es;
            clusteringComparator = comparator;
            isPackable = true;
            for (ObjectCursor<Entry> entry : entries)
            {
                boolean entryPackable = entry.value.packableOffset().isPresent();

                // tracks serialized size assuming isPackable may become false in the future, otherwise
                // know we know if isPackable = true, serializedSize = 0, depsite what the value of the variable
                // actually is
                serializedSize = entryPackable ? ENTRY_UNPACKED_SIZE : entry.value.serializedSize();
                isPackable &= entryPackable;
            }
        }

        public ObjectSet<Entry> getEntries()
        {
            return entries;
        }

        public void add(long partitionKeyPosition)
        {
            add(partitionKeyPosition, Clustering.EMPTY);
        }

        public void add(long partitionKeyPosition, Clustering clustering)
        {
            for (Entry e : this)
            {
                // if this is the same partition offset then its not a collision
                // add the clustering (there is a new row indexed for a partition
                // that the builder is already aware of)
                if (e.partitionOffset == partitionKeyPosition)
                {
                    // if the clustering is non-empty then add it to the entry
                    // otherwise there is nothing to do
                    if (clustering != Clustering.EMPTY)
                        serializedSize += e.addClustering(clustering);

                    return;
                }

            }

            // if we've reached this point this is either the first entry or a collision
            Entry e =  (Clustering.EMPTY == clustering) ? new Entry(partitionKeyPosition)
                                                        : new Entry(partitionKeyPosition, clusteringComparator, clustering);

            isPackable &= e.packableOffset().isPresent();
            serializedSize += e.packableOffset().isPresent() ? ENTRY_UNPACKED_SIZE : e.serializedSize();
            entries.add(e);
        }

        public void add(Entry entry)
        {
            for (Entry ours : this)
            {
                if (ours.partitionOffset == entry.partitionOffset)
                {
                    // nothing to do here if both are packable and the same partition position
                    if (ours.packableOffset().isPresent() && entry.packableOffset().isPresent())
                        return;

                    // for the same partition its expected that both entries are packable or not
                    if ((ours.packableOffset().isPresent() && !entry.packableOffset().isPresent()) ||
                        !ours.packableOffset().isPresent() && entry.packableOffset().isPresent())
                        throw new IllegalStateException("same partition, different types of clustering");

                    serializedSize += ours.addClustering(entry);
                    return;
                }
            }

            // if we've reached this point the entry is either the first or a collision
            entries.add(entry);
            serializedSize += entry.packableOffset().isPresent() ? ENTRY_UNPACKED_SIZE : entry.serializedSize();
            isPackable &= entry.packableOffset().isPresent();
        }

        public void add(Entries es)
        {
            es.forEach(this::add);
        }

        public int serializedSize()
        {
            return isPackable ? 0 : serializedSize;
        }

        public boolean isPackable()
        {
            return isPackable;
        }

        public Iterator<Entry> iterator()
        {
            return Iterators.transform(entries.iterator(), oc -> oc.value);
        }

        public int size()
        {
            return entries.size();
        }

        public void write(DataOutputPlus out) throws IOException
        {
            // all entries are packable so nothing is written to the data layer for these entries
            if (isPackable)
                return;

            // The number of entries isn't written because when being reconstructed its not needed.
            // Each individual entry is read given its offset (since the common case is one entry its better
            // to not waste the space
            for (Entry e : this)
                e.write(out);
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;

            if (!(o instanceof Entries)) return false;

            Entries entries1 = (Entries) o;

            return new EqualsBuilder().append(entries, entries1.entries)
                                      .append(isPackable, entries1.isPackable)
                                      .append(serializedSize, entries1.serializedSize)
                                      .isEquals();
        }

        public int hashCode()
        {
            return new HashCodeBuilder(17, 37)
                   .append(entries)
                   .toHashCode();
        }
    }

    public static class Entry
    {
        private final long partitionOffset;
        private final ClusteringComparator clusteringComparator;
        private final NavigableSet<Clustering> clusterings;

        public static Entry fromFile(MappedBuffer buffer, ClusteringComparator clusteringComparator, long position)
        {
            long origPos = buffer.position();
            long partitionOffset = buffer.position(position).getLong();
            if (partitionOffset < 0) // packable offset that was unpacked because of collisions
            {
                buffer.position(origPos);
                return (partitionOffset == ENTRY_UNPACKED_ZERO_SENTINEL) ? new Entry(0)
                                                                         : new Entry(partitionOffset * -1);
            }

            int numClusterings = buffer.getInt();
            NavigableSet<Clustering> clusterings = new TreeSet<>(clusteringComparator);
            for (int i = 0; i < numClusterings; i++)
            {
                int clusteringSize = buffer.getInt();
                if (clusteringSize == ENTRY_STATIC_CLUSTERING_SENTINEL)
                {
                    clusterings.add(Clustering.STATIC_CLUSTERING);
                } else
                {
                    ByteBuffer clusteringBytes = buffer.getPageRegion(buffer.position(), clusteringSize);
                    buffer.position(buffer.position() + clusteringSize);
                    clusterings.add(Clustering.serializer.deserialize(clusteringBytes, 0, clusteringComparator.subtypes()));
                }
            }

            buffer.position(origPos);
            return new Entry(partitionOffset, clusteringComparator, clusterings);
        }

        public Entry(long partitionOffset)
        {
            this(partitionOffset, null, (NavigableSet<Clustering>) null);
        }

        public Entry(long partitionOffset, ClusteringComparator clusteringComparator, Clustering clustering)
        {
            this(partitionOffset, clusteringComparator, new TreeSet<>(clusteringComparator));
            ensureValidSize(clustering);
            clusterings.add(clustering);
        }

        public Entry(long partitionOffset, ClusteringComparator clusteringComparator, NavigableSet<Clustering> clusterings)
        {
            assert (clusterings == null && clusteringComparator == null) ||
                   clusteringComparator.equals(clusterings.comparator()) : "clustering comparators must be equal";

            this.partitionOffset = partitionOffset;
            this.clusteringComparator = clusteringComparator;
            this.clusterings = clusterings;


            if (clusterings != null)
                for (Clustering c : clusterings)
                    ensureValidSize(c);
        }

        /**
         * @return The position of the partition key in the sstable's partition key offset
         */
        public long partitionOffset()
        {
            return partitionOffset;
        }

        /**
         * @return {@link OptionalLong#empty()} if the data represented by this entry requires space in the data layer
         * of the tree. Otherwise, return a {@link OptionalLong}, whose value is the offset of the partition key in the
         * primary index, that can be packed into the index layer of the tree.
         */
        public OptionalLong packableOffset()
        {
            return clusterings == null ? OptionalLong.of(partitionOffset) : OptionalLong.empty();
        }

        public int addClustering(Clustering clustering)
        {
            if (clusterings == null)
                throw new IllegalStateException("cannot add clusterings to entry that doesn't have any");

            if (clusterings.add(clustering))
            {
                long size = clusteringSize(clustering);
                ensureValidSize(size);
                return (int) size;
            } else {
                return 0;
            }
        }

        public int addClustering(Entry other)
        {
            int size = 0;
            for (Clustering c: other.clusterings)
                size += clusterings.add(c) ? clusteringSize(c) : 0;

            return size;
        }

        public int serializedSize()
        {
            if (clusterings == null)
                return 0;

            int size = Long.BYTES + Integer.BYTES; // partition offset + clustering count
            for (Clustering c : clusterings)
            {
                long cs = clusteringSize(c);
                ensureValidSize(cs);
                size += (int) clusteringSize(c);
            }

            return size;
        }

        private void ensureValidSize(Clustering c)
        {
            ensureValidSize(clusteringSize(c));
        }

        private void ensureValidSize(long size)
        {
            if (size > ENTRY_MAX_CLUSTERING_SIZE)
                throw new IllegalStateException(String.format("Clustering of size %d greater than max allowed size %d",
                                                              size, ENTRY_MAX_CLUSTERING_SIZE));
        }

        private long clusteringSize(Clustering c)
        {
            // clustering size + clustering bytes

            return Clustering.STATIC_CLUSTERING == c ? Integer.BYTES
                                                     : Integer.BYTES + Clustering.serializer.serializedSize(c, 0, clusteringComparator.subtypes());
        }

        public void write(DataOutputPlus out) throws IOException
        {
            // if an entry that could be packed is asked to be written then there were collisions
            // and the packed entry was "downgraded" to an unpacked one. In that case, it needs to be
            // distinguishable from other entries. To do so, the fact that offsets are always positive is
            // exploited. To represent 0, we use Long.MIN_VALUE since abs(Long.MIN_VALUE) > MAX_OFFSET.
            if (packableOffset().isPresent())
            {
                out.writeLong(partitionOffset == 0 ? ENTRY_UNPACKED_ZERO_SENTINEL : partitionOffset * -1);
            }
            else
            {
                out.writeLong(partitionOffset);
                out.writeInt(clusterings.size());
                for (Clustering clustering : clusterings)
                {
                    if (Clustering.STATIC_CLUSTERING == clustering)
                    {
                        // static clustering's can't be serialized so we indicate that we are pointing to one
                        // with a flag.
                        out.writeInt(ENTRY_STATIC_CLUSTERING_SENTINEL);
                    } else
                    {
                        out.writeInt((int) Clustering.serializer.serializedSize(clustering, 0, clusteringComparator.subtypes()));
                        Clustering.serializer.serialize(clustering, out, 0, clusteringComparator.subtypes());
                    }
                }

            }

        }

        public boolean equals(Object o)
        {
            if (this == o) return true;

            if (!(o instanceof Entry)) return false;

            Entry entry = (Entry) o;

            return new EqualsBuilder()
                   .append(partitionOffset, entry.partitionOffset)
                   .append(clusterings, entry.clusterings)
                   .isEquals();
        }

        public int hashCode()
        {
            return new HashCodeBuilder(17, 37)
                   .append(partitionOffset)
                   .toHashCode();
        }
    }
}