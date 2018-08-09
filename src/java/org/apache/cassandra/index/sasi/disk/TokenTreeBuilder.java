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
import java.util.*;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.Pair;

import com.carrotsearch.hppc.ObjectSet;

public interface TokenTreeBuilder extends Iterable<Pair<Long, ObjectSet<TokenTreeBuilder.Entry>>>
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
    final byte ENTRY_TYPE_MASK = 0x03;
    final short ENTRY_DATA_MASK = 0x0FF0;
    final byte ENTRY_DATA_SHIFT = 4;
    final short ENTRY_IS_PACKABLE = 0;
    final short ENTRY_NOT_PACKABLE = 1;

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

    default void add(Long token, long position)
    {
        add(token, new Entry(position));
    }

    void add(Long token, Entry entry);
    void add(SortedMap<Long, ObjectSet<Entry>> data);
    void add(Iterator<Pair<Long, ObjectSet<Entry>>> data);
    void add(TokenTreeBuilder ttb);

    boolean isEmpty();
    long getTokenCount();

    TokenTreeBuilder finish();

    int serializedSize();
    void write(DataOutputPlus out) throws IOException;

    public static class Entry
    {
        private final long partitionOffset;

        public Entry(long partitionOffset)
        {
            this.partitionOffset = partitionOffset;
        }

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
            return OptionalLong.of(partitionOffset);
        }

        /**
         * @return the amount of bytes needed in the data layer for this entry. If {@link #packableOffset()} returns
         * any {@link OptionalLong} other than {@link OptionalLong#empty()} than this value will be 0.
         */
        public long serializedSize()
        {
            return 0;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;

            if (!(o instanceof Entry)) return false;

            Entry entry = (Entry) o;
            return entry.partitionOffset == partitionOffset;
        }

        public int hashCode()
        {
            return new HashCodeBuilder(17, 37)
                   .append(partitionOffset)
                   .toHashCode();
        }
    }
}