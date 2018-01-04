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

import com.carrotsearch.hppc.ObjectOpenHashSet;
import com.carrotsearch.hppc.ObjectSet;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.Pair;

import com.carrotsearch.hppc.LongOpenHashSet;
import com.carrotsearch.hppc.LongSet;
import com.carrotsearch.hppc.cursors.LongCursor;

public class DynamicTokenTreeBuilder extends AbstractTokenTreeBuilder
{
    private final SortedMap<Long, ObjectSet<TokenTreeEntry>> tokens = new TreeMap();

    public DynamicTokenTreeBuilder()
    {}

    public DynamicTokenTreeBuilder(TokenTreeBuilder data)
    {
        add(data);
    }

    public DynamicTokenTreeBuilder(SortedMap<Long, ObjectSet<TokenTreeEntry>> data)
    {
        add(data);
    }

    public void add(Long token, long partitionPosition, Clustering clustering, long rowPosition)
    {
        TokenTreeEntry entry;
        if (clustering.equals(Clustering.EMPTY))
            entry = new TokenTreeEntry.PartitionOnly(partitionPosition);
        else if (clustering.kind() == ClusteringPrefix.Kind.STATIC_CLUSTERING)
            entry = new TokenTreeEntry.PartitionWithStaticRow(partitionPosition);
        else
            entry = new TokenTreeEntry.PartitionAndClusterings(partitionPosition, clustering, rowPosition);

        add(token, entry);
    }

    public void add(Long token, TokenTreeEntry entry) {
        ObjectSet<TokenTreeEntry> found = tokens.get(token);
        if (found == null)
            tokens.put(token, (found = new ObjectOpenHashSet(2)));

        // TODO (jwest): this is broken because it doesn't merge entries
        found.add(entry);
    }

    public void add(Iterator<Pair<Long, ObjectSet<TokenTreeEntry>>> data)
    {
        while (data.hasNext())
        {
            Pair<Long, ObjectSet<TokenTreeEntry>> entry = data.next();
            for (ObjectCursor<TokenTreeEntry> c : entry.right)
                add(entry.left, c.value);
        }
    }

    public void add(SortedMap<Long, ObjectSet<TokenTreeEntry>> data)
    {
        for (Map.Entry<Long, ObjectSet<TokenTreeEntry>> newEntry : data.entrySet())
        {
            ObjectSet<TokenTreeEntry> found = tokens.get(newEntry.getKey());
            if (found == null)
                tokens.put(newEntry.getKey(), (found = new ObjectOpenHashSet<>(newEntry.getValue().size())));

            for (ObjectCursor<TokenTreeEntry> cursor : newEntry.getValue())
                found.add(cursor.value);
        }
    }

    public Iterator<Pair<Long, ObjectSet<TokenTreeEntry>>> iterator()
    {
        final Iterator<Map.Entry<Long, ObjectSet<TokenTreeEntry>>> iterator = tokens.entrySet().iterator();
        return new AbstractIterator<Pair<Long, ObjectSet<TokenTreeEntry>>>()
        {
            protected Pair<Long, ObjectSet<TokenTreeEntry>> computeNext()
            {
                if (!iterator.hasNext())
                    return endOfData();

                Map.Entry<Long, ObjectSet<TokenTreeEntry>> entry = iterator.next();
                return Pair.create(entry.getKey(), entry.getValue());
            }
        };
    }

    public boolean isEmpty()
    {
        return tokens.size() == 0;
    }

    // TODO (jwest): this implementation is O(number of entries) but can we add up the sizes while entries are being added (if careful to handle merging)
    protected int serializedDataSize()
    {
        int size = 0;
        for (Map.Entry<Long, ObjectSet<TokenTreeEntry>> tokenAndEntries : tokens.entrySet()) {
            for (ObjectCursor<TokenTreeEntry> entry : tokenAndEntries.getValue()) {
                size += entry.value.serializedSize();
            }
        }

        return size;
    }

    protected void writeData(DataOutputPlus out) throws IOException
    {
        for (Map.Entry<Long, ObjectSet<TokenTreeEntry>> tokenAndEntries : tokens.entrySet()) {
            for (ObjectCursor<TokenTreeEntry> entry : tokenAndEntries.getValue()) {
                if (!entry.value.packableOffset().isPresent()) {
                    entry.value.write(out);
                }

            }
        }
    }


    protected void constructTree()
    {
        tokenCount = tokens.size();
        treeMinToken = tokens.firstKey();
        treeMaxToken = tokens.lastKey();
        numBlocks = 1;

        // special case the tree that only has a single block in it (so we don't create a useless root)
        if (tokenCount <= TOKENS_PER_BLOCK)
        {
            leftmostLeaf = new DynamicLeaf(tokens);
            rightmostLeaf = leftmostLeaf;
            root = leftmostLeaf;
        }
        else
        {
            root = new InteriorNode();
            rightmostParent = (InteriorNode) root;

            int i = 0;
            Leaf lastLeaf = null;
            Long firstToken = tokens.firstKey();
            Long finalToken = tokens.lastKey();
            Long lastToken;
            for (Long token : tokens.keySet())
            {
                if (i == 0 || (i % TOKENS_PER_BLOCK != 0 && i != (tokenCount - 1)))
                {
                    i++;
                    continue;
                }

                lastToken = token;
                Leaf leaf = (i != (tokenCount - 1) || token.equals(finalToken)) ?
                        new DynamicLeaf(tokens.subMap(firstToken, lastToken)) : new DynamicLeaf(tokens.tailMap(firstToken));

                if (i == TOKENS_PER_BLOCK)
                    leftmostLeaf = leaf;
                else
                    lastLeaf.next = leaf;

                rightmostParent.add(leaf);
                lastLeaf = leaf;
                rightmostLeaf = leaf;
                firstToken = lastToken;
                i++;
                numBlocks++;

                if (token.equals(finalToken))
                {
                    Leaf finalLeaf = new DynamicLeaf(tokens.tailMap(token));
                    lastLeaf.next = finalLeaf;
                    rightmostParent.add(finalLeaf);
                    rightmostLeaf = finalLeaf;
                    numBlocks++;
                }
            }

        }
    }

    private class DynamicLeaf extends Leaf
    {
        private final SortedMap<Long, ObjectSet<TokenTreeEntry>> tokens;

        DynamicLeaf(SortedMap<Long, ObjectSet<TokenTreeEntry>> data)
        {
            super(data.firstKey(), data.lastKey());
            tokens = data;
        }

        public int tokenCount()
        {
            return tokens.size();
        }

        public boolean isSerializable()
        {
            return true;
        }

        protected long serializeData(ByteBuffer buf, long curDataOffset)
        {
            long offset = curDataOffset;
            for (Map.Entry<Long, ObjectSet<TokenTreeEntry>> entry : tokens.entrySet())
            {
                LeafEntry le = createEntry(entry.getKey(), entry.getValue(), offset);
                offset += le.dataNeeded();
                le.serialize(buf);
            }

            // TODO (jwest): would it be better to just change to return the new offset?
            return (offset - curDataOffset);
        }

    }
}
