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

import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.Pair;

public class DynamicTokenTreeBuilder extends AbstractTokenTreeBuilder
{
    private final SortedMap<Long, Entries> tokens = new TreeMap<>();

    public DynamicTokenTreeBuilder(ClusteringComparator clusteringComparator)
    {
        super(clusteringComparator);
    }

    public DynamicTokenTreeBuilder(TokenTreeBuilder data)
    {
        this(data.clusteringComparator());
        add(data);
    }

    public DynamicTokenTreeBuilder(ClusteringComparator clusteringComparator, SortedMap<Long, Entries> data)
    {
        this(clusteringComparator);
        add(data);
    }

    public void add(Long token, long partitionKeyPosition)
    {
        Entries found = tokens.get(token);
        if (found == null)
            tokens.put(token, (found = new Entries(clusteringComparator)));

        found.add(partitionKeyPosition);
    }

    public void add(Iterator<Pair<Long, Entries>> data)
    {
        while (data.hasNext())
        {
            Pair<Long, Entries> item = data.next();
            Entries entries = tokens.get(item.left);
            if (entries == null)
                tokens.put(item.left, (entries = new Entries(clusteringComparator)));

            for (Entry e : item.right)
                entries.add(e);
        }
    }

    public void add(SortedMap<Long, Entries> data)
    {
        for (Map.Entry<Long, Entries> newEntries : data.entrySet())
        {
            Entries entries = tokens.get(newEntries.getKey());
            if (entries == null)
                tokens.put(newEntries.getKey(), (entries = new Entries(clusteringComparator)));

            for (Entry newEntry : newEntries.getValue())
                entries.add(newEntry);
        }
    }

    public Iterator<Pair<Long, Entries>> iterator()
    {
        final Iterator<Map.Entry<Long, Entries>> iterator = tokens.entrySet().iterator();
        return new AbstractIterator<Pair<Long, Entries>>()
        {
            protected Pair<Long, Entries> computeNext()
            {
                if (!iterator.hasNext())
                    return endOfData();

                Map.Entry<Long, Entries> tokenAndEntry = iterator.next();
                return Pair.create(tokenAndEntry.getKey(), tokenAndEntry.getValue());
            }
        };
    }

    public boolean isEmpty()
    {
        return tokens.size() == 0;
    }

    protected Iterator<Entries> entriesIterator()
    {
        return tokens.values().iterator();
    }

    protected int dataLayerSize()
    {
        int size = 0;
        for (Entries es : tokens.values())
            size += es.serializedSize();

        return size;
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
        private final SortedMap<Long, Entries> tokens;

        DynamicLeaf(SortedMap<Long, Entries> data)
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

        protected long serializeData(ByteBuffer buf, long dataLayerOffset)
        {
            long offset = dataLayerOffset;
            for (Map.Entry<Long, Entries> entry : tokens.entrySet())
            {
                LeafEntry le = createEntry(entry.getKey(), entry.getValue(), offset);
                offset += le.dataNeeded();
                le.serialize(buf);
            }

            return offset - dataLayerOffset;
        }


    }
}
