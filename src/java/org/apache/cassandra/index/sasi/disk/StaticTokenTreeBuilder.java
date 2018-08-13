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
import java.util.Iterator;
import java.util.SortedMap;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.index.sasi.utils.CombinedTerm;
import org.apache.cassandra.index.sasi.utils.RangeIterator;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.Pair;

import com.google.common.collect.Iterators;

/**
 * Intended usage of this class is to be used in place of {@link DynamicTokenTreeBuilder}
 * when multiple index segments produced by {@link PerSSTableIndexWriter} are stitched together
 * by {@link PerSSTableIndexWriter#complete()}.
 *
 * This class uses the RangeIterator, now provided by
 * {@link CombinedTerm#getEntryIterator()}, to iterate the data twice.
 * The first iteration builds the tree with leaves that contain only enough
 * information to build the upper layers -- these leaves do not store more
 * than their minimum and maximum tokens plus their total size, which makes them
 * un-serializable.
 *
 * When the tree is written to disk the final layer is not
 * written. Its at this point the data is iterated once again to write
 * the leaves to disk. This (logarithmically) reduces copying of the
 * token values while building and writing upper layers of the tree,
 * removes the use of SortedMap when combining SAs, and relies on the
 * memory mapped SAs otherwise, greatly improving performance and no
 * longer causing OOMs when TokenTree sizes are big.
 *
 * See https://issues.apache.org/jira/browse/CASSANDRA-11383 for more details.
 */
@SuppressWarnings("resource")
public class StaticTokenTreeBuilder extends AbstractTokenTreeBuilder
{
    private final CombinedTerm combinedTerm;
    private int dataLayerSize;

    public StaticTokenTreeBuilder(CombinedTerm term)
    {
        super(term.clusteringComparator());
        combinedTerm = term;
        dataLayerSize = 0;
    }

    public void add(Long token, long partitionKeyPosition, Clustering clustering)
    {
        throw new UnsupportedOperationException();
    }

    public void add(SortedMap<Long, Entries> data)
    {
        throw new UnsupportedOperationException();
    }

    public void add(Iterator<Pair<Long, Entries>> data)
    {
        throw new UnsupportedOperationException();
    }

    public boolean isEmpty()
    {
        return tokenCount == 0;
    }

    public Iterator<Pair<Long, Entries>> iterator()
    {
        Iterator<IndexEntry> iterator = combinedTerm.getEntryIterator();
        return new AbstractIterator<Pair<Long, Entries>>()
        {
            protected Pair<Long, Entries> computeNext()
            {
                if (!iterator.hasNext())
                    return endOfData();

                IndexEntry entry = iterator.next();
                return Pair.create(entry.get(), entry.getOffsets());
            }
        };
    }

    public long getTokenCount()
    {
        return tokenCount;
    }

    @Override
    public void writeTree(DataOutputPlus out) throws IOException
    {
        // if the root is not a leaf then none of the leaves have been written (all are PartialLeaf)
        // so write out the last layer of the tree by converting PartialLeaf to StaticLeaf and
        // iterating the data once more
        super.writeTree(out);
        if (root.isLeaf())
            return;

        RangeIterator<Long, IndexEntry> entries = combinedTerm.getEntryIterator();
        ByteBuffer blockBuffer = ByteBuffer.allocate(BLOCK_BYTES);
        Iterator<Node> leafIterator = leftmostLeaf.levelIterator();
        long dataLayerOffset = 0;
        while (leafIterator.hasNext())
        {
            Leaf leaf = (Leaf) leafIterator.next();
            Leaf writeableLeaf = new StaticLeaf(Iterators.limit(entries, leaf.tokenCount()), leaf);
            dataLayerOffset += writeableLeaf.serialize(-1, dataLayerOffset, blockBuffer);
            flushBuffer(blockBuffer, out, true);
        }

    }

    protected Iterator<Entries> entriesIterator()
    {
        return Iterators.transform(combinedTerm.getEntryIterator(), e -> e.getOffsets());
    }

    protected int dataLayerSize()
    {
        return dataLayerSize;
    }

    protected void constructTree()
    {
        RangeIterator<Long, IndexEntry> entries = combinedTerm.getEntryIterator();

        tokenCount = 0;
        dataLayerSize = 0;
        treeMinToken = entries.getMinimum();
        treeMaxToken = entries.getMaximum();
        numBlocks = 1;

        root = new InteriorNode();
        rightmostParent = (InteriorNode) root;
        Leaf lastLeaf = null;
        Long lastToken, firstToken = null;
        int leafSize = 0;
        while (entries.hasNext())
        {
            IndexEntry entry = entries.next();
            Long token = entry.get();
            if (firstToken == null)
                firstToken = token;

            dataLayerSize += entry.getOffsets().serializedSize();
            tokenCount++;
            leafSize++;


            // skip until the last token in the leaf
            if (tokenCount % TOKENS_PER_BLOCK != 0 && token != treeMaxToken)
                continue;

            lastToken = token;
            Leaf leaf = new PartialLeaf(firstToken, lastToken, leafSize);
            if (lastLeaf == null) // first leaf created
                leftmostLeaf = leaf;
            else
                lastLeaf.next = leaf;


            rightmostParent.add(leaf);
            lastLeaf = rightmostLeaf = leaf;
            firstToken = null;
            numBlocks++;
            leafSize = 0;
        }

        // if the tree is really a single leaf the empty root interior
        // node must be discarded
        if (root.tokenCount() == 0)
        {
            numBlocks = 1;
            root = new StaticLeaf(combinedTerm.getEntryIterator(), treeMinToken, treeMaxToken, tokenCount, true);
        }
    }

    // This denotes the leaf which only has min/max and token counts
    // but doesn't have any associated data yet, so it can't be serialized.
    private class PartialLeaf extends Leaf
    {
        private final int size;
        public PartialLeaf(Long min, Long max, int count)
        {
            super(min, max);
            size = count;
        }

        public int tokenCount()
        {
            return size;
        }

        public long serializeData(ByteBuffer buf, long dataLayerOffset)
        {
            throw new UnsupportedOperationException();
        }

        public boolean isSerializable()
        {
            return false;
        }
    }

    // This denotes the leaf which has been filled with data and is ready to be serialized
    private class StaticLeaf extends Leaf
    {
        private final Iterator<IndexEntry> entries;
        private final int count;
        private final boolean isLast;

        public StaticLeaf(Iterator<IndexEntry> entries, Leaf leaf)
        {
            this(entries, leaf.smallestToken(), leaf.largestToken(), leaf.tokenCount(), leaf.isLastLeaf());
        }

        public StaticLeaf(Iterator<IndexEntry> entries, Long min, Long max, long count, boolean isLastLeaf)
        {
            super(min, max);

            this.count = (int) count; // downcast is safe since leaf size is always < Integer.MAX_VALUE
            this.entries = entries;
            this.isLast = isLastLeaf;
        }

        public boolean isLastLeaf()
        {
            return isLast;
        }

        public int tokenCount()
        {
            return count;
        }

        public long serializeData(ByteBuffer buf, long dataLayerOffset)
        {
            long offset = dataLayerOffset;
            while (entries.hasNext())
            {
                IndexEntry entry = entries.next();
                LeafEntry le = createEntry(entry.get(), entry.getOffsets(), offset);
                offset += le.dataNeeded();
                le.serialize(buf);
            }

            return offset - dataLayerOffset;
        }

        public boolean isSerializable()
        {
            return true;
        }
    }
}
