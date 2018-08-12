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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.cursors.LongCursor;

public abstract class AbstractTokenTreeBuilder implements TokenTreeBuilder
{
    protected final ClusteringComparator clusteringComparator;
    protected int numBlocks;
    protected Node root;
    protected InteriorNode rightmostParent;
    protected Leaf leftmostLeaf;
    protected Leaf rightmostLeaf;
    protected long tokenCount = 0;
    protected long treeMinToken;
    protected long treeMaxToken;

    public AbstractTokenTreeBuilder(ClusteringComparator clusteringComparator)
    {
        this.clusteringComparator = clusteringComparator;
    }

    public ClusteringComparator clusteringComparator()
    {
        return clusteringComparator;
    }

    public void add(TokenTreeBuilder other)
    {
        add(other.iterator());
    }

    public TokenTreeBuilder finish()
    {
        if (root == null)
            constructTree();

        return this;
    }

    public long getTokenCount()
    {
        return tokenCount;
    }

    public int serializedSize()
    {
        return indexLayerSize() + dataLayerSize();
    }

    protected abstract int dataLayerSize();
    protected int indexLayerSize()
    {
        if (numBlocks == 1)
            return BLOCK_HEADER_BYTES +
                   ((int) tokenCount * BLOCK_ENTRY_BYTES) +
                   (((Leaf) root).overflowCollisionCount() * OVERFLOW_ENTRY_BYTES);
        else
            return numBlocks * BLOCK_BYTES;
    }

    public void write(DataOutputPlus out) throws IOException
    {
        writeTree(out);
        writeData(out);
    }

    protected void writeTree(DataOutputPlus out) throws IOException
    {
        ByteBuffer blockBuffer = ByteBuffer.allocate(BLOCK_BYTES);
        Iterator<Node> levelIterator = root.levelIterator();
        long childBlockIndex = 1;
        long dataLayerOffset = 0;

        while (levelIterator != null)
        {
            Node firstChild = null;
            while (levelIterator.hasNext())
            {
                Node block = levelIterator.next();

                if (firstChild == null && !block.isLeaf())
                    firstChild = ((InteriorNode) block).children.get(0);

                if (block.isSerializable())
                {
                    dataLayerOffset += block.serialize(childBlockIndex, dataLayerOffset, blockBuffer);
                    flushBuffer(blockBuffer, out, numBlocks != 1);
                }

                childBlockIndex += block.childCount();
            }

            levelIterator = (firstChild == null) ? null : firstChild.levelIterator();
        }
    }

    protected void writeData(DataOutputPlus out) throws IOException
    {
        Iterator<Entries> entriesIterator = entriesIterator();
        while (entriesIterator.hasNext())
            entriesIterator.next().write(out);
    }

    protected abstract Iterator<Entries> entriesIterator();
    protected abstract void constructTree();

    protected void flushBuffer(ByteBuffer buffer, DataOutputPlus o, boolean align) throws IOException
    {
        // seek to end of last block before flushing
        if (align)
            alignBuffer(buffer, BLOCK_BYTES);

        buffer.flip();
        o.write(buffer);
        buffer.clear();
    }

    protected abstract class Node
    {
        protected InteriorNode parent;
        protected Node next;
        protected Long nodeMinToken, nodeMaxToken;

        public Node(Long minToken, Long maxToken)
        {
            nodeMinToken = minToken;
            nodeMaxToken = maxToken;
        }


        /***
         * @return True if the node can be serialized to a given ByteBuffer, otherwise false. If false,
         * {@link #serialize(long, long, ByteBuffer)} is not supported.
         */
        public abstract boolean isSerializable();

        /**
         * @param childBlockIndex The block index of this node's first child. This is only meaningful for nodes
         *                        that have children. The block index is level-based. For a root with N children,
         *                        the roots block index is 0, its children are 1 through N. Its first grandchild is
         *                        N+1, its second grandchild is N+2, and so on.
         * @param dataLayerOffset The current offset into the data layer of the tree. This offset might be encoded as a
         *                        pointer to the token
         * @param buf the buffer to serialize into
         * @see #isSerializable()
         * @return the number of bytes that will need to be consumed in the data layer of the tree when storing this block
         *
         */
        public abstract long serialize(long childBlockIndex, long dataLayerOffset, ByteBuffer buf);

        public abstract int childCount();

        public abstract int tokenCount();

        public Long smallestToken()
        {
            return nodeMinToken;
        }

        public Long largestToken()
        {
            return nodeMaxToken;
        }

        /**
         * @return An iterator that iterates over the current block and any blocks to the right of it
         * (those with greater tokens on the same level)
         */
        public Iterator<Node> levelIterator()
        {
            return new LevelIterator(this);
        }

        public boolean isLeaf()
        {
            return (this instanceof Leaf);
        }

        protected boolean isLastLeaf()
        {
            return this == rightmostLeaf;
        }

        protected boolean isRoot()
        {
            return this == root;
        }

        protected void updateTokenRange(long token)
        {
            nodeMinToken = nodeMinToken == null ? token : Math.min(nodeMinToken, token);
            nodeMaxToken = nodeMaxToken == null ? token : Math.max(nodeMaxToken, token);
        }

        protected void serializeHeader(ByteBuffer buf)
        {
            Header header;
            if (isRoot())
                header = new RootHeader();
            else if (!isLeaf())
                header = new InteriorNodeHeader();
            else
                header = new LeafHeader();

            header.serialize(buf);
            alignBuffer(buf, BLOCK_HEADER_BYTES);
        }

        private abstract class Header
        {
            public void serialize(ByteBuffer buf)
            {
                buf.put(infoByte())
                   .putShort((short) (tokenCount()))
                   .putLong(nodeMinToken)
                   .putLong(nodeMaxToken);
            }

            protected abstract byte infoByte();
        }

        private class RootHeader extends Header
        {
            public void serialize(ByteBuffer buf)
            {
                super.serialize(buf);
                writeMagic(buf);
                buf.putLong(tokenCount)
                   .putLong(treeMinToken)
                   .putLong(treeMaxToken)
                   .putLong(indexLayerSize());
            }

            protected byte infoByte()
            {
                // if leaf, set leaf indicator and last leaf indicator (bits 0 & 1)
                // if not leaf, clear both bits
                return (byte) ((isLeaf()) ? 3 : 0);
            }

            protected void writeMagic(ByteBuffer buf)
            {
                if (Descriptor.CURRENT.version.hasMagic())
                    buf.putShort(Descriptor.CURRENT.version.magic);
            }
        }

        private class InteriorNodeHeader extends Header
        {
            // bit 0 (leaf indicator) & bit 1 (last leaf indicator) cleared
            protected byte infoByte()
            {
                return 0;
            }
        }

        private class LeafHeader extends Header
        {
            // bit 0 set as leaf indicator
            // bit 1 set if this is last leaf of data
            protected byte infoByte()
            {
                byte infoByte = 1;
                infoByte |= (isLastLeaf()) ? (1 << LAST_LEAF_SHIFT) : 0;

                return infoByte;
            }
        }

    }

    protected abstract class Leaf extends Node
    {
        protected LongArrayList overflowCollisions;

        public Leaf(Long minToken, Long maxToken)
        {
            super(minToken, maxToken);
        }

        public int childCount()
        {
            return 0;
        }

        public int overflowCollisionCount() {
            return overflowCollisions == null ? 0 : overflowCollisions.size();
        }

        protected void serializeOverflowCollisions(ByteBuffer buf)
        {
            if (overflowCollisions != null)
                for (LongCursor entry : overflowCollisions)
                    buf.putLong(entry.value);
        }

        public long serialize(long childBlockIndex, long dataLayerOffset, ByteBuffer buf)
        {
            serializeHeader(buf);
            long dataLayerBytesConsumed = serializeData(buf, dataLayerOffset);
            serializeOverflowCollisions(buf);

            return dataLayerBytesConsumed;
        }

        protected abstract long serializeData(ByteBuffer buf, long dataLayerOffset);

        protected LeafEntry createEntry(final long tok, final Entries entries, long dataLayerOffset)
        {
            switch (entries.size())
            {
                case 0:
                    throw new AssertionError("no entries for token " + tok);
                case 1:
                    Entry entry = (Entry) entries.getEntries().toArray()[0];
                    long offset = entries.isPackable() ? entry.packableOffset().getAsLong() : dataLayerOffset;
                    if (offset > MAX_OFFSET)
                        throw new AssertionError("offset " + offset + " cannot be greater than " + MAX_OFFSET);
                    else if (offset <= Integer.MAX_VALUE)
                        return new SimpleLeafEntry(tok, entry, offset);
                    else
                        return new FactoredOffsetLeafEntry(tok, entry, offset);
                case 2:
                    long[] offsets;
                    Object[] entryArr = entries.getEntries().toArray();
                    Entry entry1 = (Entry) entryArr[0];
                    Entry entry2 = (Entry) entryArr[1];

                    // if both entries are packable into the index layer of the tree then those are
                    // the offsets stored. otherwise, both entries will be stored in the data layer
                    // and the offsets stored in the tree are pointers into the data layer
                    if (entry1.packableOffset().isPresent() && entry2.packableOffset().isPresent())
                        offsets = new long[] { entry1.partitionOffset(),
                                               entry2.partitionOffset() };
                    else
                        offsets = new long[] { dataLayerOffset,
                                               dataLayerOffset + (entry1.packableOffset().isPresent() ? ENTRY_UNPACKED_SIZE
                                                                                                      : entry1.serializedSize() )};
                    if (offsets[0] <= Integer.MAX_VALUE && offsets[1] <= Integer.MAX_VALUE &&
                        (offsets[0] <= Short.MAX_VALUE || offsets[1] <= Short.MAX_VALUE))
                        return new PackedCollisionLeafEntry(tok, new Entry[] {entry1, entry2}, offsets);
                    else
                        return createOverflowEntry(tok, entries, dataLayerOffset);
                default:
                    return createOverflowEntry(tok, entries, dataLayerOffset);
            }
        }

        private LeafEntry createOverflowEntry(final long tok, Entries entries, final long dataLayerOffset)
        {
            if (overflowCollisions == null)
                overflowCollisions = new LongArrayList();

            long dataNeeded = 0;
            boolean isPackable = entries.isPackable();
            LongArrayList newCollisions = new LongArrayList();
            for (Entry e : entries)
            {
                newCollisions.add(isPackable ? e.packableOffset().getAsLong() : dataLayerOffset + dataNeeded);
                if (isPackable)
                    continue;

                dataNeeded += e.packableOffset().isPresent() ? ENTRY_UNPACKED_SIZE: e.serializedSize();
            }

            LeafEntry entry = new OverflowCollisionLeafEntry(tok,
                                                             isPackable,
                                                             dataNeeded,
                                                             (short) overflowCollisions.size(),
                                                             (short) newCollisions.size());

            if ((overflowCollisions.size() + newCollisions.size()) >= OVERFLOW_TRAILER_CAPACITY)
                throw new AssertionError("cannot have more than " + OVERFLOW_TRAILER_CAPACITY + " overflow collisions per leaf");
            else
                overflowCollisions.addAll(newCollisions);

            return entry;
        }

        /**
         * bits 0-10: unused
         * bit 11: Data Mask. If set, this indicates the offset points to the data layer of the tree.
         *         If unset, the offset points of the partition key in the sstable's primary index.
         * bits 12-15: EntryType
         * bits 16-31: offset extra (used to store collisions or parts of large offsets)
         * bits 32-95: token (of the partition key)
         * bits 96-127: offset data (used to store offsets, or parts of offsets, that fit into an 32 bits
         */
        protected abstract class LeafEntry
        {
            protected final long token;

            abstract public EntryType type();
            abstract public int offsetData();
            abstract public short offsetExtra();
            abstract public long dataNeeded();
            abstract public short dataMask();

            public LeafEntry(final long tok)
            {
                token = tok;
            }

            public void serialize(ByteBuffer buf)
            {
                buf.putShort((short) ((dataMask() << ENTRY_DATA_SHIFT) | type().ordinal()))
                   .putShort(offsetExtra())
                   .putLong(token)
                   .putInt(offsetData());
            }

        }


        protected abstract class SingleEntryLeafEntry extends LeafEntry
        {

            protected final Entry entry;

            public SingleEntryLeafEntry(final long tok, final Entry entry)
            {
                super(tok);
                this.entry = entry;
            }

            public long dataNeeded()
            {
                return entry.serializedSize();
            }

            public short dataMask()
            {
                // for backwards compatability, its important that if the offset is package (only a partition offset)
                // then the bit in the mask is unset. Otherwise, if the partition offset isn't packable because
                // there is more data in the entry, then the bit is set to indicate the entry is stored in the data layer
                // and the offset written in the index layer is a pointer to the data layer
                return entry.packableOffset().isPresent() ? ENTRY_IS_PACKABLE : ENTRY_NOT_PACKABLE;
            }
        }

        // assumes there is a single offset and the offset is <= Integer.MAX_VALUE
        protected class SimpleLeafEntry extends SingleEntryLeafEntry
        {
            private final long offset;

            public SimpleLeafEntry(final long tok, final Entry entry, final long off)
            {
                super(tok, entry);
                offset = off;
            }

            public EntryType type()
            {
                return EntryType.SIMPLE;
            }

            public int offsetData()
            {
                return (int) offset;
            }

            public short offsetExtra()
            {
                return 0;
            }

        }

        // assumes there is a single offset and Integer.MAX_VALUE < offset <= MAX_OFFSET
        // take the middle 32 bits of offset (or the top 32 when considering offset is max 48 bits)
        // and store where offset is normally stored. take bottom 16 bits of offset and store in entry header
        private class FactoredOffsetLeafEntry extends SingleEntryLeafEntry
        {
            private final long offset;

            public FactoredOffsetLeafEntry(final long tok, final Entry entry, final long off)
            {
                super(tok, entry);
                offset = off;
            }

            public EntryType type()
            {
                return EntryType.FACTORED;
            }

            public int offsetData()
            {
                return (int) (offset >>> Short.SIZE);
            }

            public short offsetExtra()
            {
                // exta offset is supposed to be an unsigned 16-bit integer
                return (short) offset;
            }
        }

        // holds an entry with two offsets that can be packed in an int & a short
        // the int offset is stored where offset is normally stored. short offset is
        // stored in entry header
        private class PackedCollisionLeafEntry extends LeafEntry
        {
            private Entry smallerEntry;
            private short smallerOffset;
            private Entry largerEntry;
            private int largerOffset;

            public PackedCollisionLeafEntry(final long tok, final Entry[] entries, final long[] offs)
            {
                super(tok);

                smallerOffset = (short) Math.min(offs[0], offs[1]);
                largerOffset = (int) Math.max(offs[0], offs[1]);

                if (smallerOffset == offs[0])
                {
                    smallerEntry = entries[0];
                    largerEntry = entries[1];
                } else {
                    smallerEntry = entries[1];
                    largerEntry = entries[0];
                }
            }

            public EntryType type()
            {
                return EntryType.PACKED;
            }

            public int offsetData()
            {
                return largerOffset;
            }

            public short offsetExtra()
            {
                return smallerOffset;
            }

            public long dataNeeded()
            {
                if (largerEntry.packableOffset().isPresent() && smallerEntry.packableOffset().isPresent())
                    return 0;

                long dataNeeded = largerEntry.packableOffset().isPresent() ? ENTRY_UNPACKED_SIZE : largerEntry.serializedSize();
                dataNeeded += smallerEntry.packableOffset().isPresent() ? ENTRY_UNPACKED_SIZE : smallerEntry.serializedSize();

                return dataNeeded;
            }

            public short dataMask()
            {
                // if both entries have packable offsets then both are written in the tree layer
                // otherwise both are written in the data layer
                return largerEntry.packableOffset().isPresent() &&
                       smallerEntry.packableOffset().isPresent() ? ENTRY_IS_PACKABLE : ENTRY_NOT_PACKABLE;
            }
        }

        // holds an entry with three or more offsets, or two offsets that cannot
        // be packed into an int & a short. the index into the overflow list
        // is stored where the offset is normally stored. the number of overflowed offsets
        // for the entry is stored in the entry header
        private class OverflowCollisionLeafEntry extends LeafEntry
        {
            private final short startIndex;
            private final short count;
            private final boolean isPackable;
            private final long dataNeeded;

            public OverflowCollisionLeafEntry(final long tok,
                                              final boolean allPackable,
                                              final long dataLayerSize,
                                              final short collisionStartIndex,
                                              final short collisionCount)
            {
                super(tok);
                isPackable = allPackable;
                dataNeeded = dataLayerSize;
                startIndex = collisionStartIndex;
                count = collisionCount;

            }

            public EntryType type()
            {
                return EntryType.OVERFLOW;
            }

            public int offsetData()
            {
                return startIndex;
            }

            public short offsetExtra()
            {
                return count;
            }

            public long dataNeeded()
            {
                return dataNeeded;
            }

            public short dataMask()
            {
                return isPackable ? ENTRY_IS_PACKABLE : ENTRY_NOT_PACKABLE;
            }
        }

    }

    protected class InteriorNode extends Node
    {
        protected List<Long> tokens = new ArrayList<>(TOKENS_PER_BLOCK);
        protected List<Node> children = new ArrayList<>(TOKENS_PER_BLOCK + 1);
        protected int position = 0;

        public InteriorNode()
        {
            super(null, null);
        }

        public boolean isSerializable()
        {
            return true;
        }

        public long serialize(long childBlockIndex, long dataLayerOffset, ByteBuffer buf)
        {
            serializeHeader(buf);
            serializeTokens(buf);
            serializeChildOffsets(childBlockIndex, buf);

            // interior nodes do not point to and do not consume any space in the data layer of the tree
            return 0;
        }

        public int childCount()
        {
            return children.size();
        }

        public int tokenCount()
        {
            return tokens.size();
        }

        public Long smallestToken()
        {
            return tokens.get(0);
        }

        protected void add(Long token, InteriorNode leftChild, InteriorNode rightChild)
        {
            int pos = tokens.size();
            if (pos == TOKENS_PER_BLOCK)
            {
                InteriorNode sibling = split();
                sibling.add(token, leftChild, rightChild);

            }
            else
            {
                if (leftChild != null)
                    children.add(pos, leftChild);

                if (rightChild != null)
                {
                    children.add(pos + 1, rightChild);
                    rightChild.parent = this;
                }

                updateTokenRange(token);
                tokens.add(pos, token);
            }
        }

        protected void add(Leaf node)
        {

            if (position == (TOKENS_PER_BLOCK + 1))
            {
                rightmostParent = split();
                rightmostParent.add(node);
            }
            else
            {

                node.parent = this;
                children.add(position, node);
                position++;

                // the first child is referenced only during bulk load. we don't take a value
                // to store into the tree, one is subtracted since position has already been incremented
                // for the next node to be added
                if (position - 1 == 0)
                    return;


                // tokens are inserted one behind the current position, but 2 is subtracted because
                // position has already been incremented for the next add
                Long smallestToken = node.smallestToken();
                updateTokenRange(smallestToken);
                tokens.add(position - 2, smallestToken);
            }

        }

        protected InteriorNode split()
        {
            Pair<Long, InteriorNode> splitResult = splitBlock();
            Long middleValue = splitResult.left;
            InteriorNode sibling = splitResult.right;
            InteriorNode leftChild = null;

            // create a new root if necessary
            if (parent == null)
            {
                parent = new InteriorNode();
                root = parent;
                sibling.parent = parent;
                leftChild = this;
                numBlocks++;
            }

            parent.add(middleValue, leftChild, sibling);

            return sibling;
        }

        protected Pair<Long, InteriorNode> splitBlock()
        {
            final int splitPosition = TOKENS_PER_BLOCK - 2;
            InteriorNode sibling = new InteriorNode();
            sibling.parent = parent;
            next = sibling;

            Long middleValue = tokens.get(splitPosition);

            for (int i = splitPosition; i < TOKENS_PER_BLOCK; i++)
            {
                if (i != TOKENS_PER_BLOCK && i != splitPosition)
                {
                    long token = tokens.get(i);
                    sibling.updateTokenRange(token);
                    sibling.tokens.add(token);
                }

                Node child = children.get(i + 1);
                child.parent = sibling;
                sibling.children.add(child);
                sibling.position++;
            }

            for (int i = TOKENS_PER_BLOCK; i >= splitPosition; i--)
            {
                if (i != TOKENS_PER_BLOCK)
                    tokens.remove(i);

                if (i != splitPosition)
                    children.remove(i);
            }

            nodeMinToken = smallestToken();
            nodeMaxToken = tokens.get(tokens.size() - 1);
            numBlocks++;

            return Pair.create(middleValue, sibling);
        }

        protected boolean isFull()
        {
            return (position >= TOKENS_PER_BLOCK + 1);
        }

        private void serializeTokens(ByteBuffer buf)
        {
            tokens.forEach(buf::putLong);
        }

        private void serializeChildOffsets(long childBlockIndex, ByteBuffer buf)
        {
            for (int i = 0; i < children.size(); i++)
                buf.putLong((childBlockIndex + i) * BLOCK_BYTES);
        }
    }

    public static class LevelIterator extends AbstractIterator<Node>
    {
        private Node currentNode;

        LevelIterator(Node first)
        {
            currentNode = first;
        }

        public Node computeNext()
        {
            if (currentNode == null)
                return endOfData();

            Node returnNode = currentNode;
            currentNode = returnNode.next;

            return returnNode;
        }
    }


    protected static void alignBuffer(ByteBuffer buffer, int blockSize)
    {
        long curPos = buffer.position();
        if ((curPos & (blockSize - 1)) != 0) // align on the block boundary if needed
            buffer.position((int) FBUtilities.align(curPos, blockSize));
    }

}
