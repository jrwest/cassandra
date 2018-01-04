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
import java.util.Optional;
import java.util.Set;

import com.carrotsearch.hppc.ObjectSet;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongSet;
import com.carrotsearch.hppc.cursors.LongCursor;

public abstract class AbstractTokenTreeBuilder implements TokenTreeBuilder
{
    protected int numBlocks;
    protected Node root;
    protected InteriorNode rightmostParent;
    protected Leaf leftmostLeaf;
    protected Leaf rightmostLeaf;
    protected long tokenCount = 0;
    protected long treeMinToken;
    protected long treeMaxToken;

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

    // TODO (jwest): need to update to account for data portion
    // TODO (jwest): is returning an int here for both the index layer and data layer too small?
    public int serializedSize()
    {
        if (numBlocks == 1)
            return BLOCK_HEADER_BYTES +
                   ((int) tokenCount * BLOCK_ENTRY_BYTES) +
                   (((Leaf) root).overflowCollisionCount() * OVERFLOW_ENTRY_BYTES) +
                   serializedDataSize();
        else
            // TODO (jwest): should we block align the data portion
            return numBlocks * BLOCK_BYTES + serializedDataSize();
    }

    protected abstract int serializedDataSize();


    // TODO (jwest): need to separate this out into two methods "writeIndex" and "writeData" so that sub-classes can
    // override the individual parts.

    public void write(DataOutputPlus out) throws IOException {
        writeIndex(out);
        writeData(out);
    }

    protected void writeIndex(DataOutputPlus out) throws IOException
    {
        ByteBuffer blockBuffer = ByteBuffer.allocate(BLOCK_BYTES);
        Iterator<Node> levelIterator = root.levelIterator();
        long childBlockIndex = 1;
        long dataOffset = 0;

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
                    dataOffset += block.serialize(childBlockIndex, blockBuffer, dataOffset);
                    flushBuffer(blockBuffer, out, numBlocks != 1);
                }

                childBlockIndex += block.childCount();
            }

            levelIterator = (firstChild == null) ? null : firstChild.levelIterator();
        }
    }

    protected abstract void writeData(DataOutputPlus out) throws IOException;
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

        public abstract boolean isSerializable();
        public abstract long serialize(long childBlockIndex, ByteBuffer buf, long curDataOffset);
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
                   .putLong(treeMaxToken);
            }

            protected byte infoByte()
            {
                // if leaf, set leaf indicator and last leaf indicator (bits 0 & 1)
                // if not leaf, clear both bits
                return (byte) ((isLeaf()) ? 3 : 0);
            }

            protected void writeMagic(ByteBuffer buf)
            {
                switch (Descriptor.CURRENT_VERSION)
                {
                    case Descriptor.VERSION_AB:
                        buf.putShort(AB_MAGIC);
                        break;

                    default:
                        break;
                }

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
                for (LongCursor offset : overflowCollisions)
                    buf.putLong(offset.value);
        }


        public long serialize(long childBlockIndex, ByteBuffer buf, long curDataOffset)
        {
            serializeHeader(buf);
            long dataBytesNeeded = serializeData(buf, curDataOffset);
            serializeOverflowCollisions(buf);

            return dataBytesNeeded;
        }


        protected abstract long serializeData(ByteBuffer buf, long curDataOffset);

        // TODO (jwest): this needs to take the calculated offset into the data portion for some operations
        // TODO (jwest): it also needs to return somehow (maybe via leaf entry) how much space in the data layer it will consume
        protected LeafEntry createEntry(final long tok, final ObjectSet<TokenTreeEntry> offsets, long curDataOffset)
        {
            int offsetCount = offsets.size();
            switch (offsetCount)
            {
                case 0:
                    throw new AssertionError("no offsets for token " + tok);
                case 1:
                    TokenTreeEntry entry = offsets.iterator().next().value;
                    //if (entry
                    // TODO (jwest): change this to operate on TokenTreeEntry:
                    //               - move checking of whether or not the offset should be facted to TTE
                    //               - factoring can apply to tree data or sstable data offset
                    //               - change TTE to return "the offset" (optional so that we can use data offset in case where entry should be written to data layer)
                    //long offset = ((TokenTreeEntry) offsets.toArray()[0]).getPartitionOffset();

                    // TODO (jwest): need to move this assert somewhere since we can't check it here with the given interface, for now assume its fine
                    //if (offset > MAX_OFFSET)
                   // throw new AssertionError("offset " + offset + " cannot be greater than " + MAX_OFFSET);
                    //else if (offset <= Integer.MAX_VALUE)
                     //   return new SimpleLeafEntry(tok, offset);
                    //else
                     //   return new FactoredOffsetLeafEntry(tok, offset);
                    Optional<Long> packableOffset = entry.packableOffset();
                    return packableOffset.map(offset -> createSimpleOrPackedEntry(tok, entry, offset))
                                         .orElseGet(() -> createSimpleOrPackedEntry(tok, entry, curDataOffset));
                case 2:
                    /*TokenTreeEntry[] entries = offsets.toArray(TokenTreeEntry.class);
                    if (entries[0].getPartitionOffset() <= Integer.MAX_VALUE && entries[1].getPartitionOffset() <= Integer.MAX_VALUE &&
                        (entries[0].getPartitionOffset() <= Short.MAX_VALUE || entries[1].getPartitionOffset() <= Short.MAX_VALUE))
                        return new PackedCollisionLeafEntry(tok, entries[0], entries[1]);
                    else
                        return createOverflowEntry(tok, offsetCount, offsets);*/
                default:
                    throw new UnsupportedOperationException("don't support 2 or more collisions yet");
                    /*return createOverflowEntry(tok, offsetCount, offsets);*/
            }
        }

        private LeafEntry createSimpleOrPackedEntry(final long tok, final TokenTreeEntry entry, long offset) {
            if (offset > MAX_OFFSET)
                throw new AssertionError("offset " + offset + " cannot be greater than " + MAX_OFFSET);
            else if (offset <= Integer.MAX_VALUE)
                return new SimpleLeafEntry(tok, entry, offset);
            else
                return new FactoredOffsetLeafEntry(tok, entry, offset);
        }

        private LeafEntry createOverflowEntry(final long tok, final int offsetCount, final ObjectSet<TokenTreeEntry> entries)
        {
            if (overflowCollisions == null)
                overflowCollisions = new LongArrayList();

            LeafEntry entry = new OverflowCollisionLeafEntry(tok, (short) overflowCollisions.size(), (short) offsetCount);
            for (ObjectCursor<TokenTreeEntry> c : entries)
            {
                if (overflowCollisions.size() == OVERFLOW_TRAILER_CAPACITY)
                    throw new AssertionError("cannot have more than " + OVERFLOW_TRAILER_CAPACITY + " overflow collisions per leaf");
                else
                    overflowCollisions.add(c.value.getPartitionOffset());
            }
            return entry;
        }

        protected abstract class LeafEntry
        {
            protected final long token;

            abstract public EntryType type();
            abstract public int offsetData();
            abstract public short offsetExtra();
            abstract public long dataNeeded();

            /**
             * @return a bitmask where each set bit represents a {@link TokenTreeEntry}
             * whose data is stored in the data layer of the tree ({@link #dataNeeded()} > 0).
             * While a short is returned only the first 8 bits are used.
             */
            abstract public short dataMask();

            public LeafEntry(final long tok)
            {
                token = tok;
            }

            public void serialize(ByteBuffer buf)
            {
                //header: bits [0,3] are for type, bits [4,11] for dataMask() and the remaining are unused
                short header = (short) ((dataMask() << ENTRY_DATA_SHIFT) | type().ordinal());
                buf.putShort(header)
                   .putShort(offsetExtra())
                   .putLong(token)
                   .putInt(offsetData());
            }

        }

        protected abstract class SingleEntryLeaf extends LeafEntry {
            private final TokenTreeEntry entry;
            private final long offset;

            public SingleEntryLeaf(final long tok, final TokenTreeEntry e, final long off) {
                super(tok);
                entry = e;
                offset = off;
            }

            public long dataNeeded()
            {
                return entry.serializedSize();
            }

            public short dataMask()
            {
                // if the entry has a single offset the mask is 0, otherwse, the entry is stored in the data layer
                // and the mask is 1
                return (short) (entry.packableOffset().isPresent() ? 0 : 1);
            }

            protected long offset() {
                return offset;
            }

            protected TokenTreeEntry entry() {
                return entry;
            }
        }


        // assumes there is a single offset and the offset is <= Integer.MAX_VALUE
        protected class SimpleLeafEntry extends SingleEntryLeaf
        {

            public SimpleLeafEntry(final long tok, final TokenTreeEntry e, final long off)
            {
                super(tok, e, off);
            }

            public EntryType type()
            {
                return EntryType.SIMPLE;
            }

            public int offsetData()
            {
                return (int) offset();
            }

            public short offsetExtra()
            {
                return 0;
            }

            public long dataNeeded()
            {
                return entry().serializedSize();
            }
        }

        // assumes there is a single offset and Integer.MAX_VALUE < offset <= MAX_OFFSET
        // take the middle 32 bits of offset (or the top 32 when considering offset is max 48 bits)
        // and store where offset is normally stored. take bottom 16 bits of offset and store in entry header
        private class FactoredOffsetLeafEntry extends SingleEntryLeaf
        {
            public FactoredOffsetLeafEntry(final long tok, final TokenTreeEntry e, final long off)
            {
                super(tok, e, off);
            }

            public EntryType type()
            {
                return EntryType.FACTORED;
            }

            public int offsetData()
            {
                return (int) (offset() >>> Short.SIZE);
            }

            public short offsetExtra()
            {
                // exta offset is supposed to be an unsigned 16-bit integer
                return (short) offset();
            }
        }

        // holds an entry with two offsets that can be packed in an int & a short
        // the int offset is stored where offset is normally stored. short offset is
        // stored in entry header
        private class PackedCollisionLeafEntry extends LeafEntry
        {
            private short smallerOffset;
            private int largerOffset;

            public PackedCollisionLeafEntry(final long tok, final TokenTreeEntry entry1, TokenTreeEntry entry2)
            {
                super(tok);

                smallerOffset = (short) Math.min(entry1.getPartitionOffset(), entry2.getPartitionOffset());
                largerOffset = (int) Math.max(entry1.getPartitionOffset(), entry2.getPartitionOffset());
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
                throw new RuntimeException("not implemented");
            }

            public short dataMask()
            {
                throw new RuntimeException("not implemented");
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

            public OverflowCollisionLeafEntry(final long tok, final short collisionStartIndex, final short collisionCount)
            {
                super(tok);
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
                throw new RuntimeException("not implemented");
            }

            public short dataMask()
            {
                throw new RuntimeException("not implemented");
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

        public long serialize(long childBlockIndex, ByteBuffer buf, long curDataOffset)
        {
            serializeHeader(buf);
            serializeTokens(buf);
            serializeChildOffsets(childBlockIndex, buf);

            return 0; // writing interior nodes does not increase size of data portion of the tree
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
