package org.apache.cassandra.index.sasi.disk;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;

import com.google.common.collect.Iterators;

import com.carrotsearch.hppc.LongSet;
import org.apache.cassandra.index.sasi.utils.CombinedTerm;
import org.apache.cassandra.index.sasi.utils.CombinedValue;
import org.apache.cassandra.index.sasi.utils.RangeIterator;
import org.apache.cassandra.index.sasi.utils.RangeUnionIterator;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.Pair;

public class StaticTokenTreeBuilder extends AbstractTokenTreeBuilder
{
    private final CombinedTerm combinedTerm;

    public StaticTokenTreeBuilder(CombinedTerm term)
    {
        combinedTerm = term;
    }

    public void add(Long token, long keyPosition)
    {
        throw new UnsupportedOperationException();
    }

    public void add(SortedMap<Long, LongSet> data)
    {
        throw new UnsupportedOperationException();
    }

    public void add(Iterator<Pair<Long, LongSet>> data)
    {
        throw new UnsupportedOperationException();
    }

    public boolean isEmpty()
    {
        return combinedTerm.getTokenIterator().getCount() == 0;
    }

    public Iterator<Pair<Long, LongSet>> iterator()
    {
        final Iterator<Token> iterator = combinedTerm.getTokenIterator();
        return new AbstractIterator<Pair<Long, LongSet>>()
        {
            protected Pair<Long, LongSet> computeNext()
            {
                if (!iterator.hasNext())
                    return endOfData();

                Token token = iterator.next();
                return Pair.create(token.get(), token.getOffsets());
            }
        };
    }

    public long getTokenCount()
    {
        return combinedTerm.getTokenIterator().getCount();
    }

    @Override
    public void write(DataOutputPlus out) throws IOException
    {
        // if the root is not a leaf then none of the leaves have been written (all are UnwriteableLeaf)
        // so write out the last layer of the tree by converting UnwriteableLeaf to WriteableLeaf and
        // iterating the data once more
        super.write(out);
        if (root.isLeaf())
            return;

        RangeIterator<Long, Token> tokens = combinedTerm.getTokenIterator();
        ByteBuffer blockBuffer = ByteBuffer.allocate(BLOCK_BYTES);
        Iterator<Node> leafIterator = leftmostLeaf.levelIterator();
        while (leafIterator.hasNext())
        {
            Leaf leaf = (Leaf) leafIterator.next();
            Leaf writeableLeaf = new WriteableLeaf(Iterators.limit(tokens, leaf.tokenCount()), leaf);
            writeableLeaf.serialize(-1, blockBuffer);
            flushBuffer(blockBuffer, out, true);
        }

    }

    protected void bulkLoad()
    {
        RangeIterator<Long, Token> tokens = combinedTerm.getTokenIterator();

        tokenCount = tokens.getCount();
        treeMinToken = tokens.getMinimum();
        treeMaxToken = tokens.getMaximum();
        numBlocks = 1;

        if (tokenCount <= TOKENS_PER_BLOCK)
        {
            leftmostLeaf = new WriteableLeaf(tokens, tokens.getMinimum(), tokens.getMaximum(), tokens.getCount(), true);
            rightmostLeaf = leftmostLeaf;
            root = leftmostLeaf;
        }
        else
        {
            root = new InteriorNode();
            rightmostParent = (InteriorNode) root;

            // build all the leaves except for maybe
            // the last leaf which is not completely full
            Leaf lastLeaf = null;
            long numFullLeaves = tokenCount / TOKENS_PER_BLOCK;
            for (long i = 0; i < numFullLeaves; i++) {
                Long firstToken = tokens.next().get();
                for (int j = 1; j < (TOKENS_PER_BLOCK - 1); j++)
                    tokens.next();

                Long lastToken = tokens.next().get();
                Leaf leaf = new UnwriteableLeaf(firstToken, lastToken, TOKENS_PER_BLOCK);

                if (lastLeaf == null)
                    leftmostLeaf = leaf;
                else
                    lastLeaf.next = leaf;

                rightmostParent.add(leaf);
                lastLeaf = rightmostLeaf = leaf;
                numBlocks++;
            }

            // build the last leaf out of any remaining tokens if necessary
            // safe downcast since TOKENS_PER_BLOCK is an int
            int remainingTokens = (int) (tokenCount % TOKENS_PER_BLOCK);
            if (remainingTokens != 0)
            {
                Long firstToken = tokens.next().get();
                Long lastToken = firstToken;
                while (tokens.hasNext())
                    lastToken = tokens.next().get();

                Leaf leaf = new UnwriteableLeaf(firstToken, lastToken, remainingTokens);
                rightmostParent.add(leaf);
                lastLeaf.next = rightmostLeaf = leaf;
                numBlocks++;
            }
        }
    }

    private class UnwriteableLeaf extends Leaf
    {
        private final int size;
        public UnwriteableLeaf(Long min, Long max, int count)
        {
            nodeMinToken = min;
            nodeMaxToken = max;
            size = count;
        }

        public int tokenCount()
        {
            return size;
        }

        public void serializeData(ByteBuffer buf)
        {
            throw new UnsupportedOperationException();
        }

        public boolean isWriteable()
        {
            return false;
        }
    }

    private class WriteableLeaf extends Leaf
    {
        private final Iterator<Token> tokens;
        private final int count;
        private final boolean isLast;

        public WriteableLeaf(Iterator<Token> tokens, Leaf leaf)
        {
            this(tokens, leaf.smallestToken(), leaf.largestToken(), leaf.tokenCount(), leaf.isLastLeaf());
        }

        public WriteableLeaf(Iterator<Token> tokens, Long min, Long max, long count, boolean isLastLeaf)
        {
            nodeMinToken = min;
            nodeMaxToken = max;
            this.count = (int) count; // downcast is safe since leaf size is always < Integer.MAX_VALUE
            this.tokens = tokens;
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

        public void serializeData(ByteBuffer buf)
        {
            while (tokens.hasNext())
            {
                Token entry = tokens.next();
                createEntry(entry.get(), entry.getOffsets()).serialize(buf);
            }
        }

        public boolean isWriteable()
        {
            return true;
        }

    }
}
