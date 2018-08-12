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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.*;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import com.carrotsearch.hppc.ObjectOpenHashSet;
import com.carrotsearch.hppc.ObjectSet;
import com.carrotsearch.hppc.cursors.LongCursor;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sasi.disk.TokenTreeBuilder.EntryType;
import org.apache.cassandra.index.sasi.utils.CombinedTerm;
import org.apache.cassandra.index.sasi.utils.CombinedValue;
import org.apache.cassandra.index.sasi.utils.MappedBuffer;
import org.apache.cassandra.index.sasi.utils.RangeIterator;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.index.sasi.utils.RangeUnionIterator;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.utils.MurmurHash;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriter;

import org.junit.Assert;

import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import com.carrotsearch.hppc.LongOpenHashSet;
import com.carrotsearch.hppc.LongSet;
import org.apache.cassandra.utils.Pair;

import com.google.common.base.Function;

import static org.apache.cassandra.index.sasi.disk.TokenTreeBuilder.Entries;
import static org.apache.cassandra.index.sasi.disk.TokenTreeBuilder.Entry;

public class TokenTreeTest
{
    private static final Function<Long, DecoratedKey> KEY_CONVERTER = new KeyConverter();

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    final static Entries singleOffset = new Entries(longSetToObjSet(new LongOpenHashSet() {{ add(1); }}), null);
    final static Entries bigSingleOffset = new Entries(longSetToObjSet(new LongOpenHashSet() {{ add(2147521562L); }}), null);
    final static Entries shortPackableCollision = new Entries(longSetToObjSet(new LongOpenHashSet() {{ add(2L); add(3L); }}), null); // can pack two shorts
    final static Entries intPackableCollision = new Entries(longSetToObjSet(new LongOpenHashSet() {{ add(6L); add(((long) Short.MAX_VALUE) + 1); }}), null); // can pack int & short
    final static Entries multiCollision =  new Entries(longSetToObjSet(new LongOpenHashSet() {{ add(3L); add(4L); add(5L); }}), null); // can't pack
    final static Entries unpackableCollision = new Entries(longSetToObjSet(new LongOpenHashSet() {{ add(((long) Short.MAX_VALUE) + 1); add(((long) Short.MAX_VALUE) + 2); }}), null); // can't pack

    final static ClusteringComparator singleTextColumnClustering = new ClusteringComparator(UTF8Type.instance);
    final static NavigableSet<Clustering> singleTextClustering = new TreeSet<Clustering>(singleTextColumnClustering)
    {{
        add(singleTextColumnClustering.make("foo"));
    }};
    final static Entry entryWithClustering = new Entry(1, singleTextColumnClustering, singleTextClustering);
    final static ObjectSet<Entry> entryWithClusteringsSet = new ObjectOpenHashSet<Entry>() {{ add(entryWithClustering); }};
    final static Entries singleWithClusterings = new Entries(entryWithClusteringsSet, singleTextColumnClustering);

    final static SortedMap<Long, Entries> simpleTokenMap = new TreeMap<Long, Entries>()
    {{
        put(1L, bigSingleOffset); put(3L, shortPackableCollision); put(4L, intPackableCollision); put(6L, singleOffset);
        put(9L, multiCollision); put(10L, unpackableCollision); put(12L, singleOffset); put(13L, singleOffset);
        put(15L, singleOffset); put(16L, singleOffset); put(20L, singleOffset); put(22L, singleOffset);
        put(25L, singleOffset); put(26L, singleOffset); put(27L, singleOffset); put(28L, singleOffset);
        put(40L, singleOffset); put(50L, singleOffset); put(100L, singleOffset); put(101L, singleOffset);
        put(102L, singleOffset); put(103L, singleOffset); put(108L, singleOffset); put(110L, singleOffset);
        put(112L, singleOffset); put(115L, singleOffset); put(116L, singleOffset); put(120L, singleOffset);
        put(121L, singleOffset); put(122L, singleOffset); put(123L, singleOffset); put(125L, singleOffset);
    }};

    final static SortedMap<Long, Entries> bigTokensMap = new TreeMap<Long, Entries>()
    {{
        for (long i = 0; i < 1000000; i++)
            put(i, singleOffset);
    }};

    final static SortedMap<Long, Entries> bigTokensWithClusteringsMap = new TreeMap<Long, Entries>()
    {{
        for (long i = 0; i < 1000000; i++)
            put(i, singleWithClusterings);
    }};

    @FunctionalInterface
    private static interface CheckedConsumer<C> {
        public void accept(C c) throws Exception;
    }

    final static List<Pair<ClusteringComparator, SortedMap<Long, Entries>>> tokenMaps = Arrays.asList(
      Pair.create(new ClusteringComparator(), simpleTokenMap),
      Pair.create(new ClusteringComparator(), bigTokensMap),
      Pair.create(singleTextColumnClustering, bigTokensWithClusteringsMap)
    );
    private void forAllTokenMaps(CheckedConsumer<Pair<ClusteringComparator, SortedMap<Long, Entries>>> c) throws Exception {
        for (Pair<ClusteringComparator, SortedMap<Long, Entries>> tokens : tokenMaps)
            c.accept(tokens);
    }

    final static SequentialWriterOption DEFAULT_OPT = SequentialWriterOption.newBuilder().bufferSize(4096).build();

    @Test
    public void testSerializedSizeDynamic() throws Exception
    {
        forAllTokenMaps(tokens -> testSerializedSize(new DynamicTokenTreeBuilder(tokens.left, tokens.right)));
    }

    @Test
    public void testSerializedSizeStatic() throws Exception
    {
        forAllTokenMaps(tokens -> testSerializedSize(new StaticTokenTreeBuilder(new FakeCombinedTerm(tokens.left,
                                                                                                     tokens.right))));
    }


    public void testSerializedSize(final TokenTreeBuilder builder) throws Exception
    {
        builder.finish();
        final File treeFile = FileUtils.createTempFile("token-tree-size-test", "tt");
        treeFile.deleteOnExit();

        try (SequentialWriter writer = new SequentialWriter(treeFile, DEFAULT_OPT))
        {
            builder.write(writer);
            writer.sync();
        }

        Assert.assertEquals(builder.serializedSize(), Files.size(treeFile.toPath()));
    }

    @Test
    public void buildSerializeAndIterateDynamic() throws Exception
    {
        forAllTokenMaps(tokens -> buildSerializeAndIterate(new DynamicTokenTreeBuilder(tokens.left, tokens.right),
                                                           tokens.right));
    }

    @Test
    public void buildSerializeAndIterateStatic() throws Exception
    {
        forAllTokenMaps(tokens ->
                        buildSerializeAndIterate(new StaticTokenTreeBuilder(new FakeCombinedTerm(tokens.left,
                                                                                                 tokens.right)),
                                                 tokens.right));
    }


    public void buildSerializeAndIterate(TokenTreeBuilder builder,
                                         SortedMap<Long, Entries> tokenMap) throws Exception
    {

        builder.finish();
        final File treeFile = FileUtils.createTempFile("token-tree-iterate-test1", "tt");
        treeFile.deleteOnExit();

        try (SequentialWriter writer = new SequentialWriter(treeFile, DEFAULT_OPT))
        {
            builder.write(writer);
            writer.sync();
        }

        final RandomAccessReader reader = RandomAccessReader.open(treeFile);
        final TokenTree tokenTree = new TokenTree(builder.clusteringComparator(), new MappedBuffer(reader));

        final Iterator<IndexEntry> tokenIterator = tokenTree.iterator(KEY_CONVERTER);
        final Iterator<Map.Entry<Long, Entries>> listIterator = tokenMap.entrySet().iterator();
        while (tokenIterator.hasNext() && listIterator.hasNext())
        {
            IndexEntry treeNext = tokenIterator.next();
            Map.Entry<Long, Entries> listNext = listIterator.next();

            Assert.assertEquals(listNext.getKey(), treeNext.get());
            Assert.assertEquals(convert(listNext.getValue()), convert(treeNext));
        }

        Assert.assertFalse("token iterator not finished", tokenIterator.hasNext());
        Assert.assertFalse("list iterator not finished", listIterator.hasNext());

        reader.close();
    }

    @Test
    public void buildSerializeAndGetDynamic() throws Exception
    {
        buildSerializeAndGet(false);
    }

    @Test
    public void buildSerializeAndGetStatic() throws Exception
    {
        buildSerializeAndGet(true);
    }

    public void buildSerializeAndGet(boolean isStatic) throws Exception
    {
        final long tokMin = 0;
        final long tokMax = 1000;

        final TokenTree tokenTree = generateTree(tokMin, tokMax, isStatic);

        for (long i = 0; i <= tokMax; i++)
        {
            TokenTree.OnDiskIndexEntry result = tokenTree.get(i, KEY_CONVERTER);
            Assert.assertNotNull("failed to find object for token " + i, result);

            Entries found = result.getOffsets();
            Assert.assertEquals(1, found.size());
            Assert.assertEquals(i, ((TokenTreeBuilder.Entry) found.getEntries().toArray()[0]).partitionOffset());
        }

        Assert.assertNull("found missing object", tokenTree.get(tokMax + 10, KEY_CONVERTER));
    }

    @Test
    public void buildSerializeIterateAndSkipDynamic() throws Exception
    {
        forAllTokenMaps(tokens -> buildSerializeIterateAndSkip(new DynamicTokenTreeBuilder(tokens.left, tokens.right), tokens.right));
    }

    @Test
    public void buildSerializeIterateAndSkipStatic() throws Exception
    {
        forAllTokenMaps(tokens ->
                        buildSerializeIterateAndSkip(new StaticTokenTreeBuilder(new FakeCombinedTerm(tokens.left,
                                                                                                     tokens.right)),
                                                     tokens.right));
    }

    // works with maps other than bigTokensMap but skips to a rather large token
    // so likely for maps other than bigTokensMap skipping is not tested by this.
    public void buildSerializeIterateAndSkip(TokenTreeBuilder builder, SortedMap<Long, Entries> tokens) throws Exception
    {
        builder.finish();
        final File treeFile = FileUtils.createTempFile("token-tree-iterate-test2", "tt");
        treeFile.deleteOnExit();

        try (SequentialWriter writer = new SequentialWriter(treeFile, DEFAULT_OPT))
        {
            builder.write(writer);
            writer.sync();
        }

        final RandomAccessReader reader = RandomAccessReader.open(treeFile);
        final TokenTree tokenTree = new TokenTree(builder.clusteringComparator(), new MappedBuffer(reader));

        final RangeIterator<Long, IndexEntry> treeIterator = tokenTree.iterator(KEY_CONVERTER);
        final RangeIterator<Long, IndexEntryWithOffsets> listIterator = new EntrySetSkippableIterator(tokens);

        long lastToken = 0L;
        while (treeIterator.hasNext() && lastToken < 12)
        {
            IndexEntry treeNext = treeIterator.next();
            IndexEntryWithOffsets listNext = listIterator.next();

            Assert.assertEquals(listNext.token, (lastToken = treeNext.get()));
            Assert.assertEquals(convert(listNext.offsets), convert(treeNext));
        }

        treeIterator.skipTo(100548L);
        listIterator.skipTo(100548L);

        while (treeIterator.hasNext() && listIterator.hasNext())
        {
            IndexEntry treeNext = treeIterator.next();
            IndexEntryWithOffsets listNext = listIterator.next();

            Assert.assertEquals(listNext.token, (long) treeNext.get());
            Assert.assertEquals(convert(listNext.offsets), convert(treeNext));

        }

        Assert.assertFalse("Tree iterator not completed", treeIterator.hasNext());
        Assert.assertFalse("List iterator not completed", listIterator.hasNext());

        reader.close();
    }

    @Test
    public void skipPastEndDynamic() throws Exception
    {
        skipPastEnd(new DynamicTokenTreeBuilder(new ClusteringComparator(), simpleTokenMap), simpleTokenMap);
    }

    @Test
    public void skipPastEndStatic() throws Exception
    {
        skipPastEnd(new StaticTokenTreeBuilder(new FakeCombinedTerm(new ClusteringComparator(), simpleTokenMap)),
                    simpleTokenMap);
    }

    public void skipPastEnd(TokenTreeBuilder builder, SortedMap<Long, Entries> tokens) throws Exception
    {
        builder.finish();
        final File treeFile = FileUtils.createTempFile("token-tree-skip-past-test", "tt");
        treeFile.deleteOnExit();

        try (SequentialWriter writer = new SequentialWriter(treeFile, DEFAULT_OPT))
        {
            builder.write(writer);
            writer.sync();
        }

        final RandomAccessReader reader = RandomAccessReader.open(treeFile);
        final RangeIterator<Long, IndexEntry> tokenTree = new TokenTree(builder.clusteringComparator(),
                                                                        new MappedBuffer(reader)).iterator(KEY_CONVERTER);

        tokenTree.skipTo(tokens.lastKey() + 10);
    }

    @Test
    public void testIndexEntryMergeDyanmic() throws Exception
    {
        testIndexEntryMerge(false);
    }

    @Test
    public void testIndexEntryMergeStatic() throws Exception
    {
        testIndexEntryMerge(true);
    }

    public void testIndexEntryMerge(boolean isStatic) throws Exception
    {
        final long min = 0, max = 1000;

        // two different trees with the same offsets
        TokenTree treeA = generateTree(min, max, isStatic);
        TokenTree treeB = generateTree(min, max, isStatic);

        RangeIterator<Long, IndexEntry> a = treeA.iterator(new KeyConverter());
        RangeIterator<Long, IndexEntry> b = treeB.iterator(new KeyConverter());

        long count = min;
        while (a.hasNext() && b.hasNext())
        {
            final IndexEntry entryA = a.next();
            final IndexEntry entryB = b.next();

            // merging of two OnDiskIndexEntry
            entryA.merge(entryB);
            // merging with RAM IndexEntry with different offset
            entryA.merge(new IndexEntryWithOffsets(entryA.get(), new Entries(convert(count + 1), new ClusteringComparator())));
            // and RAM token with the same offset
            entryA.merge(new IndexEntryWithOffsets(entryA.get(), new Entries(convert(count), new ClusteringComparator())));

            // should fail when trying to merge different tokens
            try
            {
                entryA.merge(new IndexEntryWithOffsets(entryA.get() + 1, new Entries(convert(count), new ClusteringComparator())));
                Assert.fail();
            }
            catch (IllegalArgumentException e)
            {
                // expected
            }

            final Set<Long> offsets = new TreeSet<>();
            for (DecoratedKey key : entryA)
                 offsets.add(LongType.instance.compose(key.getKey()));

            Set<Long> expected = new TreeSet<>();
            {
                expected.add(count);
                expected.add(count + 1);
            }

            Assert.assertEquals(expected, offsets);
            count++;
        }

        Assert.assertEquals(max, count - 1);
    }

    @Test
    public void testEntryTypeOrdinalLookup()
    {
        Assert.assertEquals(EntryType.SIMPLE, EntryType.of(EntryType.SIMPLE.ordinal()));
        Assert.assertEquals(EntryType.PACKED, EntryType.of(EntryType.PACKED.ordinal()));
        Assert.assertEquals(EntryType.FACTORED, EntryType.of(EntryType.FACTORED.ordinal()));
        Assert.assertEquals(EntryType.OVERFLOW, EntryType.of(EntryType.OVERFLOW.ordinal()));
    }

    @Test
    public void testMergingOfEqualTokenTrees() throws Exception
    {
        testMergingOfEqualTokenTrees(new ClusteringComparator(), simpleTokenMap);
        testMergingOfEqualTokenTrees(new ClusteringComparator(), bigTokensMap);
    }

    public void testMergingOfEqualTokenTrees(ClusteringComparator comparator,
                                             SortedMap<Long, Entries> tokensMap) throws Exception
    {
        TokenTreeBuilder tokensA = new DynamicTokenTreeBuilder(comparator, tokensMap);
        TokenTreeBuilder tokensB = new DynamicTokenTreeBuilder(comparator, tokensMap);

        TokenTree a = buildTree(tokensA);
        TokenTree b = buildTree(tokensB);

        TokenTreeBuilder tokensC = new StaticTokenTreeBuilder(new CombinedTerm(comparator,null, null)
        {
            public RangeIterator<Long, IndexEntry> getEntryIterator()
            {
                RangeIterator.Builder<Long, IndexEntry> union = RangeUnionIterator.builder();
                union.add(a.iterator(new KeyConverter()));
                union.add(b.iterator(new KeyConverter()));

                return union.build();
            }
        });

        TokenTree c = buildTree(tokensC);
        Assert.assertEquals(tokensMap.size(), c.getCount());

        Iterator<IndexEntry> tokenIterator = c.iterator(KEY_CONVERTER);
        Iterator<Map.Entry<Long, Entries>> listIterator = tokensMap.entrySet().iterator();
        while (tokenIterator.hasNext() && listIterator.hasNext())
        {
            IndexEntry treeNext = tokenIterator.next();
            Map.Entry<Long, Entries> listNext = listIterator.next();

            Assert.assertEquals(listNext.getKey(), treeNext.get());
            Assert.assertEquals(convert(listNext.getValue()), convert(treeNext));
        }

        for (Map.Entry<Long, Entries> entry : tokensMap.entrySet())
        {
            TokenTree.OnDiskIndexEntry result = c.get(entry.getKey(), KEY_CONVERTER);
            Assert.assertNotNull("failed to find object for token " + entry.getKey(), result);

            Entries found = result.getOffsets();
            Assert.assertEquals(entry.getValue(), found);

        }
    }

    private static LongSet objSetToLongSet(ObjectSet<TokenTreeBuilder.Entry> entries)
    {
        LongSet r = new LongOpenHashSet(entries.size());
        for (ObjectCursor<TokenTreeBuilder.Entry> e : entries)
            r.add(e.value.partitionOffset());

        return r;
    }

    private static ObjectSet<TokenTreeBuilder.Entry> longSetToObjSet(LongSet offsets)
    {
        ObjectSet<TokenTreeBuilder.Entry> r = new ObjectOpenHashSet<>(offsets.size());
        for (LongCursor l : offsets)
            r.add(new Entry(l.value));

        return r;
    }

    private static TokenTree buildTree(TokenTreeBuilder builder) throws Exception
    {
        builder.finish();
        final File treeFile = FileUtils.createTempFile("token-tree-", "db");
        treeFile.deleteOnExit();

        try (SequentialWriter writer = new SequentialWriter(treeFile, DEFAULT_OPT))
        {
            builder.write(writer);
            writer.sync();
        }

        final RandomAccessReader reader = RandomAccessReader.open(treeFile);
        return new TokenTree(builder.clusteringComparator(), new MappedBuffer(reader));
    }

    private static class EntrySetSkippableIterator extends RangeIterator<Long, IndexEntryWithOffsets>
    {
        private final PeekingIterator<Map.Entry<Long, Entries>> elements;

        EntrySetSkippableIterator(SortedMap<Long, Entries> elms)
        {
            super(elms.firstKey(), elms.lastKey(), elms.size());
            elements = Iterators.peekingIterator(elms.entrySet().iterator());
        }

        @Override
        public IndexEntryWithOffsets computeNext()
        {
            if (!elements.hasNext())
                return endOfData();

            Map.Entry<Long, Entries> next = elements.next();
            return new IndexEntryWithOffsets(next.getKey(), next.getValue());
        }

        @Override
        protected void performSkipTo(Long nextToken)
        {
            while (elements.hasNext())
            {
                if (Long.compare(elements.peek().getKey(), nextToken) >= 0)
                {
                    break;
                }

                elements.next();
            }
        }

        @Override
        public void close() throws IOException
        {
            // nothing to do here
        }
    }

    public static class FakeCombinedTerm extends CombinedTerm
    {
        private final SortedMap<Long, Entries> tokens;

        public FakeCombinedTerm(ClusteringComparator clusteringComparator,
                                SortedMap<Long, Entries> tokens)
        {
            super(clusteringComparator, null, null);
            this.tokens = tokens;
        }

        public RangeIterator<Long, IndexEntry> getEntryIterator()
        {
            return new TokenMapIterator(tokens);
        }
    }

    public static class TokenMapIterator extends RangeIterator<Long, IndexEntry>
    {
        public final Iterator<Map.Entry<Long, Entries>> iterator;

        public TokenMapIterator(SortedMap<Long, Entries> tokens)
        {
            super(tokens.firstKey(), tokens.lastKey(), tokens.size());
            iterator = tokens.entrySet().iterator();
        }

        public IndexEntry computeNext()
        {
            if (!iterator.hasNext())
                return endOfData();

            Map.Entry<Long, Entries> entry = iterator.next();
            return new IndexEntryWithOffsets(entry.getKey(), entry.getValue());
        }

        public void close() throws IOException
        {

        }

        public void performSkipTo(Long next)
        {
            throw new UnsupportedOperationException();
        }
    }

    public static class IndexEntryWithOffsets extends IndexEntry
    {
        private final Entries offsets;

        public IndexEntryWithOffsets(long token, final Entries offsets)
        {
            super(token);
            this.offsets = offsets;
        }

        @Override
        public Entries getOffsets()
        {
            return offsets;
        }

        @Override
        public void merge(CombinedValue<Long> other)
        {}

        @Override
        public int compareTo(CombinedValue<Long> o)
        {
            return Long.compare(token, o.get());
        }

        @Override
        public boolean equals(Object other)
        {
            if (!(other instanceof IndexEntryWithOffsets))
                return false;

            IndexEntryWithOffsets o = (IndexEntryWithOffsets) other;
            return token == o.token && offsets.equals(o.offsets);
        }

        @Override
        public int hashCode()
        {
            return new HashCodeBuilder().append(token).build();
        }

        @Override
        public String toString()
        {
            return String.format("TokenValue(token: %d, offsets: %s)", token, offsets);
        }

        @Override
        public Iterator<DecoratedKey> iterator()
        {
            List<DecoratedKey> keys = new ArrayList<>(offsets.size());
            for (TokenTreeBuilder.Entry offset : offsets)
                 keys.add(dk(offset.partitionOffset()));

            return keys.iterator();
        }
    }

    private static Set<DecoratedKey> convert(Entries entries)
    {
        Set<DecoratedKey> keys = new HashSet<>();
        for (TokenTreeBuilder.Entry entry : entries)
            keys.add(KEY_CONVERTER.apply(entry.partitionOffset()));

        return keys;
    }

    private static Set<DecoratedKey> convert(IndexEntry results)
    {
        Set<DecoratedKey> keys = new HashSet<>();
        for (DecoratedKey key : results)
            keys.add(key);

        return keys;
    }

    private static ObjectSet<TokenTreeBuilder.Entry> convert(long... values)
    {
        ObjectSet<TokenTreeBuilder.Entry> result = new ObjectOpenHashSet<>(values.length);
        for (long v : values)
            result.add(new Entry(v));

        return result;
    }

    private static class KeyConverter implements Function<Long, DecoratedKey>
    {
        @Override
        public DecoratedKey apply(Long offset)
        {
            return dk(offset);
        }
    }

    private static DecoratedKey dk(Long token)
    {
        ByteBuffer buf = ByteBuffer.allocate(8);
        buf.putLong(token);
        buf.flip();
        Long hashed = MurmurHash.hash2_64(buf, buf.position(), buf.remaining(), 0);
        return new BufferDecoratedKey(new Murmur3Partitioner.LongToken(hashed), buf);
    }

    private static TokenTree generateTree(final long minToken, final long maxToken, boolean isStatic) throws IOException
    {
        final ClusteringComparator clusteringComparator = new ClusteringComparator();
        final SortedMap<Long, Entries> toks = new TreeMap<Long, Entries>()
        {{
                for (long i = minToken; i <= maxToken; i++)
                {
                    Entries offsetSet = new Entries(clusteringComparator);
                    offsetSet.add(new Entry(i));
                    put(i, offsetSet);
                }
        }};

        final TokenTreeBuilder builder = isStatic ? new StaticTokenTreeBuilder(new FakeCombinedTerm(clusteringComparator, toks))
                                                  : new DynamicTokenTreeBuilder(clusteringComparator, toks);
        builder.finish();
        final File treeFile = FileUtils.createTempFile("token-tree-get-test", "tt");
        treeFile.deleteOnExit();

        try (SequentialWriter writer = new SequentialWriter(treeFile, DEFAULT_OPT))
        {
            builder.write(writer);
            writer.sync();
        }

        RandomAccessReader reader = null;

        try
        {
            reader = RandomAccessReader.open(treeFile);
            return new TokenTree(builder.clusteringComparator(), new MappedBuffer(reader));
        }
        finally
        {
            FileUtils.closeQuietly(reader);
        }
    }
}
