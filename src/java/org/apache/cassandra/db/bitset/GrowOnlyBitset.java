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

package org.apache.cassandra.db.bitset;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.sasi.plan.Expression;
import org.apache.cassandra.index.sasi.utils.AbstractIterator;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.roaringbitmap.RoaringBitmap;

// TODO (jwest): do we want to '"clone" to not modify original bitset?
public class GrowOnlyBitset
{
    private final RoaringBitmap bitset;

    public GrowOnlyBitset() {
        this(new RoaringBitmap());
    }

    private GrowOnlyBitset(RoaringBitmap bitset)
    {
        this.bitset = bitset;
    }

    public boolean isTombstone()
    {
        return false;
    }

    public Iterator<ByteBuffer> bufferIterator()
    {
        return new ValueBufferIterator(this);
    }

    public boolean satisifies(Expression e)
    {
        if (isTombstone())
            return false;

        Integer value = Int32Type.instance.compose(e.lower.value);
        return bitset.contains(value);
    }

    public GrowOnlyBitset merge(GrowOnlyBitset other)
    {
        if (other.isTombstone())
            return other.merge(this);

        bitset.or(other.bitset);
        return this;
    }

    public void or(GrowOnlyBitset other)
    {
        if (other.isTombstone() || isTombstone())
            return;

        bitset.or(other.bitset);
    }

    public void and(GrowOnlyBitset other)
    {
        if (other.isTombstone() || isTombstone())
            return;

        bitset.and(other.bitset);
    }

    public void xor(GrowOnlyBitset other)
    {
        if (other.isTombstone() || isTombstone())
            return;

        bitset.xor(other.bitset);
    }

    public void add(int value) {
        bitset.add(value);
    }

    public static GrowOnlyBitset deserialize(ByteBuffer bytes)
    {
        ByteBuffer buffer = bytes.duplicate();
        if (buffer.remaining() == 0)
            return new Tombstone();

        DataInput in = new DataInputStream(new DataInputBuffer(buffer, false));
        try
        {
            RoaringBitmap bitset = new RoaringBitmap();
            bitset.deserialize(in);
            return new GrowOnlyBitset(bitset);
        }
        catch (IOException e)
        {
            throw new MarshalException("corrupted bitset", e);
        }
    }

    public ByteBuffer serialize()
    {
        if (isTombstone())
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;

        DataOutputBuffer out = new DataOutputBuffer(bitset.serializedSizeInBytes());
        try
        {
            bitset.serialize(out);
            ByteBuffer buf = ByteBuffer.wrap(out.getData());
            return buf;
        }
        catch (IOException e)
        {
            throw new RuntimeException("unable to serialize bitset", e);
        }
    }

    public static class Tombstone extends GrowOnlyBitset
    {
        public Tombstone() {
            // don't waste space on a bitmap instance since we won't use it
            super(null);
        }

        @Override
        public boolean isTombstone()
        {
            return true;
        }

        @Override
        public GrowOnlyBitset merge(GrowOnlyBitset other)
        {
            return this;
        }

        @Override
        public void add(int value)
        {
            // nop
        }
    }

    public static class ValueBufferIterator extends AbstractIterator<ByteBuffer>
    {
        private final Iterator<Integer> it;

        public ValueBufferIterator(GrowOnlyBitset bitset) {
            if (bitset.isTombstone())
                it = Collections.<Integer>emptyList().iterator();
            else
                it = bitset.bitset.iterator();
        }

        public ByteBuffer computeNext()
        {
            if (!it.hasNext())
                return endOfData();

            return Int32Type.instance.decompose(it.next());
        }
    }
}
