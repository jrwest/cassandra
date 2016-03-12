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

package org.apache.cassandra.db.marshal;


import java.nio.ByteBuffer;

import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.db.bitset.GrowOnlyBitset;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.roaringbitmap.RoaringBitmap;

// TODO (jwest): for indexing it may make more sense to extend CollectionType.
// can't extend SetType because we want T=RoaringBitmap not Integer because
// serializer must take RoaringBitmap not Int
public class BitsetType extends AbstractType<GrowOnlyBitset>
{
    public static final BitsetType instance = new BitsetType();

    public BitsetType() {
        // TODO (jwest): use CUSTOM and use popcount (order by size?)
        super(ComparisonType.NOT_COMPARABLE);
    }

    @Override
    public boolean isBitset()
    {
        return true;
    }

    @Override
    public BitsetSerializer getSerializer() {
        return BitsetSerializer.instance;
    }

    @Override
    public Term fromJSONObject(Object parsed) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer fromString(String source) {
        throw new UnsupportedOperationException();
    }

    public static class BitsetSerializer implements TypeSerializer<GrowOnlyBitset> {
        public static final BitsetSerializer instance = new BitsetSerializer();

        @Override
        public ByteBuffer serialize(GrowOnlyBitset bitset) {
            return bitset.serialize();
        }

        @Override
        public GrowOnlyBitset deserialize(ByteBuffer bytes) {
            return GrowOnlyBitset.deserialize(bytes);
        }

        @Override
        public void validate(ByteBuffer bytes) throws MarshalException
        {
            GrowOnlyBitset.deserialize(bytes);
        }

        @Override
        public String toString(GrowOnlyBitset bitset) {
            throw new RuntimeException("not implemented");
        }

        @Override
        public Class<GrowOnlyBitset> getType() {
            return GrowOnlyBitset.class;
        }
    }
}
