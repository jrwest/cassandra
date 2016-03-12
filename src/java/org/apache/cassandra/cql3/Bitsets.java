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

package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.bitset.GrowOnlyBitset;
import org.apache.cassandra.db.marshal.BitsetType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.exceptions.InvalidRequestException;

public class Bitsets
{
    public static ColumnSpecification valueSpecOf(ColumnSpecification column) {
       return new ColumnSpecification(column.ksName, column.cfName, column.name, Int32Type.instance);
    }

    public static class Setter extends Operation
    {
        public Setter(ColumnDefinition column, Term t)
        {
            super(column, t);
        }

        public void execute(DecoratedKey partitionKey, UpdateParameters params) throws InvalidRequestException
        {
            Term.Terminal value = t.bind(params.options);
            if (value == null)
                return;

            if (((Sets.Value) value).elements.size() > 0)
                throw new InvalidRequestException("The values of bitset columns cannot be set unless they are " +
                                                  "being cleared (set to the empty set {}");

            params.addTombstone(column);
        }
    }

    public static class Adder extends Operation
    {
        public Adder(ColumnDefinition column, Term t)
        {
            super(column, t);
        }

        public void execute(DecoratedKey partitionKey, UpdateParameters params) throws InvalidRequestException
        {
            Term.Terminal value = t.bind(params.options);
            if (value == null)
                return;

            GrowOnlyBitset bitset = new GrowOnlyBitset();
            for (ByteBuffer bb : ((Sets.Value) value).elements)
            {
                bitset.add(Int32Type.instance.compose(bb));
            }

            params.addCell(column, BitsetType.instance.decompose(bitset));
        }
    }
}
