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
package org.apache.cassandra.db;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.bitset.GrowOnlyBitset;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.rows.Cell;

public abstract class Conflicts
{
    private Conflicts() {}

    public enum Resolution { LEFT_WINS, MERGE, RIGHT_WINS };

    public static Resolution resolveRegular(long leftTimestamp,
                                            boolean leftLive,
                                            int leftLocalDeletionTime,
                                            ByteBuffer leftValue,
                                            long rightTimestamp,
                                            boolean rightLive,
                                            int rightLocalDeletionTime,
                                            ByteBuffer rightValue)
    {
        if (leftTimestamp != rightTimestamp)
            return leftTimestamp < rightTimestamp ? Resolution.RIGHT_WINS : Resolution.LEFT_WINS;

        if (leftLive != rightLive)
            return leftLive ? Resolution.RIGHT_WINS : Resolution.LEFT_WINS;

        int c = leftValue.compareTo(rightValue);
        if (c < 0)
            return Resolution.RIGHT_WINS;
        else if (c > 0)
            return Resolution.LEFT_WINS;

        // Prefer the longest ttl if relevant
        return leftLocalDeletionTime < rightLocalDeletionTime ? Resolution.RIGHT_WINS : Resolution.LEFT_WINS;
    }

    public static Resolution resolveCounter(long leftTimestamp,
                                            boolean leftLive,
                                            ByteBuffer leftValue,
                                            long rightTimestamp,
                                            boolean rightLive,
                                            ByteBuffer rightValue)
    {
        // No matter what the counter cell's timestamp is, a tombstone always takes precedence. See CASSANDRA-7346.
        if (!leftLive)
            // left is a tombstone: it has precedence over right if either right is not a tombstone, or left has a greater timestamp
            return rightLive || leftTimestamp > rightTimestamp ? Resolution.LEFT_WINS : Resolution.RIGHT_WINS;

        // If right is a tombstone, since left isn't one, it has precedence
        if (!rightLive)
            return Resolution.RIGHT_WINS;

        return Resolution.MERGE;
    }

    public static ByteBuffer mergeCounterValues(ByteBuffer left, ByteBuffer right)
    {
        return CounterContext.instance().merge(left, right);
    }

    public static Resolution resolveBitset(Cell c1, Cell c2)
    {
        if (c1.isTombstone())
            return Resolution.LEFT_WINS;
        else if (c2.isTombstone())
            return Resolution.RIGHT_WINS;
        else
            return Resolution.MERGE;
    }

    public static ByteBuffer mergeBitsetValues(ByteBuffer left, ByteBuffer right)
    {
        GrowOnlyBitset bitset1 = GrowOnlyBitset.deserialize(left);
        GrowOnlyBitset bitset2 = GrowOnlyBitset.deserialize(right);
        return bitset1.merge(bitset2).serialize();
    }

}
