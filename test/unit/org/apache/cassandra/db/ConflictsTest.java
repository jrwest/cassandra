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
import java.util.Iterator;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;

public class ConflictsTest
{

    public static CFMetaData cfMetaData = CFMetaData.compile("CREATE TABLE bar(pk int primary key, c1 int)", "foo");
    public static ColumnDefinition columnDefinition;
    static {
        Iterator<ColumnDefinition> cds = cfMetaData.allColumnsInSelectOrder();
        while (cds.hasNext())
        {
            ColumnDefinition cd = cds.next();
            if (cd.isRegular())
            {
                columnDefinition = cd;
                break;
            }
        }

    }

    @Test
    public void resolveRegularIdempotencyWithEqualFields()
    {
        // the time of the read being performed
        int nowInSec = 6;

        // we use a single timestamp to represent multiple writes USING timestamp = X
        long timestamp = 1;

        // 2 ttls to simulate ttls reducing in time since they are relative (remaining time)
        int ttlA = 2;
        int ttlB = 1;

        // local deletion time is the same to simulate nowInSec + ttlA/B being equal
        int localDeletionTime = 7;

        // these are purposefully the same value to simulate the same write happening twice (idempotency)
        ByteBuffer valueA = createValue(2);
        ByteBuffer valueB = createValue(2);

        // Create cells to simulate what the read path is doing
        Cell cellA = new BufferCell(columnDefinition, timestamp, ttlA, localDeletionTime, valueA, null);
        Cell cellB = new BufferCell(columnDefinition, timestamp, ttlB, localDeletionTime, valueB, null);

        Conflicts.Resolution resolution1 = Conflicts.resolveRegular(cellA.timestamp(),
                                                                    cellA.isLive(nowInSec),
                                                                    cellA.ttl(),
                                                                    cellA.localDeletionTime(),
                                                                    cellA.value(),
                                                                    cellB.timestamp(),
                                                                    cellB.isLive(nowInSec),
                                                                    cellB.ttl(),
                                                                    cellB.localDeletionTime(),
                                                                    cellB.value());

        Conflicts.Resolution resolution2 = Conflicts.resolveRegular(cellB.timestamp(),
                                                                    cellB.isLive(nowInSec),
                                                                    cellB.ttl(),
                                                                    cellB.localDeletionTime(),
                                                                    cellB.value(),
                                                                    cellA.timestamp(),
                                                                    cellA.isLive(nowInSec),
                                                                    cellA.ttl(),
                                                                    cellA.localDeletionTime(),
                                                                    cellA.value());

        // its expected that Conflcits.resolveRegular chooses the same Cell regardless of if
        // the order is passed, so it is expected that by changing the order, the Resolution
        // returned would change as well
        Assert.assertTrue(String.format("found equal resolutions (%s, %s)", resolution1, resolution2),
                          resolution1 != resolution2);
    }

    public ByteBuffer createValue(int value)
    {
        ByteBuffer buf = ByteBuffer.allocate(4);
        buf.putInt(value);
        buf.flip();

        return buf;
    }

}
