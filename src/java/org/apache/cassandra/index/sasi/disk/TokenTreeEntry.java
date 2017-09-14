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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.carrotsearch.hppc.LongArrayList;

/**
 * A data entry in the token tree. Contains the offset
 * of a partition key in the index file and the offsets
 * of the rows in the data file.
 */
public class TokenTreeEntry
{

    private long partitionOffset;
    private LongArrayList rowOffsets;

    public TokenTreeEntry(long partitionOffset)
    {
        this.partitionOffset = partitionOffset;
        this.rowOffsets = null;
    }

    /**
     * @return an offset in an sstable index file pointing
     * to the start of a RowIndexEntry
     */
    public long getPartitionOffset()
    {
        return partitionOffset;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        TokenTreeEntry that = (TokenTreeEntry) o;

        return new EqualsBuilder()
               .append(partitionOffset, that.partitionOffset)
               .append(rowOffsets, that.rowOffsets)
               .isEquals();
    }

    public int hashCode()
    {
        return new HashCodeBuilder(17, 37)
               .append(partitionOffset)
               .toHashCode();
    }
}
