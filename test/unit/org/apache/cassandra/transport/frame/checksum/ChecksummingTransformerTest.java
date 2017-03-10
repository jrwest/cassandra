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

package org.apache.cassandra.transport.frame.checksum;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.cassandra.transport.Frame;
import org.apache.cassandra.transport.frame.compress.Compressor;
import org.apache.cassandra.transport.frame.compress.LZ4Compressor;
import org.apache.cassandra.transport.frame.compress.SnappyCompressor;
import org.apache.cassandra.utils.ChecksumType;

/**
 * Created by mkjellman on 3/6/17.
 */
public class ChecksummingTransformerTest
{
    private static final Random RANDOM = new Random();
    private static final int DEFAULT_BLOCK_SIZE = 1 << 15;
    private static final EnumSet<Frame.Header.Flag> FLAGS = EnumSet.of(Frame.Header.Flag.COMPRESSED, Frame.Header.Flag.CHECKSUMMED);

    @Test
    public void roundTripSmall() throws IOException
    {
        String randomString = generateRandomWord(10);
        roundTrip(randomString, null, ChecksumType.CRC32, DEFAULT_BLOCK_SIZE);
        roundTrip(randomString, null, ChecksumType.Adler32, DEFAULT_BLOCK_SIZE);
        roundTrip(randomString, LZ4Compressor.INSTANCE, ChecksumType.CRC32, DEFAULT_BLOCK_SIZE);
        roundTrip(randomString, LZ4Compressor.INSTANCE, ChecksumType.Adler32, DEFAULT_BLOCK_SIZE);
        roundTrip(randomString, SnappyCompressor.INSTANCE, ChecksumType.CRC32, DEFAULT_BLOCK_SIZE);
        roundTrip(randomString, SnappyCompressor.INSTANCE, ChecksumType.Adler32, DEFAULT_BLOCK_SIZE);
    }

    @Test
    public void roundTripSimple() throws IOException
    {
        testRoundTrips(null, ChecksumType.CRC32);
        testRoundTrips(null, ChecksumType.Adler32);
        testRoundTrips(LZ4Compressor.INSTANCE, ChecksumType.CRC32);
        testRoundTrips(LZ4Compressor.INSTANCE, ChecksumType.Adler32);
        testRoundTrips(SnappyCompressor.INSTANCE, ChecksumType.CRC32);
        testRoundTrips(SnappyCompressor.INSTANCE, ChecksumType.Adler32);
    }

    private static void testRoundTrips(Compressor compressor, ChecksumType checksum) throws IOException
    {
        // encode with multiple block sizes to make sure they all work
        String randomString = generateRandomWord(1 << 18);
        roundTrip(randomString, compressor, checksum, 1 << 14); // 16kb
        roundTrip(randomString, compressor, checksum, 1 << 15); // 32kb
        roundTrip(randomString, compressor, checksum, 1 << 16); // 64kb
        roundTrip(randomString, compressor, checksum, 1 << 21); // 2mb

        String highlyCompressableString = "bbbbbbbbbb";
        roundTrip(highlyCompressableString, compressor, checksum, 1 << 14); // 16kb
        roundTrip(highlyCompressableString, compressor, checksum, 1 << 15); // 32kb
        roundTrip(highlyCompressableString, compressor, checksum, 1 << 16); // 64kb
        roundTrip(highlyCompressableString, compressor, checksum, 1 << 21); // 2mb
    }

    private static void roundTrip(String input, Compressor compressor, ChecksumType checksum, int blockSize) throws IOException
    {
        ChecksummingTransformer transformer = new ChecksummingTransformer(checksum, blockSize, compressor);
        byte[] expectedBytes = input.getBytes();
        ByteBuf expectedBuf = Unpooled.wrappedBuffer(expectedBytes);

        ByteBuf outbound = transformer.transformOutbound(expectedBuf);
        ByteBuf inbound = transformer.transformInbound(outbound, FLAGS);
        // reset reader index on expectedBuf back to 0 as it will have been entirely consumed by the transformOutbound() call
        expectedBuf.readerIndex(0);
        Assert.assertEquals(expectedBuf, inbound);
    }

    private static String generateRandomWord(int length)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++)
        {
            sb.append((char) (RANDOM.nextInt(26) + 'a'));
        }
        return sb.toString();
    }
}
