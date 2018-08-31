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

import java.util.EnumSet;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.transport.Frame;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.transport.frame.compress.Compressor;
import org.apache.cassandra.transport.frame.compress.LZ4Compressor;
import org.apache.cassandra.transport.frame.compress.SnappyCompressor;
import org.apache.cassandra.utils.ChecksumType;
import org.quicktheories.core.Gen;

import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.*;

public class ChecksummingTransformerTest
{
    private static final int DEFAULT_BLOCK_SIZE = 1 << 15;
    private static final int MAX_INPUT_SIZE = 1 << 18;
    private static final EnumSet<Frame.Header.Flag> FLAGS = EnumSet.of(Frame.Header.Flag.COMPRESSED, Frame.Header.Flag.CHECKSUMMED);

    @BeforeClass
    public static void init()
    {
        // required as static ChecksummingTransformer instances read default block size from config
        DatabaseDescriptor.clientInitialization();
    }

    @Test
    public void roundTripSafetyProperty()
    {
        qt().withExamples(500)
            .forAll(inputs(),
                    compressors(),
                    checksumTypes(),
                    blocksizes())
            .checkAssert(this::roundTrip);
    }

    @Test
    public void roundTripZeroLengthInput()
    {
        qt().forAll(zeroLengthInputs(),
                    compressors(),
                    checksumTypes(),
                    blocksizes())
            .checkAssert(this::roundTrip);
    }

    @Test
    public void decompressingUncompressedFrameThrowsProtocolException()
    {
        qt().withExamples(50)
            .forAll(inputs(),
                    compressors(),
                    checksumTypes(),
                    blocksizes())
            .checkAssert(this::roundTripWithUncompressedInput);
    }

    private void roundTrip(String input, Compressor compressor, ChecksumType checksum, int blockSize)
    {
        System.out.println("Input Size: " + input.length() + ", compressor: " + compressor + ", checksum type: " + checksum + ", block size: " + blockSize);
        ChecksummingTransformer transformer = new ChecksummingTransformer(checksum, blockSize, compressor);
        byte[] expectedBytes = input.getBytes();
        ByteBuf expectedBuf = Unpooled.wrappedBuffer(expectedBytes);

        ByteBuf outbound = transformer.transformOutbound(expectedBuf);
        ByteBuf inbound = transformer.transformInbound(outbound, FLAGS);

        // reset reader index on expectedBuf back to 0 as it will have been entirely consumed by the transformOutbound() call
        expectedBuf.readerIndex(0);
        Assert.assertEquals(expectedBuf, inbound);
    }

    private void roundTripWithUncompressedInput(String input, Compressor compressor, ChecksumType checksum, int blockSize)
    {
        System.out.println("Input Size: " + input.length() + ", compressor: " + compressor + ", checksum type: " + checksum + ", block size: " + blockSize);
        if (compressor == null)
            return;

        ChecksummingTransformer inboundTransformer = new ChecksummingTransformer(checksum, blockSize, compressor);
        ChecksummingTransformer outboundTransformer = new ChecksummingTransformer(checksum, blockSize, null);
        byte[] expectedBytes = input.getBytes();
        ByteBuf expectedBuf = Unpooled.wrappedBuffer(expectedBytes);
        ByteBuf outbound = outboundTransformer.transformOutbound(expectedBuf);
        try
        {
            inboundTransformer.transformInbound(outbound, FLAGS);
            Assert.fail("Expected ProtocolException");
        }
        catch(ProtocolException p)
        {
            // expected
        }
    }

    private Gen<String> inputs()
    {
        Gen<String> randomStrings = strings().basicMultilingualPlaneAlphabet().ofLengthBetween(0, MAX_INPUT_SIZE);
        Gen<String> highlyCompressable = strings().betweenCodePoints('c', 'e').ofLengthBetween(1, MAX_INPUT_SIZE);
        return randomStrings.mix(highlyCompressable, 50);
    }

    private Gen<String> zeroLengthInputs()
    {
        return strings().ascii().ofLength(0);
    }

    private Gen<Compressor> compressors()
    {
        return arbitrary().pick(null, LZ4Compressor.INSTANCE, SnappyCompressor.INSTANCE);
    }

    private Gen<ChecksumType> checksumTypes()
    {
        return arbitrary().enumValuesWithNoOrder(ChecksumType.class);
    }

    private Gen<Integer> blocksizes()
    {
        return arbitrary().constant(DEFAULT_BLOCK_SIZE);
    }

}
