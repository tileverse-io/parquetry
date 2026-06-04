/*
 * Copyright (c) 2026 Multivers.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.tileverse.parquetry.internal.write;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.data.WriteOptions.BloomFilterConfig;
import io.tileverse.parquetry.format.BloomFilterHeader;
import io.tileverse.parquetry.format.codec.ParquetFormatDeserializer;
import io.tileverse.parquetry.internal.filter.bloom.BloomFilterReader;
import io.tileverse.parquetry.internal.filter.bloom.SplitBlockBloomFilter;

class BloomFilterBuilderTest {

    private static final BloomFilterConfig SMALL = new BloomFilterConfig(0.01, 1_000L);

    @Test
    void int32RoundTripHasNoFalseNegativesAndBoundedFalsePositives() throws IOException {
        BloomFilterBuilder builder = new BloomFilterBuilder(SMALL);
        for (int i = 0; i < 1_000; i++) {
            builder.add(i);
        }

        SplitBlockBloomFilter reader = roundTrip(builder);

        for (int i = 0; i < 1_000; i++) {
            assertThat(reader.mightContainInt(i)).as("present(%d)", i).isTrue();
        }
        int falsePositives = 0;
        for (int i = 10_000; i < 11_000; i++) {
            if (reader.mightContainInt(i)) {
                falsePositives++;
            }
        }
        assertThat(falsePositives)
                .as("false positives over 1000 known-absent ints; budget = 2 * fpp * n")
                .isLessThanOrEqualTo(20);
    }

    @Test
    void int64RoundTripHasNoFalseNegatives() throws IOException {
        BloomFilterBuilder builder = new BloomFilterBuilder(SMALL);
        for (long i = 0; i < 1_000; i++) {
            builder.add(i);
        }

        SplitBlockBloomFilter reader = roundTrip(builder);

        for (long i = 0; i < 1_000; i++) {
            assertThat(reader.mightContainLong(i)).as("present(%d)", i).isTrue();
        }
    }

    @Test
    void floatRoundTripHasNoFalseNegatives() throws IOException {
        BloomFilterBuilder builder = new BloomFilterBuilder(SMALL);
        for (int i = 0; i < 1_000; i++) {
            builder.add((float) i + 0.5f);
        }

        SplitBlockBloomFilter reader = roundTrip(builder);

        for (int i = 0; i < 1_000; i++) {
            assertThat(reader.mightContainFloat((float) i + 0.5f))
                    .as("present(%f)", (float) i + 0.5f)
                    .isTrue();
        }
    }

    @Test
    void doubleRoundTripHasNoFalseNegatives() throws IOException {
        BloomFilterBuilder builder = new BloomFilterBuilder(SMALL);
        for (int i = 0; i < 1_000; i++) {
            builder.add((double) i + 0.25);
        }

        SplitBlockBloomFilter reader = roundTrip(builder);

        for (int i = 0; i < 1_000; i++) {
            assertThat(reader.mightContainDouble((double) i + 0.25))
                    .as("present(%f)", (double) i + 0.25)
                    .isTrue();
        }
    }

    @Test
    void byteArrayRoundTripHasNoFalseNegatives() throws IOException {
        BloomFilterBuilder builder = new BloomFilterBuilder(SMALL);
        byte[][] keys = new byte[1_000][];
        for (int i = 0; i < 1_000; i++) {
            keys[i] = ("key-" + i).getBytes(StandardCharsets.UTF_8);
            builder.add(MemorySegment.ofArray(keys[i]).asReadOnly());
        }

        SplitBlockBloomFilter reader = roundTrip(builder);

        for (int i = 0; i < 1_000; i++) {
            assertThat(reader.mightContainBytes(keys[i])).as("present(%d)", i).isTrue();
        }
    }

    @Test
    void emptyBuilderReadsBackAsValidEmptyFilter() throws IOException {
        BloomFilterBuilder builder = new BloomFilterBuilder(SMALL);

        SplitBlockBloomFilter reader = roundTrip(builder);

        // An empty SBBF has every block all-zeros; mightContain returns false for any hash because the first probe
        // word will be 0.
        for (int i = 0; i < 100; i++) {
            assertThat(reader.mightContainInt(i))
                    .as("brand-new empty filter should report absent for %d", i)
                    .isFalse();
        }
    }

    @Test
    void writeToReturnsHeaderPlusBitsetByteLength() throws IOException {
        BloomFilterBuilder builder = new BloomFilterBuilder(SMALL);
        builder.add(42);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BloomFilterPlacement placement = builder.writeTo(out, 1024L);

        assertThat(placement.byteOffset()).isEqualTo(1024L);
        assertThat(placement.byteLength()).isEqualTo(out.size());
        assertThat(out.size()).isGreaterThan(builder.bitsetByteSize());
    }

    @Test
    void emittedHeaderCarriesSpecMandatedAlgorithmHashAndCompression() throws IOException {
        BloomFilterBuilder builder = new BloomFilterBuilder(SMALL);
        builder.add(123);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        builder.writeTo(out);

        BloomFilterHeader header =
                ParquetFormatDeserializer.readBloomFilterHeader(new ByteArrayInputStream(out.toByteArray()));
        assertThat(header.algorithm()).isEqualTo(BloomFilterHeader.Algorithm.SPLIT_BLOCK);
        assertThat(header.hash()).isEqualTo(BloomFilterHeader.HashStrategy.XXHASH);
        assertThat(header.compression()).isEqualTo(BloomFilterHeader.Compression.UNCOMPRESSED);
        assertThat(header.numBytes()).isEqualTo(builder.bitsetByteSize());
    }

    /** Round-trips through {@link BloomFilterReader#decode} on the same bytes the builder emitted. */
    private static SplitBlockBloomFilter roundTrip(BloomFilterBuilder builder) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        builder.writeTo(out);
        return io.tileverse.parquetry.internal.filter.bloom.BloomFilterReader.decode(
                ByteBuffer.wrap(out.toByteArray()));
    }
}
