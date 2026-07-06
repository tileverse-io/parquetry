/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.format.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.BloomFilterHeader;
import io.tileverse.parquetry.format.ParquetFormatException;

/**
 * Direct unit coverage for the Thrift {@code BloomFilterHeader} deserializer: happy path with every defined case, the
 * four missing-required-field rejections, and the three unknown-union-case rejections. Hand-rolls the on-disk bytes so
 * the test is independent of any encoder.
 */
class BloomFilterHeaderDeserializerTest {

    @Test
    void roundTripsCanonicalHeader() {
        byte[] bytes = synthesizeHeader(1024, 1, 1, 1);
        BloomFilterHeader header = ParquetFormatDeserializer.readBloomFilterHeader(new ByteArrayInputStream(bytes));
        assertThat(header.numBytes()).isEqualTo(1024);
        assertThat(header.algorithm()).isEqualTo(BloomFilterHeader.Algorithm.SPLIT_BLOCK);
        assertThat(header.hash()).isEqualTo(BloomFilterHeader.HashStrategy.XXHASH);
        assertThat(header.compression()).isEqualTo(BloomFilterHeader.Compression.UNCOMPRESSED);
    }

    @Test
    void rejectsUnknownAlgorithmVariant() {
        // Algorithm union with case id 2 (no such case defined).
        ByteArrayInputStream in = new ByteArrayInputStream(synthesizeHeader(64, 2, 1, 1));
        assertThatThrownBy(() -> ParquetFormatDeserializer.readBloomFilterHeader(in))
                .isInstanceOf(ParquetFormatException.class)
                .hasMessageContaining("Unknown BloomFilterAlgorithm");
    }

    @Test
    void rejectsUnknownHashVariant() {
        ByteArrayInputStream in = new ByteArrayInputStream(synthesizeHeader(64, 1, 2, 1));
        assertThatThrownBy(() -> ParquetFormatDeserializer.readBloomFilterHeader(in))
                .isInstanceOf(ParquetFormatException.class)
                .hasMessageContaining("Unknown BloomFilterHash");
    }

    @Test
    void rejectsUnknownCompressionVariant() {
        ByteArrayInputStream in = new ByteArrayInputStream(synthesizeHeader(64, 1, 1, 2));
        assertThatThrownBy(() -> ParquetFormatDeserializer.readBloomFilterHeader(in))
                .isInstanceOf(ParquetFormatException.class)
                .hasMessageContaining("Unknown BloomFilterCompression");
    }

    @Test
    void rejectsMissingNumBytes() {
        // Omit field 1 - bytes start with field 2 (algorithm). Reader sees algorithm/hash/compression but no numBytes.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        // field 2 (delta=2, struct) algorithm
        out.write((2 << 4) | 0xC);
        writeEmptyVariant(out);
        // field 3 (delta=1, struct) hash
        out.write((1 << 4) | 0xC);
        writeEmptyVariant(out);
        // field 4 (delta=1, struct) compression
        out.write((1 << 4) | 0xC);
        writeEmptyVariant(out);
        out.write(0);
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        assertThatThrownBy(() -> ParquetFormatDeserializer.readBloomFilterHeader(in))
                .isInstanceOf(ParquetFormatException.class)
                .hasMessageContaining("numBytes");
    }

    @Test
    void rejectsMissingAlgorithm() {
        // Only field 1 then STOP.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write((1 << 4) | 0x5);
        writeVarint(out, zigZag32(64));
        out.write(0);
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        assertThatThrownBy(() -> ParquetFormatDeserializer.readBloomFilterHeader(in))
                .isInstanceOf(ParquetFormatException.class)
                .hasMessageContaining("algorithm");
    }

    @Test
    void rejectsMissingHash() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write((1 << 4) | 0x5);
        writeVarint(out, zigZag32(64));
        out.write((1 << 4) | 0xC);
        writeEmptyVariant(out);
        out.write(0);
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        assertThatThrownBy(() -> ParquetFormatDeserializer.readBloomFilterHeader(in))
                .isInstanceOf(ParquetFormatException.class)
                .hasMessageContaining("hash");
    }

    @Test
    void rejectsMissingCompression() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write((1 << 4) | 0x5);
        writeVarint(out, zigZag32(64));
        out.write((1 << 4) | 0xC);
        writeEmptyVariant(out);
        out.write((1 << 4) | 0xC);
        writeEmptyVariant(out);
        out.write(0);
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        assertThatThrownBy(() -> ParquetFormatDeserializer.readBloomFilterHeader(in))
                .isInstanceOf(ParquetFormatException.class)
                .hasMessageContaining("compression");
    }

    // --- helpers ---

    /**
     * Synthesizes a complete {@code BloomFilterHeader} with the supplied union case ids. Case id 1 is the only defined
     * value for all three unions; passing 2 etc. lets us exercise the rejection paths.
     */
    private static byte[] synthesizeHeader(
            int numBytes, int algorithmVariant, int hashVariant, int compressionVariant) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write((1 << 4) | 0x5);
        writeVarint(out, zigZag32(numBytes));
        out.write((1 << 4) | 0xC);
        writeVariant(out, algorithmVariant);
        out.write((1 << 4) | 0xC);
        writeVariant(out, hashVariant);
        out.write((1 << 4) | 0xC);
        writeVariant(out, compressionVariant);
        out.write(0);
        return out.toByteArray();
    }

    /**
     * Writes a single-case union with an arbitrary case field id. Each case is itself an empty struct (STOP-only body);
     * the union's STOP byte trails.
     */
    private static void writeVariant(ByteArrayOutputStream out, int variantId) {
        if (variantId <= 15) {
            out.write((variantId << 4) | 0xC);
        } else {
            // Long-form: delta-zero header byte then absolute zigzag varint of the field id.
            out.write(0xC);
            writeVarint(out, zigZag32(variantId));
        }
        out.write(0); // case struct STOP
        out.write(0); // union STOP
    }

    private static void writeEmptyVariant(ByteArrayOutputStream out) {
        writeVariant(out, 1);
    }

    private static int zigZag32(int n) {
        return (n << 1) ^ (n >> 31);
    }

    private static void writeVarint(ByteArrayOutputStream out, int value) {
        while ((value & ~0x7f) != 0) {
            out.write((value & 0x7f) | 0x80);
            value >>>= 7;
        }
        out.write(value & 0x7f);
    }
}
