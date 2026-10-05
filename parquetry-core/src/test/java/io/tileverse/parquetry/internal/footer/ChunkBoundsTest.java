/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.internal.footer;

import static io.tileverse.parquetry.format.ParquetLayouts.INT32;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.CompressionCodec;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.format.Statistics;

/**
 * Covers the statistics bounds kept by the compact footer. The deprecated min and max fields were computed in signed
 * order; a pruning tier comparing binary values as unsigned bytes must not see them, unless the two are equal.
 */
class ChunkBoundsTest {

    /** A name with a UTF-8 leading byte of 0xC3, negative as a signed byte: below "Bern" in signed order only. */
    private static final String OLTEN = "\u00D6lten";

    private static final String RECENT_PARQUET_MR =
            "parquet-mr version 1.9.0 (build 38262e2c80015d0935dad20f8e18f2d6f9fbd03c)";
    private static final String AFFECTED_PARQUET_MR = "parquet-mr version 1.7.0 (build 1234567890abcdef)";

    @Test
    void modernBoundsAreKeptWhateverTheType() {
        Statistics stats = Statistics.builder()
                .minValue(utf8("Bern"))
                .maxValue(utf8(OLTEN))
                .build();
        ChunkBounds bounds = ChunkBounds.of(footerWrittenBy(Optional.empty()));
        ColumnMetaData meta = chunk(PhysicalType.BYTE_ARRAY, stats);

        assertThat(text(bounds.min(meta))).isEqualTo("Bern");
        assertThat(text(bounds.max(meta))).isEqualTo(OLTEN);
    }

    @Test
    void legacyBoundsOfASignedNumericTypeAreKept() {
        Statistics stats = Statistics.builder().min(int32(-5)).max(int32(9)).build();
        ChunkBounds bounds = ChunkBounds.of(footerWrittenBy(Optional.empty()));
        ColumnMetaData meta = chunk(PhysicalType.INT32, stats);

        assertThat(bounds.min(meta).get(INT32, 0)).isEqualTo(-5);
        assertThat(bounds.max(meta).get(INT32, 0)).isEqualTo(9);
    }

    @Test
    void legacyBinaryBoundsInSignedOrderAreDropped() {
        Statistics stats =
                Statistics.builder().min(utf8(OLTEN)).max(utf8("Bern")).build();
        ChunkBounds bounds = ChunkBounds.of(footerWrittenBy(Optional.of(RECENT_PARQUET_MR)));
        ColumnMetaData meta = chunk(PhysicalType.BYTE_ARRAY, stats);

        assertThat(bounds.min(meta)).isEqualTo(MemorySegment.NULL);
        assertThat(bounds.max(meta)).isEqualTo(MemorySegment.NULL);
    }

    @Test
    void legacyFixedLengthBoundsInSignedOrderAreDropped() {
        Statistics stats = Statistics.builder()
                .min(bytes(0x80, 0x00))
                .max(bytes(0x7F, 0xFF))
                .build();
        ChunkBounds bounds = ChunkBounds.of(footerWrittenBy(Optional.of(RECENT_PARQUET_MR)));
        ColumnMetaData meta = chunk(PhysicalType.FIXED_LEN_BYTE_ARRAY, stats);

        assertThat(bounds.min(meta)).isEqualTo(MemorySegment.NULL);
        assertThat(bounds.max(meta)).isEqualTo(MemorySegment.NULL);
    }

    @Test
    void equalLegacyBinaryBoundsAreKept() {
        Statistics stats =
                Statistics.builder().min(utf8("Bern")).max(utf8("Bern")).build();
        ChunkBounds bounds = ChunkBounds.of(footerWrittenBy(Optional.of(RECENT_PARQUET_MR)));
        ColumnMetaData meta = chunk(PhysicalType.BYTE_ARRAY, stats);

        assertThat(text(bounds.min(meta))).isEqualTo("Bern");
        assertThat(text(bounds.max(meta))).isEqualTo("Bern");
    }

    @Test
    void equalLegacyBinaryBoundsFromAnAffectedParquetMrAreDropped() {
        Statistics stats =
                Statistics.builder().min(utf8("Bern")).max(utf8("Bern")).build();
        ChunkBounds bounds = ChunkBounds.of(footerWrittenBy(Optional.of(AFFECTED_PARQUET_MR)));
        ColumnMetaData meta = chunk(PhysicalType.BYTE_ARRAY, stats);

        assertThat(bounds.min(meta)).isEqualTo(MemorySegment.NULL);
        assertThat(bounds.max(meta)).isEqualTo(MemorySegment.NULL);
    }

    @Test
    void chunkWithoutStatisticsKeepsNoBound() {
        ChunkBounds bounds = ChunkBounds.of(footerWrittenBy(Optional.empty()));
        ColumnMetaData meta = ColumnMetaData.builder()
                .type(PhysicalType.INT32)
                .codec(CompressionCodec.UNCOMPRESSED)
                .pathInSchema(List.of("v"))
                .numValues(1L)
                .totalCompressedSize(1L)
                .dataPageOffset(4L)
                .build();

        assertThat(bounds.min(meta)).isEqualTo(MemorySegment.NULL);
        assertThat(bounds.max(meta)).isEqualTo(MemorySegment.NULL);
    }

    static Stream<Arguments> writers() {
        return Stream.of(
                Arguments.of(
                        Optional.of("parquet-mr version 1.8.0 (build 0fda28af84b9746396014ad6a415b90592a98b3b)"), true),
                Arguments.of(Optional.of(RECENT_PARQUET_MR), true),
                Arguments.of(
                        Optional.of("parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)"),
                        true),
                Arguments.of(Optional.of("parquet-mr version 2.0.0"), true),
                Arguments.of(Optional.of("parquet-cpp-arrow version 14.0.1"), true),
                Arguments.of(
                        Optional.of("impala version 1.3.0-INTERNAL (build 8a48ddb1eff84592b3fc06bc6f51ec120e1fffc9)"),
                        true),
                Arguments.of(Optional.of(AFFECTED_PARQUET_MR), false),
                Arguments.of(Optional.of("parquet-mr version 1.5.0-cdh5.4.0 (build abc)"), false),
                Arguments.of(Optional.of("parquet-mr"), false),
                Arguments.of(Optional.of("  "), false),
                Arguments.of(Optional.empty(), false));
    }

    @ParameterizedTest(name = "{0}: {1}")
    @MethodSource("writers")
    void binaryStatisticsAreReliableFromParquetMr18OnAndFromOtherWriters(Optional<String> createdBy, boolean reliable) {
        assertThat(ChunkBounds.binaryStatisticsReliable(createdBy)).isEqualTo(reliable);
    }

    private static FileMetaData footerWrittenBy(Optional<String> createdBy) {
        return FileMetaData.builder().version(1).createdBy(createdBy).build();
    }

    private static ColumnMetaData chunk(PhysicalType type, Statistics stats) {
        return ColumnMetaData.builder()
                .type(type)
                .codec(CompressionCodec.UNCOMPRESSED)
                .pathInSchema(List.of("v"))
                .numValues(2L)
                .totalCompressedSize(1L)
                .dataPageOffset(4L)
                .statistics(Optional.of(stats))
                .build();
    }

    private static MemorySegment utf8(String value) {
        return MemorySegment.ofArray(value.getBytes(StandardCharsets.UTF_8));
    }

    private static MemorySegment bytes(int... values) {
        byte[] array = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            array[i] = (byte) values[i];
        }
        return MemorySegment.ofArray(array);
    }

    private static MemorySegment int32(int value) {
        MemorySegment segment = MemorySegment.ofArray(new byte[Integer.BYTES]);
        segment.set(INT32, 0L, value);
        return segment;
    }

    private static String text(MemorySegment segment) {
        return new String(segment.toArray(JAVA_BYTE), StandardCharsets.UTF_8);
    }
}
