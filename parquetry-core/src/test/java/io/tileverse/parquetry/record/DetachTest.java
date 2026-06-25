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
package io.tileverse.parquetry.record;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testsupport.CorpusFixtures;

/**
 * Pins the detach contract: a detached value owns its bytes and stays readable after the producing batch closes, while
 * scalars round-trip unchanged and detaching is idempotent.
 */
class DetachTest {

    private static final Path FIXTURE =
            CorpusFixtures.parquetTestingData().resolve("data_index_bloom_encoding_stats.parquet");

    private static final ColumnPath STRING = ColumnPath.of("String");

    @Test
    void detachedBinaryValueOutlivesTheBatch() {
        byte[] expected;
        Object detached;
        try (ByteRangeSource source = ByteRangeSource.ofFile(FIXTURE)) {
            ParquetFileReader dataset = ParquetFileReader.open(source);
            try (Stream<ParquetRecord> rows =
                    dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                ParquetRecord first = rows.findFirst().orElseThrow();
                MemorySegment live = (MemorySegment) first.get(STRING);
                expected = live.toArray(JAVA_BYTE);
                detached = Detach.detach(first.get(STRING));
            }
        }

        assertThat(detached).isInstanceOf(MemorySegment.class);
        MemorySegment detachedSegment = (MemorySegment) detached;
        assertThat(detachedSegment.isReadOnly())
                .as("detached binary value is read-only")
                .isTrue();
        assertThat(detachedSegment.toArray(JAVA_BYTE))
                .as("detached bytes survive batch close")
                .isEqualTo(expected);
    }

    @Test
    void detachedRecordOutlivesTheBatch() {
        byte[] expected;
        ParquetRecord detached;
        try (ByteRangeSource source = ByteRangeSource.ofFile(FIXTURE)) {
            ParquetFileReader dataset = ParquetFileReader.open(source);
            try (Stream<ParquetRecord> rows =
                    dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                ParquetRecord first = rows.findFirst().orElseThrow();
                MemorySegment live = (MemorySegment) first.get(STRING);
                expected = live.toArray(JAVA_BYTE);
                detached = first.detach();
            }
        }

        MemorySegment detachedSegment = (MemorySegment) detached.get(STRING);
        assertThat(detachedSegment.toArray(JAVA_BYTE))
                .as("detached record bytes survive batch close")
                .isEqualTo(expected);
        assertThat(detachedSegment.isReadOnly()).isTrue();
    }

    @Test
    void detachedRecordIsIdempotent() {
        byte[] expected;
        ParquetRecord twiceDetached;
        try (ByteRangeSource source = ByteRangeSource.ofFile(FIXTURE)) {
            ParquetFileReader dataset = ParquetFileReader.open(source);
            try (Stream<ParquetRecord> rows =
                    dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                ParquetRecord first = rows.findFirst().orElseThrow();
                expected = ((MemorySegment) first.get(STRING)).toArray(JAVA_BYTE);
                twiceDetached = first.detach().detach();
            }
        }

        assertThat(((MemorySegment) twiceDetached.get(STRING)).toArray(JAVA_BYTE))
                .as("detaching an already-detached record yields equivalent bytes")
                .isEqualTo(expected);
    }

    @Test
    void detachListOfSegmentsCopiesElementsAndKeepsNulls() {
        MemorySegment original = MemorySegment.ofArray(new byte[] {1, 2, 3});
        List<Object> source = new ArrayList<>();
        source.add(original);
        source.add(null);

        Object result = Detach.detach(source);

        assertThat(result).isInstanceOf(List.class);
        @SuppressWarnings("unchecked")
        List<Object> detached = (List<Object>) result;
        assertThat(detached).hasSize(2);
        assertThat(detached.get(1)).as("null elements are preserved").isNull();

        MemorySegment detachedElement = (MemorySegment) detached.get(0);
        assertThat(detachedElement.toArray(JAVA_BYTE)).isEqualTo(new byte[] {1, 2, 3});
        assertThat(detachedElement.isReadOnly()).isTrue();
        assertThat(detachedElement)
                .as("detached element is a fresh segment, not the original")
                .isNotSameAs(original);
    }

    @Test
    void detachScalarReturnsEqualValue() {
        assertThat(Detach.detach(42)).isEqualTo(42);
        assertThat(Detach.detach("hello")).isEqualTo("hello");
        assertThat(Detach.detach(null)).isNull();
    }

    @Test
    void detachSegmentIsIdempotent() {
        MemorySegment original = MemorySegment.ofArray(new byte[] {9, 8, 7}).asReadOnly();

        Object once = Detach.detach(original);
        Object twice = Detach.detach(once);

        assertThat(((MemorySegment) twice).toArray(JAVA_BYTE)).isEqualTo(new byte[] {9, 8, 7});
    }
}
