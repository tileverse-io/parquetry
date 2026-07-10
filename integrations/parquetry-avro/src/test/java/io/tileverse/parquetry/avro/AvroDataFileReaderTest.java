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
package io.tileverse.parquetry.avro;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.testkit.TestCorpus;

class AvroDataFileReaderTest {

    private static final String SCHEMA = "{\"type\":\"record\",\"name\":\"row\",\"fields\":["
            + "{\"name\":\"id\",\"type\":\"long\"},"
            + "{\"name\":\"label\",\"type\":\"string\"},"
            + "{\"name\":\"value\",\"type\":\"double\"},"
            + "{\"name\":\"flag\",\"type\":\"boolean\"}]}";

    private static byte[] row(long id, String label, double value, boolean flag) {
        return new AvroTestEncoder()
                .longValue(id)
                .string(label)
                .doubleValue(value)
                .booleanValue(flag)
                .toBytes();
    }

    @Test
    void readsNullCodecBlock() {
        byte[] ocf = new OcfTestWriter()
                .schema(SCHEMA)
                .codec("null")
                .block(List.of(row(1, "a", 1.5, true), row(2, "b", -3.0, false)), false)
                .build();
        try (AvroDataFileReader reader = AvroDataFileReader.open(new InMemoryByteRangeSource(ocf))) {
            List<AvroRecord> records = reader.records().toList();
            assertThat(records).hasSize(2);
            assertThat(records.get(0).get("id")).isEqualTo(1L);
            assertThat(records.get(0).get("label")).isEqualTo("a");
            assertThat(records.get(0).get("value")).isEqualTo(1.5);
            assertThat(records.get(0).get("flag")).isEqualTo(Boolean.TRUE);
            assertThat(records.get(1).get("id")).isEqualTo(2L);
        }
    }

    @Test
    void readsDeflateCodecBlock() {
        byte[] ocf = new OcfTestWriter()
                .schema(SCHEMA)
                .block(List.of(row(10, "x", 2.0, true)), true)
                .build();
        try (AvroDataFileReader reader = AvroDataFileReader.open(new InMemoryByteRangeSource(ocf))) {
            List<AvroRecord> records = reader.records().toList();
            assertThat(records).hasSize(1);
            assertThat(records.get(0).get("label")).isEqualTo("x");
        }
    }

    @Test
    void readsAcrossMultipleBlocks() {
        byte[] ocf = new OcfTestWriter()
                .schema(SCHEMA)
                .codec("null")
                .block(List.of(row(1, "a", 1.0, true)), false)
                .block(List.of(row(2, "b", 2.0, false), row(3, "c", 3.0, true)), false)
                .build();
        try (AvroDataFileReader reader = AvroDataFileReader.open(new InMemoryByteRangeSource(ocf))) {
            assertThat(reader.records().map(r -> r.get("id")).toList()).containsExactly(1L, 2L, 3L);
        }
    }

    @Test
    void doesNotReadSecondBlockUntilConsumed() {
        OcfTestWriter writer = new OcfTestWriter()
                .schema(SCHEMA)
                .codec("null")
                .block(List.of(row(1, "a", 1.0, true)), false)
                .block(List.of(row(2, "b", 2.0, false)), false);
        byte[] ocf = writer.build();
        long firstBlockEnd = writer.firstBlockEnd();
        CountingByteRangeSource counting = new CountingByteRangeSource(new InMemoryByteRangeSource(ocf));
        try (AvroDataFileReader reader = AvroDataFileReader.open(counting)) {
            AvroRecord first = reader.records().findFirst().orElseThrow();
            assertThat(first.get("id")).isEqualTo(1L);
            assertThat(counting.maxReadEnd()).isLessThan(ocf.length);
            assertThat(counting.maxReadEnd()).isLessThanOrEqualTo(firstBlockEnd);
        }
    }

    @Test
    void eachRecordStreamStartsAtTheFirstBlock() {
        byte[] ocf = new OcfTestWriter()
                .schema(SCHEMA)
                .codec("null")
                .block(List.of(row(1, "a", 1.0, true)), false)
                .block(List.of(row(2, "b", 2.0, false), row(3, "c", 3.0, true)), false)
                .build();
        try (AvroDataFileReader reader = AvroDataFileReader.open(new InMemoryByteRangeSource(ocf))) {
            assertThat(reader.records().count()).isEqualTo(3);
            assertThat(reader.records().count()).isEqualTo(3);
        }
    }

    @Test
    void concurrentlyOpenRecordStreamsIterateIndependently() {
        byte[] ocf = new OcfTestWriter()
                .schema(SCHEMA)
                .codec("null")
                .block(List.of(row(1, "a", 1.0, true)), false)
                .block(List.of(row(2, "b", 2.0, false), row(3, "c", 3.0, true)), false)
                .build();
        try (AvroDataFileReader reader = AvroDataFileReader.open(new InMemoryByteRangeSource(ocf))) {
            Iterator<AvroRecord> first = reader.records().iterator();
            Iterator<AvroRecord> second = reader.records().iterator();
            assertThat(interleavedIds(first, second)).containsExactly(1L, 1L, 2L, 2L, 3L, 3L);
            assertThat(first.hasNext()).isFalse();
            assertThat(second.hasNext()).isFalse();
        }
    }

    @Test
    void eachResolvedRecordStreamStartsAtTheFirstBlock() {
        byte[] ocf = new OcfTestWriter()
                .schema(SCHEMA)
                .codec("null")
                .block(List.of(row(1, "a", 1.0, true)), false)
                .block(List.of(row(2, "b", 2.0, false), row(3, "c", 3.0, true)), false)
                .build();
        AvroSchema readerSchema = AvroSchema.parse(
                "{\"type\":\"record\",\"name\":\"row\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}");
        try (AvroDataFileReader reader = AvroDataFileReader.open(new InMemoryByteRangeSource(ocf))) {
            assertThat(reader.records(readerSchema).count()).isEqualTo(3);
            assertThat(reader.records(readerSchema).count()).isEqualTo(3);

            Iterator<AvroRecord> first = reader.records(readerSchema).iterator();
            Iterator<AvroRecord> second = reader.records(readerSchema).iterator();
            assertThat(interleavedIds(first, second)).containsExactly(1L, 1L, 2L, 2L, 3L, 3L);
        }
    }

    /** Alternates next() between both iterators until exhaustion, collecting the id of every record seen. */
    private static List<Long> interleavedIds(Iterator<AvroRecord> first, Iterator<AvroRecord> second) {
        List<Long> ids = new ArrayList<>();
        while (first.hasNext() || second.hasNext()) {
            if (first.hasNext()) {
                ids.add((Long) first.next().get("id"));
            }
            if (second.hasNext()) {
                ids.add((Long) second.next().get("id"));
            }
        }
        return ids;
    }

    @Test
    void rejectsNonRecordReaderSchema() {
        byte[] ocf = new OcfTestWriter()
                .schema(SCHEMA)
                .codec("null")
                .block(List.of(row(1, "a", 1.0, true)), false)
                .build();
        try (AvroDataFileReader reader = AvroDataFileReader.open(new InMemoryByteRangeSource(ocf))) {
            assertThatThrownBy(() -> reader.records(AvroSchema.LONG))
                    .isInstanceOf(AvroFormatException.class)
                    .hasMessageContaining("Reader schema is not a record");
        }
    }

    @Test
    void resolvesRecordsAgainstAReaderSchema() {
        byte[] ocf = new OcfTestWriter()
                .schema(SCHEMA)
                .codec("null")
                .block(List.of(row(1, "a", 1.5, true), row(2, "b", -3.0, false)), false)
                .build();
        AvroSchema readerSchema = AvroSchema.parse("{\"type\":\"record\",\"name\":\"row\",\"fields\":["
                + "{\"name\":\"id\",\"type\":\"long\"},"
                + "{\"name\":\"flag\",\"type\":\"boolean\"},"
                + "{\"name\":\"source\",\"type\":\"string\",\"default\":\"ocf\"}]}");
        try (AvroDataFileReader reader = AvroDataFileReader.open(new InMemoryByteRangeSource(ocf))) {
            List<AvroRecord> records = reader.records(readerSchema).toList();
            assertThat(records).hasSize(2);
            assertThat(records.get(0).get("id")).isEqualTo(1L);
            assertThat(records.get(0).get("flag")).isEqualTo(Boolean.TRUE);
            assertThat(records.get(0).get("source")).isEqualTo("ocf");
            assertThat(records.get(1).get("id")).isEqualTo(2L);
            assertThat(records.get(1).get("flag")).isEqualTo(Boolean.FALSE);
        }
    }

    @Test
    void rejectsNegativeBlockRecordCount() {
        ByteArrayOutputStream framed = new ByteArrayOutputStream();
        OcfTestWriter.varLong(framed, -1);
        OcfTestWriter.varLong(framed, 0);
        framed.writeBytes(OcfTestWriter.SYNC);
        byte[] ocf = new OcfTestWriter()
                .schema(SCHEMA)
                .codec("null")
                .rawBlock(framed.toByteArray())
                .build();
        try (AvroDataFileReader reader = AvroDataFileReader.open(new InMemoryByteRangeSource(ocf))) {
            Stream<AvroRecord> records = reader.records();
            assertThatThrownBy(records::toList)
                    .isInstanceOf(AvroFormatException.class)
                    .hasMessageContaining("record count out of range: -1");
        }
    }

    @Test
    void detectsSyncMarkerCorruption() {
        byte[] ocf = new OcfTestWriter()
                .schema(SCHEMA)
                .codec("null")
                .block(List.of(row(1, "a", 1.0, true)), false)
                .build();
        ocf[ocf.length - 1] = (byte) 0xFF;
        try (AvroDataFileReader reader = AvroDataFileReader.open(new InMemoryByteRangeSource(ocf))) {
            Stream<AvroRecord> records = reader.records();
            assertThatThrownBy(records::toList)
                    .isInstanceOf(AvroFormatException.class)
                    .hasMessageContaining("sync");
        }
    }

    @Test
    void readsRealManifestAndExposesBinaryBounds(@TempDir Path tempDir) {
        Path manifest = TestCorpus.extractFile("avro-reference/iceberg-manifest/manifest.avro", tempDir);
        try (AvroDataFileReader reader = AvroDataFileReader.open(ByteRangeSource.ofFile(manifest))) {
            assertThat(reader.metadata()).containsKey("avro.schema");
            List<AvroRecord> entries = reader.records().toList();
            assertThat(entries).isNotEmpty();

            AvroRecord entry = entries.get(0);
            AvroRecord dataFile = (AvroRecord) entry.get("data_file");

            MemorySegment geometryBound = boundForField(dataFile.get("lower_bounds"), 2);
            assertThat(geometryBound).isNotNull();
            assertThat(geometryBound.byteSize()).isEqualTo(16);
        }
    }

    @Test
    void readsRealManifestList(@TempDir Path tempDir) {
        Path manifestList = TestCorpus.extractFile("avro-reference/iceberg-manifest/manifest-list.avro", tempDir);
        try (AvroDataFileReader reader = AvroDataFileReader.open(ByteRangeSource.ofFile(manifestList))) {
            List<AvroRecord> manifests = reader.records().toList();
            assertThat(manifests).isNotEmpty();
            assertThat(manifests.get(0).get("manifest_path")).isInstanceOf(String.class);
        }
    }

    /**
     * Iceberg encodes {@code lower_bounds} not as an Avro {@code map} but as the standard Avro representation of a
     * non-string-keyed map: a nullable {@code array} of {@code {key:int, value:bytes}} records (logicalType "map"). The
     * decoder yields the array branch as a {@code List<AvroRecord>} with {@code key} as {@code Integer} and
     * {@code value} as a read-only {@code MemorySegment}. This finds the byte bound for the given Iceberg field id.
     */
    private static MemorySegment boundForField(Object lowerBounds, int fieldId) {
        @SuppressWarnings("unchecked")
        List<AvroRecord> keyValues = (List<AvroRecord>) lowerBounds;
        for (AvroRecord keyValue : keyValues) {
            if (keyValue.get("key").equals(fieldId)) {
                return (MemorySegment) keyValue.get("value");
            }
        }
        return null;
    }
}
