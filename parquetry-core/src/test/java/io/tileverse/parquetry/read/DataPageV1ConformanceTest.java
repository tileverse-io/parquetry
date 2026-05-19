/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.read;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.LocalInputFile;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.dataset.ParquetDataset;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;

import io.tileverse.io.ByteBufferPool;

/**
 * V1 fixture conformance: reads each committed {@code parquet-avro 1.x} fixture under
 * {@code src/test/resources/v1-fixtures/} through the parquetry pipeline (ParquetDataset.open -> RowGroupPipeline ->
 * DataPageV1Reader) and asserts every record matches the parquet-avro oracle reading the same bytes. Mirrors the
 * structure of {@link EndToEndV2ReadTest} but consumes pre-built fixtures so the test is deterministic across
 * parquet-avro version bumps.
 *
 * <p>Companion writer: {@link io.tileverse.parquetry.read.V1FixtureGenerator} - a {@code main}-only utility that
 * (re)emits the fixtures. See its javadoc for the codec / encoding matrix.
 */
class DataPageV1ConformanceTest {

    private static final String FIXTURE_DIR = "v1-fixtures/";

    private static final ColumnPath YEAR = ColumnPath.of("year");
    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath OPTIONAL_LABEL = ColumnPath.of("optional_label");

    @ParameterizedTest(name = "{0}")
    @ValueSource(
            strings = {
                FixtureContent.PLAIN_INT_STRING,
                FixtureContent.DICT_STRING,
                FixtureContent.GZIP_INT_STRING,
                FixtureContent.SNAPPY_INT_STRING
            })
    void readsIntStringFixtureMatchesParquetAvroOracle(String fixtureName, @TempDir Path tmp) throws Exception {
        Path fixture = stageFixture(fixtureName, tmp);
        List<GenericRecord> expected = readAllViaParquetAvro(fixture);

        ByteBufferPool pool = new ByteBufferPool();
        List<ParquetRecord> actual = readAllViaParquetry(fixture, pool);

        assertThat(actual)
                .as("%s row count must match parquet-avro oracle", fixtureName)
                .hasSameSizeAs(expected);
        assertIntStringRecordsMatchOracle(fixtureName, actual, expected);
        assertThat(outstandingBorrows(pool))
                .as("pooled buffers must drain after %s", fixtureName)
                .isZero();
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {FixtureContent.NULLABLE_STRING})
    void readsNullableFixtureMatchesParquetAvroOracle(String fixtureName, @TempDir Path tmp) throws Exception {
        Path fixture = stageFixture(fixtureName, tmp);
        List<GenericRecord> expected = readAllViaParquetAvro(fixture);

        ByteBufferPool pool = new ByteBufferPool();
        List<ParquetRecord> actual = readAllViaParquetry(fixture, pool);

        assertThat(actual)
                .as("%s row count must match parquet-avro oracle", fixtureName)
                .hasSameSizeAs(expected);
        assertNullableRecordsMatchOracle(actual, expected);
        assertThat(outstandingBorrows(pool))
                .as("pooled buffers must drain after %s", fixtureName)
                .isZero();
    }

    // --- record-level assertions ---

    private static void assertIntStringRecordsMatchOracle(
            String fixtureName, List<ParquetRecord> actual, List<GenericRecord> expected) {
        String stringColumnName = intStringColumnName(fixtureName);
        ColumnPath stringColumn = ColumnPath.of(stringColumnName);
        for (int i = 0; i < expected.size(); i++) {
            ParquetRecord a = actual.get(i);
            GenericRecord e = expected.get(i);
            assertThat(a.getInt(YEAR)).as("%s year @ row %d", fixtureName, i).isEqualTo((int) e.get("year"));
            assertThat(a.getString(stringColumn))
                    .as("%s %s @ row %d", fixtureName, stringColumnName, i)
                    .isEqualTo(stringValue(e, stringColumnName));
        }
    }

    private static void assertNullableRecordsMatchOracle(List<ParquetRecord> actual, List<GenericRecord> expected) {
        for (int i = 0; i < expected.size(); i++) {
            ParquetRecord a = actual.get(i);
            GenericRecord e = expected.get(i);
            assertThat(a.getInt(ID)).as("nullable id @ row %d", i).isEqualTo((int) e.get("id"));
            Object oracleLabel = e.get("optional_label");
            if (oracleLabel == null) {
                assertThat(a.isNull(OPTIONAL_LABEL))
                        .as("nullable optional_label @ row %d should be null", i)
                        .isTrue();
            } else {
                assertThat(a.isNull(OPTIONAL_LABEL))
                        .as("nullable optional_label @ row %d should not be null", i)
                        .isFalse();
                assertThat(a.getString(OPTIONAL_LABEL))
                        .as("nullable optional_label @ row %d", i)
                        .isEqualTo(oracleLabel.toString());
            }
        }
    }

    /** Picks the string column name out of the int+string fixture filename so the oracle assertion stays generic. */
    private static String intStringColumnName(String fixtureName) {
        // Only the plain-int-string fixture uses the {@code country} column; every other int+string fixture uses
        // {@code region} so that dictionary encoding has a small distinct set to work with.
        return FixtureContent.PLAIN_INT_STRING.equals(fixtureName) ? "country" : "region";
    }

    private static String stringValue(GenericRecord parquetRecord, String column) {
        Object raw = parquetRecord.get(column);
        // parquet-avro returns org.apache.avro.util.Utf8 for string columns; ParquetRecord exposes java.lang.String.
        return (raw == null) ? null : raw.toString();
    }

    // --- pipeline drivers ---

    private static List<ParquetRecord> readAllViaParquetry(Path fixture, ByteBufferPool pool) throws IOException {
        try (Storage storage = StorageFactory.open(fixture.getParent().toUri());
                RangeReader reader =
                        storage.openRangeReader(fixture.getFileName().toString())) {
            ParquetDataset dataset = ParquetDataset.open(reader);
            ReadOptions options = ReadOptions.builder().byteBufferPool(pool).build();
            try (Stream<ParquetRecord> records = dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, options)) {
                return records.toList();
            }
        }
    }

    private static List<GenericRecord> readAllViaParquetAvro(Path fixture) throws IOException {
        List<GenericRecord> rows = new ArrayList<>();
        try (ParquetReader<GenericData.Record> reader = AvroParquetReader.<GenericData.Record>builder(
                        new LocalInputFile(fixture))
                .build()) {
            GenericData.Record parquetRecord;
            while ((parquetRecord = reader.read()) != null) {
                rows.add(parquetRecord);
            }
        }
        return rows;
    }

    // --- fixture staging ---

    /**
     * Copies a classpath fixture into {@code tmp} so the parquetry reader (file-backed RangeReader) can open it by
     * directory + name. Returns the staged path; the temp directory is cleaned up by JUnit.
     */
    private static Path stageFixture(String fixtureName, Path tmp) throws IOException {
        String resourcePath = FIXTURE_DIR + fixtureName;
        URL url = DataPageV1ConformanceTest.class.getClassLoader().getResource(resourcePath);
        if (url == null) {
            throw new IllegalStateException("Missing fixture on classpath: " + resourcePath
                    + " (regenerate via io.tileverse.parquetry.read.V1FixtureGenerator)");
        }
        Path out = tmp.resolve(fixtureName);
        try (InputStream in = url.openStream()) {
            Files.copy(in, out, StandardCopyOption.REPLACE_EXISTING);
        }
        return out;
    }

    // --- pool accounting ---

    private static long outstandingBorrows(ByteBufferPool pool) {
        ByteBufferPool.PoolStatistics stats = pool.getStatistics();
        return (stats.created() + stats.reused()) - (stats.returned() + stats.discarded());
    }
}
