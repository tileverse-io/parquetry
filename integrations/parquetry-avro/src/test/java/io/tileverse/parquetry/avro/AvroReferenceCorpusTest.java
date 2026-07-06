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
package io.tileverse.parquetry.avro;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Reads every Avro container file in the vendored {@code avro-reference} corpus with both avro-java (the oracle) and
 * this module's {@link AvroDataFileReader}, asserting that record counts and per-field values agree.
 */
class AvroReferenceCorpusTest {

    private static final byte[] PRE_13_MAGIC = {'O', 'b', 'j', 0};

    @TempDir
    static Path tempDir;

    /** A corpus file plus its corpus-relative name, the latter doubling as the test display name. */
    record Fixture(String name, Path path) {
        @Override
        public String toString() {
            return name;
        }
    }

    static List<Fixture> avroFixtures() throws IOException {
        Path root = TestCorpus.extractDirectory("avro-reference", tempDir);
        try (Stream<Path> walk = Files.walk(root)) {
            return walk.filter(path ->
                            path.toString().endsWith(".avro") || path.toString().endsWith(".avro12"))
                    .sorted()
                    .map(path -> new Fixture(root.relativize(path).toString(), path))
                    .toList();
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("avroFixtures")
    void matchesAvroJavaOracle(Fixture fixture) throws IOException {
        Path file = fixture.path();
        assumeCurrentContainerFormat(file);

        List<GenericRecord> expected = readWithAvroJava(file);
        List<AvroRecord> actual = readWithOurReader(file);

        assertThat(actual).hasSize(expected.size());
        for (int i = 0; i < expected.size(); i++) {
            assertMatches(expected.get(i), actual.get(i), i);
        }
    }

    /**
     * Files written by Avro 1.2 use the pre-1.3 data file format (magic {@code Obj\x00}), a different container layout
     * that this OCF reader does not implement; avro-java itself needs {@code DataFileReader12} for them.
     */
    private static void assumeCurrentContainerFormat(Path file) throws IOException {
        byte[] head = new byte[PRE_13_MAGIC.length];
        try (InputStream in = Files.newInputStream(file)) {
            int read = in.readNBytes(head, 0, head.length);
            assertThat(read).isEqualTo(head.length);
        }
        boolean pre13 = Arrays.equals(head, PRE_13_MAGIC);
        Assumptions.assumeFalse(pre13, "pre-1.3 Avro data file format not supported: " + file.getFileName());
    }

    private static List<GenericRecord> readWithAvroJava(Path file) throws IOException {
        List<GenericRecord> records = new ArrayList<>();
        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try (FileReader<GenericRecord> reader = DataFileReader.openReader(file.toFile(), datumReader)) {
            for (GenericRecord row : reader) {
                records.add(row);
            }
        }
        return records;
    }

    private static List<AvroRecord> readWithOurReader(Path file) throws IOException {
        byte[] bytes = Files.readAllBytes(file);
        try (AvroDataFileReader reader = AvroDataFileReader.open(new InMemoryByteRangeSource(bytes))) {
            return reader.records().toList();
        }
    }

    private static void assertMatches(GenericRecord expected, AvroRecord actual, int index) {
        for (Schema.Field field : expected.getSchema().getFields()) {
            String name = field.name();
            Object expectedValue = OracleValueNormalizer.normalize(expected.get(name));
            Object actualValue = OracleValueNormalizer.normalize(actual.get(name));
            assertThat(actualValue).as("record %d field %s", index, name).isEqualTo(expectedValue);
        }
    }
}
