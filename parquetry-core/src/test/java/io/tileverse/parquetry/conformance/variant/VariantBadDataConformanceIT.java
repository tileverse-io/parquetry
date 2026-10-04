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
package io.tileverse.parquetry.conformance.variant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.format.ParquetFormatException;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testkit.TestCorpus;
import io.tileverse.parquetry.variant.Variant;
import io.tileverse.parquetry.variant.VariantMetadata;

/**
 * Variant decoding against the {@code parquet-testing/bad_data/variants} corpus. Each file holds one row of two plain
 * required binary columns, {@code metadata} and {@code value}, encoding one Variant. A malformed file must fail with a
 * {@link ParquetFormatException}, either when the metadata and value views are built or while a consumer walks the
 * whole value. {@code duplicate_field_offsets} is legal: its fields {@code a} and {@code b} share one TRUE value.
 *
 * <p>A lockstep check asserts the directory holds exactly the classified files: a corpus refresh adding a file fails
 * here until the file is classified.
 */
class VariantBadDataConformanceIT {

    private static final String CORPUS = "parquet-testing/bad_data/variants";
    private static final ColumnPath METADATA_COLUMN = ColumnPath.of("metadata");
    private static final ColumnPath VALUE_COLUMN = ColumnPath.of("value");

    private static final String DUPLICATE_FIELD_OFFSETS = "duplicate_field_offsets.parquet";

    private static final Set<String> MALFORMED = Set.of(
            "field_id_out_of_range.parquet",
            "int_overflow_in_bounds_check.parquet",
            "malformed_child_inside_well_formed_parent.parquet",
            "negative_dictionary_size.parquet",
            "out_of_range_child_offset.parquet",
            "out_of_range_dictionary_size.parquet",
            "out_of_range_element_count.parquet",
            "over_deep_nested_children.parquet",
            "oversized_primitive_size.parquet",
            "short_string_length_exceeds_buffer.parquet",
            "truncated_primitive_size.parquet",
            "unknown_primitive_type.parquet",
            "variant_version_2_header.parquet");

    @TempDir
    static Path corpusDir;

    @BeforeAll
    static void extractCorpus() {
        TestCorpus.extractDirectory(CORPUS, corpusDir);
    }

    static Stream<String> malformedFiles() {
        return MALFORMED.stream().sorted();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("malformedFiles")
    void rejectsMalformedVariantWithAFormatError(String fileName) {
        VariantCell cell = readSingleRow(fileName);

        assertThatThrownBy(() -> walk(cell.decode()))
                .as("%s must fail decoding or walking with a format error", fileName)
                .isInstanceOf(ParquetFormatException.class);
    }

    @Test
    void readsTwoFieldsSharingOneValue() {
        Variant object = readSingleRow(DUPLICATE_FIELD_OFFSETS).decode();

        assertThat(object.type()).as("type").isEqualTo(Variant.Type.OBJECT);
        assertThat(object.fieldNames()).as("field names").containsExactly("a", "b");
        assertThat(object.getField("a").getBoolean()).as("field a").isTrue();
        assertThat(object.getField("b").getBoolean()).as("field b").isTrue();
        walk(object);
    }

    @Test
    void directoryAndClassificationStayInLockstep() throws IOException {
        Set<String> classified = new HashSet<>(MALFORMED);
        classified.add(DUPLICATE_FIELD_OFFSETS);

        assertThat(parquetFilesOnDisk())
                .as("each bad_data/variants file is classified as malformed or legal")
                .isEqualTo(classified);
    }

    private static Set<String> parquetFilesOnDisk() throws IOException {
        try (Stream<Path> files = Files.list(corpusDir)) {
            return files.map(path -> path.getFileName().toString())
                    .filter(name -> name.endsWith(".parquet"))
                    .collect(Collectors.toSet());
        }
    }

    /** The raw metadata and value bytes of one corpus row. */
    private record VariantCell(byte[] metadata, byte[] value) {

        Variant decode() {
            VariantMetadata dictionary = new VariantMetadata(readOnly(metadata));
            return Variant.of(readOnly(value), dictionary);
        }

        private static MemorySegment readOnly(byte[] bytes) {
            return MemorySegment.ofArray(bytes).asReadOnly();
        }
    }

    private static VariantCell readSingleRow(String fileName) {
        Path file = corpusDir.resolve(fileName);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            List<VariantCell> cells;
            try (Stream<ParquetRecord> records =
                    reader.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                cells = records.map(VariantBadDataConformanceIT::cellOf).toList();
            }
            assertThat(cells).as("%s holds one row", fileName).hasSize(1);
            return cells.getFirst();
        }
    }

    private static VariantCell cellOf(ParquetRecord row) {
        byte[] metadata = row.getBinary(METADATA_COLUMN);
        byte[] value = row.getBinary(VALUE_COLUMN);
        return new VariantCell(metadata, value);
    }

    /** Reads the whole value the way a recursive consumer such as a JSON renderer does. */
    private static void walk(Variant node) {
        switch (node.type()) {
            case OBJECT -> walkObject(node);
            case ARRAY -> walkArray(node);
            default -> readScalar(node);
        }
    }

    private static void walkObject(Variant object) {
        List<String> names = object.fieldNames();
        for (int i = 0; i < names.size(); i++) {
            walk(object.getFieldAtIndex(i));
        }
    }

    private static void walkArray(Variant array) {
        int count = array.numElements();
        for (int i = 0; i < count; i++) {
            walk(array.getElement(i));
        }
    }

    private static Object readScalar(Variant scalar) {
        return switch (scalar.type()) {
            case NULL -> null;
            case BOOLEAN -> scalar.getBoolean();
            case INT8 -> scalar.getByte();
            case INT16 -> scalar.getShort();
            case INT32 -> scalar.getInt();
            case INT64 -> scalar.getLong();
            case FLOAT -> scalar.getFloat();
            case DOUBLE -> scalar.getDouble();
            case DECIMAL4, DECIMAL8, DECIMAL16 -> scalar.getBigDecimal();
            case DATE -> scalar.getDateDays();
            case TIME_NTZ -> scalar.getTimeMicros();
            case TIMESTAMP_TZ, TIMESTAMP_NTZ -> scalar.getTimestampMicros();
            case TIMESTAMP_NANOS_TZ, TIMESTAMP_NANOS_NTZ -> scalar.getTimestampNanos();
            case BINARY -> scalar.getBinary();
            case STRING -> scalar.getString();
            case UUID -> scalar.getUuid();
            case OBJECT, ARRAY -> throw new IllegalArgumentException("not a scalar: " + scalar.type());
        };
    }
}
