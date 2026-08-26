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
package io.tileverse.parquetry.internal.write;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.BooleanVector;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DefaultParquetRecordBatch;
import io.tileverse.parquetry.columnar.FixedLenBinaryVector;
import io.tileverse.parquetry.columnar.FloatVector;
import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.ListVector;
import io.tileverse.parquetry.columnar.LongVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.StructVector;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.EncodingPolicy;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Round-trip proof for {@link ColumnChunkWriter#appendStriped}: nested and repeated leaves are authored through their
 * enclosing struct / list vector, which routes each leaf through the bulk striped-append path (not the flat
 * {@code appendVector} path), then read back and compared to the input. Wrapping every scalar leaf in a REQUIRED struct
 * and supplying only the struct vector forces {@code RowGroupWriter} to shred the leaf and feed its whole level and
 * value streams to {@code appendStriped}.
 */
class ColumnChunkWriterStripedTest {

    @TempDir
    Path tempDir;

    @Test
    void optionalIntUnderStructRoundTripsValuesAndNulls() throws Exception {
        ParquetSchema schema = structOf(optional("f", PrimitiveKind.INT32));
        boolean[] present = {true, false, true, false, true};
        IntVector values = IntVector.materialized(new int[] {10, 0, 30, 0, 50}, validity(present));
        ParquetRecordBatch batch = structBatch(schema, values, present.length);
        WriteOptions options =
                options().encodingPolicy("f", EncodingPolicy.FORCE_PLAIN).build();

        Path file = tempDir.resolve("optional-int.parquet");
        write(file, schema, batch, options);
        List<ParquetRecord> records = readAll(file);

        ColumnPath leaf = ColumnPath.of("s", "f");
        assertThat(records).hasSize(present.length);
        assertThat(records.get(0).getInt(leaf)).isEqualTo(10);
        assertThat(records.get(1).isNull(leaf)).isTrue();
        assertThat(records.get(2).getInt(leaf)).isEqualTo(30);
        assertThat(records.get(3).isNull(leaf)).isTrue();
        assertThat(records.get(4).getInt(leaf)).isEqualTo(50);
    }

    @Test
    void pageBoundaryMidRunSplitsIntoMultipleDataPages() throws Exception {
        ParquetSchema schema = structOf(optional("f", PrimitiveKind.INT32));
        int rows = 10;
        int[] raw = new int[rows];
        boolean[] present = new boolean[rows];
        for (int i = 0; i < rows; i++) {
            // nulls at indices 2 and 5 place a page boundary mid-run on a null cell (pageValueLimit 3)
            present[i] = i != 2 && i != 5;
            raw[i] = i * 100;
        }
        IntVector values = IntVector.materialized(raw, validity(present));
        ParquetRecordBatch batch = structBatch(schema, values, rows);
        WriteOptions options = options()
                .pageValueLimit(3)
                .encodingPolicy("f", EncodingPolicy.FORCE_PLAIN)
                .build();

        Path file = tempDir.resolve("page-boundary.parquet");
        write(file, schema, batch, options);
        List<ParquetRecord> records = readAll(file);

        ColumnPath leaf = ColumnPath.of("s", "f");
        assertThat(records).hasSize(rows);
        for (int i = 0; i < rows; i++) {
            if (present[i]) {
                assertThat(records.get(i).getInt(leaf)).isEqualTo(i * 100);
            } else {
                assertThat(records.get(i).isNull(leaf)).isTrue();
            }
        }
        assertThat(dataPageCount(file, List.of("s", "f")))
                .as("10 cells at pageValueLimit 3 must span more than one data page")
                .isGreaterThan(1);
    }

    @Test
    void binaryValuesCrossingPageByteLimitSpanMultiplePages() throws Exception {
        ParquetSchema schema = structOf(optional("b", PrimitiveKind.BYTE_ARRAY));
        int rows = 8;
        MemorySegment[] raw = new MemorySegment[rows];
        for (int i = 0; i < rows; i++) {
            raw[i] = wrap("value-payload-" + i);
        }
        BinaryVector values = BinaryVector.materialized(raw, Validity.allValid(rows));
        ParquetRecordBatch batch = structBatch(schema, values, rows);
        WriteOptions options = options()
                .pageByteLimit(32)
                .encodingPolicy("b", EncodingPolicy.FORCE_PLAIN)
                .build();

        Path file = tempDir.resolve("binary-multipage.parquet");
        write(file, schema, batch, options);
        List<ParquetRecord> records = readAll(file);

        ColumnPath leaf = ColumnPath.of("s", "b");
        assertThat(records).hasSize(rows);
        for (int i = 0; i < rows; i++) {
            assertThat(new String(records.get(i).getBinary(leaf), StandardCharsets.UTF_8))
                    .isEqualTo("value-payload-" + i);
        }
        assertThat(dataPageCount(file, List.of("s", "b")))
                .as("variable-length binary values must cross the small page byte limit into multiple pages")
                .isGreaterThan(1);
    }

    @Test
    void dictionaryActiveNumericColumnRoundTrips() throws Exception {
        ParquetSchema schema = structOf(required("n", PrimitiveKind.INT64));
        int rows = 200;
        long[] raw = new long[rows];
        for (int i = 0; i < rows; i++) {
            raw[i] = i % 4;
        }
        LongVector values = LongVector.materialized(raw, Validity.allValid(rows));
        ParquetRecordBatch batch = structBatch(schema, values, rows);
        WriteOptions options = options().build();

        Path file = tempDir.resolve("dictionary-numeric.parquet");
        write(file, schema, batch, options);
        List<ParquetRecord> records = readAll(file);

        ColumnPath leaf = ColumnPath.of("s", "n");
        assertThat(records).hasSize(rows);
        for (int i = 0; i < rows; i++) {
            assertThat(records.get(i).getLong(leaf)).isEqualTo(i % 4L);
        }
        assertThat(dictionaryPagePresent(file, List.of("s", "n")))
                .as("a low-cardinality column must be dictionary-encoded, proving the striped dictionary path ran")
                .isTrue();
    }

    @Test
    void floatUnderStructRoundTripsValuesAndNulls() throws Exception {
        ParquetSchema schema = structOf(optional("f", PrimitiveKind.FLOAT));
        boolean[] present = {true, false, true, true, false};
        FloatVector values = FloatVector.materialized(new float[] {1.5f, 0f, -2.25f, 3.75f, 0f}, validity(present));
        ParquetRecordBatch batch = structBatch(schema, values, present.length);
        WriteOptions options =
                options().encodingPolicy("f", EncodingPolicy.FORCE_PLAIN).build();

        Path file = tempDir.resolve("striped-float.parquet");
        write(file, schema, batch, options);
        List<ParquetRecord> records = readAll(file);

        ColumnPath leaf = ColumnPath.of("s", "f");
        assertThat(records).hasSize(present.length);
        assertThat(records.get(0).getFloat(leaf)).isEqualTo(1.5f);
        assertThat(records.get(1).isNull(leaf)).isTrue();
        assertThat(records.get(2).getFloat(leaf)).isEqualTo(-2.25f);
        assertThat(records.get(3).getFloat(leaf)).isEqualTo(3.75f);
        assertThat(records.get(4).isNull(leaf)).isTrue();
    }

    @Test
    void booleanUnderStructRoundTripsValuesAndNulls() throws Exception {
        ParquetSchema schema = structOf(optional("b", PrimitiveKind.BOOLEAN));
        boolean[] present = {true, true, false, true};
        BooleanVector values = BooleanVector.materialized(new boolean[] {true, false, false, true}, validity(present));
        ParquetRecordBatch batch = structBatch(schema, values, present.length);
        WriteOptions options = options().build();

        Path file = tempDir.resolve("striped-boolean.parquet");
        write(file, schema, batch, options);
        List<ParquetRecord> records = readAll(file);

        ColumnPath leaf = ColumnPath.of("s", "b");
        assertThat(records).hasSize(present.length);
        assertThat(records.get(0).getBoolean(leaf)).isTrue();
        assertThat(records.get(1).getBoolean(leaf)).isFalse();
        assertThat(records.get(2).isNull(leaf)).isTrue();
        assertThat(records.get(3).getBoolean(leaf)).isTrue();
    }

    @Test
    void fixedLenBinaryUnderStructRoundTripsValuesAndNulls() throws Exception {
        ParquetSchema schema = structOf(fixedLen("x", 4));
        boolean[] present = {true, false, true};
        MemorySegment[] raw = {wrap("aaaa"), null, wrap("cccc")};
        FixedLenBinaryVector values = FixedLenBinaryVector.materialized(raw, 4, validity(present));
        ParquetRecordBatch batch = structBatch(schema, values, present.length);
        WriteOptions options =
                options().encodingPolicy("x", EncodingPolicy.FORCE_PLAIN).build();

        Path file = tempDir.resolve("striped-fixedlen.parquet");
        write(file, schema, batch, options);
        List<ParquetRecord> records = readAll(file);

        ColumnPath leaf = ColumnPath.of("s", "x");
        assertThat(records).hasSize(present.length);
        assertThat(new String(records.get(0).getBinary(leaf), StandardCharsets.UTF_8))
                .isEqualTo("aaaa");
        assertThat(records.get(1).isNull(leaf)).isTrue();
        assertThat(new String(records.get(2).getBinary(leaf), StandardCharsets.UTF_8))
                .isEqualTo("cccc");
    }

    @Test
    void repeatedLeafRoundTripsListStructure() throws Exception {
        ParquetSchema schema = listOfOptionalInt();
        // row0 [1,2]; row1 []; row2 null; row3 [null]
        IntVector elements = IntVector.materialized(new int[] {1, 2, 0}, validity(true, true, false));
        ListVector list = new ListVector(new int[] {0, 2, 2, 2, 3}, elements, validity(true, true, false, true), 4);
        ParquetRecordBatch batch = DefaultParquetRecordBatch.ofHeap(schema, Map.of(ColumnPath.of("mylist"), list), 4);
        WriteOptions options = options().build();

        Path file = tempDir.resolve("list.parquet");
        write(file, schema, batch, options);
        List<ParquetRecord> records = readAll(file);

        ColumnPath container = ColumnPath.of("mylist");
        ColumnPath element = ColumnPath.of("mylist", "list", "element");
        assertThat(records).hasSize(4);

        assertThat(records.get(0).readList(container))
                .asInstanceOf(InstanceOfAssertFactories.LIST)
                .containsExactly(1, 2);
        assertThat(records.get(0).multiValueSize(element)).isEqualTo(2);

        assertThat(records.get(1).readList(container)).isEqualTo(List.of());
        assertThat(records.get(1).multiValueSize(element)).isZero();

        assertThat(records.get(2).readList(container)).isNull();

        List<?> row3 = records.get(3).readList(container);
        assertThat(row3).hasSize(1);
        assertThat(row3.get(0)).isNull();
        assertThat(records.get(3).multiValueSize(element)).isEqualTo(1);
    }

    // --- write / read harness ---

    private void write(Path file, ParquetSchema schema, ParquetRecordBatch batch, WriteOptions options)
            throws Exception {
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, options);
                ParquetRecordBatch owned = batch) {
            writer.writeBatch(owned);
        }
    }

    private List<ParquetRecord> readAll(Path file) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            try (Stream<ParquetRecord> rows =
                    reader.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                return rows.map(ParquetRecord::detach).toList();
            }
        }
    }

    private int dataPageCount(Path file, List<String> pathInSchema) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ColumnChunk chunk = chunkFor(ParquetFormat.readFooter(source), pathInSchema);
            long offset = chunk.offsetIndexOffset()
                    .orElseThrow(() -> new IllegalStateException("no offset index for " + pathInSchema));
            int length = chunk.offsetIndexLength()
                    .orElseThrow(() -> new IllegalStateException("no offset index length for " + pathInSchema));
            OffsetIndex offsetIndex = ParquetFormat.readOffsetIndex(source, offset, length);
            return offsetIndex.pageLocations().size();
        }
    }

    private boolean dictionaryPagePresent(Path file, List<String> pathInSchema) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ColumnChunk chunk = chunkFor(ParquetFormat.readFooter(source), pathInSchema);
            return chunk.metaData().orElseThrow().dictionaryPageOffset().isPresent();
        }
    }

    private static ColumnChunk chunkFor(FileMetaData footer, List<String> pathInSchema) {
        RowGroup rowGroup = footer.rowGroups().get(0);
        for (ColumnChunk chunk : rowGroup.columns()) {
            if (chunk.metaData().orElseThrow().pathInSchema().equals(pathInSchema)) {
                return chunk;
            }
        }
        throw new IllegalStateException("column chunk not found: " + pathInSchema);
    }

    // --- schema / batch builders ---

    private static WriteOptions.Builder options() {
        return WriteOptions.builder();
    }

    /** A schema whose single leaf lives under a REQUIRED struct {@code s}, forcing the striped-append path. */
    private static ParquetSchema structOf(SchemaNode.Primitive field) {
        SchemaNode.Group struct = new SchemaNode.Group("s", Repetition.REQUIRED, List.of(field), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(struct), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    /** Supplies the single struct field only through its enclosing struct vector, never as a flat leaf column. */
    private static ParquetRecordBatch structBatch(ParquetSchema schema, ColumnVector fieldValues, int rows) {
        String fieldName = schema.leafColumns().get(0).part(1);
        StructVector struct =
                new StructVector(Map.of(ColumnPath.of(fieldName), fieldValues), Validity.allValid(rows), rows);
        return DefaultParquetRecordBatch.ofHeap(schema, Map.of(ColumnPath.of("s"), struct), rows);
    }

    private static SchemaNode.Primitive optional(String name, PrimitiveKind kind) {
        return new SchemaNode.Primitive(name, Repetition.OPTIONAL, kind, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive required(String name, PrimitiveKind kind) {
        return new SchemaNode.Primitive(name, Repetition.REQUIRED, kind, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive fixedLen(String name, int byteWidth) {
        return new SchemaNode.Primitive(
                name,
                Repetition.OPTIONAL,
                PrimitiveKind.FIXED_LEN_BYTE_ARRAY,
                OptionalInt.of(byteWidth),
                Optional.empty(),
                -1);
    }

    private static ParquetSchema listOfOptionalInt() {
        SchemaNode.Primitive element = new SchemaNode.Primitive(
                "element", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group repeated =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        SchemaNode.Group list = new SchemaNode.Group(
                "mylist", Repetition.OPTIONAL, List.of(repeated), Optional.of(new LogicalType.ListType()), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(list), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static Validity validity(boolean... valid) {
        BitSet bits = new BitSet(valid.length);
        for (int row = 0; row < valid.length; row++) {
            if (valid[row]) {
                bits.set(row);
            }
        }
        return Validity.of(bits, valid.length);
    }

    private static MemorySegment wrap(String text) {
        return MemorySegment.ofArray(text.getBytes(StandardCharsets.UTF_8)).asReadOnly();
    }
}
