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
package io.tileverse.parquetry.iceberg;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.explain.DatasetExplainPlan;
import io.tileverse.parquetry.dataset.explain.FileExplain;
import io.tileverse.parquetry.dataset.explain.Outcome;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.LogicalType.TimeUnit;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Reads the {@code scalars} fixture (one column of every scalar Iceberg type, identity-partitioned on a timestamp, two
 * rows removed by equality deletes) and proves the presented schema, the decoded values, file-level pruning on decimal,
 * timestamp, and time bounds, identity-partition pruning on a timestamp, and initial defaults of every newly supported
 * type. Every expected value is the fixture generator's formula for the row id.
 */
class IcebergScalarTypesReadIT {

    private static final String TABLE = "scalars";
    private static final long LIVE_ROWS = 10L;
    private static final LocalDateTime P0 = LocalDateTime.of(2024, 1, 1, 0, 0, 0);
    private static final LocalDateTime P1 = LocalDateTime.of(2024, 6, 15, 12, 30, 45, 123_456_000);
    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath TS = ColumnPath.of("ts");
    private static final ColumnPath TSTZ = ColumnPath.of("tstz");
    private static final ColumnPath TS_NS = ColumnPath.of("ts_ns");
    private static final ColumnPath TSTZ_NS = ColumnPath.of("tstz_ns");
    private static final ColumnPath T = ColumnPath.of("t");
    private static final ColumnPath DEC = ColumnPath.of("dec");
    private static final ColumnPath BIGDEC = ColumnPath.of("bigdec");
    private static final ColumnPath U = ColumnPath.of("u");
    private static final ColumnPath FX = ColumnPath.of("fx");
    private static final ColumnPath BIN = ColumnPath.of("bin");
    private static final ColumnPath D = ColumnPath.of("d");
    private static final ColumnPath LABEL = ColumnPath.of("label");
    private static final UUID DEFAULT_UUID = UUID.fromString("f79c3e09-677c-4bbd-a479-3f349cb785e7");

    @TempDir
    Path tempDir;

    @Test
    void presentsEveryScalarTypeFromTheTableMetadata() {
        withDataset(extractTable(), dataset -> {
            ParquetSchema schema = dataset.schema();
            assertThat(leafNames(schema))
                    .containsExactly(
                            "id", "ts", "tstz", "ts_ns", "tstz_ns", "t", "dec", "bigdec", "u", "fx", "bin", "d",
                            "label");
            assertLeaf(
                    schema,
                    "dec",
                    PrimitiveKind.INT32,
                    OptionalInt.empty(),
                    Optional.of(new LogicalType.Decimal(2, 9)));
            assertLeaf(
                    schema,
                    "bigdec",
                    PrimitiveKind.FIXED_LEN_BYTE_ARRAY,
                    OptionalInt.of(9),
                    Optional.of(new LogicalType.Decimal(3, 20)));
            assertLeaf(
                    schema,
                    "t",
                    PrimitiveKind.INT64,
                    OptionalInt.empty(),
                    Optional.of(new LogicalType.Time(false, TimeUnit.MICROS)));
            assertLeaf(
                    schema,
                    "ts",
                    PrimitiveKind.INT64,
                    OptionalInt.empty(),
                    Optional.of(new LogicalType.Timestamp(false, TimeUnit.MICROS)));
            assertLeaf(
                    schema,
                    "tstz",
                    PrimitiveKind.INT64,
                    OptionalInt.empty(),
                    Optional.of(new LogicalType.Timestamp(true, TimeUnit.MICROS)));
            assertLeaf(
                    schema,
                    "ts_ns",
                    PrimitiveKind.INT64,
                    OptionalInt.empty(),
                    Optional.of(new LogicalType.Timestamp(false, TimeUnit.NANOS)));
            assertLeaf(
                    schema,
                    "tstz_ns",
                    PrimitiveKind.INT64,
                    OptionalInt.empty(),
                    Optional.of(new LogicalType.Timestamp(true, TimeUnit.NANOS)));
            assertLeaf(
                    schema,
                    "u",
                    PrimitiveKind.FIXED_LEN_BYTE_ARRAY,
                    OptionalInt.of(16),
                    Optional.of(new LogicalType.UuidType()));
            assertLeaf(schema, "fx", PrimitiveKind.FIXED_LEN_BYTE_ARRAY, OptionalInt.of(4), Optional.empty());
            assertLeaf(schema, "bin", PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty());
        });
    }

    @Test
    void readsEveryScalarValueOfARow() {
        withDataset(extractTable(), dataset -> {
            ParquetRecord row = onlyRow(dataset, new Predicate.Eq(ID, new Value.LongVal(1L)));
            assertThat(row.getUuid(U)).isEqualTo(uuidOf(1));
            assertThat(decimalOf(row, DEC, 2)).isEqualByComparingTo("-6.25");
            assertThat(decimalOf(row, BIGDEC, 3)).isEqualByComparingTo("123456789012345.678");
            assertThat(row.getLong(T)).isEqualTo(microsOfDay(LocalTime.of(1, 30, 15, 250_000_000)));
            assertThat(row.getLong(TS)).isEqualTo(epochMicros(P0));
            assertThat(row.getLong(TSTZ)).isEqualTo(epochMicros(P0.plusMinutes(1)));
            assertThat(row.getLong(TS_NS)).isEqualTo(P0.toEpochSecond(ZoneOffset.UTC) * 1_000_000_000L + 1L);
            assertThat(row.getLong(TSTZ_NS)).isEqualTo(P0.toEpochSecond(ZoneOffset.UTC) * 1_000_000_000L + 1L);
            assertThat(row.getBinary(FX)).isEqualTo(new byte[] {1, 2, 3, 4});
            assertThat(row.getBinary(BIN)).isEqualTo("bin-1".getBytes(UTF_8));
            assertThat(row.getInt(D)).isEqualTo((int) LocalDate.of(2024, 1, 2).toEpochDay());
            assertThat(row.getString(LABEL)).isEqualTo("row-1");
        });
    }

    @Test
    void prunesFilesByDecimalBounds() {
        withDataset(extractTable(), dataset -> {
            Predicate above = new Predicate.Gt(DEC, new Value.DecimalVal(new BigDecimal("2.00")));
            assertThat(skippedFileCount(dataset.explain(above, Projection.ALL, ReadOptions.DEFAULTS)))
                    .isEqualTo(1);
            assertThat(idsOf(dataset, above)).containsExactly(8L, 9L, 10L, 11L, 12L);

            Predicate none = new Predicate.Lt(DEC, new Value.DecimalVal(new BigDecimal("-10.00")));
            assertThat(skippedFileCount(dataset.explain(none, Projection.ALL, ReadOptions.DEFAULTS)))
                    .isEqualTo(3);
            assertThat(dataset.count(none, ReadOptions.DEFAULTS)).isZero();
        });
    }

    @Test
    void prunesFilesByTimestampAndTimeBounds() {
        withDataset(extractTable(), dataset -> {
            Predicate early = new Predicate.Lt(TSTZ, new Value.TimestampVal(LocalDateTime.of(2024, 6, 1, 0, 0), true));
            assertThat(skippedFileCount(dataset.explain(early, Projection.ALL, ReadOptions.DEFAULTS)))
                    .isEqualTo(2);
            assertThat(idsOf(dataset, early)).containsExactly(1L, 3L, 4L);

            Predicate morning = new Predicate.Lt(T, new Value.TimeVal(LocalTime.of(5, 0)));
            assertThat(skippedFileCount(dataset.explain(morning, Projection.ALL, ReadOptions.DEFAULTS)))
                    .isEqualTo(2);
            assertThat(idsOf(dataset, morning)).containsExactly(1L, 3L, 4L);
        });
    }

    @Test
    void identityPartitionOnATimestampPrunesAndPresentsItsValue() {
        withDataset(extractTable(), dataset -> {
            Predicate inP1 = new Predicate.Eq(TS, new Value.TimestampVal(P1, false));
            assertThat(skippedFileCount(dataset.explain(inP1, Projection.ALL, ReadOptions.DEFAULTS)))
                    .isEqualTo(2);
            assertThat(idsOf(dataset, inP1)).containsExactly(5L, 6L, 8L);
            try (Stream<ParquetRecord> rows = dataset.read(inP1, Projection.ALL, ReadOptions.DEFAULTS)) {
                assertThat(rows.map(row -> row.getLong(TS))).containsOnly(epochMicros(P1));
            }
        });
    }

    @Test
    void uuidEqualityFindsExactlyItsRow() {
        withDataset(
                extractTable(),
                dataset -> assertThat(idsOf(dataset, new Predicate.Eq(U, new Value.UuidVal(uuidOf(3)))))
                        .containsExactly(3L));
    }

    @Test
    void readsBackAnInitialDefaultOfEveryNewType() {
        Path tableDir = extractTable();
        List<IcebergField> evolved = new ArrayList<>(currentFields(tableDir));
        LocalDateTime stamp = LocalDateTime.of(2020, 2, 3, 4, 5, 6, 7_000);
        LocalTime timeOfDay = LocalTime.of(12, 34, 56, 789_000);
        evolved.add(new IcebergField(
                20, "def_dec", "decimal(6, 2)", false, Optional.of(new Value.DecimalVal(new BigDecimal("14.20")))));
        evolved.add(
                new IcebergField(21, "def_ts", "timestamp", false, Optional.of(new Value.TimestampVal(stamp, false))));
        evolved.add(new IcebergField(
                22, "def_tstz", "timestamptz", false, Optional.of(new Value.TimestampVal(stamp, true))));
        evolved.add(new IcebergField(23, "def_t", "time", false, Optional.of(new Value.TimeVal(timeOfDay))));
        evolved.add(new IcebergField(24, "def_u", "uuid", false, Optional.of(new Value.UuidVal(DEFAULT_UUID))));
        evolved.add(new IcebergField(
                25, "def_bin", "binary", false, Optional.of(new Value.BinaryVal(MemorySegment.ofArray(new byte[] {
                    0x0a, (byte) 0xff
                })))));
        evolved.add(new IcebergField(
                26, "def_fx", "fixed[2]", false, Optional.of(new Value.BinaryVal(MemorySegment.ofArray(new byte[] {1, 2
                })))));
        // A zero spells out one digit at scale 2; its constant leaf must still declare a precision of at least 2.
        evolved.add(new IcebergField(
                27, "def_zero", "decimal(9, 2)", false, Optional.of(new Value.DecimalVal(new BigDecimal("0.00")))));
        IcebergSchemaEvolution.evolveCurrentSchema(tableDir, evolved);

        withDataset(tableDir, dataset -> {
            assertThat(leafNames(dataset.schema()))
                    .endsWith("def_dec", "def_ts", "def_tstz", "def_t", "def_u", "def_bin", "def_fx", "def_zero");
            ParquetRecord row = onlyRow(dataset, new Predicate.Eq(ID, new Value.LongVal(1L)));
            assertThat(decimalOf(row, ColumnPath.of("def_dec"), 2)).isEqualByComparingTo("14.20");
            assertThat(row.getLong(ColumnPath.of("def_ts"))).isEqualTo(epochMicros(stamp));
            assertThat(row.getLong(ColumnPath.of("def_tstz"))).isEqualTo(epochMicros(stamp));
            assertThat(row.getLong(ColumnPath.of("def_t"))).isEqualTo(microsOfDay(timeOfDay));
            assertThat(row.getUuid(ColumnPath.of("def_u"))).isEqualTo(DEFAULT_UUID);
            assertThat(row.getBinary(ColumnPath.of("def_bin"))).isEqualTo(new byte[] {0x0a, (byte) 0xff});
            assertThat(row.getBinary(ColumnPath.of("def_fx"))).isEqualTo(new byte[] {1, 2});
            assertThat(decimalOf(row, ColumnPath.of("def_zero"), 2)).isEqualByComparingTo("0.00");
            assertThat(batchLeaf(dataset, "def_zero").logicalType()).contains(new LogicalType.Decimal(2, 2));

            Predicate isDefault =
                    new Predicate.Eq(ColumnPath.of("def_dec"), new Value.DecimalVal(new BigDecimal("14.20")));
            assertThat(dataset.count(isDefault, ReadOptions.DEFAULTS)).isEqualTo(LIVE_ROWS);
            assertThat(dataset.count(new Predicate.IsNull(ColumnPath.of("def_u")), ReadOptions.DEFAULTS))
                    .isZero();
        });
    }

    private Path extractTable() {
        return TestCorpus.extractDirectory("iceberg-scalar-types/scalars", tempDir.resolve(TABLE));
    }

    private static List<IcebergField> currentFields(Path tableDir) {
        try {
            String json = Files.readString(tableDir.resolve("metadata/v1.metadata.json"));
            return IcebergTableMetadata.read(json).fields();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void withDataset(Path tableDir, Consumer<IcebergDataset> assertions) {
        try (IcebergTableCatalog catalog = IcebergTableCatalog.openLocal(tableDir, IcebergOptions.defaults())) {
            IcebergDataset dataset = (IcebergDataset) catalog.dataset(TABLE);
            assertions.accept(dataset);
        }
    }

    private static ParquetRecord onlyRow(IcebergDataset dataset, Predicate predicate) {
        try (Stream<ParquetRecord> rows = dataset.read(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
            List<ParquetRecord> detached = rows.map(ParquetRecord::detach).toList();
            assertThat(detached).hasSize(1);
            return detached.get(0);
        }
    }

    /**
     * The leaf that a materialized batch declares for {@code leafName}. A constant column derives its leaf from the
     * default value alone; the table-level schema does not show that leaf.
     */
    private static SchemaNode.Primitive batchLeaf(IcebergDataset dataset, String leafName) {
        try (Stream<ParquetRecordBatch> batches =
                dataset.readBatches(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
            List<SchemaNode.Primitive> leaves = batches.map(batch -> {
                        try (batch) {
                            return leaf(batch.projectedSchema(), leafName);
                        }
                    })
                    .toList();
            assertThat(leaves).isNotEmpty();
            return leaves.getFirst();
        }
    }

    private static List<Long> idsOf(IcebergDataset dataset, Predicate predicate) {
        try (Stream<ParquetRecord> rows = dataset.read(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
            return rows.map(row -> row.getLong(ID)).sorted().toList();
        }
    }

    /**
     * A decimal cell is its unscaled value in the encoding chosen by the writer: INT32, INT64, or fixed-length bytes.
     */
    private static BigDecimal decimalOf(ParquetRecord row, ColumnPath column, int scale) {
        return switch (row.get(column)) {
            case Integer unscaled -> BigDecimal.valueOf(unscaled, scale);
            case Long unscaled -> BigDecimal.valueOf(unscaled, scale);
            case MemorySegment bytes -> new BigDecimal(new BigInteger(bytes.toArray(ValueLayout.JAVA_BYTE)), scale);
            case Object other -> throw new IllegalStateException(column.dot() + " is not a decimal cell: " + other);
        };
    }

    static UUID uuidOf(long id) {
        return new UUID(id, id * 1_000_003L);
    }

    private static long epochMicros(LocalDateTime value) {
        return value.toEpochSecond(ZoneOffset.UTC) * 1_000_000L + value.getNano() / 1_000L;
    }

    private static long microsOfDay(LocalTime value) {
        return value.toNanoOfDay() / 1_000L;
    }

    private static long skippedFileCount(DatasetExplainPlan plan) {
        return plan.files().stream()
                .map(FileExplain::outcome)
                .filter(outcome -> outcome == Outcome.SKIP)
                .count();
    }

    private static List<String> leafNames(ParquetSchema schema) {
        return schema.leafColumns().stream().map(ColumnPath::name).toList();
    }

    private static void assertLeaf(
            ParquetSchema schema,
            String name,
            PrimitiveKind kind,
            OptionalInt typeLength,
            Optional<LogicalType> logicalType) {
        SchemaNode.Primitive leaf = leaf(schema, name);
        assertThat(leaf.kind()).as(name).isEqualTo(kind);
        assertThat(leaf.typeLength()).as(name).isEqualTo(typeLength);
        assertThat(leaf.logicalType()).as(name).isEqualTo(logicalType);
    }

    private static SchemaNode.Primitive leaf(ParquetSchema schema, String leafName) {
        for (SchemaNode child : schema.root().children()) {
            if (child instanceof SchemaNode.Primitive primitive
                    && primitive.name().equals(leafName)) {
                return primitive;
            }
        }
        throw new IllegalArgumentException("no leaf named " + leafName);
    }
}
