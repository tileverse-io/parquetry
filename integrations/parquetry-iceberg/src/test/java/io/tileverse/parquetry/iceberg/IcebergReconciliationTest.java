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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.LogicalType.TimeUnit;
import io.tileverse.parquetry.iceberg.IcebergReconciliation.Reconciliation;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class IcebergReconciliationTest {

    private static final Map<Integer, Value> NO_PARTITIONS = Map.of();

    @Test
    void passesThroughWhenFileMatchesTableExactly() {
        IcebergSchema table = tableOf(field(1, "id", "long"), field(2, "name", "string"));
        ParquetSchema file = fileSchema(leaf("id", PrimitiveKind.INT64, 1), leaf("name", PrimitiveKind.BYTE_ARRAY, 2));

        Reconciliation result = IcebergReconciliation.reconcile(table, IcebergFileSchema.of(file), Map.of());

        assertThat(result.passThrough()).isTrue();
        assertThat(result.columns()).isEmpty();
    }

    @Test
    void reconcilesAnUnresolvedIdlessFileToAbsentFields() {
        IcebergSchema table = tableOf(field(1, "id", "long"));
        ParquetSchema file = fileSchema(leaf("id", PrimitiveKind.INT64, -1));

        Reconciliation result = IcebergReconciliation.reconcile(table, IcebergFileSchema.of(file), Map.of());

        assertThat(result.passThrough()).isFalse();
        assertThat(result.columns())
                .containsExactly(new Projection.Column.Null(ColumnPath.of("id"), new Value.LongVal(0L)));
    }

    @Test
    void passesThroughAnIdlessFileResolvedByTheImplicitMapping() {
        IcebergSchema table = tableOf(field(1, "id", "long"));
        ParquetSchema file = fileSchema(leaf("id", PrimitiveKind.INT64, -1));
        IcebergNameMapping mapping = IcebergNameMapping.fromSchema(List.of(field(1, "id", "long")));

        Reconciliation result = IcebergReconciliation.reconcile(table, IcebergFileSchema.of(file, mapping), Map.of());

        assertThat(result.passThrough()).isTrue();
    }

    @Test
    void reconcilesAnIdlessRenameThroughAMappingAlias() {
        IcebergSchema table = tableOf(field(2, "count", "long"));
        ParquetSchema file = fileSchema(leaf("n", PrimitiveKind.INT64, -1));
        IcebergNameMapping mapping = IcebergNameMapping.fromJson("[{\"field-id\": 2, \"names\": [\"n\", \"count\"]}]");

        Reconciliation result = IcebergReconciliation.reconcile(table, IcebergFileSchema.of(file, mapping), Map.of());

        assertThat(result.passThrough()).isFalse();
        assertThat(result.columns())
                .containsExactly(new Projection.Column.Physical(ColumnPath.of("count"), ColumnPath.of("n")));
    }

    @Test
    void passThroughPreservesAnExtraNonTableFileColumn() {
        IcebergSchema table = tableOf(field(1, "id", "long"));
        ParquetSchema file = fileSchema(leaf("id", PrimitiveKind.INT64, 1), leaf("lineage", PrimitiveKind.INT64, 99));

        Reconciliation result = IcebergReconciliation.reconcile(table, IcebergFileSchema.of(file), Map.of());

        assertThat(result.passThrough()).isTrue();
    }

    @Test
    void reconcilesARename() {
        IcebergSchema table = tableOf(field(2, "count", "long"));
        ParquetSchema file = fileSchema(leaf("n", PrimitiveKind.INT64, 2));

        Reconciliation result = IcebergReconciliation.reconcile(table, IcebergFileSchema.of(file), Map.of());

        assertThat(result.passThrough()).isFalse();
        assertThat(result.columns())
                .containsExactly(new Projection.Column.Physical(ColumnPath.of("count"), ColumnPath.of("n")));
    }

    @Test
    void reconcilesAnAddedFieldAsANullColumn() {
        IcebergSchema table = tableOf(field(2, "n", "long"), field(3, "label", "string"));
        ParquetSchema file = fileSchema(leaf("n", PrimitiveKind.INT64, 2));

        Reconciliation result = IcebergReconciliation.reconcile(table, IcebergFileSchema.of(file), Map.of());

        assertThat(result.passThrough()).isFalse();
        assertThat(result.columns())
                .containsExactly(
                        new Projection.Column.Physical(ColumnPath.of("n"), ColumnPath.of("n")),
                        new Projection.Column.Null(ColumnPath.of("label"), new Value.StringVal("")));
    }

    @Test
    void reconstructsAnOmittedIdentityPartitionColumnAsAConstant() {
        IcebergSchema table = tableOf(field(1, "id", "long"), field(2, "category", "string"));
        ParquetSchema file = fileSchema(leaf("id", PrimitiveKind.INT64, 1));
        Map<Integer, Value> partitionConstants = Map.of(2, new Value.StringVal("b"));

        Reconciliation result = IcebergReconciliation.reconcile(table, IcebergFileSchema.of(file), partitionConstants);

        assertThat(result.passThrough()).isFalse();
        assertThat(result.columns())
                .containsExactly(
                        new Projection.Column.Physical(ColumnPath.of("id"), ColumnPath.of("id")),
                        new Projection.Column.Constant(ColumnPath.of("category"), new Value.StringVal("b")));
    }

    @Test
    void stillInjectsNullForAnAddedNonPartitionColumn() {
        IcebergSchema table = tableOf(field(1, "id", "long"), field(3, "label", "string"));
        ParquetSchema file = fileSchema(leaf("id", PrimitiveKind.INT64, 1));

        Reconciliation result = IcebergReconciliation.reconcile(table, IcebergFileSchema.of(file), Map.of());

        assertThat(result.columns())
                .containsExactly(
                        new Projection.Column.Physical(ColumnPath.of("id"), ColumnPath.of("id")),
                        new Projection.Column.Null(ColumnPath.of("label"), new Value.StringVal("")));
    }

    @Test
    void reconstructsAnAbsentColumnFromItsInitialDefault() {
        IcebergField labelWithDefault =
                new IcebergField(3, "label", "string", false, Optional.of(new Value.StringVal("n/a")));
        IcebergSchema table = tableOf(field(1, "id", "long"), labelWithDefault);
        ParquetSchema file = fileSchema(leaf("id", PrimitiveKind.INT64, 1));

        Reconciliation result = IcebergReconciliation.reconcile(table, IcebergFileSchema.of(file), NO_PARTITIONS);

        assertThat(result.passThrough()).isFalse();
        assertThat(result.columns())
                .containsExactly(
                        new Projection.Column.Physical(ColumnPath.of("id"), ColumnPath.of("id")),
                        new Projection.Column.Constant(ColumnPath.of("label"), new Value.StringVal("n/a")));
    }

    @Test
    void partitionConstantOutranksAnInitialDefault() {
        IcebergField categoryWithDefault =
                new IcebergField(2, "category", "string", false, Optional.of(new Value.StringVal("default")));
        IcebergSchema table = tableOf(field(1, "id", "long"), categoryWithDefault);
        ParquetSchema file = fileSchema(leaf("id", PrimitiveKind.INT64, 1));
        Map<Integer, Value> partitionConstants = Map.of(2, new Value.StringVal("b"));

        Reconciliation result = IcebergReconciliation.reconcile(table, IcebergFileSchema.of(file), partitionConstants);

        assertThat(result.columns())
                .containsExactly(
                        new Projection.Column.Physical(ColumnPath.of("id"), ColumnPath.of("id")),
                        new Projection.Column.Constant(ColumnPath.of("category"), new Value.StringVal("b")));
    }

    @Test
    void reconcilesReorderAndRenameTogetherInTableOrder() {
        IcebergSchema table = tableOf(field(2, "count", "long"), field(1, "id", "long"));
        ParquetSchema file = fileSchema(leaf("id", PrimitiveKind.INT64, 1), leaf("n", PrimitiveKind.INT64, 2));

        Reconciliation result = IcebergReconciliation.reconcile(table, IcebergFileSchema.of(file), Map.of());

        assertThat(result.passThrough()).isFalse();
        assertThat(result.columns())
                .containsExactly(
                        new Projection.Column.Physical(ColumnPath.of("count"), ColumnPath.of("n")),
                        new Projection.Column.Physical(ColumnPath.of("id"), ColumnPath.of("id")));
    }

    @Test
    void dropsAFileColumnAbsentFromTheTableWhenReconciliationIsTriggered() {
        IcebergSchema table = tableOf(field(2, "count", "long"));
        ParquetSchema file = fileSchema(leaf("n", PrimitiveKind.INT64, 2), leaf("dropped", PrimitiveKind.INT64, 7));

        Reconciliation result = IcebergReconciliation.reconcile(table, IcebergFileSchema.of(file), Map.of());

        assertThat(result.passThrough()).isFalse();
        assertThat(result.columns())
                .containsExactly(new Projection.Column.Physical(ColumnPath.of("count"), ColumnPath.of("n")));
    }

    @Test
    void promotesIntToLong() {
        IcebergSchema table = tableOf(field(2, "n", "long"));
        ParquetSchema file = fileSchema(leaf("n", PrimitiveKind.INT32, 2));

        Reconciliation result = IcebergReconciliation.reconcile(table, IcebergFileSchema.of(file), Map.of());

        assertThat(result.passThrough()).isFalse();
        assertThat(result.columns())
                .containsExactly(
                        new Projection.Column.Promoted(ColumnPath.of("n"), ColumnPath.of("n"), PrimitiveKind.INT64));
    }

    @Test
    void promotesFloatToDouble() {
        IcebergSchema table = tableOf(field(2, "x", "double"));
        ParquetSchema file = fileSchema(leaf("x", PrimitiveKind.FLOAT, 2));

        Reconciliation result = IcebergReconciliation.reconcile(table, IcebergFileSchema.of(file), Map.of());

        assertThat(result.passThrough()).isFalse();
        assertThat(result.columns())
                .containsExactly(
                        new Projection.Column.Promoted(ColumnPath.of("x"), ColumnPath.of("x"), PrimitiveKind.DOUBLE));
    }

    @Test
    void failsOnAnUnsanctionedKindMismatch() {
        IcebergSchema table = tableOf(field(2, "x", "float"));
        IcebergFileSchema file = IcebergFileSchema.of(fileSchema(leaf("x", PrimitiveKind.INT64, 2)));

        assertThatThrownBy(() -> IcebergReconciliation.reconcile(table, file, NO_PARTITIONS))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("x");
    }

    @Test
    void injectsNullForAnAddedUuidTimestampAndUnknownColumn() {
        IcebergSchema table = tableOf(
                field(1, "id", "long"), field(2, "u", "uuid"), field(3, "ts", "timestamp"), field(4, "x", "unknown"));
        IcebergFileSchema file = IcebergFileSchema.of(fileSchema(leaf("id", PrimitiveKind.INT64, 1)));

        Reconciliation result = IcebergReconciliation.reconcile(table, file, NO_PARTITIONS);

        assertThat(result.columns())
                .containsExactly(
                        new Projection.Column.Physical(ColumnPath.of("id"), ColumnPath.of("id")),
                        new Projection.Column.Null(ColumnPath.of("u"), new Value.UuidVal(new UUID(0L, 0L))),
                        new Projection.Column.Null(ColumnPath.of("ts"), new Value.LongVal(0L)),
                        new Projection.Column.Null(ColumnPath.of("x"), new Value.IntVal(0)));
    }

    @Test
    void failsWhenAnAddedGeometryFieldNeedsANullColumn() {
        IcebergSchema table = tableOf(field(2, "n", "long"), field(3, "geom", "geometry"));
        IcebergFileSchema file = IcebergFileSchema.of(fileSchema(leaf("n", PrimitiveKind.INT64, 2)));

        assertThatThrownBy(() -> IcebergReconciliation.reconcile(table, file, NO_PARTITIONS))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("geom");
    }

    @Test
    void injectsNullForAnAddedBinaryDecimalAndTimeColumn() {
        IcebergSchema table = tableOf(
                field(1, "id", "long"),
                field(2, "blob", "binary"),
                field(3, "amount", "decimal(9, 2)"),
                field(4, "t", "time"));
        IcebergFileSchema file = IcebergFileSchema.of(fileSchema(leaf("id", PrimitiveKind.INT64, 1)));

        List<Projection.Column> columns = List.copyOf(
                IcebergReconciliation.reconcile(table, file, NO_PARTITIONS).columns());

        assertThat(columns.get(0)).isEqualTo(new Projection.Column.Physical(ColumnPath.of("id"), ColumnPath.of("id")));
        assertThat(columns.get(1))
                .isInstanceOfSatisfying(
                        Projection.Column.Null.class,
                        nul -> assertThat(nul.typeOf()).isInstanceOf(Value.BinaryVal.class));
        // The decimal archetype spells out the table precision at the table scale; its digits are never read.
        assertThat(columns.get(2))
                .isInstanceOfSatisfying(
                        Projection.Column.Null.class,
                        nul -> assertThat(nul.typeOf()).isInstanceOfSatisfying(Value.DecimalVal.class, dec -> {
                            assertThat(dec.value().precision()).isEqualTo(9);
                            assertThat(dec.value().scale()).isEqualTo(2);
                        }));
        assertThat(columns.get(3))
                .isEqualTo(new Projection.Column.Null(ColumnPath.of("t"), new Value.TimeVal(LocalTime.MIDNIGHT)));
    }

    @Test
    void passesThroughADecimalFileAtTheTableScale() {
        // Iceberg's writer stores decimal(9, 2) as INT32, the kind presented by the table for that precision.
        IcebergSchema table = tableOf(field(1, "amount", "decimal(9, 2)"));
        ParquetSchema file =
                fileSchema(leaf("amount", PrimitiveKind.INT32, 1, new LogicalType.Decimal(2, 9), OptionalInt.empty()));

        assertThat(IcebergReconciliation.reconcile(table, IcebergFileSchema.of(file), NO_PARTITIONS)
                        .passThrough())
                .isTrue();
    }

    @Test
    void passesThroughAFixedLengthDecimalWhereTheTablePresentsAnInteger() {
        // Another writer stored the same decimal(9, 2) as fixed-length bytes; the encoding differs, the value does not.
        IcebergSchema table = tableOf(field(1, "amount", "decimal(9, 2)"));
        ParquetSchema file = fileSchema(leaf(
                "amount", PrimitiveKind.FIXED_LEN_BYTE_ARRAY, 1, new LogicalType.Decimal(2, 9), OptionalInt.of(4)));

        assertThat(IcebergReconciliation.reconcile(table, IcebergFileSchema.of(file), NO_PARTITIONS)
                        .passThrough())
                .isTrue();
    }

    @Test
    void widensDecimalPrecisionThroughThePhysicalColumn() {
        // decimal(9, 2) INT32 in the file, decimal(18, 2) INT64 in the table: the unscaled value is kind-independent.
        IcebergSchema table = tableOf(field(1, "amount", "decimal(18, 2)"), field(2, "n", "long"));
        ParquetSchema file = fileSchema(
                leaf("amount", PrimitiveKind.INT32, 1, new LogicalType.Decimal(2, 9), OptionalInt.empty()),
                leaf("count", PrimitiveKind.INT64, 2));

        Reconciliation result = IcebergReconciliation.reconcile(table, IcebergFileSchema.of(file), NO_PARTITIONS);

        assertThat(result.passThrough()).isFalse();
        assertThat(result.columns())
                .containsExactly(
                        new Projection.Column.Physical(ColumnPath.of("amount"), ColumnPath.of("amount")),
                        new Projection.Column.Physical(ColumnPath.of("n"), ColumnPath.of("count")));
    }

    @Test
    void failsOnADecimalScaleMismatch() {
        IcebergSchema table = tableOf(field(1, "amount", "decimal(9, 2)"));
        IcebergFileSchema file = IcebergFileSchema.of(
                fileSchema(leaf("amount", PrimitiveKind.INT32, 1, new LogicalType.Decimal(3, 9), OptionalInt.empty())));

        assertThatThrownBy(() -> IcebergReconciliation.reconcile(table, file, NO_PARTITIONS))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("amount")
                .hasMessageContaining("scale");
    }

    @Test
    void failsOnAnUnannotatedDecimalFileColumn() {
        // A plain INT32 column is not a decimal, whatever kind the table presents.
        IcebergSchema table = tableOf(field(1, "amount", "decimal(9, 2)"));
        IcebergFileSchema file = IcebergFileSchema.of(fileSchema(leaf("amount", PrimitiveKind.INT32, 1)));

        assertThatThrownBy(() -> IcebergReconciliation.reconcile(table, file, NO_PARTITIONS))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("amount");
    }

    @Test
    void failsOnAnUnannotatedIntegerFileColumnForAWiderDecimal() {
        // The int-to-long widening must not admit a plain INT32 column into a table decimal.
        IcebergSchema table = tableOf(field(1, "amount", "decimal(18, 2)"));
        IcebergFileSchema file = IcebergFileSchema.of(fileSchema(leaf("amount", PrimitiveKind.INT32, 1)));

        assertThatThrownBy(() -> IcebergReconciliation.reconcile(table, file, NO_PARTITIONS))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("amount");
    }

    @Test
    void failsOnATimestampUnitMismatch() {
        IcebergSchema table = tableOf(field(1, "ts", "timestamp_ns"));
        IcebergFileSchema file = IcebergFileSchema.of(fileSchema(leaf(
                "ts", PrimitiveKind.INT64, 1, new LogicalType.Timestamp(false, TimeUnit.MICROS), OptionalInt.empty())));

        assertThatThrownBy(() -> IcebergReconciliation.reconcile(table, file, NO_PARTITIONS))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("ts")
                .hasMessageContaining("unit");
    }

    @Test
    void failsOnATimeUnitMismatch() {
        IcebergSchema table = tableOf(field(1, "t", "time"));
        IcebergFileSchema file = IcebergFileSchema.of(fileSchema(
                leaf("t", PrimitiveKind.INT64, 1, new LogicalType.Time(false, TimeUnit.NANOS), OptionalInt.empty())));

        assertThatThrownBy(() -> IcebergReconciliation.reconcile(table, file, NO_PARTITIONS))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("t")
                .hasMessageContaining("unit");
    }

    @Test
    void failsOnAFixedLengthMismatch() {
        IcebergSchema table = tableOf(field(1, "fx", "fixed[4]"));
        IcebergFileSchema file = IcebergFileSchema.of(fileSchema(new SchemaNode.Primitive(
                "fx",
                Repetition.OPTIONAL,
                PrimitiveKind.FIXED_LEN_BYTE_ARRAY,
                OptionalInt.of(8),
                Optional.empty(),
                1)));

        assertThatThrownBy(() -> IcebergReconciliation.reconcile(table, file, NO_PARTITIONS))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("fx")
                .hasMessageContaining("length");
    }

    @Test
    void failsOnAUuidFileColumnOfAnotherWidth() {
        // A uuid table field declares an implicit sixteen-byte width; any other width would fail deep in the
        // record accessor with a message about the vector shape.
        IcebergSchema table = tableOf(field(1, "uid", "uuid"));
        IcebergFileSchema file = IcebergFileSchema.of(fileSchema(
                leaf("uid", PrimitiveKind.FIXED_LEN_BYTE_ARRAY, 1, new LogicalType.UuidType(), OptionalInt.of(20))));

        assertThatThrownBy(() -> IcebergReconciliation.reconcile(table, file, NO_PARTITIONS))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("uid")
                .hasMessageContaining("16");
    }

    @Test
    void passesThroughAUuidFileColumnOfTheDeclaredWidth() {
        IcebergSchema table = tableOf(field(1, "uid", "uuid"));
        ParquetSchema file = fileSchema(
                leaf("uid", PrimitiveKind.FIXED_LEN_BYTE_ARRAY, 1, new LogicalType.UuidType(), OptionalInt.of(16)));

        assertThat(IcebergReconciliation.reconcile(table, IcebergFileSchema.of(file), NO_PARTITIONS)
                        .passThrough())
                .isTrue();
    }

    @Test
    void failsOnADecimalAnnotatedFileColumnUnderAPlainIntegerField() {
        // The table presents a bare INT32 while the file's statistics decode as decimals through the file's own
        // annotation; the pruning tiers would compare the two and keep row groups that match nothing.
        IcebergSchema table = tableOf(field(1, "n", "int"));
        IcebergFileSchema file = IcebergFileSchema.of(
                fileSchema(leaf("n", PrimitiveKind.INT32, 1, new LogicalType.Decimal(2, 9), OptionalInt.empty())));

        assertThatThrownBy(() -> IcebergReconciliation.reconcile(table, file, NO_PARTITIONS))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("n")
                .hasMessageContaining("decimal")
                .hasMessageContaining("int");
    }

    @Test
    void failsOnATimestampAnnotatedFileColumnUnderAPlainLongField() {
        IcebergSchema table = tableOf(field(1, "n", "long"));
        IcebergFileSchema file = IcebergFileSchema.of(fileSchema(leaf(
                "n", PrimitiveKind.INT64, 1, new LogicalType.Timestamp(false, TimeUnit.MICROS), OptionalInt.empty())));

        assertThatThrownBy(() -> IcebergReconciliation.reconcile(table, file, NO_PARTITIONS))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("n")
                .hasMessageContaining("timestamp")
                .hasMessageContaining("long");
    }

    @Test
    void failsOnATimeAnnotatedFileColumnUnderAPlainLongField() {
        IcebergSchema table = tableOf(field(1, "n", "long"));
        IcebergFileSchema file = IcebergFileSchema.of(fileSchema(
                leaf("n", PrimitiveKind.INT64, 1, new LogicalType.Time(false, TimeUnit.MICROS), OptionalInt.empty())));

        assertThatThrownBy(() -> IcebergReconciliation.reconcile(table, file, NO_PARTITIONS))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("n")
                .hasMessageContaining("time")
                .hasMessageContaining("long");
    }

    @Test
    void passesThroughATimestampFileAtTheTableUnit() {
        IcebergSchema table = tableOf(field(1, "ts", "timestamptz"));
        ParquetSchema file = fileSchema(leaf(
                "ts", PrimitiveKind.INT64, 1, new LogicalType.Timestamp(true, TimeUnit.MICROS), OptionalInt.empty()));

        assertThat(IcebergReconciliation.reconcile(table, IcebergFileSchema.of(file), NO_PARTITIONS)
                        .passThrough())
                .isTrue();
    }

    private static IcebergSchema tableOf(IcebergField... fields) {
        return IcebergSchema.of(List.of(fields));
    }

    private static IcebergField field(int fieldId, String name, String type) {
        return new IcebergField(fieldId, name, type, false);
    }

    private static ParquetSchema fileSchema(SchemaNode.Primitive... leaves) {
        SchemaNode.Group root =
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(leaves), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive leaf(String name, PrimitiveKind kind, int fieldId) {
        return new SchemaNode.Primitive(
                name, Repetition.OPTIONAL, kind, OptionalInt.empty(), Optional.empty(), fieldId);
    }

    private static SchemaNode.Primitive leaf(
            String name, PrimitiveKind kind, int fieldId, LogicalType logicalType, OptionalInt typeLength) {
        return new SchemaNode.Primitive(name, Repetition.OPTIONAL, kind, typeLength, Optional.of(logicalType), fieldId);
    }
}
