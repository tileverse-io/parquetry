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
package io.tileverse.parquetry.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.OutputColumn;
import io.tileverse.parquetry.filter.Value;
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
        assertThat(result.output()).isEmpty();
    }

    @Test
    void passesThroughWhenFileIsIdLess() {
        IcebergSchema table = tableOf(field(1, "id", "long"));
        ParquetSchema file = fileSchema(leaf("id", PrimitiveKind.INT64, -1));

        Reconciliation result = IcebergReconciliation.reconcile(table, IcebergFileSchema.of(file), Map.of());

        assertThat(result.passThrough()).isTrue();
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
        assertThat(result.output())
                .containsExactly(new OutputColumn.Physical(ColumnPath.of("count"), ColumnPath.of("n")));
    }

    @Test
    void reconcilesAnAddedFieldAsANullColumn() {
        IcebergSchema table = tableOf(field(2, "n", "long"), field(3, "label", "string"));
        ParquetSchema file = fileSchema(leaf("n", PrimitiveKind.INT64, 2));

        Reconciliation result = IcebergReconciliation.reconcile(table, IcebergFileSchema.of(file), Map.of());

        assertThat(result.passThrough()).isFalse();
        assertThat(result.output())
                .containsExactly(
                        new OutputColumn.Physical(ColumnPath.of("n"), ColumnPath.of("n")),
                        new OutputColumn.Null(ColumnPath.of("label"), new Value.StringVal("")));
    }

    @Test
    void reconstructsAnOmittedIdentityPartitionColumnAsAConstant() {
        IcebergSchema table = tableOf(field(1, "id", "long"), field(2, "category", "string"));
        ParquetSchema file = fileSchema(leaf("id", PrimitiveKind.INT64, 1));
        Map<Integer, Value> partitionConstants = Map.of(2, new Value.StringVal("b"));

        Reconciliation result = IcebergReconciliation.reconcile(table, IcebergFileSchema.of(file), partitionConstants);

        assertThat(result.passThrough()).isFalse();
        assertThat(result.output())
                .containsExactly(
                        new OutputColumn.Physical(ColumnPath.of("id"), ColumnPath.of("id")),
                        new OutputColumn.Constant(ColumnPath.of("category"), new Value.StringVal("b")));
    }

    @Test
    void stillInjectsNullForAnAddedNonPartitionColumn() {
        IcebergSchema table = tableOf(field(1, "id", "long"), field(3, "label", "string"));
        ParquetSchema file = fileSchema(leaf("id", PrimitiveKind.INT64, 1));

        Reconciliation result = IcebergReconciliation.reconcile(table, IcebergFileSchema.of(file), Map.of());

        assertThat(result.output())
                .containsExactly(
                        new OutputColumn.Physical(ColumnPath.of("id"), ColumnPath.of("id")),
                        new OutputColumn.Null(ColumnPath.of("label"), new Value.StringVal("")));
    }

    @Test
    void reconcilesReorderAndRenameTogetherInTableOrder() {
        IcebergSchema table = tableOf(field(2, "count", "long"), field(1, "id", "long"));
        ParquetSchema file = fileSchema(leaf("id", PrimitiveKind.INT64, 1), leaf("n", PrimitiveKind.INT64, 2));

        Reconciliation result = IcebergReconciliation.reconcile(table, IcebergFileSchema.of(file), Map.of());

        assertThat(result.passThrough()).isFalse();
        assertThat(result.output())
                .containsExactly(
                        new OutputColumn.Physical(ColumnPath.of("count"), ColumnPath.of("n")),
                        new OutputColumn.Physical(ColumnPath.of("id"), ColumnPath.of("id")));
    }

    @Test
    void dropsAFileColumnAbsentFromTheTableWhenReconciliationIsTriggered() {
        IcebergSchema table = tableOf(field(2, "count", "long"));
        ParquetSchema file = fileSchema(leaf("n", PrimitiveKind.INT64, 2), leaf("dropped", PrimitiveKind.INT64, 7));

        Reconciliation result = IcebergReconciliation.reconcile(table, IcebergFileSchema.of(file), Map.of());

        assertThat(result.passThrough()).isFalse();
        assertThat(result.output())
                .containsExactly(new OutputColumn.Physical(ColumnPath.of("count"), ColumnPath.of("n")));
    }

    @Test
    void promotesIntToLong() {
        IcebergSchema table = tableOf(field(2, "n", "long"));
        ParquetSchema file = fileSchema(leaf("n", PrimitiveKind.INT32, 2));

        Reconciliation result = IcebergReconciliation.reconcile(table, IcebergFileSchema.of(file), Map.of());

        assertThat(result.passThrough()).isFalse();
        assertThat(result.output())
                .containsExactly(
                        new OutputColumn.Promoted(ColumnPath.of("n"), ColumnPath.of("n"), PrimitiveKind.INT64));
    }

    @Test
    void promotesFloatToDouble() {
        IcebergSchema table = tableOf(field(2, "x", "double"));
        ParquetSchema file = fileSchema(leaf("x", PrimitiveKind.FLOAT, 2));

        Reconciliation result = IcebergReconciliation.reconcile(table, IcebergFileSchema.of(file), Map.of());

        assertThat(result.passThrough()).isFalse();
        assertThat(result.output())
                .containsExactly(
                        new OutputColumn.Promoted(ColumnPath.of("x"), ColumnPath.of("x"), PrimitiveKind.DOUBLE));
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
    void failsWhenAnAddedFieldHasAnUnsupportedNullType() {
        IcebergSchema table = tableOf(field(2, "n", "long"), field(3, "blob", "binary"));
        IcebergFileSchema file = IcebergFileSchema.of(fileSchema(leaf("n", PrimitiveKind.INT64, 2)));

        assertThatThrownBy(() -> IcebergReconciliation.reconcile(table, file, NO_PARTITIONS))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("blob");
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
}
