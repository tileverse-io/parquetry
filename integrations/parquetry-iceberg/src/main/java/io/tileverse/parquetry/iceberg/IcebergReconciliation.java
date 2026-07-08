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
package io.tileverse.parquetry.iceberg;

import java.time.LocalDate;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.SequencedSet;
import java.util.UUID;

import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.iceberg.IcebergFileSchema.FileColumn;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * Matches one Iceberg data file's columns back to the table's current schema by field id.
 *
 * <p>Given the table's {@link IcebergSchema} and a single file's {@link IcebergFileSchema}, this decides whether the
 * file can be read untouched by name (the fast path) or must be presented through an ordered output shape that renames,
 * reorders, null-fills, promotes, and drops columns to match the table. The output shape presents one
 * {@link Projection.Column} per table field, in table order.
 *
 * <p>Known limitations: drops are honored only when reconciliation is otherwise triggered (a file that differs from the
 * table by a pure drop takes the pass-through path and keeps the extra column, acceptable because real evolved tables
 * combine changes); reconciliation assumes top-level field ids are unique (Iceberg guarantees this, and
 * {@link IcebergFileSchema} resolves a duplicate id last-wins); decimal widening and date-to-timestamp promotions are
 * deferred until the widening substrate supports them.
 */
final class IcebergReconciliation {

    private IcebergReconciliation() {}

    /** Either read the file untouched by name, or present it through an ordered produce set. */
    record Reconciliation(boolean passThrough, SequencedSet<Projection.Column> columns) {

        Reconciliation {
            columns = new LinkedHashSet<>(columns);
        }

        static Reconciliation ofPassThrough() {
            return new Reconciliation(true, new LinkedHashSet<>());
        }

        static Reconciliation ofReconciled(SequencedSet<Projection.Column> columns) {
            return new Reconciliation(false, columns);
        }
    }

    /**
     * Reconciles {@code file} against {@code table}, returning the fast path when possible. A table field absent from
     * the file whose id appears in {@code partitionConstants} is an omitted identity-partition column: it reconstructs
     * to a {@link Projection.Column.Constant} of its partition value rather than a null column.
     */
    static Reconciliation reconcile(
            IcebergSchema table, IcebergFileSchema file, Map<Integer, Value> partitionConstants) {
        if (canPassThrough(table, file)) {
            return Reconciliation.ofPassThrough();
        }
        return Reconciliation.ofReconciled(presentTableFields(table, file, partitionConstants));
    }

    private static boolean canPassThrough(IcebergSchema table, IcebergFileSchema file) {
        for (IcebergField field : table.fields()) {
            if (!isSatisfiedAsIs(field, file)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isSatisfiedAsIs(IcebergField field, IcebergFileSchema file) {
        Optional<FileColumn> column = file.byFieldId(field.fieldId());
        if (column.isEmpty()) {
            return false;
        }
        FileColumn fileColumn = column.get();
        boolean sameName = fileColumn.path().name().equals(field.name());
        boolean sameKind = fileColumn.kind() == IcebergSchema.kindOf(field);
        return sameName && sameKind;
    }

    private static SequencedSet<Projection.Column> presentTableFields(
            IcebergSchema table, IcebergFileSchema file, Map<Integer, Value> partitionConstants) {
        SequencedSet<Projection.Column> columns =
                LinkedHashSet.newLinkedHashSet(table.fields().size());
        for (IcebergField field : table.fields()) {
            columns.add(presentField(field, file, partitionConstants));
        }
        return columns;
    }

    private static Projection.Column presentField(
            IcebergField field, IcebergFileSchema file, Map<Integer, Value> partitionConstants) {
        Optional<FileColumn> column = file.byFieldId(field.fieldId());
        if (column.isEmpty()) {
            return presentAbsentColumn(field, partitionConstants);
        }
        return presentExistingColumn(field, column.get());
    }

    private static Projection.Column presentAbsentColumn(IcebergField field, Map<Integer, Value> partitionConstants) {
        Value partitionValue = partitionConstants.get(field.fieldId());
        if (partitionValue != null) {
            return new Projection.Column.Constant(ColumnPath.of(field.name()), partitionValue);
        }
        Optional<Value> initialDefault = field.initialDefault();
        if (initialDefault.isPresent()) {
            return new Projection.Column.Constant(ColumnPath.of(field.name()), initialDefault.get());
        }
        return injectNullColumn(field);
    }

    private static Projection.Column presentExistingColumn(IcebergField field, FileColumn fileColumn) {
        ColumnPath name = ColumnPath.of(field.name());
        PrimitiveKind expectedKind = IcebergSchema.kindOf(field);
        if (fileColumn.kind() == expectedKind) {
            return new Projection.Column.Physical(name, fileColumn.path());
        }
        if (isSanctionedWidening(fileColumn.kind(), expectedKind)) {
            return new Projection.Column.Promoted(name, fileColumn.path(), expectedKind);
        }
        throw new IcebergFormatException("cannot reconcile field %s: file type %s is not promotable to %s"
                .formatted(field.name(), fileColumn.kind(), expectedKind));
    }

    private static Projection.Column injectNullColumn(IcebergField field) {
        return new Projection.Column.Null(ColumnPath.of(field.name()), nullTypeOf(field));
    }

    private static boolean isSanctionedWidening(PrimitiveKind fileKind, PrimitiveKind expectedKind) {
        boolean intToLong = fileKind == PrimitiveKind.INT32 && expectedKind == PrimitiveKind.INT64;
        boolean floatToDouble = fileKind == PrimitiveKind.FLOAT && expectedKind == PrimitiveKind.DOUBLE;
        return intToLong || floatToDouble;
    }

    private static Value nullTypeOf(IcebergField field) {
        return switch (field.type()) {
            case "int" -> new Value.IntVal(0);
            case "long" -> new Value.LongVal(0L);
            case "float" -> new Value.FloatVal(0f);
            case "double" -> new Value.DoubleVal(0d);
            case "boolean" -> new Value.BoolVal(false);
            case "date" -> new Value.DateVal(LocalDate.EPOCH);
            case "string" -> new Value.StringVal("");
            // The value is ignored; only its kind selects the all-null vector.
            case "uuid" -> new Value.UuidVal(new UUID(0L, 0L));
            case "timestamp", "timestamptz", "timestamp_ns", "timestamptz_ns" -> new Value.LongVal(0L);
            case "unknown" -> new Value.IntVal(0);
            default ->
                throw new IcebergFormatException("cannot inject a null column for added field %s of unsupported type %s"
                        .formatted(field.name(), field.type()));
        };
    }
}
