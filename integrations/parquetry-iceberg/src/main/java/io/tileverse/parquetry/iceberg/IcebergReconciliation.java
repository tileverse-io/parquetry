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

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.SequencedSet;

import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.iceberg.IcebergFileSchema.FileColumn;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.UuidConverter;

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
 * {@link IcebergFileSchema} resolves a duplicate id last-wins); a decimal reads through its physical column in any of
 * its three encodings and widens in precision freely (the value is scale-bound, not kind-bound), while a decimal scale,
 * temporal unit, or fixed or uuid length that disagrees with the table fails loud, as does a file column annotated as a
 * decimal or a temporal type under a table field that declares neither; date-to-timestamp promotion is deferred.
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
        return sameName
                && kindAgrees(field, fileColumn)
                && parameterMismatch(field, fileColumn).isEmpty();
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

    /**
     * Presents one file column as the table declares it. The type parameters are checked ahead of the kind dispatch: a
     * sanctioned widening must not admit a column whose scale, unit, or length disagrees with the table.
     */
    private static Projection.Column presentExistingColumn(IcebergField field, FileColumn fileColumn) {
        requireParameterAgreement(field, fileColumn);
        ColumnPath name = ColumnPath.of(field.name());
        PrimitiveKind expectedKind = IcebergSchema.kindOf(field);
        if (kindAgrees(field, fileColumn)) {
            return new Projection.Column.Physical(name, fileColumn.path());
        }
        if (isSanctionedWidening(fileColumn.kind(), expectedKind)) {
            return new Projection.Column.Promoted(name, fileColumn.path(), expectedKind);
        }
        throw new IcebergFormatException("cannot reconcile field %s: file type %s is not promotable to %s"
                .formatted(field.name(), fileColumn.kind(), expectedKind));
    }

    private static Projection.Column injectNullColumn(IcebergField field) {
        Value archetype = IcebergScalarValues.nullValue(field.type(), field.name());
        return new Projection.Column.Null(ColumnPath.of(field.name()), archetype);
    }

    private static boolean isSanctionedWidening(PrimitiveKind fileKind, PrimitiveKind expectedKind) {
        boolean intToLong = fileKind == PrimitiveKind.INT32 && expectedKind == PrimitiveKind.INT64;
        boolean floatToDouble = fileKind == PrimitiveKind.FLOAT && expectedKind == PrimitiveKind.DOUBLE;
        return intToLong || floatToDouble;
    }

    /**
     * The file's kind is the table's, or the column is a decimal in any of its three physical encodings: Iceberg's
     * writer stores up to nine digits as INT32 and up to eighteen as INT64, other writers store every decimal as
     * fixed-length bytes, and the unscaled value is the same in each. The produced column keeps the file's encoding.
     */
    private static boolean kindAgrees(IcebergField field, FileColumn fileColumn) {
        if (fileColumn.kind() == IcebergSchema.kindOf(field)) {
            return true;
        }
        return field.type() instanceof IcebergType.DecimalType && isDecimalEncoding(fileColumn);
    }

    private static boolean isDecimalEncoding(FileColumn fileColumn) {
        boolean integerOrFixed =
                switch (fileColumn.kind()) {
                    case INT32, INT64, FIXED_LEN_BYTE_ARRAY -> true;
                    default -> false;
                };
        return integerOrFixed && fileColumn.logicalType().orElse(null) instanceof LogicalType.Decimal;
    }

    private static void requireParameterAgreement(IcebergField field, FileColumn fileColumn) {
        Optional<String> mismatch = parameterMismatch(field, fileColumn);
        if (mismatch.isPresent()) {
            throw new IcebergFormatException("cannot reconcile field %s: %s".formatted(field.name(), mismatch.get()));
        }
    }

    /**
     * Why the file column's type parameters disagree with the table type, or empty when they agree. Iceberg never
     * changes a decimal's scale, a temporal unit, or a fixed length through evolution; a disagreement marks a
     * non-conforming file, and reading it at the table's parameters would return wrong values. Precision may differ
     * freely: a widened decimal keeps the unscaled value stored in the file, whatever width holds it.
     */
    private static Optional<String> parameterMismatch(IcebergField field, FileColumn fileColumn) {
        return switch (field.type()) {
            case IcebergType.DecimalType(int _, int scale) -> decimalMismatch(fileColumn, scale);
            case IcebergType.TimestampType(boolean _, LogicalType.TimeUnit unit) -> timestampMismatch(fileColumn, unit);
            case IcebergType.TimeType _ -> timeMismatch(fileColumn);
            case IcebergType.FixedType(int length) -> fixedMismatch(fileColumn, length);
            case IcebergType.UuidType _ -> fixedMismatch(fileColumn, UuidConverter.BYTES);
            // No other Iceberg type declares a parameter of its own, and none of them admits a file column that
            // claims one.
            default -> annotationMismatch(fileColumn, field.type());
        };
    }

    /**
     * Why a file column annotated as a decimal, a timestamp, or a time cannot present a table type that declares no
     * such parameter, or empty when the file column claims no such annotation. Statistics decode through the file's own
     * annotation, while a predicate is validated against the bare integer that the table presents; the pruning tiers
     * would then compare two unrelated value kinds, find them equal, and keep row groups that match nothing.
     */
    private static Optional<String> annotationMismatch(FileColumn fileColumn, IcebergType tableType) {
        return parameterizedAnnotation(fileColumn)
                .map(annotation -> "file column is annotated as %s but the table declares %s"
                        .formatted(annotation, tableType.token()));
    }

    /** How to name the file column's decimal or temporal annotation, absent when the column claims neither. */
    private static Optional<String> parameterizedAnnotation(FileColumn fileColumn) {
        return switch (fileColumn.logicalType().orElse(null)) {
            case LogicalType.Decimal _ -> Optional.of("a decimal");
            case LogicalType.Timestamp _ -> Optional.of("a timestamp");
            case LogicalType.Time _ -> Optional.of("a time");
            case null, default -> Optional.empty();
        };
    }

    private static Optional<String> decimalMismatch(FileColumn fileColumn, int scale) {
        if (fileColumn.logicalType().orElse(null) instanceof LogicalType.Decimal(int fileScale, int _)) {
            return fileScale == scale
                    ? Optional.empty()
                    : Optional.of("file decimal scale %d differs from the table scale %d".formatted(fileScale, scale));
        }
        return Optional.of("file column is not annotated as a decimal");
    }

    private static Optional<String> timestampMismatch(FileColumn fileColumn, LogicalType.TimeUnit unit) {
        if (fileColumn.logicalType().orElse(null)
                instanceof LogicalType.Timestamp(boolean _, LogicalType.TimeUnit fileUnit)) {
            return fileUnit == unit
                    ? Optional.empty()
                    : Optional.of("file timestamp unit %s differs from the table unit %s".formatted(fileUnit, unit));
        }
        return Optional.of("file column is not annotated as a timestamp");
    }

    private static Optional<String> timeMismatch(FileColumn fileColumn) {
        if (fileColumn.logicalType().orElse(null)
                instanceof LogicalType.Time(boolean _, LogicalType.TimeUnit fileUnit)) {
            return fileUnit == LogicalType.TimeUnit.MICROS
                    ? Optional.empty()
                    : Optional.of("file time unit %s differs from the table unit MICROS".formatted(fileUnit));
        }
        return Optional.of("file column is not annotated as a time");
    }

    private static Optional<String> fixedMismatch(FileColumn fileColumn, int length) {
        OptionalInt fileLength = fileColumn.typeLength();
        if (fileLength.isPresent() && fileLength.getAsInt() == length) {
            return Optional.empty();
        }
        String fileToken = fileLength.isPresent() ? String.valueOf(fileLength.getAsInt()) : "(absent)";
        return Optional.of("file fixed length %s differs from the table length %d".formatted(fileToken, length));
    }
}
