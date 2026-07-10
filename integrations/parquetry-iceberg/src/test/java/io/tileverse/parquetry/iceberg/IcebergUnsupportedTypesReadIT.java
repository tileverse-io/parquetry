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

import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.LogicalType.TimeUnit;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Proves an Iceberg table whose current schema declares a uuid, a timestamp of each precision and adjustment, or an
 * unknown column now opens and reads (these types were rejected at open before). The {@code v3_minimal} corpus table
 * (id#1:string, n#2:int, ten data files of ten thousand rows) is evolved in place to add each new-type column with a
 * fresh field id; the data files predate the columns, and each therefore reads as null while the table itself reads
 * normally.
 */
class IcebergUnsupportedTypesReadIT {

    private static final String TABLE = "v3_minimal";
    private static final long EXPECTED_TOTAL_ROWS = 10_000L;

    private static final IcebergField ID = new IcebergField(1, "id", "string", false);
    private static final IcebergField N = new IcebergField(2, "n", "int", false);

    @TempDir
    Path tempDir;

    @Test
    void opensAndReadsATableWithEveryNewlySupportedType() {
        Path tableDir = evolve(List.of(
                ID,
                N,
                new IcebergField(10, "u_uuid", "uuid", false),
                new IcebergField(11, "ts", "timestamp", false),
                new IcebergField(12, "tstz", "timestamptz", false),
                new IcebergField(13, "ts_ns", "timestamp_ns", false),
                new IcebergField(14, "tstz_ns", "timestamptz_ns", false),
                new IcebergField(15, "unk", "unknown", false)));

        withDataset(tableDir, dataset -> {
            ParquetSchema schema = dataset.schema();
            assertThat(leafNames(schema)).containsExactly("id", "n", "u_uuid", "ts", "tstz", "ts_ns", "tstz_ns", "unk");

            SchemaNode.Primitive uuid = leaf(schema, "u_uuid");
            assertThat(uuid.kind()).isEqualTo(PrimitiveKind.FIXED_LEN_BYTE_ARRAY);
            assertThat(uuid.typeLength()).hasValue(16);
            assertThat(uuid.logicalType()).contains(new LogicalType.UuidType());

            assertThat(leaf(schema, "ts").kind()).isEqualTo(PrimitiveKind.INT64);
            assertThat(leaf(schema, "ts").logicalType()).contains(new LogicalType.Timestamp(false, TimeUnit.MICROS));
            assertThat(leaf(schema, "tstz").logicalType()).contains(new LogicalType.Timestamp(true, TimeUnit.MICROS));
            assertThat(leaf(schema, "ts_ns").logicalType()).contains(new LogicalType.Timestamp(false, TimeUnit.NANOS));
            assertThat(leaf(schema, "tstz_ns").logicalType()).contains(new LogicalType.Timestamp(true, TimeUnit.NANOS));

            assertThat(leaf(schema, "unk").kind()).isEqualTo(PrimitiveKind.INT32);
            assertThat(leaf(schema, "unk").logicalType()).contains(new LogicalType.UnknownType());

            ParquetRecord row = firstRow(dataset);
            assertThat(row.getString(ColumnPath.of("id"))).isNotEmpty();
            for (String added : List.of("u_uuid", "ts", "tstz", "ts_ns", "tstz_ns", "unk")) {
                assertThat(row.isNull(ColumnPath.of(added)))
                        .as("added column %s reads null", added)
                        .isTrue();
            }
            assertThat(fullScanRowCount(dataset)).isEqualTo(EXPECTED_TOTAL_ROWS);
        });
    }

    @Test
    void unknownOnlyTableDerivesItsRowCountWithoutAnyPhysicalColumn() {
        Path tableDir = evolve(List.of(new IcebergField(99, "u", "unknown", false)));

        withDataset(tableDir, dataset -> {
            assertThat(leafNames(dataset.schema())).containsExactly("u");
            assertThat(fullScanRowCount(dataset)).isEqualTo(EXPECTED_TOTAL_ROWS);
            assertThat(firstRow(dataset).isNull(ColumnPath.of("u"))).isTrue();
        });
    }

    private Path evolve(List<IcebergField> evolvedFields) {
        Path tableDir = extractTable();
        return IcebergSchemaEvolution.evolveCurrentSchema(tableDir, evolvedFields);
    }

    private Path extractTable() {
        Path root = TestCorpus.extractDirectory("iceberg-geo-testbed", tempDir.resolve(TABLE));
        return root.resolve(TABLE);
    }

    private void withDataset(Path tableDir, Consumer<IcebergDataset> assertions) {
        try (IcebergTableCatalog catalog = IcebergTableCatalog.openLocal(tableDir, IcebergOptions.defaults())) {
            IcebergDataset dataset = (IcebergDataset) catalog.dataset(TABLE);
            assertions.accept(dataset);
        }
    }

    private static long fullScanRowCount(IcebergDataset dataset) {
        try (Stream<ParquetRecord> rows = dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
            return rows.count();
        }
    }

    private static ParquetRecord firstRow(IcebergDataset dataset) {
        try (Stream<ParquetRecord> rows = dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
            return rows.map(ParquetRecord::detach).findFirst().orElseThrow();
        }
    }

    private static List<String> leafNames(ParquetSchema schema) {
        return schema.leafColumns().stream().map(ColumnPath::name).toList();
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
