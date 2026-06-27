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
package io.tileverse.parquetry.internal.read;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DefaultParquetRecordBatch;
import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class BatchSpillStoreTest {

    @Test
    void spillsAndRestoresAByteIdenticalBatch(@TempDir Path dir) {
        ParquetSchema schema = singleIntColumnSchema("n");
        ParquetRecordBatch original = intBatch(schema, "n", new int[] {7, 8, 9});
        DiskBudget diskBudget = DiskBudget.ofBytes(1 << 20);

        try (BatchSpillStore store = new BatchSpillStore(dir, diskBudget, schema)) {
            Optional<SpillHandle> handle = store.trySpill(original);
            assertThat(handle).isPresent();
            assertThat(diskBudget.available()).isLessThan(diskBudget.capacity());

            try (ParquetRecordBatch restored = store.restore(handle.get())) {
                assertThat(restored.rowCount()).isEqualTo(3);
                IntVector restoredColumn = (IntVector) restored.columns().get(ColumnPath.of("n"));
                assertThat(restoredColumn.asArray()).containsExactly(7, 8, 9);
            }
            assertThat(diskBudget.available()).isEqualTo(diskBudget.capacity());
        }
    }

    @Test
    void trySpillReturnsEmptyWhenDiskBudgetIsFull(@TempDir Path dir) {
        ParquetSchema schema = singleIntColumnSchema("n");
        ParquetRecordBatch batch = intBatch(schema, "n", new int[] {1, 2, 3});
        DiskBudget tinyBudget = DiskBudget.ofBytes(1);

        try (BatchSpillStore store = new BatchSpillStore(dir, tinyBudget, schema)) {
            assertThat(store.trySpill(batch)).isEmpty();
        } finally {
            batch.close();
        }
    }

    private static ParquetRecordBatch intBatch(ParquetSchema schema, String name, int[] values) {
        Map<ColumnPath, ColumnVector> columns = new LinkedHashMap<>();
        columns.put(ColumnPath.of(name), IntVector.materialized(values, Validity.allValid(values.length)));
        return DefaultParquetRecordBatch.ofHeap(schema, columns, values.length);
    }

    private static ParquetSchema singleIntColumnSchema(String name) {
        SchemaNode.Primitive leaf = new SchemaNode.Primitive(
                name, Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root = new SchemaNode.Group("root", Repetition.REQUIRED, List.of(leaf), Optional.empty(), -1);
        return new ParquetSchema(root);
    }
}
