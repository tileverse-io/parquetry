/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.data;

import static io.tileverse.parquetry.filter.Pred.col;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.filter.ExplainPlan;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.RowGroupOutcome;
import io.tileverse.parquetry.filter.RowGroupPlan;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/** Verifies the column-index tier narrows the surviving row set once its page stats are wired into the pipeline. */
class PagePruningExplainTest {

    private static final ColumnPath V = ColumnPath.of("v");

    @TempDir
    Path tempDir;

    @Test
    void columnIndexTierNarrowsToTheSurvivingPages() throws Exception {
        Path file = writeEightRowsAcrossFourPages();
        try (Storage storage = StorageFactory.open(file.getParent().toUri());
                RangeReader reader = storage.openRangeReader(file.getFileName().toString())) {
            ParquetDataset dataset = ParquetDataset.open(reader);
            ExplainPlan plan = dataset.explain(col("v").gtEq(5), Projection.ALL, ReadOptions.DEFAULTS);

            RowGroupPlan rg = plan.rowGroups().get(0);
            assertThat(rg.outcome()).isEqualTo(RowGroupOutcome.PARTIAL);
            assertThat(rg.survivingRows()).isPresent();
            assertThat(rg.survivingRows().orElseThrow().totalRows())
                    .as("only the pages whose max >= 5 survive")
                    .isEqualTo(4L)
                    .isLessThan(rg.rowCount());
        }
    }

    private Path writeEightRowsAcrossFourPages() throws Exception {
        Path file = tempDir.resolve("page-index.parquet");
        ParquetSchema schema = flatSchema(requiredInt32("v"));
        WriteOptions options =
                WriteOptions.builder().tempDir(tempDir).pageValueLimit(2).build();
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(file), schema, options)) {
            for (int v = 0; v < 8; v++) {
                writer.write(row(v));
            }
        }
        return file;
    }

    private static ParquetSchema flatSchema(SchemaNode.Primitive... leaves) {
        List<SchemaNode> children = Stream.of(leaves).map(f -> (SchemaNode) f).toList();
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, children, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive requiredInt32(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static WriteRow row(int v) {
        Map<ColumnPath, Object> values = Map.of(V, v);
        return values::get;
    }
}
