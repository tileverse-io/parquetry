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
package io.tileverse.parquetry.record;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.Arena;
import java.lang.management.ManagementFactory;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import com.sun.management.ThreadMXBean;

import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.DefaultParquetRecordBatch;
import io.tileverse.parquetry.batch.DoubleVector;
import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.batch.StructVector;
import io.tileverse.parquetry.batch.Validity;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * The record-level filter evaluator reads a struct-nested column once per surviving row (e.g. the four bbox covering
 * comparisons a spatial filter lowers to). Resolving that nested path must reuse the struct's column layout across the
 * batch's rows rather than rebuilding it per access; otherwise a hot filter churns a fresh map and arrays per row. This
 * pins the per-access allocation to a small bound the per-row rebuild would blow through.
 */
class RowColumnsAllocationTest {

    private static final int ROWS = 200_000;
    private static final ColumnPath BBOX = ColumnPath.of("bbox");
    private static final ColumnPath NESTED_XMIN = ColumnPath.of("bbox", "xmin");

    @Test
    void resolvingANestedColumnDoesNotChurnPerRow() {
        try (ParquetRecordBatch batch = bboxBatch()) {
            touchNestedColumn(batch); // warm up the JIT and prime any per-batch layout

            long before = allocatedBytes();
            long sink = touchNestedColumn(batch);
            long allocated = allocatedBytes() - before;

            assertThat(sink).isEqualTo(ROWS); // guard against the work being optimised away
            // Reusing the struct layout holds this near the per-row record cost (~70 bytes/row). The per-row layout
            // rebuild this guards against allocated ~5-8x that; the bound sits well below the regression, with room.
            assertThat(allocated)
                    .as("nested-column resolution over %d rows allocated %d bytes", ROWS, allocated)
                    .isLessThan(20L * 1024 * 1024);
        }
    }

    private static long touchNestedColumn(ParquetRecordBatch batch) {
        long present = 0;
        for (int row = 0; row < ROWS; row++) {
            if (!batch.materialize(row).isNull(NESTED_XMIN)) {
                present++;
            }
        }
        return present;
    }

    private static long allocatedBytes() {
        ThreadMXBean threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        return threads.getCurrentThreadAllocatedBytes();
    }

    /** A batch of {@link #ROWS} rows: a {@code bbox} struct with four double children, all non-null. */
    private static ParquetRecordBatch bboxBatch() {
        Map<ColumnPath, ColumnVector> children = new LinkedHashMap<>();
        for (String corner : new String[] {"xmin", "xmax", "ymin", "ymax"}) {
            children.put(ColumnPath.of(corner), DoubleVector.materialized(new double[ROWS], Validity.allValid(ROWS)));
        }
        StructVector bbox = new StructVector(children, Validity.allValid(ROWS), ROWS);
        return new DefaultParquetRecordBatch(bboxSchema(), Map.of(BBOX, bbox), ROWS, Arena.ofConfined());
    }

    private static ParquetSchema bboxSchema() {
        List<SchemaNode> corners =
                List.of(doubleLeaf("xmin"), doubleLeaf("xmax"), doubleLeaf("ymin"), doubleLeaf("ymax"));
        SchemaNode.Group bbox = new SchemaNode.Group("bbox", Repetition.OPTIONAL, corners, Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(bbox), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive doubleLeaf(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.OPTIONAL, PrimitiveKind.DOUBLE, OptionalInt.empty(), Optional.empty(), -1);
    }
}
