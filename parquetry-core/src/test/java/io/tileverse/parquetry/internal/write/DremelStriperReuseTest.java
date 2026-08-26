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

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DefaultParquetRecordBatch;
import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.ListVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Guards the two ways one striper and its scratch are reused, on the repeated-leaf path. Across batches: striping a
 * batch through a striper or a workspace that has already handled a larger batch must yield exactly what a fresh one
 * yields for that same batch, which pins that resetting between batches leaks no prior-batch state into the next one.
 * Across threads: one striper shredding the same leaf on many threads at once, each into its own workspace, must yield
 * what a single thread yields, which pins that the striper holds no per-call state for concurrent callers to corrupt.
 */
class DremelStriperReuseTest {

    @Test
    void reusedStriperStripesTheSecondBatchLikeAFreshOne() {
        ParquetSchema schema = listOfOptionalInt();
        ParquetRecordBatch larger = batchWithElements(schema, 20);
        ParquetRecordBatch smaller = batchWithElements(schema, 3);

        DremelStriper reused = new DremelStriper(schema);
        reused.stripe(larger);
        List<StripedLeaf> reusedSmaller = reused.stripe(smaller);

        List<StripedLeaf> freshSmaller = new DremelStriper(schema).stripe(smaller);
        assertThat(reusedSmaller).isEqualTo(freshSmaller);
    }

    @Test
    void aWorkspaceReusedAfterALargerBatchExposesOnlyTheSmallerBatchEntries() {
        ParquetSchema schema = listOfOptionalInt();
        ParquetRecordBatch larger = batchWithElements(schema, 20);
        ParquetRecordBatch smaller = batchWithElements(schema, 3);
        ColumnPath leaf = schema.leafColumns().get(0);
        DremelStriper striper = new DremelStriper(schema);
        LeafStripeWorkspace workspace = new LeafStripeWorkspace();

        striper.stripeLeaf(leaf, larger, workspace);
        StripedLeaf reusedSmaller = striper.stripeLeaf(leaf, smaller, workspace);
        StripedLeaf freshSmaller = striper.stripeLeaf(leaf, smaller, new LeafStripeWorkspace());

        assertThat(reusedSmaller)
                .as("a grown backing must not leak the larger batch's tail into the smaller result")
                .isEqualTo(freshSmaller);
        assertThat(reusedSmaller.defLevelsRaw().length)
                .as("the backing keeps its grown capacity")
                .isGreaterThan(reusedSmaller.entryCount());
    }

    @Test
    void oneStriperServesConcurrentCallers() throws Exception {
        ParquetSchema schema = listOfOptionalInt();
        ParquetRecordBatch batch = batchWithElements(schema, 64);
        ColumnPath leaf = schema.leafColumns().get(0);
        DremelStriper striper = new DremelStriper(schema);

        StripedLeaf sequential = striper.stripeLeaf(leaf, batch, new LeafStripeWorkspace());

        List<Callable<StripedLeaf>> jobs = new ArrayList<>();
        for (int i = 0; i < 32; i++) {
            jobs.add(() -> striper.stripeLeaf(leaf, batch, new LeafStripeWorkspace()));
        }
        List<StripedLeaf> concurrent = new ArrayList<>();
        try (ExecutorService pool = Executors.newFixedThreadPool(8)) {
            for (Future<StripedLeaf> done : pool.invokeAll(jobs)) {
                concurrent.add(done.get());
            }
        }

        assertThat(concurrent)
                .as("one striper serving many threads must produce what one thread produces")
                .allSatisfy(result -> assertThat(result).isEqualTo(sequential));
    }

    /**
     * A single-column batch whose optional list holds {@code elementCount} present integers in one row. A larger count
     * grows the striping scratch past its initial capacity, exercising the reset-after-growth path the guard depends
     * on.
     */
    private static ParquetRecordBatch batchWithElements(ParquetSchema schema, int elementCount) {
        int[] data = new int[elementCount];
        for (int i = 0; i < elementCount; i++) {
            data[i] = i;
        }
        IntVector elements = IntVector.materialized(data, Validity.allValid(elementCount));
        ListVector list = new ListVector(new int[] {0, elementCount}, elements, validity(true), 1);
        return heapBatch(schema, Map.of(ColumnPath.of("mylist"), list), 1);
    }

    private static ParquetSchema listOfOptionalInt() {
        SchemaNode.Primitive element = primitive("element", Repetition.OPTIONAL);
        SchemaNode.Group repeated =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        SchemaNode.Group list = new SchemaNode.Group(
                "mylist", Repetition.OPTIONAL, List.of(repeated), Optional.of(new LogicalType.ListType()), -1);
        return schemaOf(list);
    }

    private static SchemaNode.Primitive primitive(String name, Repetition repetition) {
        return new SchemaNode.Primitive(
                name, repetition, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static ParquetSchema schemaOf(SchemaNode child) {
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(child), Optional.empty(), -1);
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

    private static ParquetRecordBatch heapBatch(ParquetSchema schema, Map<ColumnPath, ColumnVector> columns, int rows) {
        return DefaultParquetRecordBatch.ofHeap(schema, columns, rows);
    }
}
