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
package io.tileverse.parquetry.arrow.cdi;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.util.BitSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.arrow.columnar.ArrowBufferCodec;
import io.tileverse.parquetry.arrow.columnar.EncodedNode;
import io.tileverse.parquetry.arrow.ipc.ArrowField;
import io.tileverse.parquetry.arrow.ipc.LogicalColumns;
import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Exports an {@link EncodedNode} into an {@code ArrowArray} over the C Data Interface, borrowing its buffers from a
 * {@link SegmentPool}, and imports it back with arrow-java's {@code Data.importVector}. The test confirms the values
 * and null positions reconstruct, that releasing the imported array returns every borrowed buffer to the pool, and that
 * a second release is a no-op.
 */
class ArrowArrayExportIT {

    @Test
    void exportsAnIntColumnWithNullsAndReturnsBuffersOnRelease() {
        SegmentPool pool = SegmentPool.create();
        assertThat(pool.stats().outstandingBorrows()).isZero();

        EncodedNode node = ArrowBufferCodec.encode(intColumnWithNull());
        try (Arena arena = Arena.ofShared();
                RootAllocator allocator = new RootAllocator();
                CDataDictionaryProvider provider = new CDataDictionaryProvider()) {
            MemorySegment schema = ArrowSchemaExporter.export(intField(), arena);
            MemorySegment array = arena.allocate(CDataLayouts.ARROW_ARRAY);
            ArrowArrayExporter.export(node, pool, array);
            assertThat(pool.stats().outstandingBorrows()).isPositive();

            try (FieldVector vector = Data.importVector(
                    allocator, ArrowArray.wrap(array.address()), ArrowSchema.wrap(schema.address()), provider)) {
                org.apache.arrow.vector.IntVector imported = (org.apache.arrow.vector.IntVector) vector;
                assertThat(imported.getValueCount()).isEqualTo(3);
                assertThat(imported.get(0)).isEqualTo(1);
                assertThat(imported.isNull(1)).isTrue();
                assertThat(imported.get(2)).isEqualTo(3);
            }
            assertThat(pool.stats().outstandingBorrows()).isZero();
        }
        pool.close();
    }

    @Test
    void doubleReleaseIsANoOp() throws Throwable {
        SegmentPool pool = SegmentPool.create();
        EncodedNode node = ArrowBufferCodec.encode(intColumnWithNull());
        try (Arena arena = Arena.ofShared()) {
            MemorySegment array = arena.allocate(CDataLayouts.ARROW_ARRAY);
            ArrowArrayExporter.export(node, pool, array);

            MemorySegment releasePointer = CDataLayouts.arrayRelease(array);
            MethodHandle release = Linker.nativeLinker()
                    .downcallHandle(releasePointer, FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

            release.invoke(array);
            assertThat(pool.stats().outstandingBorrows()).isZero();

            release.invoke(array); // second release finds no reclaim state and does nothing
            assertThat(pool.stats().outstandingBorrows()).isZero();
        }
        pool.close();
    }

    private static IntVector intColumnWithNull() {
        BitSet valid = new BitSet(3);
        valid.set(0);
        valid.set(2);
        return IntVector.materialized(new int[] {1, 0, 3}, Validity.of(valid, 3));
    }

    private static ArrowField intField() {
        SchemaNode.Primitive id = new SchemaNode.Primitive(
                "id", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), 0);
        ParquetSchema schema =
                new ParquetSchema(new SchemaNode.Group("root", Repetition.REQUIRED, List.of(id), Optional.empty(), -1));
        return LogicalColumns.of(schema, Optional.empty()).get(0).field();
    }
}
