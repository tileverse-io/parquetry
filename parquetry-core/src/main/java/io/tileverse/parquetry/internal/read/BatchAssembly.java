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
package io.tileverse.parquetry.internal.read;

import java.util.List;
import java.util.Map;

import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * Wraps a batch's row-aligned LIST and MAP groups in the requested {@link BatchForm}: the eager Arrow-shape vectors for
 * {@link BatchForm#ASSEMBLED}, or the lazy level vectors for {@link BatchForm#LEVELS}. The level form acquires a
 * batch-owned {@link BatchLevels} into {@code acquiredBuffers}, which the owning batch closes.
 */
final class BatchAssembly {

    private BatchAssembly() {}

    @SuppressWarnings("java:S107") // the assembly inputs; a parameter object would only relocate the arity
    static Map<ColumnPath, ColumnVector> assemble(
            BatchForm form,
            LevelAssemblyPlans plans,
            ParquetSchema schema,
            Map<ColumnPath, ColumnVector> leafVectors,
            Map<ColumnPath, LevelSlice> repLevels,
            Map<ColumnPath, LevelSlice> defLevels,
            int batchRows,
            DecodeBufferAllocator allocator,
            List<AutoCloseable> acquiredBuffers) {
        return switch (form) {
            case ASSEMBLED ->
                NestedVectorAssembler.assembleNestedViews(schema, leafVectors, repLevels, defLevels, batchRows);
            case LEVELS ->
                LevelVectorAssembler.assembleLevelForm(
                        plans.forLeaves(leafVectors.keySet()),
                        schema,
                        leafVectors,
                        repLevels,
                        defLevels,
                        batchRows,
                        allocator,
                        acquiredBuffers);
        };
    }
}
