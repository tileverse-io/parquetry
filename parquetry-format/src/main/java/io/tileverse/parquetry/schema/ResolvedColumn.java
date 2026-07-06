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
package io.tileverse.parquetry.schema;

import java.util.List;

/**
 * The outcome of resolving a logical column path against a {@link ParquetSchema}.
 *
 * @param physical the physical leaf path, including the synthetic LIST {@code list}/{@code element} and MAP
 *     {@code key_value} levels the file stores.
 * @param kind the leaf's physical type, for literal coercion at predicate-build time.
 * @param repeatedPhysicalIndices the indices into {@link #physical()} at which a {@code repeated} level occurs. Empty
 *     means the leaf is single-valued; non-empty marks it multi-valued (a list/map element).
 */
public record ResolvedColumn(ColumnPath physical, PrimitiveKind kind, List<Integer> repeatedPhysicalIndices) {

    public ResolvedColumn {
        repeatedPhysicalIndices = List.copyOf(repeatedPhysicalIndices);
    }

    /** True when the leaf path crosses at least one repeated boundary, i.e. the operand is multi-valued. */
    public boolean isRepeated() {
        return !repeatedPhysicalIndices.isEmpty();
    }
}
