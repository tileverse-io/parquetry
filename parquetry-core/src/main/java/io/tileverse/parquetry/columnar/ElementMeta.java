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
package io.tileverse.parquetry.columnar;

import io.tileverse.parquetry.columnar.ListMeta.StructMeta;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * The shape of one value node in a level-backed group's metadata tree: a list's element or a struct's field, resolved
 * once against the projected leaves when the owning vector is built. Each permitted record holds exactly the metadata
 * its shape needs, and the navigation path dispatches on the family with record-pattern switches.
 *
 * <p>Instances are created only by the {@link ListMeta} and {@link MapMeta} factory tree and are batch-constant: the
 * tree is fully precomputed at vector construction and read on every access without further allocation.
 */
public sealed interface ElementMeta {

    /**
     * The definition level at which this node's value is present; an entry whose definition level is below this holds a
     * null value.
     */
    int presenceDef();

    /**
     * A primitive node read from a single leaf vector.
     *
     * @param presenceDef the leaf's max definition level
     * @param leaf the leaf path the values are read from; the factory only builds this shape when the projection kept
     *     the leaf
     * @param leafOrdinal the leaf's ordinal in the owning vector's leaf order; the level-backed element record reads
     *     the scalar value by this ordinal instead of resolving the leaf path per access
     */
    record Scalar(int presenceDef, ColumnPath leaf, int leafOrdinal) implements ElementMeta {}

    /**
     * A nested list node, present at the inner list group's definition level. The inner list's structure leaf and
     * element resolution live in {@link ListMeta}.
     *
     * @param list the inner list's metadata
     */
    record NestedList(ListMeta list) implements ElementMeta {
        @Override
        public int presenceDef() {
            return list.groupDefLevel();
        }
    }

    /**
     * A struct node read through its field tree.
     *
     * @param struct the struct's resolved fields and structure leaf
     */
    record Struct(StructMeta struct) implements ElementMeta {
        @Override
        public int presenceDef() {
            return struct.presenceDef();
        }
    }

    /**
     * A map node, present at the map group's definition level. The {@code key_value} resolution lives in
     * {@link MapMeta}.
     *
     * @param map the map's metadata
     */
    record MapEntry(MapMeta map) implements ElementMeta {
        @Override
        public int presenceDef() {
            return map.groupDefLevel();
        }
    }

    /**
     * An unshredded Variant node's two binary leaves.
     *
     * @param presenceDef the Variant node's max definition level
     * @param metadataLeaf the {@code metadata} leaf path, or {@code null} when the projection dropped it
     * @param valueLeaf the {@code value} leaf path, or {@code null} when the projection dropped it
     */
    record VariantLeaves(int presenceDef, ColumnPath metadataLeaf, ColumnPath valueLeaf) implements ElementMeta {}

    /**
     * A node the projection dropped entirely: no descendant leaf is present in the owning vector. The lenient accessors
     * read such a node as null; the typed accessors reject it as a mismatch.
     *
     * @param presenceDef the node's max definition level
     */
    record Absent(int presenceDef) implements ElementMeta {}
}
