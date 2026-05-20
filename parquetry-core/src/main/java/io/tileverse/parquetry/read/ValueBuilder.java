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
package io.tileverse.parquetry.read;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.tileverse.parquetry.materializer.RowAccessor;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Materializes one logical row position's value for one column subtree of the projected schema.
 *
 * <p>The {@link RecordAssembler} builds a {@code ValueBuilder} tree at construction (one root builder per top-level
 * projected column) and then calls {@link #read()} once per row to drive the assembly. Builders consume one or more
 * {@code (repLevel, defLevel, value)} triples from the {@link ColumnReader}s they wrap, returning whatever Java shape
 * the Parquet schema demands: a boxed primitive or read-only {@link java.lang.foreign.MemorySegment} for a leaf, an
 * {@link List} for a {@code LIST}-annotated or legacy repeated group, a {@link Map} for a {@code MAP}-annotated group,
 * or a nested {@link RowAccessor} for a non-repeated struct.
 *
 * <h2>Contract</h2>
 *
 * <ul>
 *   <li>Before each {@code read()} call all wrapped {@code ColumnReader}s are positioned at the leading triple of the
 *       current top-level row (or at end-of-stream).
 *   <li>{@code read()} consumes triples until either the row ends (any nested list runs to completion) or the reader's
 *       {@code currentRepetitionLevel()} drops below the depth this builder operates at.
 *   <li>After {@code read()} returns, every wrapped reader is positioned at the next row's leading triple (or
 *       end-of-stream).
 *   <li>Returns {@code null} when the column / group / list is fully null in this row. Returns {@link List#of()} for a
 *       non-null but empty list, and {@link Map#of()} for an empty map.
 * </ul>
 *
 * <p>The sealed hierarchy mirrors the four assembly shapes: {@link LeafValueBuilder} for a non-repeated primitive leaf,
 * {@link StructValueBuilder} for a non-repeated group (legacy struct), {@link ListValueBuilder} for a
 * {@code LIST}-annotated group or a legacy repeated leaf / group, {@link MapValueBuilder} for a {@code MAP}-annotated
 * group.
 */
sealed interface ValueBuilder
        permits ValueBuilder.LeafValueBuilder,
                ValueBuilder.ListValueBuilder,
                ValueBuilder.MapValueBuilder,
                ValueBuilder.StructValueBuilder {

    /**
     * Read one row position's value, advancing the underlying column readers across all triples that belong to this
     * row.
     */
    Object read();

    // --- non-repeated primitive leaf ---

    /**
     * Reads one value from a single non-repeated {@link ColumnReader}. Returns the boxed primitive (or read-only
     * {@link java.lang.foreign.MemorySegment} for binary / INT96) at {@code maxDef}, or {@code null} when the leaf or
     * any optional ancestor is null.
     */
    final class LeafValueBuilder implements ValueBuilder {

        private final ColumnReader reader;

        LeafValueBuilder(ColumnReader reader) {
            this.reader = reader;
        }

        @Override
        public Object read() {
            if (!reader.hasNext()) {
                throw new IllegalStateException(
                        "Column " + reader.columnPath().dot() + " exhausted while assembler still has work to do");
            }
            boolean present = reader.currentDefinitionLevel() == reader.maxDefinitionLevel();
            Object value = present ? reader.currentValue() : null;
            reader.consume();
            return value;
        }
    }

    // --- non-repeated nested group ---

    /**
     * Reads one struct value (non-repeated group) by delegating to each child builder in order. Returns the assembled
     * {@link RowAccessor}, or {@code null} when the optional struct itself was null in the source row.
     *
     * <p>Struct-null detection uses the definition-level of any descendant leaf: if every descendant leaf reports
     * {@code currentDefinitionLevel() < structDefLevel} then the struct itself is null at this row position. The leaves
     * are still consumed so the readers advance.
     */
    final class StructValueBuilder implements ValueBuilder {

        private final List<ChildBinding> children;
        private final ColumnPath structPath;
        private final int structDefLevel;
        private final List<ColumnReader> structLeaves;

        StructValueBuilder(
                ColumnPath structPath,
                int structDefLevel,
                List<ChildBinding> children,
                List<ColumnReader> structLeaves) {
            this.structPath = structPath;
            this.structDefLevel = structDefLevel;
            this.children = List.copyOf(children);
            this.structLeaves = List.copyOf(structLeaves);
        }

        @Override
        public Object read() {
            boolean isNull = isStructNull();
            Map<ColumnPath, Object> values = HashMap.newHashMap(children.size());
            for (ChildBinding child : children) {
                Object childValue = child.builder.read();
                if (!isNull && childValue != null) {
                    values.put(child.path, childValue);
                }
            }
            if (isNull) {
                return null;
            }
            return new SubRecordRowAccessor(structPath, values);
        }

        private boolean isStructNull() {
            // The struct is null iff every descendant leaf reports def < structDefLevel at this row position.
            for (ColumnReader leaf : structLeaves) {
                if (!leaf.hasNext()) {
                    return false; // exhausted readers will trip the LeafValueBuilder; conservative.
                }
                if (leaf.currentDefinitionLevel() >= structDefLevel) {
                    return false;
                }
            }
            return true;
        }
    }

    // --- LIST or legacy repeated group ---

    /**
     * Reads one list value for either a {@code LIST}-annotated group or a legacy repeated primitive / group. Drives
     * iteration via a "drive leaf" - any deep descendant leaf whose rep / def levels follow the list. While the
     * driver's repetition level is at or above {@code repLevel}, the builder pulls one element per advance, recursing
     * into the element builder.
     *
     * <p>Null vs empty list semantics: returns {@code null} when the optional list group itself was null in the row
     * ({@code driveLeaf.currentDefinitionLevel() < listDefLevel}). Returns {@link List#of()} when the list is non-null
     * but empty ({@code driveLeaf.currentDefinitionLevel() == listDefLevel}). Returns a populated {@link ArrayList}
     * otherwise.
     */
    final class ListValueBuilder implements ValueBuilder {

        private final ValueBuilder element;
        private final ColumnReader driveLeaf;
        private final List<ColumnReader> elementLeaves;
        private final int repLevel;
        private final int listDefLevel;

        ListValueBuilder(
                int repLevel,
                int listDefLevel,
                ValueBuilder element,
                ColumnReader driveLeaf,
                List<ColumnReader> elementLeaves) {
            this.repLevel = repLevel;
            this.listDefLevel = listDefLevel;
            this.element = element;
            this.driveLeaf = driveLeaf;
            this.elementLeaves = List.copyOf(elementLeaves);
        }

        @Override
        public Object read() {
            if (!driveLeaf.hasNext()) {
                throw new IllegalStateException(
                        "Drive leaf " + driveLeaf.columnPath().dot() + " exhausted while assembler still has work");
            }
            int currentDef = driveLeaf.currentDefinitionLevel();
            if (currentDef < listDefLevel) {
                // List itself is null; consume the placeholder triple from every leaf in the element.
                consumeNullPlaceholder();
                return null;
            }
            if (currentDef == listDefLevel) {
                // Non-null empty list; consume the placeholder.
                consumeNullPlaceholder();
                return List.of();
            }
            // List has at least one element.
            List<Object> items = new ArrayList<>();
            items.add(element.read());
            while (driveLeaf.hasNext() && driveLeaf.currentRepetitionLevel() >= repLevel) {
                items.add(element.read());
            }
            return items;
        }

        /**
         * Advances every leaf in the list's element subtree once past the current null / empty placeholder triple. For
         * a null or empty list the encoder emits a single triple per leaf at the appropriate definition level with no
         * value payload; we consume that single triple to keep the readers aligned with the next row.
         */
        private void consumeNullPlaceholder() {
            for (ColumnReader leaf : elementLeaves) {
                if (leaf.hasNext()) {
                    leaf.consume();
                }
            }
        }
    }

    // --- MAP ---

    /**
     * Reads one map value for a {@code MAP}-annotated group. Implementation is a {@code ListValueBuilder} variant whose
     * element is a two-child struct (key, value); the {@code (key, value)} pairs are folded into a
     * {@link LinkedHashMap} so iteration order matches the file's order, mirroring what {@code parquet-avro} and
     * {@code parquet-cpp} surface.
     */
    final class MapValueBuilder implements ValueBuilder {

        private final ValueBuilder keyBuilder;
        private final ValueBuilder valueBuilder;
        private final ColumnReader driveLeaf;
        private final List<ColumnReader> entryLeaves;
        private final int repLevel;
        private final int mapDefLevel;

        MapValueBuilder(
                int repLevel,
                int mapDefLevel,
                ValueBuilder keyBuilder,
                ValueBuilder valueBuilder,
                ColumnReader driveLeaf,
                List<ColumnReader> entryLeaves) {
            this.repLevel = repLevel;
            this.mapDefLevel = mapDefLevel;
            this.keyBuilder = keyBuilder;
            this.valueBuilder = valueBuilder;
            this.driveLeaf = driveLeaf;
            this.entryLeaves = List.copyOf(entryLeaves);
        }

        @Override
        public Object read() {
            if (!driveLeaf.hasNext()) {
                throw new IllegalStateException(
                        "Drive leaf " + driveLeaf.columnPath().dot() + " exhausted while assembler still has work");
            }
            int currentDef = driveLeaf.currentDefinitionLevel();
            if (currentDef < mapDefLevel) {
                consumeNullPlaceholder();
                return null;
            }
            if (currentDef == mapDefLevel) {
                consumeNullPlaceholder();
                return Map.of();
            }
            Map<Object, Object> entries = new LinkedHashMap<>();
            putOneEntry(entries);
            while (driveLeaf.hasNext() && driveLeaf.currentRepetitionLevel() >= repLevel) {
                putOneEntry(entries);
            }
            return entries;
        }

        private void putOneEntry(Map<Object, Object> entries) {
            Object key = (keyBuilder != null) ? keyBuilder.read() : null;
            Object value = (valueBuilder != null) ? valueBuilder.read() : null;
            entries.put(key, value);
        }

        private void consumeNullPlaceholder() {
            for (ColumnReader leaf : entryLeaves) {
                if (leaf.hasNext()) {
                    leaf.consume();
                }
            }
        }
    }

    /** Pairs a child builder with the {@link ColumnPath} it materializes into in the parent struct's row accessor. */
    record ChildBinding(ColumnPath path, ValueBuilder builder) {}
}
