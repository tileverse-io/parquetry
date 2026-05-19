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
package io.tileverse.parquetry.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import io.tileverse.parquetry.format.FieldRepetitionType;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.format.SchemaElement;

/**
 * Converts the depth-first flat {@code List<SchemaElement>} from
 * {@link io.tileverse.parquetry.format.FileMetaData#schema()} into the nested {@link Schema} tree.
 *
 * <p>The Parquet Thrift wire format stores the schema as a pre-order traversal of the tree: the first element is always
 * the root group, followed by its children recursively. Group elements carry {@code numChildren > 0}; primitive (leaf)
 * elements have {@code type} present and no {@code numChildren}.
 *
 * <p>A single mutable {@link Cursor} tracks the current position in the list and is threaded through recursive calls.
 * Each call to {@link #consumeNext} advances the cursor by one (the current element) plus however many descendants it
 * owns.
 */
public final class SchemaBuilder {

    private SchemaBuilder() {}

    /**
     * Builds a {@link Schema} from the depth-first flattened schema element list.
     *
     * @param elements the flat list from {@link io.tileverse.parquetry.format.FileMetaData#schema()}
     * @return the reconstructed schema tree
     * @throws IllegalArgumentException if {@code elements} is empty or the root is not a group
     */
    public static Schema build(List<SchemaElement> elements) {
        if (elements.isEmpty()) {
            throw new IllegalArgumentException("Schema elements list is empty");
        }
        Cursor cursor = new Cursor(elements, 0);
        Field rootField = consumeNext(cursor);
        if (!(rootField instanceof Field.Group rootGroup)) {
            throw new IllegalArgumentException(
                    "Schema root must be a group, got: " + rootField.getClass().getSimpleName());
        }
        return new Schema(rootGroup);
    }

    /**
     * Consumes the next element (and all its descendants) from {@code cursor}, returning the corresponding
     * {@link Field}.
     *
     * <p>After this method returns, {@code cursor.position} points to the element immediately after the subtree rooted
     * at the consumed element.
     */
    private static Field consumeNext(Cursor cursor) {
        SchemaElement element = cursor.elements.get(cursor.position);
        cursor.position++;

        Repetition repetition = mapRepetition(element.repetitionType());
        int fieldId = element.fieldId().orElse(-1);

        if (element.numChildren().isPresent()) {
            return consumeGroup(cursor, element, repetition, fieldId);
        }
        return consumePrimitive(element, repetition, fieldId);
    }

    private static Field.Group consumeGroup(Cursor cursor, SchemaElement element, Repetition repetition, int fieldId) {
        int childCount = element.numChildren().getAsInt();
        List<Field> children = new ArrayList<>(childCount);
        for (int i = 0; i < childCount; i++) {
            children.add(consumeNext(cursor));
        }
        return new Field.Group(element.name(), repetition, children, element.logicalType(), fieldId);
    }

    private static Field.Primitive consumePrimitive(SchemaElement element, Repetition repetition, int fieldId) {
        PhysicalType type = element.type()
                .orElseThrow(() -> new IllegalStateException("Leaf SchemaElement missing type: " + element.name()));
        PrimitiveKind kind = mapKind(type);
        OptionalInt typeLength = element.typeLength();
        return new Field.Primitive(element.name(), repetition, kind, typeLength, element.logicalType(), fieldId);
    }

    private static Repetition mapRepetition(Optional<FieldRepetitionType> rep) {
        // Thrift spec: missing repetition type defaults to REQUIRED
        return switch (rep.orElse(FieldRepetitionType.REQUIRED)) {
            case REQUIRED -> Repetition.REQUIRED;
            case OPTIONAL -> Repetition.OPTIONAL;
            case REPEATED -> Repetition.REPEATED;
        };
    }

    private static PrimitiveKind mapKind(PhysicalType type) {
        return switch (type) {
            case BOOLEAN -> PrimitiveKind.BOOLEAN;
            case INT32 -> PrimitiveKind.INT32;
            case INT64 -> PrimitiveKind.INT64;
            case INT96 -> PrimitiveKind.INT96;
            case FLOAT -> PrimitiveKind.FLOAT;
            case DOUBLE -> PrimitiveKind.DOUBLE;
            case BYTE_ARRAY -> PrimitiveKind.BYTE_ARRAY;
            case FIXED_LEN_BYTE_ARRAY -> PrimitiveKind.FIXED_LEN_BYTE_ARRAY;
        };
    }

    /**
     * Mutable position pointer threaded through recursive calls to track where we are in the flat schema element list.
     * Using a small wrapper object is cleaner than returning index values from each recursive call.
     */
    private static final class Cursor {
        final List<SchemaElement> elements;
        int position;

        Cursor(List<SchemaElement> elements, int position) {
            this.elements = elements;
            this.position = position;
        }
    }
}
