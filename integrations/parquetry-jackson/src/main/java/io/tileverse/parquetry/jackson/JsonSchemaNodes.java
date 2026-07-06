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
package io.tileverse.parquetry.jackson;

import java.util.Optional;

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Resolves the JSON-relevant shape of a schema group using only public schema API. Mirrors the conventions the core
 * read pipeline applies when it assembles nested vectors, kept here because those core helpers are not public.
 */
final class JsonSchemaNodes {

    enum Kind {
        STRUCT,
        LIST,
        MAP,
        VARIANT
    }

    private JsonSchemaNodes() {}

    /**
     * Classifies a group by its logical type, falling back to repetition: a repeated group with no logical type is a
     * legacy two-level list, and any other annotation-free group is a struct.
     */
    static Kind classify(SchemaNode.Group group) {
        Optional<LogicalType> logicalType = group.logicalType();
        if (logicalType.isPresent()) {
            LogicalType type = logicalType.get();
            if (type instanceof LogicalType.Variant) {
                return Kind.VARIANT;
            }
            if (type instanceof LogicalType.ListType) {
                return Kind.LIST;
            }
            if (type instanceof LogicalType.MapType) {
                return Kind.MAP;
            }
        }
        if (group.repetition() == Repetition.REPEATED) {
            return Kind.LIST;
        }
        return Kind.STRUCT;
    }

    /**
     * The element type of a list. The three-level encoding wraps the element in a repeated group ({@code repeated group
     * list { <element> }}); the legacy two-level encoding puts a repeated primitive directly under the list group,
     * which is itself the element.
     */
    static SchemaNode elementNode(SchemaNode.Group listGroup) {
        SchemaNode repeated = listGroup.children().get(0);
        return switch (repeated) {
            case SchemaNode.Group wrapper -> wrapper.children().get(0);
            case SchemaNode.Primitive element -> element;
        };
    }

    static SchemaNode keyNode(SchemaNode.Group mapGroup) {
        return keyValue(mapGroup).children().get(0);
    }

    static SchemaNode valueNode(SchemaNode.Group mapGroup) {
        return keyValue(mapGroup).children().get(1);
    }

    static boolean isStringLike(Optional<LogicalType> logicalType) {
        return logicalType
                .map(type -> type instanceof LogicalType.StringType
                        || type instanceof LogicalType.EnumType
                        || type instanceof LogicalType.JsonType)
                .orElse(false);
    }

    private static SchemaNode.Group keyValue(SchemaNode.Group mapGroup) {
        return (SchemaNode.Group) mapGroup.children().get(0);
    }
}
