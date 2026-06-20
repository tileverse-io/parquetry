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
package io.tileverse.parquetry.avro;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * A parsed Avro schema as a sealed algebraic data type. By default decode is against the file's embedded write schema;
 * {@link AvroDataFileReader#records(AvroSchema)} resolves it against a reader schema instead. The decoder interprets
 * logical-type annotations; the annotation itself is exposed through {@link #logicalType()}.
 */
public sealed interface AvroSchema
        permits AvroSchema.Primitive,
                AvroSchema.Record,
                AvroSchema.Array,
                AvroSchema.Map,
                AvroSchema.Union,
                AvroSchema.Fixed,
                AvroSchema.Enum,
                AvroSchema.Ref {

    enum Type {
        NULL,
        BOOLEAN,
        INT,
        LONG,
        FLOAT,
        DOUBLE,
        STRING,
        BYTES,
        RECORD,
        ARRAY,
        MAP,
        UNION,
        FIXED,
        ENUM
    }

    Type type();

    /** A schema node with an Avro name: record, fixed, or enum. {@code namespace()} is nullable. */
    sealed interface Named permits Record, Fixed, Enum {
        String name();

        String namespace();

        List<String> aliases();

        default String fullName() {
            return namespace() == null ? name() : namespace() + "." + name();
        }
    }

    /** The logical-type annotation on this node, when one is present. */
    default Optional<LogicalType> logicalType() {
        return Optional.empty();
    }

    /** Parses an Avro schema JSON document. */
    static AvroSchema parse(String json) {
        return AvroSchemaParser.parse(json);
    }

    AvroSchema NULL = new Primitive(Type.NULL, null);
    AvroSchema BOOLEAN = new Primitive(Type.BOOLEAN, null);
    AvroSchema INT = new Primitive(Type.INT, null);
    AvroSchema LONG = new Primitive(Type.LONG, null);
    AvroSchema FLOAT = new Primitive(Type.FLOAT, null);
    AvroSchema DOUBLE = new Primitive(Type.DOUBLE, null);
    AvroSchema STRING = new Primitive(Type.STRING, null);
    AvroSchema BYTES = new Primitive(Type.BYTES, null);

    /** One of the scalar Avro types that has no further structure; {@code logical} is nullable. */
    record Primitive(Type type, LogicalType logical) implements AvroSchema {
        @Override
        public Optional<LogicalType> logicalType() {
            return Optional.ofNullable(logical);
        }
    }

    /** A record: an ordered list of named fields. {@code namespace} is nullable. */
    record Record(String name, String namespace, List<String> aliases, List<Field> fields)
            implements AvroSchema, Named {
        public Record {
            aliases = List.copyOf(aliases);
            fields = List.copyOf(fields);
        }

        @Override
        public Type type() {
            return Type.RECORD;
        }

        public Optional<Field> field(String fieldName) {
            for (Field field : fields) {
                if (field.name().equals(fieldName)) {
                    return Optional.of(field);
                }
            }
            return Optional.empty();
        }
    }

    /**
     * A field within a record. {@code attributes} keeps the field's custom JSON attributes verbatim (for example
     * Iceberg's {@code field-id}) as the generic JSON tree, for the caller to interpret; a JSON-null attribute is
     * treated as absent. {@code defaultValue} is the raw JSON default tree and is meaningful only when
     * {@code hasDefault} is true; a JSON null default is Java null. Immutability is shallow: nested List/Map attribute
     * values and {@code defaultValue} are the raw parse tree, not deep-copied; callers must treat them as read-only.
     */
    record Field(
            String name,
            int position,
            AvroSchema schema,
            Object defaultValue,
            boolean hasDefault,
            List<String> aliases,
            java.util.Map<String, Object> attributes) {
        public Field {
            aliases = List.copyOf(aliases);
            attributes = java.util.Map.copyOf(attributes);
        }

        /** A field with no default, no aliases, and no custom attributes. */
        public static Field of(String name, int position, AvroSchema schema) {
            return new Field(name, position, schema, null, false, List.of(), java.util.Map.of());
        }

        public Optional<Object> attribute(String key) {
            return Optional.ofNullable(attributes.get(key));
        }

        public OptionalInt fieldId() {
            if (attributes.get("field-id") instanceof Number number) {
                return OptionalInt.of(number.intValue());
            }
            return OptionalInt.empty();
        }
    }

    /** An array of a single element type. */
    record Array(AvroSchema element) implements AvroSchema {
        @Override
        public Type type() {
            return Type.ARRAY;
        }
    }

    /** A map of string keys to a single value type. */
    record Map(AvroSchema values) implements AvroSchema {
        @Override
        public Type type() {
            return Type.MAP;
        }
    }

    /** A union of branch schemas; the common shape is {@code [null, T]}. Never empty. */
    record Union(List<AvroSchema> branches) implements AvroSchema {
        public Union {
            if (branches.isEmpty()) {
                throw new IllegalArgumentException("Union must have at least one branch");
            }
            branches = List.copyOf(branches);
        }

        @Override
        public Type type() {
            return Type.UNION;
        }
    }

    /** A fixed-length byte sequence of {@code size} bytes. {@code namespace} and {@code logical} are nullable. */
    record Fixed(String name, String namespace, List<String> aliases, int size, LogicalType logical)
            implements AvroSchema, Named {
        public Fixed {
            aliases = List.copyOf(aliases);
        }

        @Override
        public Type type() {
            return Type.FIXED;
        }

        @Override
        public Optional<LogicalType> logicalType() {
            return Optional.ofNullable(logical);
        }
    }

    /**
     * An enumeration of ordered symbols; encoded as an int index. {@code namespace} is nullable; {@code defaultSymbol}
     * is nullable and backs schema resolution.
     */
    record Enum(String name, String namespace, List<String> aliases, List<String> symbols, String defaultSymbol)
            implements AvroSchema, Named {
        public Enum {
            aliases = List.copyOf(aliases);
            symbols = List.copyOf(symbols);
        }

        @Override
        public Type type() {
            return Type.ENUM;
        }
    }

    /**
     * A by-name reference to a named type, late-bound by the parser. Recursive schemas (trees, linked structures) are
     * cycles through Ref nodes; everything else in the model stays immutable. The parser binds every Ref before the
     * schema escapes; an unbound Ref read afterwards is a parser bug.
     *
     * <p>Equality is identity, deliberately: identity is what terminates structural equals/hashCode through a cycle;
     * two independent parses of the same recursive schema are never {@code equals()} once a Ref is in the graph.
     */
    final class Ref implements AvroSchema {
        private final String fullName;

        // volatile: the parser binds before the schema escapes, but consumers may publish a parsed schema across
        // threads through a plain field; without it a racing reader could legally observe an unbound Ref.
        private volatile AvroSchema target;

        Ref(String fullName) {
            this.fullName = fullName;
        }

        public String fullName() {
            return fullName;
        }

        public AvroSchema target() {
            if (target == null) {
                throw new IllegalStateException("Unbound schema reference: " + fullName);
            }
            return target;
        }

        void bind(AvroSchema resolved) {
            if (target != null) {
                throw new IllegalStateException("Reference already bound: " + fullName);
            }
            target = resolved;
        }

        @Override
        public Type type() {
            return target().type();
        }

        @Override
        public Optional<LogicalType> logicalType() {
            return target().logicalType();
        }
    }
}
