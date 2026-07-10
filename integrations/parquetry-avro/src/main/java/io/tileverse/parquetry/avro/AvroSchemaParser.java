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
package io.tileverse.parquetry.avro;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Turns the generic JSON tree from {@link JsonReader} into an {@link AvroSchema}. Named types resolve by full name with
 * namespace inheritance per the Avro specification; recursive references become late-bound {@link AvroSchema.Ref}
 * nodes, and because binding happens after the whole document is parsed, forward references are accepted as a
 * documented leniency (the spec requires definition before use). Field defaults are kept as the raw JSON tree, aliases
 * are resolved against the enclosing namespace, and logical-type annotations are parsed with spec-invalid ones ignored.
 * Malformed schemas raise {@link AvroFormatException}.
 */
final class AvroSchemaParser {

    private static final Set<String> FIELD_NON_ATTRIBUTE_KEYS =
            Set.of("name", "type", "doc", "default", "aliases", "order");
    private static final Pattern NAME_SEGMENT = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private final Map<String, AvroSchema> namedTypes = new HashMap<>();
    private final Map<String, AvroSchema.Ref> pendingRefs = new HashMap<>();

    private AvroSchemaParser() {}

    static AvroSchema parse(String json) {
        AvroSchemaParser parser = new AvroSchemaParser();
        AvroSchema root = parser.toSchema(JsonReader.parse(json), null);
        parser.bindPendingRefs();
        return root;
    }

    private void bindPendingRefs() {
        for (Map.Entry<String, AvroSchema.Ref> pending : pendingRefs.entrySet()) {
            AvroSchema target = namedTypes.get(pending.getKey());
            if (target == null) {
                throw new AvroFormatException("Unknown Avro type or unresolved reference: " + pending.getKey());
            }
            pending.getValue().bind(target);
        }
    }

    @SuppressWarnings("unchecked")
    private AvroSchema toSchema(Object node, String enclosingNamespace) {
        return switch (node) {
            case String name -> primitiveOrReference(name, enclosingNamespace);
            case List<?> branches -> union((List<Object>) branches, enclosingNamespace);
            case Map<?, ?> object -> complex((Map<String, Object>) object, enclosingNamespace);
            case null -> throw new AvroFormatException("Null schema node");
            default -> throw new AvroFormatException("Unexpected schema node: " + node.getClass());
        };
    }

    private AvroSchema primitiveOrReference(String name, String enclosingNamespace) {
        AvroSchema primitive = primitive(name);
        if (primitive != null) {
            return primitive;
        }
        String fullName = qualify(name, enclosingNamespace);
        AvroSchema known = namedTypes.get(fullName);
        if (known == null && !name.contains(".")) {
            // Unqualified names may also refer to a type defined with no namespace.
            known = namedTypes.get(name);
            if (known != null) {
                fullName = name;
            }
        }
        if (known != null) {
            return known;
        }
        // Possibly a recursive (or forward) reference to a type still being defined; bind after the parse.
        return pendingRefs.computeIfAbsent(fullName, AvroSchema.Ref::new);
    }

    private String qualify(String name, String enclosingNamespace) {
        if (name.contains(".") || enclosingNamespace == null) {
            return name;
        }
        return enclosingNamespace + "." + name;
    }

    private AvroSchema primitive(String name) {
        return switch (name) {
            case "null" -> AvroSchema.NULL;
            case "boolean" -> AvroSchema.BOOLEAN;
            case "int" -> AvroSchema.INT;
            case "long" -> AvroSchema.LONG;
            case "float" -> AvroSchema.FLOAT;
            case "double" -> AvroSchema.DOUBLE;
            case "string" -> AvroSchema.STRING;
            case "bytes" -> AvroSchema.BYTES;
            default -> null;
        };
    }

    private AvroSchema union(List<Object> branchNodes, String enclosingNamespace) {
        if (branchNodes.isEmpty()) {
            throw new AvroFormatException("Empty union");
        }
        List<AvroSchema> branches = new ArrayList<>(branchNodes.size());
        for (Object branch : branchNodes) {
            branches.add(toSchema(branch, enclosingNamespace));
        }
        return new AvroSchema.Union(branches);
    }

    private AvroSchema complex(Map<String, Object> object, String enclosingNamespace) {
        String type = requireString(object, "type");
        return switch (type) {
            case "record", "error" -> parseRecord(object, enclosingNamespace);
            case "array" -> new AvroSchema.Array(toSchema(requireNode(object, "items"), enclosingNamespace));
            case "map" -> new AvroSchema.Map(toSchema(requireNode(object, "values"), enclosingNamespace));
            case "fixed" -> parseFixed(object, enclosingNamespace);
            case "enum" -> parseEnum(object, enclosingNamespace);
            default -> annotatedPrimitive(type, object);
        };
    }

    /** A primitive declared as an object, possibly annotated: {@code {"type":"long","logicalType":...}}. */
    private AvroSchema annotatedPrimitive(String type, Map<String, Object> object) {
        AvroSchema primitive = primitive(type);
        if (primitive == null) {
            throw new AvroFormatException("Unsupported Avro type object: " + type);
        }
        LogicalType logical = logicalType(object, primitive.type(), -1);
        if (logical == null) {
            return primitive;
        }
        return new AvroSchema.Primitive(primitive.type(), logical);
    }

    /**
     * Parses a logicalType annotation against the underlying type. Returns null when the annotation is absent or
     * spec-invalid for the underlying type (the spec requires invalid annotations to be ignored), and
     * {@link LogicalType.Unknown} when the name is one this reader does not interpret. {@code fixedSize} is -1 unless
     * the underlying type is fixed.
     */
    private LogicalType logicalType(Map<String, Object> object, AvroSchema.Type underlying, int fixedSize) {
        Object nameNode = object.get("logicalType");
        if (!(nameNode instanceof String name)) {
            return null;
        }
        LogicalType candidate =
                switch (name) {
                    case "decimal" -> decimal(object);
                    case "uuid" -> LogicalType.Uuid.INSTANCE;
                    case "date" -> LogicalType.Date.INSTANCE;
                    case "time-millis" -> LogicalType.TimeMillis.INSTANCE;
                    case "time-micros" -> LogicalType.TimeMicros.INSTANCE;
                    case "timestamp-millis" -> LogicalType.TimestampMillis.INSTANCE;
                    case "timestamp-micros" -> LogicalType.TimestampMicros.INSTANCE;
                    case "local-timestamp-millis" -> LogicalType.LocalTimestampMillis.INSTANCE;
                    case "local-timestamp-micros" -> LogicalType.LocalTimestampMicros.INSTANCE;
                    case "duration" -> LogicalType.Duration.INSTANCE;
                    default -> new LogicalType.Unknown(name);
                };
        if (candidate == null) {
            // Malformed decimal attributes: ignore the annotation.
            return null;
        }
        return appliesTo(candidate, underlying, fixedSize) ? candidate : null;
    }

    /**
     * Null when precision/scale are missing, non-integral, or out of range (the annotation is then ignored per the
     * spec). JsonReader yields Long for JSON integers; a Double would truncate silently.
     */
    private LogicalType decimal(Map<String, Object> object) {
        if (!(object.get("precision") instanceof Long precision)) {
            return null;
        }
        Object scaleNode = object.get("scale");
        if (scaleNode != null && !(scaleNode instanceof Long)) {
            return null;
        }
        int scale = scaleNode instanceof Long scaleValue ? scaleValue.intValue() : 0;
        try {
            return new LogicalType.Decimal(precision.intValue(), scale);
        } catch (IllegalArgumentException _) {
            return null;
        }
    }

    private boolean appliesTo(LogicalType logical, AvroSchema.Type underlying, int fixedSize) {
        return switch (logical) {
            case LogicalType.Decimal _ -> underlying == AvroSchema.Type.BYTES || underlying == AvroSchema.Type.FIXED;
            case LogicalType.Uuid _ ->
                underlying == AvroSchema.Type.STRING || (underlying == AvroSchema.Type.FIXED && fixedSize == 16);
            case LogicalType.Date _, LogicalType.TimeMillis _ -> underlying == AvroSchema.Type.INT;
            case LogicalType.TimeMicros _,
                    LogicalType.TimestampMillis _,
                    LogicalType.TimestampMicros _,
                    LogicalType.LocalTimestampMillis _,
                    LogicalType.LocalTimestampMicros _ -> underlying == AvroSchema.Type.LONG;
            case LogicalType.Duration _ -> underlying == AvroSchema.Type.FIXED && fixedSize == 12;
            // Names this reader does not interpret are preserved verbatim on any underlying type.
            case LogicalType.Unknown _ -> true;
        };
    }

    private AvroSchema parseRecord(Map<String, Object> object, String enclosingNamespace) {
        NamedTypeName typeName = namedTypeName(object, enclosingNamespace);
        List<Object> fieldNodes = requireList(object, "fields");
        List<AvroSchema.Field> fields = new ArrayList<>(fieldNodes.size());
        for (int position = 0; position < fieldNodes.size(); position++) {
            Object fieldNode = fieldNodes.get(position);
            if (!(fieldNode instanceof Map<?, ?> fieldObject)) {
                throw new AvroFormatException("Avro record field is not an object: " + fieldNode);
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> typedFieldObject = (Map<String, Object>) fieldObject;
            fields.add(field(typedFieldObject, position, typeName.namespace()));
        }
        AvroSchema.Record recordSchema =
                new AvroSchema.Record(typeName.name(), typeName.namespace(), typeName.aliases(), fields);
        return register(recordSchema.fullName(), recordSchema);
    }

    private AvroSchema parseFixed(Map<String, Object> object, String enclosingNamespace) {
        NamedTypeName typeName = namedTypeName(object, enclosingNamespace);
        int size = requireInt(object, "size");
        if (size < 0) {
            throw new AvroFormatException("Fixed size must be non-negative: " + size);
        }
        LogicalType logical = logicalType(object, AvroSchema.Type.FIXED, size);
        AvroSchema.Fixed fixed =
                new AvroSchema.Fixed(typeName.name(), typeName.namespace(), typeName.aliases(), size, logical);
        return register(fixed.fullName(), fixed);
    }

    private AvroSchema parseEnum(Map<String, Object> object, String enclosingNamespace) {
        NamedTypeName typeName = namedTypeName(object, enclosingNamespace);
        List<String> symbols = symbols(object);
        String defaultSymbol = object.get("default") instanceof String symbol ? symbol : null;
        if (defaultSymbol != null && !symbols.contains(defaultSymbol)) {
            throw new AvroFormatException("Enum default is not a symbol: " + defaultSymbol);
        }
        AvroSchema.Enum anEnum =
                new AvroSchema.Enum(typeName.name(), typeName.namespace(), typeName.aliases(), symbols, defaultSymbol);
        return register(anEnum.fullName(), anEnum);
    }

    private record NamedTypeName(String name, String namespace, List<String> aliases) {}

    /** Splits name/namespace per the spec (a dotted name wins over the namespace attribute), resolves aliases. */
    private NamedTypeName namedTypeName(Map<String, Object> object, String enclosingNamespace) {
        String rawName = requireString(object, "name");
        String namespace;
        String name;
        int lastDot = rawName.lastIndexOf('.');
        if (lastDot >= 0) {
            namespace = rawName.substring(0, lastDot);
            name = rawName.substring(lastDot + 1);
        } else {
            name = rawName;
            namespace = object.get("namespace") instanceof String ns ? ns : enclosingNamespace;
        }
        if (namespace != null && namespace.isEmpty()) {
            namespace = null;
        }
        validateName(name);
        if (namespace != null) {
            for (String segment : namespace.split("\\.", -1)) {
                validateName(segment);
            }
        }
        List<String> aliases = new ArrayList<>();
        if (object.get("aliases") instanceof List<?> rawAliases) {
            for (Object alias : rawAliases) {
                if (!(alias instanceof String aliasName)) {
                    throw new AvroFormatException("Alias is not a string: " + alias);
                }
                aliases.add(qualify(aliasName, namespace));
            }
        }
        return new NamedTypeName(name, namespace, List.copyOf(aliases));
    }

    private void validateName(String name) {
        if (!NAME_SEGMENT.matcher(name).matches()) {
            throw new AvroFormatException("Invalid Avro name: " + name);
        }
    }

    private List<String> symbols(Map<String, Object> object) {
        List<Object> raw = requireList(object, "symbols");
        List<String> symbols = new ArrayList<>(raw.size());
        for (Object symbol : raw) {
            if (!(symbol instanceof String string)) {
                throw new AvroFormatException("Avro enum symbol is not a string: " + symbol);
            }
            validateName(string);
            symbols.add(string);
        }
        return symbols;
    }

    private AvroSchema.Field field(Map<String, Object> fieldNode, int position, String enclosingNamespace) {
        String name = requireString(fieldNode, "name");
        AvroSchema fieldSchema = toSchema(requireNode(fieldNode, "type"), enclosingNamespace);
        // containsKey distinguishes an explicit JSON-null default from no default at all.
        boolean hasDefault = fieldNode.containsKey("default");
        Object defaultValue = fieldNode.get("default");
        List<String> aliases = new ArrayList<>();
        if (fieldNode.get("aliases") instanceof List<?> rawAliases) {
            for (Object alias : rawAliases) {
                if (!(alias instanceof String aliasName)) {
                    throw new AvroFormatException("Alias is not a string: " + alias);
                }
                aliases.add(aliasName);
            }
        }
        Map<String, Object> attributes = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : fieldNode.entrySet()) {
            // JSON-null values are skipped: Field's attribute map rejects nulls, and through attribute()'s
            // Optional a null-valued attribute is indistinguishable from an absent one.
            if (!FIELD_NON_ATTRIBUTE_KEYS.contains(entry.getKey()) && entry.getValue() != null) {
                attributes.put(entry.getKey(), entry.getValue());
            }
        }
        return new AvroSchema.Field(name, position, fieldSchema, defaultValue, hasDefault, aliases, attributes);
    }

    private AvroSchema register(String fullName, AvroSchema named) {
        if (namedTypes.putIfAbsent(fullName, named) != null) {
            throw new AvroFormatException("Duplicate named type: " + fullName);
        }
        return named;
    }

    private static Object requireNode(Map<String, Object> object, String key) {
        Object value = object.get(key);
        if (value == null) {
            throw new AvroFormatException("Avro schema object missing \"" + key + "\": " + object);
        }
        return value;
    }

    private static String requireString(Map<String, Object> object, String key) {
        Object value = object.get(key);
        if (!(value instanceof String string)) {
            throw new AvroFormatException("Avro schema object missing string \"" + key + "\": " + object);
        }
        return string;
    }

    private static List<Object> requireList(Map<String, Object> object, String key) {
        Object value = object.get(key);
        if (!(value instanceof List<?> list)) {
            throw new AvroFormatException("Avro schema object missing array \"" + key + "\": " + object);
        }
        @SuppressWarnings("unchecked")
        List<Object> typed = (List<Object>) list;
        return typed;
    }

    /** JsonReader yields Long for JSON integers; anything else (including a Double) is not a valid integer here. */
    private static int requireInt(Map<String, Object> object, String key) {
        Object value = object.get(key);
        if (!(value instanceof Long number)) {
            throw new AvroFormatException("Avro schema object missing integer \"" + key + "\": " + object);
        }
        return number.intValue();
    }
}
