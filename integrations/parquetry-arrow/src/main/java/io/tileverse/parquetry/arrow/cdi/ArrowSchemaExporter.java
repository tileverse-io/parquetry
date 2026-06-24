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

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import io.tileverse.parquetry.arrow.ipc.ArrowField;

/**
 * Exports an {@link ArrowField} type tree into an {@code ArrowSchema} C struct over the C Data Interface. The format
 * string, name, flags, extension metadata, and recursive children are written into segments allocated from the caller's
 * {@link Arena} (a stream-lifetime arena). A single release upcall stub, shared across the tree, marks a struct
 * released by nulling its release pointer; the arena owns the memory and frees it when it closes.
 *
 * @see <a href="https://arrow.apache.org/docs/format/CDataInterface.html">Arrow C Data Interface</a>
 */
// The release callback is bound as an FFM upcall stub by name (java:S1144 cannot trace the reflective binding) and
// must catch Throwable because an exception escaping an upcall stub crashes the JVM (java:S1181).
@SuppressWarnings({"java:S1144", "java:S1181"})
final class ArrowSchemaExporter {

    private static final System.Logger LOGGER = System.getLogger(ArrowSchemaExporter.class.getName());

    /** ARROW_FLAG_NULLABLE from the C Data Interface (bit 1). */
    private static final long FLAG_NULLABLE = 2L;

    private static final MemorySegment RELEASE_SCHEMA = releaseStub();

    private ArrowSchemaExporter() {
        // utility
    }

    /** Exports {@code field} and its children into a freshly allocated {@code ArrowSchema} segment in {@code arena}. */
    static MemorySegment export(ArrowField field, Arena arena) {
        MemorySegment schema = arena.allocate(CDataLayouts.ARROW_SCHEMA);
        exportInto(schema, field, arena);
        return schema;
    }

    /** Exports {@code field} into the caller-provided {@code out} struct, allocating its children in {@code arena}. */
    static void exportInto(MemorySegment out, ArrowField field, Arena arena) {
        fillNode(out, field, arena);
    }

    private static MemorySegment exportNode(ArrowField field, Arena arena) {
        MemorySegment schema = arena.allocate(CDataLayouts.ARROW_SCHEMA);
        fillNode(schema, field, arena);
        return schema;
    }

    private static void fillNode(MemorySegment schema, ArrowField field, Arena arena) {
        CDataLayouts.schemaSetFormat(schema, arena.allocateFrom(formatString(field)));
        CDataLayouts.schemaSetName(schema, arena.allocateFrom(field.name()));
        CDataLayouts.schemaSetMetadata(schema, encodeMetadata(field.extensionMetadata(), arena));
        CDataLayouts.schemaSetFlags(schema, field.nullable() ? FLAG_NULLABLE : 0L);
        exportChildren(schema, field, arena);
        CDataLayouts.schemaSetDictionary(schema, MemorySegment.NULL);
        CDataLayouts.schemaSetRelease(schema, RELEASE_SCHEMA);
        CDataLayouts.schemaSetPrivateData(schema, MemorySegment.NULL);
    }

    private static void exportChildren(MemorySegment schema, ArrowField field, Arena arena) {
        List<ArrowField> children = field.children();
        if (children.isEmpty()) {
            CDataLayouts.schemaSetChildren(schema, 0L, MemorySegment.NULL);
            return;
        }
        MemorySegment childPointers = arena.allocate(ValueLayout.ADDRESS, children.size());
        for (int i = 0; i < children.size(); i++) {
            MemorySegment child = exportNode(children.get(i), arena);
            childPointers.setAtIndex(ValueLayout.ADDRESS, i, child);
        }
        CDataLayouts.schemaSetChildren(schema, children.size(), childPointers);
    }

    private static String formatString(ArrowField field) {
        return switch (field.kind()) {
            case PRIMITIVE -> ArrowFormat.of(field.leaf());
            case LIST -> "+l";
            case STRUCT, VARIANT -> "+s";
            case MAP -> "+m";
        };
    }

    /**
     * Encodes Arrow field metadata in the C Data Interface binary layout: an {@code int32} pair count, then for each
     * pair an {@code int32} key length, the key bytes, an {@code int32} value length, and the value bytes, all
     * native-endian. Returns {@link MemorySegment#NULL} when there is no metadata.
     */
    private static MemorySegment encodeMetadata(Map<String, String> metadata, Arena arena) {
        if (metadata.isEmpty()) {
            return MemorySegment.NULL;
        }
        long size = Integer.BYTES;
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            size += Integer.BYTES + utf8Length(entry.getKey());
            size += Integer.BYTES + utf8Length(entry.getValue());
        }
        MemorySegment encoded = arena.allocate(size);
        encoded.set(ValueLayout.JAVA_INT, 0L, metadata.size());
        long cursor = Integer.BYTES;
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            cursor = writeLengthPrefixed(encoded, cursor, entry.getKey());
            cursor = writeLengthPrefixed(encoded, cursor, entry.getValue());
        }
        return encoded;
    }

    private static long writeLengthPrefixed(MemorySegment target, long cursor, String text) {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        target.set(ValueLayout.JAVA_INT, cursor, bytes.length);
        long next = cursor + Integer.BYTES;
        MemorySegment.copy(MemorySegment.ofArray(bytes), 0L, target, next, bytes.length);
        return next + bytes.length;
    }

    private static int utf8Length(String text) {
        return text.getBytes(StandardCharsets.UTF_8).length;
    }

    /**
     * Marks the schema released by nulling its release pointer. Wrapped in a catch-all because an exception escaping an
     * FFM upcall stub crashes the JVM; the consumer only needs the pointer nulled, which happens before anything that
     * could throw.
     */
    private static void releaseSchema(MemorySegment self) {
        try {
            MemorySegment schema = self.reinterpret(CDataLayouts.ARROW_SCHEMA.byteSize());
            CDataLayouts.schemaSetRelease(schema, MemorySegment.NULL);
        } catch (Throwable t) {
            LOGGER.log(System.Logger.Level.WARNING, "ArrowSchema release callback failed", t);
        }
    }

    private static MemorySegment releaseStub() {
        return CDataStubs.upcall(
                MethodHandles.lookup(),
                "releaseSchema",
                MethodType.methodType(void.class, MemorySegment.class),
                FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
    }
}
