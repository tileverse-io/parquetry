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

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemoryLayout.PathElement;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;

/**
 * FFM mirrors of the three Arrow C Data Interface structs and typed accessors over their fields. Every pointer field is
 * {@link ValueLayout#ADDRESS} and every integer field is {@link ValueLayout#JAVA_LONG}, matching the fixed C ABI on a
 * 64-bit platform. The field order is the order the specification fixes; a consumer compiled against the C headers and
 * a segment written through these accessors agree on every offset.
 *
 * @see <a href="https://arrow.apache.org/docs/format/CDataInterface.html">Arrow C Data Interface</a>
 */
final class CDataLayouts {

    private CDataLayouts() {
        // constants and accessors only
    }

    // Arrow C ABI field names shared by the layout declarations and the matching var-handle lookups, which keeps a
    // field's declared name and the path its accessor reads from in lockstep.
    private static final String N_CHILDREN = "n_children";
    private static final String CHILDREN = "children";
    private static final String DICTIONARY = "dictionary";
    private static final String RELEASE = "release";
    private static final String PRIVATE_DATA = "private_data";

    static final MemoryLayout ARROW_SCHEMA = MemoryLayout.structLayout(
                    ValueLayout.ADDRESS.withName("format"),
                    ValueLayout.ADDRESS.withName("name"),
                    ValueLayout.ADDRESS.withName("metadata"),
                    ValueLayout.JAVA_LONG.withName("flags"),
                    ValueLayout.JAVA_LONG.withName(N_CHILDREN),
                    ValueLayout.ADDRESS.withName(CHILDREN),
                    ValueLayout.ADDRESS.withName(DICTIONARY),
                    ValueLayout.ADDRESS.withName(RELEASE),
                    ValueLayout.ADDRESS.withName(PRIVATE_DATA))
            .withName("ArrowSchema");

    static final MemoryLayout ARROW_ARRAY = MemoryLayout.structLayout(
                    ValueLayout.JAVA_LONG.withName("length"),
                    ValueLayout.JAVA_LONG.withName("null_count"),
                    ValueLayout.JAVA_LONG.withName("offset"),
                    ValueLayout.JAVA_LONG.withName("n_buffers"),
                    ValueLayout.JAVA_LONG.withName(N_CHILDREN),
                    ValueLayout.ADDRESS.withName("buffers"),
                    ValueLayout.ADDRESS.withName(CHILDREN),
                    ValueLayout.ADDRESS.withName(DICTIONARY),
                    ValueLayout.ADDRESS.withName(RELEASE),
                    ValueLayout.ADDRESS.withName(PRIVATE_DATA))
            .withName("ArrowArray");

    static final MemoryLayout ARROW_ARRAY_STREAM = MemoryLayout.structLayout(
                    ValueLayout.ADDRESS.withName("get_schema"),
                    ValueLayout.ADDRESS.withName("get_next"),
                    ValueLayout.ADDRESS.withName("get_last_error"),
                    ValueLayout.ADDRESS.withName(RELEASE),
                    ValueLayout.ADDRESS.withName(PRIVATE_DATA))
            .withName("ArrowArrayStream");

    private static final VarHandle SCHEMA_FORMAT = fieldHandle(ARROW_SCHEMA, "format");
    private static final VarHandle SCHEMA_NAME = fieldHandle(ARROW_SCHEMA, "name");
    private static final VarHandle SCHEMA_METADATA = fieldHandle(ARROW_SCHEMA, "metadata");
    private static final VarHandle SCHEMA_FLAGS = fieldHandle(ARROW_SCHEMA, "flags");
    private static final VarHandle SCHEMA_N_CHILDREN = fieldHandle(ARROW_SCHEMA, N_CHILDREN);
    private static final VarHandle SCHEMA_CHILDREN = fieldHandle(ARROW_SCHEMA, CHILDREN);
    private static final VarHandle SCHEMA_DICTIONARY = fieldHandle(ARROW_SCHEMA, DICTIONARY);
    private static final VarHandle SCHEMA_RELEASE = fieldHandle(ARROW_SCHEMA, RELEASE);
    private static final VarHandle SCHEMA_PRIVATE_DATA = fieldHandle(ARROW_SCHEMA, PRIVATE_DATA);

    private static final VarHandle ARRAY_LENGTH = fieldHandle(ARROW_ARRAY, "length");
    private static final VarHandle ARRAY_NULL_COUNT = fieldHandle(ARROW_ARRAY, "null_count");
    private static final VarHandle ARRAY_OFFSET = fieldHandle(ARROW_ARRAY, "offset");
    private static final VarHandle ARRAY_N_BUFFERS = fieldHandle(ARROW_ARRAY, "n_buffers");
    private static final VarHandle ARRAY_N_CHILDREN = fieldHandle(ARROW_ARRAY, N_CHILDREN);
    private static final VarHandle ARRAY_BUFFERS = fieldHandle(ARROW_ARRAY, "buffers");
    private static final VarHandle ARRAY_CHILDREN = fieldHandle(ARROW_ARRAY, CHILDREN);
    private static final VarHandle ARRAY_DICTIONARY = fieldHandle(ARROW_ARRAY, DICTIONARY);
    private static final VarHandle ARRAY_RELEASE = fieldHandle(ARROW_ARRAY, RELEASE);
    private static final VarHandle ARRAY_PRIVATE_DATA = fieldHandle(ARROW_ARRAY, PRIVATE_DATA);

    private static final VarHandle STREAM_GET_SCHEMA = fieldHandle(ARROW_ARRAY_STREAM, "get_schema");
    private static final VarHandle STREAM_GET_NEXT = fieldHandle(ARROW_ARRAY_STREAM, "get_next");
    private static final VarHandle STREAM_GET_LAST_ERROR = fieldHandle(ARROW_ARRAY_STREAM, "get_last_error");
    private static final VarHandle STREAM_RELEASE = fieldHandle(ARROW_ARRAY_STREAM, RELEASE);
    private static final VarHandle STREAM_PRIVATE_DATA = fieldHandle(ARROW_ARRAY_STREAM, PRIVATE_DATA);

    // ArrowSchema accessors

    static void schemaSetFormat(MemorySegment schema, MemorySegment format) {
        SCHEMA_FORMAT.set(schema, 0L, format);
    }

    static MemorySegment schemaFormat(MemorySegment schema) {
        return (MemorySegment) SCHEMA_FORMAT.get(schema, 0L);
    }

    static void schemaSetName(MemorySegment schema, MemorySegment name) {
        SCHEMA_NAME.set(schema, 0L, name);
    }

    static void schemaSetMetadata(MemorySegment schema, MemorySegment metadata) {
        SCHEMA_METADATA.set(schema, 0L, metadata);
    }

    static void schemaSetFlags(MemorySegment schema, long flags) {
        SCHEMA_FLAGS.set(schema, 0L, flags);
    }

    static void schemaSetChildren(MemorySegment schema, long count, MemorySegment children) {
        SCHEMA_N_CHILDREN.set(schema, 0L, count);
        SCHEMA_CHILDREN.set(schema, 0L, children);
    }

    static void schemaSetDictionary(MemorySegment schema, MemorySegment dictionary) {
        SCHEMA_DICTIONARY.set(schema, 0L, dictionary);
    }

    static void schemaSetRelease(MemorySegment schema, MemorySegment release) {
        SCHEMA_RELEASE.set(schema, 0L, release);
    }

    static MemorySegment schemaRelease(MemorySegment schema) {
        return (MemorySegment) SCHEMA_RELEASE.get(schema, 0L);
    }

    static void schemaSetPrivateData(MemorySegment schema, MemorySegment privateData) {
        SCHEMA_PRIVATE_DATA.set(schema, 0L, privateData);
    }

    static MemorySegment schemaPrivateData(MemorySegment schema) {
        return (MemorySegment) SCHEMA_PRIVATE_DATA.get(schema, 0L);
    }

    // ArrowArray accessors

    static long arrayLength(MemorySegment array) {
        return (long) ARRAY_LENGTH.get(array, 0L);
    }

    static void arraySetLength(MemorySegment array, long length) {
        ARRAY_LENGTH.set(array, 0L, length);
    }

    static void arraySetNullCount(MemorySegment array, long nullCount) {
        ARRAY_NULL_COUNT.set(array, 0L, nullCount);
    }

    static void arraySetOffset(MemorySegment array, long offset) {
        ARRAY_OFFSET.set(array, 0L, offset);
    }

    static void arraySetBuffers(MemorySegment array, long count, MemorySegment buffers) {
        ARRAY_N_BUFFERS.set(array, 0L, count);
        ARRAY_BUFFERS.set(array, 0L, buffers);
    }

    static void arraySetChildren(MemorySegment array, long count, MemorySegment children) {
        ARRAY_N_CHILDREN.set(array, 0L, count);
        ARRAY_CHILDREN.set(array, 0L, children);
    }

    static void arraySetDictionary(MemorySegment array, MemorySegment dictionary) {
        ARRAY_DICTIONARY.set(array, 0L, dictionary);
    }

    static void arraySetRelease(MemorySegment array, MemorySegment release) {
        ARRAY_RELEASE.set(array, 0L, release);
    }

    static MemorySegment arrayRelease(MemorySegment array) {
        return (MemorySegment) ARRAY_RELEASE.get(array, 0L);
    }

    static void arraySetPrivateData(MemorySegment array, MemorySegment privateData) {
        ARRAY_PRIVATE_DATA.set(array, 0L, privateData);
    }

    static MemorySegment arrayPrivateData(MemorySegment array) {
        return (MemorySegment) ARRAY_PRIVATE_DATA.get(array, 0L);
    }

    // ArrowArrayStream accessors

    static void streamSetGetSchema(MemorySegment stream, MemorySegment getSchema) {
        STREAM_GET_SCHEMA.set(stream, 0L, getSchema);
    }

    static void streamSetGetNext(MemorySegment stream, MemorySegment getNext) {
        STREAM_GET_NEXT.set(stream, 0L, getNext);
    }

    static void streamSetGetLastError(MemorySegment stream, MemorySegment getLastError) {
        STREAM_GET_LAST_ERROR.set(stream, 0L, getLastError);
    }

    static void streamSetRelease(MemorySegment stream, MemorySegment release) {
        STREAM_RELEASE.set(stream, 0L, release);
    }

    static MemorySegment streamRelease(MemorySegment stream) {
        return (MemorySegment) STREAM_RELEASE.get(stream, 0L);
    }

    static void streamSetPrivateData(MemorySegment stream, MemorySegment privateData) {
        STREAM_PRIVATE_DATA.set(stream, 0L, privateData);
    }

    static MemorySegment streamPrivateData(MemorySegment stream) {
        return (MemorySegment) STREAM_PRIVATE_DATA.get(stream, 0L);
    }

    private static VarHandle fieldHandle(MemoryLayout layout, String field) {
        return layout.varHandle(PathElement.groupElement(field));
    }
}
