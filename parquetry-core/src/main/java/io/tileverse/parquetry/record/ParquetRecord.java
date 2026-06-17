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
package io.tileverse.parquetry.record;

import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * A single row of a Parquet result set: a lazy, positional view over one batch.
 *
 * <p>A record addresses its columns by position (the projected-schema order of the row's columns) or, as a convenience,
 * by {@link ColumnPath}. Positional access reads straight from the column vector with no boxing and no map lookup; the
 * {@code ColumnPath} overloads resolve the path to its index once and delegate.
 *
 * <p>Two accessor families express ownership:
 *
 * <ul>
 *   <li>{@code read*} returns a live, zero-copy view valid only while the producing batch is open: {@link #readBinary}
 *       hands the value's own backing to a caller {@link BinaryView}; {@link #readStruct}/{@link #readList}/
 *       {@link #readMap} return sub-views over the same batch.
 *   <li>{@code get*} returns an owned value safe to keep: the primitive getters return a value, {@link #getBinary}
 *       copies the bytes into a {@code byte[]}, {@link #getString} decodes an owned {@code String}.
 * </ul>
 *
 * <p>The record is a live view; reading it after its batch closes is unsupported. A caller that retains a row past its
 * batch (a cache, a buffered or sorted collection) calls {@link #detach()} first, which copies every value out.
 *
 * <p>Typed accessors fail fast with {@link io.tileverse.parquetry.schema.ParquetSchemaException} when the requested
 * Java type does not match the column's kind. This is treated as a programming error: a mismatch means the caller
 * picked the wrong accessor for the column.
 */
public sealed interface ParquetRecord permits DefaultParquetRecord, DetachedParquetRecord, LevelElementRecord {

    /** The projected schema this record was assembled against (not the full file schema). */
    ParquetSchema schema();

    /** The number of columns addressable by position in this record. */
    int columnCount();

    /** The path of the column at {@code col}. */
    ColumnPath columnPath(int col);

    /** Whether the value at {@code col} is null (the leaf was null, or a struct ancestor was null). */
    boolean isNull(int col);

    boolean getBoolean(int col);

    int getInt(int col);

    long getLong(int col);

    float getFloat(int col);

    double getDouble(int col);

    /** UTF-8 decodes the binary column at {@code col} into an owned {@code String}, or {@code null} for a null cell. */
    String getString(int col);

    /** Copies the binary column at {@code col} into a fresh owned {@code byte[]}, or {@code null} for a null cell. */
    byte[] getBinary(int col);

    /**
     * The {@link UUID} value at {@code col}, decoding a 16-byte big-endian FIXED_LEN_BYTE_ARRAY (Parquet's UUID logical
     * type). Returns {@code null} when the column is NULL for this row. Throws a
     * {@link io.tileverse.parquetry.schema.ParquetSchemaException} when the column is not a 16-byte fixed-length binary
     * column, consistent with the other typed accessors.
     */
    UUID getUuid(int col);

    /**
     * Reads the binary column at {@code col} in place: the view receives the value's own backing segment, the value's
     * offset within it, and its byte length, with no slice or copy. Returns the view's result, or {@code null} for a
     * null cell (the view is not invoked).
     */
    <R> R readBinary(int col, BinaryView<R> view);

    /** The struct at {@code col} as a live sub-record, or {@code null} when the struct value is null. */
    ParquetRecord readStruct(int col);

    /** The repeated values at {@code col} as live sub-records, or {@code null} when the value is null. */
    List<ParquetRecord> readList(int col);

    /** The map at {@code col} as a live map, or {@code null} when the value is null. */
    // S1452: map key/value types are dynamic per the column's key/value kinds
    @SuppressWarnings("java:S1452")
    Map<?, ?> readMap(int col);

    /**
     * The generic value at {@code col}: a boxed primitive, a read-only {@link MemorySegment} for binary / INT96 /
     * FIXED_LEN_BYTE_ARRAY, a live sub-record/list/map for nested columns, or {@code null}. This is the introspection
     * path and returns RAW PHYSICAL values regardless of logical type (a DATE comes back as the INT32 epoch day, a UUID
     * column as its 16-byte segment). Logical materialization is the consumer's choice: inspect {@link #schema()} for
     * the column's physical kind and logical type, then call the matching typed accessor (for example
     * {@link #getUuid(ColumnPath)}). Typed and {@code read*} accessors are preferred on the hot path.
     */
    Object get(int col);

    boolean isNull(ColumnPath col);

    boolean getBoolean(ColumnPath col);

    int getInt(ColumnPath col);

    long getLong(ColumnPath col);

    float getFloat(ColumnPath col);

    double getDouble(ColumnPath col);

    String getString(ColumnPath col);

    byte[] getBinary(ColumnPath col);

    /** The {@link UUID} value at {@code col}; see {@link #getUuid(int)}. */
    UUID getUuid(ColumnPath col);

    <R> R readBinary(ColumnPath col, BinaryView<R> view);

    ParquetRecord readStruct(ColumnPath col);

    List<ParquetRecord> readList(ColumnPath col);

    @SuppressWarnings("java:S1452")
    Map<?, ?> readMap(ColumnPath col);

    Object get(ColumnPath col);

    /**
     * The flattened element values at a repeated (list/map) leaf {@code leafPath}: every value reached by descending
     * struct children and list/map elements along the path. Returns an empty list when the leaf's repeated ancestor is
     * empty, null, or absent. For a single-valued path this is the singleton of {@link #get(ColumnPath)} (or empty when
     * the value is null).
     */
    List<Object> multiValue(ColumnPath leafPath);

    /**
     * The size of the repeated leaf's universe at {@code leafPath}, INCLUDING null elements. Unlike
     * {@link #multiValue(ColumnPath)}, which drops nulls because callers read element values, this counts every present
     * element. A quantified ALL/ONE filter treats a null element as a present, non-matching member of the universe.
     */
    int multiValueSize(ColumnPath leafPath);

    /**
     * Returns a self-contained copy of this record that owns its data and stays valid after the producing batch closes.
     * Detaching is idempotent and shares the same immutable {@link #schema()} reference.
     */
    ParquetRecord detach();

    /**
     * Reads a binary value straight from its column's backing. The implementation passes the value's own backing
     * segment plus its {@code offset} and {@code length}; the view reads without slicing or copying.
     */
    @FunctionalInterface
    interface BinaryView<R> {
        R read(MemorySegment backing, long offset, long length);
    }
}
