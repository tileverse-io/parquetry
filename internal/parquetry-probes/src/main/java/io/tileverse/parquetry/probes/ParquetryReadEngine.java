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
package io.tileverse.parquetry.probes;

import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.tileverse.parquetry.data.ParquetReader;
import io.tileverse.parquetry.data.ParquetRuntime;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.geo.jts.JtsGeometryFilter;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * parquetry's read arm. Opens the dataset once and reuses it across reads (the long-lived server model) and walks every
 * value of each surviving row in place via the typed positional accessors, mirroring a streaming consumer (e.g. a
 * GeoTools datastore) that pulls each attribute as it iterates and never retains the record. No {@code detach()}: that
 * copy is only for callers that hold a record past its batch, which a streaming reader does not do.
 */
final class ParquetryReadEngine implements ReadEngine {

    private final ReadContext context;
    private ByteRangeSource source;
    private ParquetReader reader;
    private long sink;

    ParquetryReadEngine(ReadContext context) {
        this.context = context;
    }

    @Override
    public String name() {
        return "parquetry";
    }

    @Override
    public long read(Scenario scenario) {
        Predicate predicate = predicate(scenario);
        try (Stream<ParquetRecord> records = reader().read(predicate, projection(), ReadOptions.DEFAULTS)) {
            long count = 0L;
            for (ParquetRecord rec : (Iterable<ParquetRecord>) records::iterator) {
                walkRecord(rec, rec.schema().root());
                count++;
            }
            return count;
        }
    }

    @Override
    public long checksum() {
        return sink;
    }

    @Override
    public void close() {
        if (source == null) {
            return;
        }
        source.close();
        source = null;
        reader = null;
    }

    /** Opened once and reused across reads; synchronized so concurrent reads share a single open reader. */
    private synchronized ParquetReader reader() {
        if (reader == null) {
            source = ByteRangeSource.ofFile(context.file());
            reader = ParquetReader.open(source, runtime(), Optional.empty());
        }
        return reader;
    }

    /**
     * Binds the read runtime. Honors {@code parquetry.probe.decodeAhead} to override the per-read decode-ahead window
     * (how many row groups one read decodes concurrently); 1 makes a read effectively serial, which isolates intra-read
     * parallelism from inter-read concurrency. Unset uses the default heuristic.
     */
    private static ParquetRuntime runtime() {
        String decodeAhead = System.getProperty("parquetry.probe.decodeAhead");
        if (decodeAhead == null) {
            return ParquetRuntime.defaultRuntime();
        }
        return ParquetRuntime.builder()
                .maxDecodeAhead(Integer.parseInt(decodeAhead))
                .build();
    }

    /**
     * The output projection. Honors {@code parquetry.probe.columns} (comma-separated leaf paths, e.g.
     * {@code geometry,subtype,id}) to materialize only those columns, modelling a real query that selects a few
     * attributes rather than the whole row. Predicate columns are still read for filtering regardless. Unset projects
     * every column.
     */
    private static Projection projection() {
        List<String> columns = ProbeColumns.requested();
        if (columns.isEmpty()) {
            return Projection.ALL;
        }
        Set<ColumnPath> kept =
                columns.stream().map(name -> ColumnPath.of(name.split("\\."))).collect(Collectors.toSet());
        return Projection.of(kept);
    }

    private Predicate predicate(Scenario scenario) {
        return switch (scenario) {
            case NO_FILTER -> Predicate.ALWAYS_TRUE;
            case ATTRIBUTE -> attribute();
            case SPATIAL -> spatial();
            case ATTRIBUTE_AND_SPATIAL -> new Predicate.And(List.of(attribute(), spatial()));
        };
    }

    private Predicate attribute() {
        return new Predicate.Eq(context.attributeColumn(), new Value.StringVal(context.attributeValue()));
    }

    private Predicate spatial() {
        return Predicate.geometryFilter(JtsGeometryFilter.intersects(context.geometryColumn(), context.queryDiamond()));
    }

    /**
     * Reads each top-level field positionally: non-repeated primitive leaves use the typed positional accessors (no
     * boxing; binary read in place), groups and repeated leaves descend by their materialized value.
     */
    private void walkRecord(ParquetRecord rec, SchemaNode.Group group) {
        List<SchemaNode> children = group.children();
        for (int col = 0; col < children.size(); col++) {
            touchColumn(rec, col, children.get(col));
        }
    }

    private void touchColumn(ParquetRecord rec, int col, SchemaNode node) {
        if (rec.isNull(col)) {
            return;
        }
        if (node instanceof SchemaNode.Primitive primitive && primitive.repetition() != Repetition.REPEATED) {
            touchPrimitive(rec, col, primitive.kind());
        } else {
            touchValue(rec.get(col), node);
        }
    }

    private void touchPrimitive(ParquetRecord rec, int col, PrimitiveKind kind) {
        switch (kind) {
            case BOOLEAN -> sink += rec.getBoolean(col) ? 1L : 0L;
            case INT32 -> sink += rec.getInt(col);
            case INT64 -> sink += rec.getLong(col);
            case FLOAT -> sink += (long) rec.getFloat(col);
            case DOUBLE -> sink += (long) rec.getDouble(col);
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> rec.readBinary(col, this::addLength);
        }
    }

    /** Binary view that adds the value's length to the sink without copying; returns null (the result is unused). */
    private Object addLength(MemorySegment backing, long offset, long length) {
        sink += length;
        return null;
    }

    /**
     * Forces the value to materialize fully. The kind comes from what {@code get()} returns - a {@link ParquetRecord}
     * for a struct cell, a {@link List} for a list, a {@link Map} for a map, otherwise a scalar - and {@code node} is
     * the matching schema node used only to descend struct and list-element records.
     */
    private void touchValue(Object value, SchemaNode node) {
        switch (value) {
            case null -> {
                // null cell, nothing to materialize
            }
            case ParquetRecord nested -> walkRecord(nested, (SchemaNode.Group) node);
            case List<?> list -> {
                SchemaNode element = listElement((SchemaNode.Group) node);
                for (Object item : list) {
                    touchValue(item, element);
                }
            }
            case Map<?, ?> map ->
                map.forEach((key, mapped) -> {
                    sink += scalarHash(key);
                    sink += scalarHash(mapped);
                });
            case MemorySegment segment -> sink += segment.byteSize();
            default -> sink += value.hashCode();
        }
    }

    private static long scalarHash(Object value) {
        if (value instanceof MemorySegment segment) {
            return segment.byteSize();
        }
        return value == null ? 0L : value.hashCode();
    }

    /** The element node of a 3-level LIST group ({@code list -> element}), falling back to a 2-level repeated child. */
    private static SchemaNode listElement(SchemaNode.Group listGroup) {
        SchemaNode middle = listGroup.children().get(0);
        if (middle instanceof SchemaNode.Group repeated) {
            return repeated.children().get(0);
        }
        return middle;
    }
}
