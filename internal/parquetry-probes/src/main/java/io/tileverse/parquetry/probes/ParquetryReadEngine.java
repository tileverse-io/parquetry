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
package io.tileverse.parquetry.probes;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.geo.JtsGeometryFilter;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.observe.QueryStats;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.runtime.ParquetRuntime;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.tileverse.ByteRangeSources;

/**
 * parquetry's read arm. Opens the dataset once and reuses it across reads (the long-lived server model) and walks every
 * value of each surviving row in place via the typed positional accessors, mirroring a streaming consumer (e.g. a
 * GeoTools datastore) that pulls each attribute as it iterates and never retains the record. No {@code detach()}: that
 * copy is only for callers that hold a record past its batch, which a streaming reader does not do.
 */
final class ParquetryReadEngine implements ReadEngine {

    private final ReadContext context;
    private final ProbeQueryObserver analyzeObserver;
    private final ReadOptions readOptions;
    private ByteRangeSource source;
    private Storage remoteStorage;
    private RangeReader remoteReader;
    private ParquetFileReader reader;
    private long sink;

    /** One reused binary view, modelling a consumer that reads every binary cell through a stable callback. */
    private final ParquetRecord.BinaryView<Object> lengthView = this::addLength;

    ParquetryReadEngine(ReadContext context) {
        this(context, false);
    }

    ParquetryReadEngine(ReadContext context, boolean analyze) {
        this.context = context;
        this.analyzeObserver = analyze ? new ProbeQueryObserver() : null;
        this.readOptions = analyze
                ? ReadOptions.DEFAULTS.toBuilder()
                        .queryObserver(analyzeObserver)
                        .build()
                : ReadOptions.DEFAULTS;
    }

    @Override
    public String name() {
        return "parquetry";
    }

    @Override
    public long read(Scenario scenario) {
        Predicate predicate = predicate(scenario);
        try (Stream<ParquetRecord> records = reader().read(predicate, projection(), readOptions)) {
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
    public Optional<QueryStats> analyzeStats() {
        return analyzeObserver == null ? Optional.empty() : analyzeObserver.snapshot();
    }

    @Override
    public void close() {
        if (source == null) {
            return;
        }
        source.close();
        closeRemote();
        source = null;
        reader = null;
    }

    /** Closes the tileverse-storage handles backing a remote read; a no-op for the local-file path. */
    private void closeRemote() {
        try {
            if (remoteReader != null) {
                remoteReader.close();
                remoteReader = null;
            }
            if (remoteStorage != null) {
                remoteStorage.close();
                remoteStorage = null;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Opened once and reused across reads; synchronized so concurrent reads share a single open reader. */
    private synchronized ParquetFileReader reader() {
        if (reader == null) {
            source = openSource();
            reader = ParquetFileReader.open(source, runtime(), Optional.empty());
        }
        return reader;
    }

    /**
     * Opens the byte source for the dataset. With {@code parquetry.probe.url} set, reads over the network through
     * tileverse-storage (HTTP/S3/...), letting a latency-injecting file server stand in for object storage so the
     * fetch-ahead prefetcher runs against real round-trip latency. Unset reads the local file directly.
     */
    private ByteRangeSource openSource() {
        String url = System.getProperty("parquetry.probe.url");
        if (url == null) {
            return ByteRangeSource.ofFile(context.file());
        }
        return openRemoteSource(URI.create(url));
    }

    private ByteRangeSource openRemoteSource(URI fileUri) {
        URI container = fileUri.resolve(".");
        remoteStorage = StorageFactory.open(container, remoteStorageProperties());
        remoteReader = remoteStorage.openRangeReader(fileUri);
        return ByteRangeSources.from(remoteReader);
    }

    /**
     * The Parquet-tuned storage cache configuration. Because parquetry already coalesces column chunks into
     * row-group-sized ranges, the right cache granularity is one entry per coalesced range - exactly what the
     * tileverse-storage cache stores. Defaults model the production intent - caching on - matching
     * {@code parquetry-tileverse-storage}.
     *
     * <p>Override for benchmarking: {@code parquetry.probe.httpCache=false} turns caching off entirely, keeping the
     * cross-read dedup from hiding fetch latency across measurement waves.
     */
    private static Properties remoteStorageProperties() {
        Properties props = new Properties();
        boolean caching = Boolean.parseBoolean(System.getProperty("parquetry.probe.httpCache", "true"));
        props.setProperty("storage.caching.enabled", Boolean.toString(caching));
        return props;
    }

    /**
     * Binds the read runtime. Honors two independent overrides so the two concurrency layers can be measured apart:
     * {@code parquetry.probe.decodeAhead} sets the per-read decode-ahead window (how many row groups one read decodes
     * concurrently; 0 decodes every row group inline), and {@code parquetry.probe.prefetchDepth} sets the fetch-ahead
     * window (how many upcoming row-group byte fetches overlap the current decode; 0 fetches inline). Either unset
     * keeps its default. Both unset uses the default heuristic runtime.
     */
    private static ParquetRuntime runtime() {
        String decodeAhead = System.getProperty("parquetry.probe.decodeAhead");
        String prefetchDepth = System.getProperty("parquetry.probe.prefetchDepth");
        if (decodeAhead == null && prefetchDepth == null) {
            return ParquetRuntime.defaultRuntime();
        }
        ParquetRuntime.Builder builder = ParquetRuntime.builder();
        if (decodeAhead != null) {
            builder.maxDecodeAhead(Integer.parseInt(decodeAhead));
        }
        if (prefetchDepth != null) {
            builder.prefetchDepth(Integer.parseInt(prefetchDepth));
        }
        return builder.build();
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
        return Projection.ofPhysical(kept);
    }

    private Predicate predicate(Scenario scenario) {
        return switch (scenario) {
            case NO_FILTER -> Predicate.ALWAYS_TRUE;
            case ATTRIBUTE -> attribute();
            case BBOX -> bbox();
            case SPATIAL -> spatial();
            case ATTRIBUTE_AND_SPATIAL -> new Predicate.And(List.of(attribute(), spatial()));
        };
    }

    /**
     * The bbox-only spatial filter: a bbox rectangle relation handed to parquetry as a predicate, no exact geometry
     * test. parquetry applies its own ergonomics - lowering it to numeric comparisons on the covering columns when the
     * file has them (no WKB decoded), or evaluating it from each geometry's WKB envelope otherwise.
     */
    private Predicate bbox() {
        return new Predicate.Spatial.BboxIntersects(context.geometryColumn(), context.queryEnvelope());
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
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> rec.readBinary(col, lengthView);
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
