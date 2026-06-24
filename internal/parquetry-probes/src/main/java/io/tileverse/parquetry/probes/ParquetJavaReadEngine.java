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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.geo.JtsGeometryFilter;
import io.tileverse.parquetry.internal.filter.spatial.WkbEnvelope;

/**
 * parquet-java 1.17.0's read arm: a {@link ParquetReader} over {@link LocalInputFile} (no Hadoop filesystem), opened
 * per read. parquet-java has no spatial concept. The spatial filter pushes a numeric prefilter on the {@code bbox}
 * covering columns (row-group and page pruning) and applies the exact geometry test app-side with the same JTS gate
 * parquetry uses. Each {@code Group} is read out in full (or restricted to the requested columns when
 * {@code parquetry.probe.columns} is set), mirroring a consumer that materializes each row.
 */
final class ParquetJavaReadEngine implements ReadEngine {

    private final ReadContext context;
    private long sink;
    private MessageType fileSchema;

    ParquetJavaReadEngine(ReadContext context) {
        this.context = context;
    }

    @Override
    public String name() {
        return "parquet-java";
    }

    @Override
    public long read(Scenario scenario) throws IOException {
        FilterCompat.Filter filter = filter(scenario);
        RowGate gate = rowGate(scenario);
        try (ParquetReader<Group> reader = groupReader(filter, projectedSchema(scenario))) {
            long count = 0L;
            Group group = reader.read();
            while (group != null) {
                if (gate.passes(group)) {
                    consumeGroup(group);
                    count++;
                }
                group = reader.read();
            }
            return count;
        }
    }

    /** A post-read row test parquet-java applies app-side, because parquet-java cannot evaluate it during the scan. */
    @FunctionalInterface
    private interface RowGate {
        boolean passes(Group group);
    }

    /**
     * The app-side gate for a scenario. parquet-java's pushdown handles the numeric work it can; the exact spatial test
     * is bolted on here, mirroring what a real application over parquet-java must do. SPATIAL re-checks each candidate
     * with the JTS gate. BBOX needs no gate when the file has covering columns (the numeric pushdown is exact for the
     * box relation); without them it re-derives each geometry's bbox from its WKB envelope - the same primitive
     * parquetry uses internally, no JTS geometry built.
     */
    private RowGate rowGate(Scenario scenario) {
        return switch (scenario) {
            case NO_FILTER, ATTRIBUTE -> group -> true;
            case SPATIAL, ATTRIBUTE_AND_SPATIAL -> {
                JtsGeometryFilter exactGate =
                        JtsGeometryFilter.intersects(context.geometryColumn(), context.queryDiamond());
                yield group -> passesExactGate(group, exactGate);
            }
            case BBOX -> {
                if (context.bboxCoveringAvailable()) {
                    yield group -> true;
                }
                Predicate.Spatial bboxGate =
                        new Predicate.Spatial.BboxIntersects(context.geometryColumn(), context.queryEnvelope());
                yield group -> passesBboxGate(group, bboxGate);
            }
        };
    }

    @Override
    public long checksum() {
        return sink;
    }

    private ParquetReader<Group> groupReader(FilterCompat.Filter filter, MessageType requestedSchema)
            throws IOException {
        ParquetReader.Builder<Group> builder = new ParquetReader.Builder<>(new LocalInputFile(context.file())) {
            @Override
            protected ReadSupport<Group> getReadSupport() {
                return new GroupReadSupport();
            }
        };
        if (filter != FilterCompat.NOOP) {
            builder = builder.withFilter(filter);
        }
        if (requestedSchema != null) {
            builder = builder.set(ReadSupport.PARQUET_READ_SCHEMA, requestedSchema.toString());
        }
        return builder.build();
    }

    /**
     * The requested read schema from {@code parquetry.probe.columns}, or {@code null} to read every column. Selects the
     * named output fields plus this scenario's predicate columns - the bbox covering and geometry for spatial, the
     * attribute column for the equality filter - because parquet-java must materialize a filter's columns to evaluate
     * it. This mirrors parquetry, which always reads predicate columns for filtering on top of the output projection.
     */
    private MessageType projectedSchema(Scenario scenario) {
        List<String> columns = ProbeColumns.requested();
        if (columns.isEmpty()) {
            return null;
        }
        Set<String> wanted = columns.stream()
                .map(ParquetJavaReadEngine::topLevel)
                .collect(java.util.stream.Collectors.toCollection(HashSet::new));
        boolean spatial = scenario == Scenario.SPATIAL || scenario == Scenario.ATTRIBUTE_AND_SPATIAL;
        boolean attribute = scenario == Scenario.ATTRIBUTE || scenario == Scenario.ATTRIBUTE_AND_SPATIAL;
        if (spatial) {
            wanted.add(topLevel(context.geometryColumnName()));
            if (context.bboxCoveringAvailable()) {
                wanted.add(context.bboxColumn());
            }
        }
        if (scenario == Scenario.BBOX) {
            if (context.bboxCoveringAvailable()) {
                wanted.add(context.bboxColumn());
            } else {
                wanted.add(topLevel(context.geometryColumnName()));
            }
        }
        if (attribute) {
            wanted.add(topLevel(context.attributeColumnName()));
        }
        MessageType full = fileSchema();
        List<Type> fields = full.getFields().stream()
                .filter(field -> wanted.contains(field.getName()))
                .toList();
        return new MessageType(full.getName(), fields);
    }

    private static String topLevel(String columnPath) {
        return columnPath.split("\\.")[0];
    }

    private MessageType fileSchema() {
        if (fileSchema == null) {
            try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(context.file()))) {
                fileSchema = reader.getFileMetaData().getSchema();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return fileSchema;
    }

    private FilterCompat.Filter filter(Scenario scenario) {
        return switch (scenario) {
            case NO_FILTER -> FilterCompat.NOOP;
            case ATTRIBUTE -> FilterCompat.get(attributeFilterPredicate());
            case BBOX -> spatialPrefilter() == null ? FilterCompat.NOOP : FilterCompat.get(spatialPrefilter());
            case SPATIAL -> spatialPrefilter() == null ? FilterCompat.NOOP : FilterCompat.get(spatialPrefilter());
            case ATTRIBUTE_AND_SPATIAL -> {
                FilterPredicate attribute = attributeFilterPredicate();
                FilterPredicate bbox = spatialPrefilter();
                yield FilterCompat.get(bbox == null ? attribute : FilterApi.and(attribute, bbox));
            }
        };
    }

    private FilterPredicate attributeFilterPredicate() {
        return FilterApi.eq(
                FilterApi.binaryColumn(context.attributeColumnName()), Binary.fromString(context.attributeValue()));
    }

    private FilterPredicate spatialPrefilter() {
        return context.bboxCoveringAvailable() ? bboxIntersectsPredicate() : null;
    }

    /**
     * A row's bbox [xmin,xmax,ymin,ymax] overlaps the query envelope iff {@code xmin <= qMaxX and xmax >= qMinX and
     * ymin <= qMaxY and ymax >= qMinY}. This is the lowering parquetry's spatial covering rewrite does internally.
     */
    private FilterPredicate bboxIntersectsPredicate() {
        String bbox = context.bboxColumn();
        FilterPredicate xOverlap = FilterApi.and(
                FilterApi.ltEq(
                        FilterApi.doubleColumn(bbox + ".xmin"),
                        context.queryEnvelope().maxX()),
                FilterApi.gtEq(
                        FilterApi.doubleColumn(bbox + ".xmax"),
                        context.queryEnvelope().minX()));
        FilterPredicate yOverlap = FilterApi.and(
                FilterApi.ltEq(
                        FilterApi.doubleColumn(bbox + ".ymin"),
                        context.queryEnvelope().maxY()),
                FilterApi.gtEq(
                        FilterApi.doubleColumn(bbox + ".ymax"),
                        context.queryEnvelope().minY()));
        return FilterApi.and(xOverlap, yOverlap);
    }

    private boolean passesExactGate(Group group, JtsGeometryFilter exactGate) {
        Binary wkb = group.getBinary(context.geometryColumnName(), 0);
        MemorySegment segment = MemorySegment.ofArray(wkb.getBytes()).asReadOnly();
        return exactGate.gate(segment).isPresent();
    }

    private boolean passesBboxGate(Group group, Predicate.Spatial bboxGate) {
        Binary wkb = group.getBinary(context.geometryColumnName(), 0);
        MemorySegment segment = MemorySegment.ofArray(wkb.getBytes()).asReadOnly();
        return WkbEnvelope.matches(bboxGate, segment);
    }

    /** Recursively reads every value of {@code group}, materializing each primitive, and folds it into the sink. */
    private void consumeGroup(Group group) {
        GroupType type = group.getType();
        for (int field = 0; field < type.getFieldCount(); field++) {
            int repetitions = group.getFieldRepetitionCount(field);
            Type fieldType = type.getType(field);
            for (int i = 0; i < repetitions; i++) {
                if (fieldType.isPrimitive()) {
                    consumePrimitive(
                            group, field, i, fieldType.asPrimitiveType().getPrimitiveTypeName());
                } else {
                    consumeGroup(group.getGroup(field, i));
                }
            }
        }
    }

    private void consumePrimitive(Group group, int field, int index, PrimitiveTypeName kind) {
        switch (kind) {
            case BOOLEAN -> sink += group.getBoolean(field, index) ? 1 : 0;
            case INT32 -> sink += group.getInteger(field, index);
            case INT64 -> sink += group.getLong(field, index);
            case FLOAT -> sink += (long) group.getFloat(field, index);
            case DOUBLE -> sink += (long) group.getDouble(field, index);
            case INT96, BINARY, FIXED_LEN_BYTE_ARRAY ->
                sink += group.getBinary(field, index).length();
        }
    }
}
