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
package io.tileverse.parquetry.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.filter.prune.ColumnStatistics;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.internal.filter.spatial.SpatialBoundsSource;
import io.tileverse.parquetry.internal.footer.ChunkMeta;
import io.tileverse.parquetry.internal.footer.CompactFooter;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Pins the reader's memory footprint: an open {@link ParquetFileReader} keeps the packed planning form of the footer,
 * and the only decoded wire records still reachable from it are the per-column ones listed in
 * {@link #ACCEPTED_WIRE_TYPES} - never a row group, a column chunk, or the footer record itself. The file statistics
 * derived from that footer hold no part of it either. A dataset of several hundred files multiplies the footprint of a
 * single reader, and the wire records dominate that total, hence the type-level check rather than a size assertion.
 */
class FooterRetentionTest {

    /** Home of the decoded thrift records: a value of this package is one of them. */
    private static final String WIRE_PACKAGE = "io.tileverse.parquetry.format";

    /** Home of parquetry's own types, and the boundary of the walk below. */
    private static final String PARQUETRY_PACKAGE = "io.tileverse.parquetry";

    /**
     * The two wire type subtrees that a reader may retain: each entry admits its subtypes too, which is what covers the
     * whole sealed {@link LogicalType} family. Both are per column rather than per column chunk.
     *
     * <ul>
     *   <li>{@link BoundingBox} - the compact bounding box of the spatial planning tier, held per geometry column and
     *       row group by the bounds source and read by the pruner before any fetch.
     *   <li>{@link LogicalType} - the annotation declared by a schema leaf, part of {@link ParquetSchema} by design and
     *       reachable from every reader that exposes its schema.
     * </ul>
     */
    private static final List<Class<?>> ACCEPTED_WIRE_TYPES = List.of(BoundingBox.class, LogicalType.class);

    /** A GeoParquet 1.1 file of 29 row groups, giving a per-row-group leak more than one record to hold. */
    private static final String MULTI_ROW_GROUP_FIXTURE = "parquetry/geo/buildings-gp110-bbox-covering.parquet";

    /** Three distinct names, giving the written file a min and a max that differ. */
    private static final String[] NAMES = {"delta", "alpha", "omega"};

    /**
     * Backstop for the walk below. Termination comes from its identity set, which visits every object once; this cap
     * bounds a graph deeper than any reader builds, and every path cut short is reported rather than passed over.
     */
    private static final int MAX_WALK_DEPTH = 16;

    @TempDir
    Path tempDir;

    @Test
    void readerDeclaresNoWireFooterField() {
        List<String> wireFields = new ArrayList<>();
        for (Field field : ParquetFileReader.class.getDeclaredFields()) {
            if (isWireType(field.getType())) {
                wireFields.add(field.getName() + " : " + field.getType().getName());
            }
        }
        assertThat(wireFields)
                .as("fields of ParquetFileReader declared as wire footer types")
                .isEmpty();
    }

    @Test
    void anOpenReaderReachesNoWireRecordBeyondItsPerColumnPlanningValues() {
        Path file = TestCorpus.extractFile(MULTI_ROW_GROUP_FIXTURE, tempDir);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            int rowGroups = reader.rowGroups().size();
            assertThat(rowGroups)
                    .as("the fixture needs several row groups for this check to discriminate")
                    .isGreaterThan(1);
            assertThat(reader.compactFooter().rowGroupCount())
                    .as("row groups known to the retained planning footer")
                    .isEqualTo(rowGroups);

            WireValueWalk walk = wireValuesHeldBy(reader);
            assertThat(walk.truncatedAt())
                    .as("paths where the depth cap cut the walk short, leaving part of the reader unchecked")
                    .isEmpty();
            assertThat(unexpectedWireValues(walk))
                    .as("wire records reachable from the reader, beyond the accepted per-column ones")
                    .isEmpty();
            assertThat(planningBoxesIn(walk))
                    .as("planning boxes of the bounds source, each one reachable from the reader")
                    .containsAll(planningBoxesHeldBy(reader.spatialBounds()));
        }
    }

    /**
     * Every distinct planning box inside {@code boundsSource}, found by the same walk that the reader gets above. The
     * price quoted to the cache is bounded here too, because that price is only as good as the objects behind it. It
     * counts slots, one per filled row-group slot plus one per column at file level, and a column of one filled slot
     * aliases the two, because the union of a single box is that same box. Halving is therefore the worst that the
     * aliasing can do: such a column prices two slots and holds one box, and a column of several filled slots prices
     * exactly what it holds.
     */
    private static List<BoundingBox> planningBoxesHeldBy(SpatialBoundsSource boundsSource) {
        WireValueWalk walk = new WireValueWalk();
        walk.visit(boundsSource, "boundsSource", 0);
        List<BoundingBox> boxes = planningBoxesIn(walk);
        int pricedSlots = boundsSource.retainedBoxCount();
        assertThat(pricedSlots)
                .as("the fixture needs priced planning slots for this check to discriminate")
                .isPositive();
        assertThat(boxes)
                .as("distinct planning boxes inside the bounds source, never more than the slots priced by the source")
                .hasSizeBetween(pricedSlots / 2, pricedSlots);
        return boxes;
    }

    /**
     * The check above bites only if the walk opens a plain object, because that is where a stray wire record would
     * hide: a reader field of any parquetry type, opened by nothing else. This double puts one there.
     */
    @Test
    void theWalkReportsAWireRecordHiddenBehindAPlainField() {
        Path file = TestCorpus.extractFile(MULTI_ROW_GROUP_FIXTURE, tempDir);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            FileMetaData wireFooter = ParquetFormat.readFooter(source);

            WireValueWalk walk = new WireValueWalk();
            walk.visit(new PlainFieldHolder(wireFooter), "holder", 0);

            assertThat(walk.found())
                    .as("wire records reachable through a plain object's field")
                    .singleElement()
                    .extracting(RetainedWireValue::type)
                    .isEqualTo(FileMetaData.class);
        }
    }

    /** Stands in for a reader field of any parquetry type, with a decoded wire record behind it. */
    private record PlainFieldHolder(FileMetaData footer) {}

    private static WireValueWalk wireValuesHeldBy(ParquetFileReader reader) {
        WireValueWalk walk = new WireValueWalk();
        for (Field field : ParquetFileReader.class.getDeclaredFields()) {
            if (!Modifier.isStatic(field.getModifiers())) {
                walk.visit(valueOf(field, reader), field.getName(), 0);
            }
        }
        return walk;
    }

    private static List<RetainedWireValue> unexpectedWireValues(WireValueWalk walk) {
        List<RetainedWireValue> unexpected = new ArrayList<>();
        for (RetainedWireValue value : walk.found()) {
            if (!isAccepted(value.type())) {
                unexpected.add(value);
            }
        }
        return unexpected;
    }

    private static boolean isAccepted(Class<?> type) {
        for (Class<?> accepted : ACCEPTED_WIRE_TYPES) {
            if (accepted.isAssignableFrom(type)) {
                return true;
            }
        }
        return false;
    }

    private static List<BoundingBox> planningBoxesIn(WireValueWalk walk) {
        List<BoundingBox> boxes = new ArrayList<>();
        for (RetainedWireValue value : walk.found()) {
            if (value.value() instanceof BoundingBox box) {
                boxes.add(box);
            }
        }
        return boxes;
    }

    /** One reachable wire record, named by the path that the walk took to it. */
    private record RetainedWireValue(String path, Object value) {

        Class<?> type() {
            return value.getClass();
        }

        @Override
        public String toString() {
            return path + " = " + type().getName();
        }
    }

    /**
     * A reflective walk over everything reachable within parquetry from a given object, recording each value whose
     * class belongs to the wire package. A container is opened by element and every other parquetry value by declared
     * instance field, up its superclasses for as long as they stay parquetry's own. Two rules bound it: a value whose
     * runtime class lies outside parquetry's packages is never opened, and neither is a value already recorded.
     *
     * <p>Opening a plain object is what makes the walk bite. A field declared as {@code List<RowGroup>} erases to
     * {@code java.util.List} and an array class reports no package at all, hence a declared type alone shows neither
     * shape; a wire record behind an ordinary object's field is just as invisible to it.
     */
    private static final class WireValueWalk {

        private final Set<Object> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        private final List<RetainedWireValue> found = new ArrayList<>();
        private final List<String> truncatedAt = new ArrayList<>();

        void visit(Object value, String path, int depth) {
            if (value == null) {
                return;
            }
            if (depth > MAX_WALK_DEPTH) {
                truncatedAt.add(path);
                return;
            }
            if (!visited.add(value)) {
                return;
            }
            Class<?> type = value.getClass();
            if (isWireType(type)) {
                found.add(new RetainedWireValue(path, value));
                return;
            }
            if (isContainer(value)) {
                visitElements(elementsOf(value), path, depth);
                return;
            }
            if (isParquetryType(type)) {
                visitFields(value, type, path, depth);
            }
        }

        private void visitElements(List<Object> elements, String path, int depth) {
            for (int i = 0; i < elements.size(); i++) {
                visit(elements.get(i), path + "[" + i + "]", depth + 1);
            }
        }

        private void visitFields(Object value, Class<?> type, String path, int depth) {
            for (Class<?> declaring = type; isParquetryType(declaring); declaring = declaring.getSuperclass()) {
                for (Field field : declaring.getDeclaredFields()) {
                    if (!Modifier.isStatic(field.getModifiers())) {
                        visit(valueOf(field, value), path + "." + field.getName(), depth + 1);
                    }
                }
            }
        }

        List<RetainedWireValue> found() {
            return found;
        }

        List<String> truncatedAt() {
            return truncatedAt;
        }
    }

    /**
     * Whether {@code value} holds other values by position or key rather than by field. A {@link Collection} rather
     * than any {@link Iterable}: a {@link Path} iterates over freshly built name elements, which no identity set can
     * catch and no walk can finish.
     */
    private static boolean isContainer(Object value) {
        return value instanceof Optional<?>
                || value instanceof Map<?, ?>
                || value instanceof Collection<?>
                || value instanceof Object[];
    }

    /** The elements of a container. A map contributes its keys and its values. */
    private static List<Object> elementsOf(Object value) {
        if (value instanceof Optional<?> optional) {
            return optional.isPresent() ? List.of(optional.orElseThrow()) : List.of();
        }
        if (value instanceof Map<?, ?> map) {
            List<Object> elements = new ArrayList<>(map.size() * 2);
            elements.addAll(map.keySet());
            elements.addAll(map.values());
            return elements;
        }
        if (value instanceof Collection<?> collection) {
            return new ArrayList<>(collection);
        }
        if (value instanceof Object[] array) {
            return Arrays.asList(array);
        }
        return List.of();
    }

    // java:S3011 - reading the private fields is the assertion; there is no public view of what a reader retains.
    @SuppressWarnings("java:S3011")
    private static Object valueOf(Field field, Object owner) {
        field.setAccessible(true);
        try {
            return field.get(owner);
        } catch (IllegalAccessException e) {
            throw new AssertionError("Could not read field " + field.getName(), e);
        }
    }

    /** Whether {@code type} is a decoded wire record. */
    private static boolean isWireType(Class<?> type) {
        return packageOf(type).startsWith(WIRE_PACKAGE);
    }

    /** Whether {@code type} is parquetry's own, and therefore open to the walk. */
    private static boolean isParquetryType(Class<?> type) {
        return type != null && packageOf(type).startsWith(PARQUETRY_PACKAGE);
    }

    /** The package of {@code type}, looking through array types, which report no package of their own. */
    private static String packageOf(Class<?> type) {
        Class<?> element = type;
        while (element.isArray()) {
            element = element.componentType();
        }
        Package declaringPackage = element.getPackage();
        return declaringPackage == null ? "" : declaringPackage.getName();
    }

    /**
     * A catalog keeps one set of file statistics per file for as long as its store is open, while the packed footer
     * behind them lives on the cache's own schedule. A decoded bound pointing into that footer would hold the whole
     * packed blob of every file, which is why every bound holds a copy of just its own bytes.
     */
    @Test
    void fileStatisticsPinNoFooterBlob() throws Exception {
        Path file = FileStatsFixtures.writeStringColumn(tempDir, "name", NAMES);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            ColumnStatistics name = reader.fileStats().columns().get(ColumnPath.of("name"));
            assertThat(name)
                    .as("aggregated footer statistics of the string column")
                    .isNotNull();

            List<MemorySegment> bounds = binaryBoundsOf(name);
            assertThat(bounds).as("decoded binary bounds of the string column").hasSize(2);

            List<MemorySegment> windows = statisticWindowsOf(reader.compactFooter());
            assertThat(windows).as("statistic windows into the packed footer").isNotEmpty();
            for (MemorySegment bound : bounds) {
                assertThat(windows)
                        .as("packed footer windows sharing memory with a decoded bound")
                        .noneMatch(window -> window.asOverlappingSlice(bound).isPresent());
            }
        }
    }

    /** The min and the max of {@code stats}, as the segments behind their decoded binary values. */
    private static List<MemorySegment> binaryBoundsOf(ColumnStatistics stats) {
        List<MemorySegment> segments = new ArrayList<>(2);
        for (Optional<Value> bound : List.of(stats.min(), stats.max())) {
            if (bound.orElse(null) instanceof Value.BinaryVal(MemorySegment bytes)) {
                segments.add(bytes);
            }
        }
        return segments;
    }

    /** Every min and max window into the packed footer, across its row groups and its leaf columns. */
    private static List<MemorySegment> statisticWindowsOf(CompactFooter footer) {
        List<MemorySegment> windows = new ArrayList<>();
        for (int rowGroup = 0; rowGroup < footer.rowGroupCount(); rowGroup++) {
            for (int leaf = 0; leaf < footer.leafCount(); leaf++) {
                addStatisticWindows(footer.chunkIfPresent(rowGroup, leaf), windows);
            }
        }
        return windows;
    }

    private static void addStatisticWindows(Optional<ChunkMeta> chunk, List<MemorySegment> windows) {
        chunk.flatMap(ChunkMeta::minValue).ifPresent(windows::add);
        chunk.flatMap(ChunkMeta::maxValue).ifPresent(windows::add);
    }
}
