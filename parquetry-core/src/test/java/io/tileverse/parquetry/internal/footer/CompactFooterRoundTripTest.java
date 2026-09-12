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
package io.tileverse.parquetry.internal.footer;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.CompressionCodec;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.GeospatialStatistics;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.ParquetFormatException;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.format.Statistics;
import io.tileverse.parquetry.internal.filter.bloom.SplitBlockBloomFilter;
import io.tileverse.parquetry.internal.filter.spatial.SpatialBoundsSource;
import io.tileverse.parquetry.internal.read.FetchPlan;
import io.tileverse.parquetry.internal.read.IndexSectionLoader;
import io.tileverse.parquetry.internal.read.RowGroupChunks;
import io.tileverse.parquetry.internal.read.RowGroupFetcher;
import io.tileverse.parquetry.internal.read.RowGroupSurvivor;
import io.tileverse.parquetry.internal.read.TestFetchers;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * The compact planning form must answer every planning question with the same value as the wire footer. For each corpus
 * file the whole footer is encoded and then every row-group lane and every (row group, leaf) chunk field is compared
 * against the wire record as the scan path reads it today.
 *
 * <p>The fixtures are chosen to cover the shapes encoded specially by the blob - dictionary pages, page indexes, bloom
 * filters, nested leaf paths, native geospatial bounds, absent statistics, and several row groups - and
 * {@link #theFixtureSetExercisesEveryEncodedShape()} fails if a future corpus change stops exercising one of them.
 */
class CompactFooterRoundTripTest {

    private static final String ALLTYPES_PLAIN = "parquet-testing/data/alltypes_plain.parquet";
    private static final String ALLTYPES_DICTIONARY = "parquet-testing/data/alltypes_dictionary.parquet";
    private static final String INDEX_BLOOM_STATS = "parquet-testing/data/data_index_bloom_encoding_stats.parquet";
    private static final String BINARY_TRUNCATED_MIN_MAX = "parquet-testing/data/binary_truncated_min_max.parquet";
    private static final String DICT_PAGE_OFFSET_ZERO = "parquet-testing/data/dict-page-offset-zero.parquet";
    private static final String NESTED_MAPS = "parquet-testing/data/nested_maps.snappy.parquet";
    private static final String NESTED_LISTS = "parquet-testing/data/nested_lists.snappy.parquet";
    private static final String NATIVE_GEOMETRY = "geoparquet-testing/data/encodings/point-native-geometry.parquet";
    private static final String GEOPARQUET_BUILDINGS = "parquetry/geo/buildings-gp110-bbox-covering.parquet";

    @TempDir
    Path tempDir;

    static Stream<String> corpusFiles() {
        return Stream.of(
                ALLTYPES_PLAIN,
                ALLTYPES_DICTIONARY,
                INDEX_BLOOM_STATS,
                BINARY_TRUNCATED_MIN_MAX,
                DICT_PAGE_OFFSET_ZERO,
                NESTED_MAPS,
                NESTED_LISTS,
                NATIVE_GEOMETRY,
                GEOPARQUET_BUILDINGS);
    }

    @ParameterizedTest
    @MethodSource("corpusFiles")
    void roundTripMatchesWireRecords(String resource) {
        Path file = TestCorpus.extractFile(resource, tempDir);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            FileMetaData wire = ParquetFormat.readFooter(source);
            ParquetSchema schema = ParquetFileReader.open(source).schema();
            LeafIndex leaves = LeafIndex.of(schema);
            CompactFooter compact = CompactFooter.encode(wire, leaves);

            assertThat(compact.rowGroupCount()).isEqualTo(wire.rowGroups().size());
            assertThat(compact.leafCount()).isEqualTo(schema.leafColumns().size());

            for (int rg = 0; rg < compact.rowGroupCount(); rg++) {
                assertRowGroupMatchesWire(compact, rg, wire.rowGroups().get(rg));
                assertEveryLeafMatchesWire(compact, leaves, rg, wire.rowGroups().get(rg));
            }
        }
    }

    /**
     * Pins every layout constant at once: the header, the row-group lane stride, the chunk record stride, the geo entry
     * stride, and the verbatim statistics arena must account for the whole blob and nothing more.
     */
    @ParameterizedTest
    @MethodSource("corpusFiles")
    void theBlobIsExactlyTheSizeTheLayoutPrescribes(String resource) {
        Path file = TestCorpus.extractFile(resource, tempDir);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            FileMetaData wire = ParquetFormat.readFooter(source);
            ParquetSchema schema = ParquetFileReader.open(source).schema();
            CompactFooter compact = CompactFooter.encode(wire, LeafIndex.of(schema));

            long rowGroups = compact.rowGroupCount();
            long leaves = compact.leafCount();
            long expected = 40L
                    + 8L * rowGroups
                    + 96L * rowGroups * leaves
                    + 64L * geoBboxCount(wire)
                    + statisticsByteCount(wire);

            assertThat(compact.byteSize()).isEqualTo(expected);
        }
    }

    /**
     * The blob answers how many chunks record a native geospatial extent from its header offsets alone, which is what
     * lets the spatial tier rule a file out before reading a single chunk flag. The answer must equal the count taken
     * off the wire footer, for a geospatial file and a plain one alike.
     */
    @ParameterizedTest
    @MethodSource("corpusFiles")
    void theGeoExtentCountMatchesTheWireFooter(String resource) {
        Path file = TestCorpus.extractFile(resource, tempDir);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            FileMetaData wire = ParquetFormat.readFooter(source);
            ParquetSchema schema = ParquetFileReader.open(source).schema();
            CompactFooter compact = CompactFooter.encode(wire, LeafIndex.of(schema));

            assertThat(compact.geoExtentCount()).isEqualTo((int) geoBboxCount(wire));
        }
    }

    /** Pins both ends of {@link #theGeoExtentCountMatchesTheWireFooter(String)}: neither vector is vacuous. */
    @Test
    void onlyAFileWithNativeGeospatialStatisticsCountsGeoExtents() {
        assertThat(geoExtentCountOf(NATIVE_GEOMETRY))
                .as("the native geospatial fixture must record at least one extent")
                .isPositive();
        assertThat(geoExtentCountOf(ALLTYPES_PLAIN))
                .as("a file with no geospatial statistics records none")
                .isZero();
    }

    private int geoExtentCountOf(String resource) {
        Path file = TestCorpus.extractFile(resource, tempDir);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            FileMetaData wire = ParquetFormat.readFooter(source);
            ParquetSchema schema = ParquetFileReader.open(source).schema();
            return CompactFooter.encode(wire, LeafIndex.of(schema)).geoExtentCount();
        }
    }

    /**
     * The spatial pruning tier must read the same per-row-group and file-level extents from the blob as it read from
     * the wire footer. The expected side is derived here by the walk that the tier itself ran before the blob replaced
     * it, and the geo metadata is withheld to leave the dispatch only the native tier or nothing.
     */
    @Test
    void nativeGeometryBoundsMatchTheWireFooter() {
        Path file = TestCorpus.extractFile(NATIVE_GEOMETRY, tempDir);
        Map<ColumnPath, List<Optional<BoundingBox>>> fromWire = assertNativeBoundsMatchTheWireFooter(file);

        BoundingBox anyBox = fromWire.values().iterator().next().stream()
                .flatMap(Optional::stream)
                .findFirst()
                .orElseThrow();
        assertThat(anyBox.zmin())
                .as("this vector covers the absent-Z case; the 2D corpus fixture must stay 2D")
                .isEmpty();
        assertThat(anyBox.mmin()).isEmpty();
    }

    /**
     * The Z and M counterpart of {@link #nativeGeometryBoundsMatchTheWireFooter()}. No vendored corpus fixture records
     * a 3D geospatial extent, hence the file is written here with parquetry's own writer, across two row groups, which
     * forces the file-level union to fold Z and M as well as X and Y.
     */
    @Test
    void nativeGeometryBoundsKeepTheZAndMExtents() throws IOException {
        Path file = writePointZmFixture();
        Map<ColumnPath, List<Optional<BoundingBox>>> fromWire = assertNativeBoundsMatchTheWireFooter(file);

        BoundingBox firstGroup = fromWire.values().iterator().next().getFirst().orElseThrow();
        assertThat(firstGroup.zmin())
                .as("the fixture must record a Z extent, else this vector proves nothing")
                .isPresent();
        assertThat(firstGroup.mmin())
                .as("the fixture must record an M extent, else this vector proves nothing")
                .isPresent();
    }

    /**
     * Each half of the Z and M extents is flagged on its own, which is what lets a box with one half recorded and the
     * other not survive the round trip as it stands. A conforming writer pairs them; the wire record does not require
     * it, and the file-level union folds each half separately.
     */
    @Test
    void aHalfRecordedZOrMExtentRoundTripsHalfPresent() {
        BoundingBox halfRecorded = new BoundingBox(
                1.0,
                2.0,
                3.0,
                4.0,
                OptionalDouble.of(5.0),
                OptionalDouble.empty(),
                OptionalDouble.empty(),
                OptionalDouble.of(6.0));

        ParquetSchema schema = geometrySchema();
        CompactFooter compact = CompactFooter.encode(geometryFooter(halfRecorded), LeafIndex.of(schema));
        BoundingBox stored = compact.chunk(0, 0).geoExtent().orElseThrow();

        assertThat(stored.zmin())
                .as("a recorded zmin survives without its zmax")
                .hasValue(5.0);
        assertThat(stored.zmax()).as("an unrecorded zmax stays absent").isEmpty();
        assertThat(stored.mmin()).as("an unrecorded mmin stays absent").isEmpty();
        assertThat(stored.mmax())
                .as("a recorded mmax survives without its mmin")
                .hasValue(6.0);
    }

    /**
     * Asserts that every bound read through the blob matches the wire footer, and returns the wire-derived boxes for a
     * caller to make further claims about the fixture itself.
     */
    private Map<ColumnPath, List<Optional<BoundingBox>>> assertNativeBoundsMatchTheWireFooter(Path file) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            FileMetaData wire = ParquetFormat.readFooter(source);
            ParquetSchema schema = ParquetFileReader.open(source).schema();
            LeafIndex leaves = LeafIndex.of(schema);
            CompactFooter compact = CompactFooter.encode(wire, leaves);

            SpatialBoundsSource fromBlob = SpatialBoundsSource.of(compact, leaves, schema, Optional.empty());
            Map<ColumnPath, List<Optional<BoundingBox>>> fromWire = nativeBoxesOffTheWireFooter(wire);

            assertThat(fromWire)
                    .as("the fixture must exercise the native geospatial tier")
                    .isNotEmpty();
            fromWire.forEach((geometry, wireBoxes) -> assertBoundsMatchWire(fromBlob, geometry, wireBoxes));
            return fromWire;
        }
    }

    /** Four ISO PointZM geometries over two row groups, written through the writer's geospatial statistics path. */
    private Path writePointZmFixture() throws IOException {
        ColumnPath geometry = ColumnPath.of("geometry");
        SchemaNode.Primitive leaf = new SchemaNode.Primitive(
                "geometry", Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        ParquetSchema schema = new ParquetSchema(
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(leaf), Optional.empty(), -1));
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .rowGroupSize(RowGroupSize.rows(2))
                .crsEpsg("geometry", 4326)
                .build();

        Path file = tempDir.resolve("point-zm.parquet");
        try (OutputStream out = Files.newOutputStream(file);
                ParquetFileWriter writer = ParquetFileWriter.create(out, schema, options)) {
            ParquetRecordBatchBuilder appender = writer.appender(1);
            for (int i = 0; i < 4; i++) {
                MemorySegment wkb = MemorySegment.ofArray(isoPointZm(i, 2.0 * i, 10.0 + i, 100.0 + i));
                WriteFixtures.appendRow(appender, schema, Map.of(geometry, wkb));
            }
        }
        return file;
    }

    /** ISO WKB PointZM (type code 3001), which JTS cannot emit: its writer flags Z and M via EWKB high bits. */
    private static byte[] isoPointZm(double x, double y, double z, double m) {
        ByteBuffer buffer = ByteBuffer.allocate(37).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 1);
        buffer.putInt(3001);
        buffer.putDouble(x);
        buffer.putDouble(y);
        buffer.putDouble(z);
        buffer.putDouble(m);
        return buffer.array();
    }

    /** The walk that the native tier ran over the wire footer before it read the blob instead. */
    private static Map<ColumnPath, List<Optional<BoundingBox>>> nativeBoxesOffTheWireFooter(FileMetaData wire) {
        List<RowGroup> rowGroups = wire.rowGroups();
        Map<ColumnPath, List<Optional<BoundingBox>>> perColumn = new LinkedHashMap<>();
        for (int i = 0; i < rowGroups.size(); i++) {
            for (ColumnChunk chunk : rowGroups.get(i).columns()) {
                recordWireChunk(chunk, i, rowGroups.size(), perColumn);
            }
        }
        perColumn.entrySet().removeIf(e -> e.getValue().stream().allMatch(Optional::isEmpty));
        return perColumn;
    }

    private static void recordWireChunk(
            ColumnChunk chunk,
            int rowGroupIndex,
            int rowGroupCount,
            Map<ColumnPath, List<Optional<BoundingBox>>> perColumn) {
        Optional<ColumnMetaData> meta = chunk.metaData();
        if (meta.isEmpty()) {
            return;
        }
        ColumnMetaData m = meta.orElseThrow();
        Optional<BoundingBox> bbox = m.geospatialStatistics().flatMap(GeospatialStatistics::bbox);
        if (bbox.isEmpty()) {
            return;
        }
        ColumnPath path = ColumnPath.of(m.pathInSchema());
        List<Optional<BoundingBox>> slots = perColumn.computeIfAbsent(path, _ -> emptySlots(rowGroupCount));
        slots.set(rowGroupIndex, bbox);
    }

    private static List<Optional<BoundingBox>> emptySlots(int count) {
        List<Optional<BoundingBox>> slots = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            slots.add(Optional.empty());
        }
        return slots;
    }

    private static void assertBoundsMatchWire(
            SpatialBoundsSource fromBlob, ColumnPath geometry, List<Optional<BoundingBox>> wireBoxes) {
        String column = geometry.dot();
        for (int rg = 0; rg < wireBoxes.size(); rg++) {
            assertSameExtent(fromBlob.rowGroupBounds(geometry, rg), wireBoxes.get(rg), column + " row group " + rg);
        }
        assertSameExtent(fromBlob.fileBounds(geometry), unionOf(wireBoxes), column + " file bounds");
    }

    private static Optional<BoundingBox> unionOf(List<Optional<BoundingBox>> boxes) {
        BoundingBox union = null;
        for (Optional<BoundingBox> box : boxes) {
            if (box.isPresent()) {
                union = union == null ? box.orElseThrow() : union(union, box.orElseThrow());
            }
        }
        return Optional.ofNullable(union);
    }

    /** The union policy of the native tier: element-wise on X and Y, and on each optional half that either side has. */
    private static BoundingBox union(BoundingBox a, BoundingBox b) {
        return new BoundingBox(
                Math.min(a.xmin(), b.xmin()),
                Math.max(a.xmax(), b.xmax()),
                Math.min(a.ymin(), b.ymin()),
                Math.max(a.ymax(), b.ymax()),
                minOptional(a.zmin(), b.zmin()),
                maxOptional(a.zmax(), b.zmax()),
                minOptional(a.mmin(), b.mmin()),
                maxOptional(a.mmax(), b.mmax()));
    }

    private static OptionalDouble minOptional(OptionalDouble a, OptionalDouble b) {
        if (a.isEmpty() || b.isEmpty()) {
            return a.isEmpty() ? b : a;
        }
        return OptionalDouble.of(Math.min(a.getAsDouble(), b.getAsDouble()));
    }

    private static OptionalDouble maxOptional(OptionalDouble a, OptionalDouble b) {
        if (a.isEmpty() || b.isEmpty()) {
            return a.isEmpty() ? b : a;
        }
        return OptionalDouble.of(Math.max(a.getAsDouble(), b.getAsDouble()));
    }

    /** Extent equality is component-wise, down to which optional halves are present. */
    private static void assertSameExtent(Optional<BoundingBox> actual, Optional<BoundingBox> expected, String where) {
        assertThat(actual.isPresent()).as("%s presence", where).isEqualTo(expected.isPresent());
        if (expected.isEmpty()) {
            return;
        }
        BoundingBox read = actual.orElseThrow();
        BoundingBox wire = expected.orElseThrow();
        assertThat(read.xmin()).as("%s xmin", where).isEqualTo(wire.xmin());
        assertThat(read.xmax()).as("%s xmax", where).isEqualTo(wire.xmax());
        assertThat(read.ymin()).as("%s ymin", where).isEqualTo(wire.ymin());
        assertThat(read.ymax()).as("%s ymax", where).isEqualTo(wire.ymax());
        assertThat(read.zmin()).as("%s zmin", where).isEqualTo(wire.zmin());
        assertThat(read.zmax()).as("%s zmax", where).isEqualTo(wire.zmax());
        assertThat(read.mmin()).as("%s mmin", where).isEqualTo(wire.mmin());
        assertThat(read.mmax()).as("%s mmax", where).isEqualTo(wire.mmax());
    }

    @Test
    void theFixtureSetExercisesEveryEncodedShape() {
        Set<EncodedShape> covered = EnumSet.noneOf(EncodedShape.class);
        corpusFiles().forEach(resource -> covered.addAll(shapesEncodedIn(resource)));

        assertThat(covered)
                .as("the fixture set must exercise every shape encoded specially by the blob")
                .containsExactlyInAnyOrder(EncodedShape.values());
    }

    @Test
    void aLeafWithNoChunkInTheRowGroupFailsLoud() {
        FooterMissingAColumn missing = encodeWithoutOneColumn();
        CompactFooter compact = missing.compact();
        int leaf = missing.droppedLeaf();

        assertThatThrownBy(() -> compact.chunk(0, leaf))
                .isInstanceOf(ParquetFormatException.class)
                .hasMessageContainingAll("Row group 0", "leaf ordinal " + leaf);
    }

    @Test
    void aChunkPathUnknownToTheSchemaFailsTheEncode() {
        Path file = TestCorpus.extractFile(ALLTYPES_PLAIN, tempDir);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            FileMetaData wire = ParquetFormat.readFooter(source);
            ParquetSchema schema = ParquetFileReader.open(source).schema();
            RowGroup strayColumn =
                    rowGroupOf(wire.rowGroups().getFirst(), List.of(chunkOf(chunkMetaData("not_in_schema", 1024L))));
            FileMetaData malformed = withRowGroup(wire, strayColumn);
            LeafIndex leaves = LeafIndex.of(schema);

            assertThatThrownBy(() -> CompactFooter.encode(malformed, leaves))
                    .isInstanceOf(ParquetFormatException.class)
                    .hasMessageContaining("not_in_schema");
        }
    }

    @Test
    void twoChunksForTheSameLeafInOneRowGroupFailTheEncode() {
        Path file = TestCorpus.extractFile(ALLTYPES_PLAIN, tempDir);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            FileMetaData wire = ParquetFormat.readFooter(source);
            ParquetSchema schema = ParquetFileReader.open(source).schema();
            RowGroup first = wire.rowGroups().getFirst();
            ColumnChunk repeated = first.columns().getFirst();
            ColumnPath path = pathOf(repeated);
            FileMetaData malformed = withRowGroup(wire, rowGroupOf(first, List.of(repeated, repeated)));
            LeafIndex leaves = LeafIndex.of(schema);

            assertThatThrownBy(() -> CompactFooter.encode(malformed, leaves))
                    .isInstanceOf(ParquetFormatException.class)
                    .hasMessageContainingAll(path.dot(), "row group 0", "appears more than once");
        }
    }

    /**
     * A chunk length that no fetch can span costs that one column, never the file. The encode records the length as
     * unusable and leaves every other column of the row group readable; the failure lands on the reader planning a
     * fetch of the column, which is where the column has a name to report.
     */
    @Test
    void aCompressedSizeTooLargeForTheIntLaneFailsOnlyThatColumnsFetch() {
        Path file = TestCorpus.extractFile(ALLTYPES_PLAIN, tempDir);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            FileMetaData wire = ParquetFormat.readFooter(source);
            ParquetSchema schema = ParquetFileReader.open(source).schema();
            LeafIndex leaves = LeafIndex.of(schema);
            ColumnPath oversized = leaves.pathOf(0);
            ColumnPath intact = leaves.pathOf(1);
            RowGroup malformedGroup = withOversizedChunk(wire.rowGroups().getFirst(), oversized);

            CompactFooter compact = CompactFooter.encode(withRowGroup(wire, malformedGroup), leaves);

            assertThat(compact.chunk(0, 0).compressedSizeBeyondRange())
                    .as("the oversized chunk records a length that no fetch can span")
                    .isTrue();
            assertChunkMatchesWire(
                    compact.chunk(0, 1),
                    metaDataByPath(malformedGroup).get(intact),
                    chunkByPath(malformedGroup).get(intact),
                    "the column beside the oversized one");
            assertThatThrownBy(() -> planWholeChunkFetch(source, schema, compact, oversized))
                    .isInstanceOf(ParquetFormatException.class)
                    .hasMessageContainingAll(oversized.dot(), "row group 0");
        }
    }

    /** The same row group with {@code target}'s chunk claiming a length beyond what a fetch can address. */
    private static RowGroup withOversizedChunk(RowGroup template, ColumnPath target) {
        long tooLarge = Integer.MAX_VALUE + 1L;
        List<ColumnChunk> columns = new ArrayList<>(template.columns());
        for (int i = 0; i < columns.size(); i++) {
            if (pathOf(columns.get(i)).equals(target)) {
                columns.set(i, chunkOf(chunkMetaData(target.dot(), tooLarge)));
            }
        }
        return rowGroupOf(template, columns);
    }

    /**
     * Plans the whole-chunk fetch of {@code column} out of the compact footer's first row group, the read step that
     * first needs the chunk's byte length. No index section is read, and no byte of the file is fetched.
     */
    private static FetchPlan planWholeChunkFetch(
            ByteRangeSource source, ParquetSchema schema, CompactFooter compact, ColumnPath column) {
        RowGroupChunks chunks = RowGroupChunks.of(compact, 0, LeafIndex.of(schema), schema, indexSectionsNotRead());
        ParquetSchema projected = schema.project(Set.of(column));
        RowGroupFetcher fetcher = TestFetchers.over(source, schema, projected, SegmentPool.getDefault());
        return fetcher.planFor(RowGroupSurvivor.full(chunks), Optional.empty());
    }

    private static IndexSectionLoader indexSectionsNotRead() {
        return new IndexSectionLoader() {
            @Override
            public OffsetIndex readOffsetIndex(long offset, int length) {
                throw new AssertionError("planning a whole-chunk fetch reads no index section");
            }

            @Override
            public ColumnIndex readColumnIndex(long offset, int length) {
                throw new AssertionError("planning a whole-chunk fetch reads no index section");
            }

            @Override
            public SplitBlockBloomFilter readBloom(long offset, int length) {
                throw new AssertionError("planning a whole-chunk fetch reads no bloom filter");
            }
        };
    }

    /**
     * A bloom filter is a pruning accelerator, and a file whose writer recorded an unusable length for one is still a
     * readable file. The encode therefore drops the locator instead of failing, which leaves the bloom tier off for
     * that column and every other tier untouched.
     */
    @Test
    void aBloomFilterLengthTooLargeForTheIntLaneDropsTheLocator() {
        Path file = TestCorpus.extractFile(ALLTYPES_PLAIN, tempDir);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            FileMetaData wire = ParquetFormat.readFooter(source);
            ParquetSchema schema = ParquetFileReader.open(source).schema();
            ColumnPath leaf = LeafIndex.of(schema).pathOf(0);
            long tooLarge = Integer.MAX_VALUE + 1L;
            ColumnMetaData meta = ColumnMetaData.builder()
                    .type(PhysicalType.INT32)
                    .codec(CompressionCodec.UNCOMPRESSED)
                    .pathInSchema(List.of(leaf.dot()))
                    .numValues(1L)
                    .totalCompressedSize(64L)
                    .dataPageOffset(4L)
                    .bloomFilterOffset(OptionalLong.of(2048L))
                    .bloomFilterLength(OptionalLong.of(tooLarge))
                    .build();
            FileMetaData malformed =
                    withRowGroup(wire, rowGroupOf(wire.rowGroups().getFirst(), List.of(chunkOf(meta))));

            ChunkMeta chunk =
                    CompactFooter.encode(malformed, LeafIndex.of(schema)).chunk(0, 0);

            assertThat(chunk.bloomFilterOffset())
                    .as("an unusable length leaves the filter unreachable")
                    .isEmpty();
            assertThat(chunk.bloomFilterLength()).as("bloomFilterLength").isEmpty();
        }
    }

    @Test
    void leafIndexReportsUnknownPathsAsMissing() {
        Path file = TestCorpus.extractFile(NESTED_MAPS, tempDir);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetSchema schema = ParquetFileReader.open(source).schema();
            LeafIndex leaves = LeafIndex.of(schema);

            assertThat(leaves.size()).isEqualTo(schema.leafColumns().size());
            assertThat(leaves.ordinalOf(ColumnPath.of("no_such_column"))).isEqualTo(-1);
            for (int leaf = 0; leaf < leaves.size(); leaf++) {
                ColumnPath path = leaves.pathOf(leaf);
                assertThat(leaves.ordinalOf(path))
                        .as("round trip of %s", path.dot())
                        .isEqualTo(leaf);
            }
        }
    }

    // --- per-file comparison against the wire records ---

    private static void assertRowGroupMatchesWire(CompactFooter compact, int rg, RowGroup wireGroup) {
        assertThat(compact.numRows(rg)).as("row group %d numRows", rg).isEqualTo(wireGroup.numRows());
    }

    private static void assertEveryLeafMatchesWire(
            CompactFooter compact, LeafIndex leaves, int rg, RowGroup wireGroup) {
        Map<ColumnPath, ColumnMetaData> wireByPath = metaDataByPath(wireGroup);
        Map<ColumnPath, ColumnChunk> chunkByPath = chunkByPath(wireGroup);
        assertThat(wireByPath)
                .as("row group %d: every wire chunk must land on a distinct schema leaf", rg)
                .hasSize(wireGroup.columns().size());

        for (int leaf = 0; leaf < compact.leafCount(); leaf++) {
            ColumnPath path = leaves.pathOf(leaf);
            ColumnMetaData meta = wireByPath.get(path);
            if (meta == null) {
                assertAbsentChunkFailsLoud(compact, rg, leaf, path);
                continue;
            }
            assertChunkMatchesWire(
                    compact.chunk(rg, leaf), meta, chunkByPath.get(path), "row group " + rg + " leaf " + path.dot());
        }
    }

    private static void assertAbsentChunkFailsLoud(CompactFooter compact, int rg, int leaf, ColumnPath path) {
        assertThatThrownBy(() -> compact.chunk(rg, leaf))
                .as("row group %d has no chunk for %s", rg, path.dot())
                .isInstanceOf(ParquetFormatException.class)
                .hasMessageContainingAll("Row group " + rg, "leaf ordinal " + leaf);
    }

    private static void assertChunkMatchesWire(
            ChunkMeta chunk, ColumnMetaData meta, ColumnChunk wireChunk, String where) {
        assertThat(chunk.dataPageOffset()).as("%s dataPageOffset", where).isEqualTo(meta.dataPageOffset());
        assertThat(chunk.dictionaryPageOffset())
                .as("%s dictionaryPageOffset", where)
                .isEqualTo(realDictionaryPageOffset(meta));
        assertThat(chunk.numValues()).as("%s numValues", where).isEqualTo(meta.numValues());
        assertThat(chunk.totalCompressedSize())
                .as("%s totalCompressedSize", where)
                .isEqualTo((int) meta.totalCompressedSize());
        assertThat(chunk.codec()).as("%s codec", where).isEqualTo(meta.codec());
        assertThat(chunk.type()).as("%s type", where).isEqualTo(meta.type());

        assertColumnIndexMatchesWire(chunk, wireChunk, where);
        assertOffsetIndexMatchesWire(chunk, wireChunk, where);
        assertBloomFilterMatchesWire(chunk, meta, where);
        assertStatisticsMatchWire(chunk, meta, where);
        assertGeoExtentMatchesWire(chunk, meta, where);
        assertChunkStartMatchesWire(chunk, meta, where);
    }

    private static void assertColumnIndexMatchesWire(ChunkMeta chunk, ColumnChunk wireChunk, String where) {
        boolean located = wireChunk.columnIndexOffset().isPresent()
                && wireChunk.columnIndexLength().isPresent();
        if (!located) {
            assertThat(chunk.columnIndexOffset())
                    .as("%s columnIndexOffset", where)
                    .isEmpty();
            return;
        }
        assertThat(chunk.columnIndexOffset())
                .as("%s columnIndexOffset", where)
                .hasValue(wireChunk.columnIndexOffset().getAsLong());
        assertThat(chunk.columnIndexLength())
                .as("%s columnIndexLength", where)
                .isEqualTo(wireChunk.columnIndexLength().getAsInt());
    }

    private static void assertOffsetIndexMatchesWire(ChunkMeta chunk, ColumnChunk wireChunk, String where) {
        boolean located = wireChunk.offsetIndexOffset().isPresent()
                && wireChunk.offsetIndexLength().isPresent();
        if (!located) {
            assertThat(chunk.offsetIndexOffset())
                    .as("%s offsetIndexOffset", where)
                    .isEmpty();
            return;
        }
        assertThat(chunk.offsetIndexOffset())
                .as("%s offsetIndexOffset", where)
                .hasValue(wireChunk.offsetIndexOffset().getAsLong());
        assertThat(chunk.offsetIndexLength())
                .as("%s offsetIndexLength", where)
                .isEqualTo(wireChunk.offsetIndexLength().getAsInt());
    }

    private static void assertBloomFilterMatchesWire(ChunkMeta chunk, ColumnMetaData meta, String where) {
        assertThat(chunk.bloomFilterOffset()).as("%s bloomFilterOffset", where).isEqualTo(meta.bloomFilterOffset());
        OptionalInt expectedLength = meta.bloomFilterLength().isPresent()
                ? OptionalInt.of((int) meta.bloomFilterLength().getAsLong())
                : OptionalInt.empty();
        assertThat(chunk.bloomFilterLength()).as("%s bloomFilterLength", where).isEqualTo(expectedLength);
    }

    private static void assertStatisticsMatchWire(ChunkMeta chunk, ColumnMetaData meta, String where) {
        Optional<Statistics> stats = meta.statistics();
        assertThat(chunk.nullCount())
                .as("%s nullCount", where)
                .isEqualTo(stats.map(Statistics::nullCount).orElse(OptionalLong.empty()));
        assertBytesMatch(chunk.minValue(), stats.map(Statistics::preferredMin), where + " minValue");
        assertBytesMatch(chunk.maxValue(), stats.map(Statistics::preferredMax), where + " maxValue");
    }

    /** Every component of the wire box, down to which optional halves it records. */
    private static void assertGeoExtentMatchesWire(ChunkMeta chunk, ColumnMetaData meta, String where) {
        Optional<BoundingBox> expected = meta.geospatialStatistics().flatMap(GeospatialStatistics::bbox);
        assertSameExtent(chunk.geoExtent(), expected, where + " geoExtent");
    }

    private static void assertChunkStartMatchesWire(ChunkMeta chunk, ColumnMetaData meta, String where) {
        long dictionaryPageOffset = meta.dictionaryPageOffset().orElse(0L);
        boolean dictionaryPrecedesData = dictionaryPageOffset > 0 && dictionaryPageOffset < meta.dataPageOffset();
        long expectedStart = dictionaryPrecedesData ? dictionaryPageOffset : meta.dataPageOffset();
        assertThat(chunk.chunkStart()).as("%s chunkStart", where).isEqualTo(expectedStart);
    }

    private static void assertBytesMatch(Optional<MemorySegment> actual, Optional<MemorySegment> expected, String as) {
        MemorySegment wire = expected.orElse(MemorySegment.NULL);
        if (wire == MemorySegment.NULL) {
            assertThat(actual).as(as).isEmpty();
            return;
        }
        assertThat(actual).as(as).isPresent();
        MemorySegment stored = actual.orElseThrow();
        assertThat(stored.isReadOnly()).as("%s must be read-only", as).isTrue();
        assertThat(stored.toArray(JAVA_BYTE)).as(as).isEqualTo(wire.toArray(JAVA_BYTE));
    }

    // --- fixture-coverage bookkeeping ---

    private enum EncodedShape {
        DICTIONARY_PAGE,
        COLUMN_INDEX,
        OFFSET_INDEX,
        BLOOM_FILTER,
        NESTED_LEAF,
        GEO_BBOX,
        ABSENT_STATISTICS,
        SEVERAL_ROW_GROUPS
    }

    private Set<EncodedShape> shapesEncodedIn(String resource) {
        Path file = TestCorpus.extractFile(resource, tempDir);
        Set<EncodedShape> shapes = EnumSet.noneOf(EncodedShape.class);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            FileMetaData wire = ParquetFormat.readFooter(source);
            ParquetSchema schema = ParquetFileReader.open(source).schema();
            LeafIndex leaves = LeafIndex.of(schema);
            CompactFooter compact = CompactFooter.encode(wire, leaves);
            if (compact.rowGroupCount() > 1) {
                shapes.add(EncodedShape.SEVERAL_ROW_GROUPS);
            }
            for (int rg = 0; rg < compact.rowGroupCount(); rg++) {
                Set<ColumnPath> present =
                        metaDataByPath(wire.rowGroups().get(rg)).keySet();
                for (int leaf = 0; leaf < compact.leafCount(); leaf++) {
                    ColumnPath path = leaves.pathOf(leaf);
                    if (present.contains(path)) {
                        collectShapes(compact.chunk(rg, leaf), path, shapes);
                    }
                }
            }
        }
        return shapes;
    }

    private static void collectShapes(ChunkMeta chunk, ColumnPath path, Set<EncodedShape> shapes) {
        if (chunk.dictionaryPageOffset().isPresent()) {
            shapes.add(EncodedShape.DICTIONARY_PAGE);
        }
        if (chunk.columnIndexOffset().isPresent()) {
            shapes.add(EncodedShape.COLUMN_INDEX);
        }
        if (chunk.offsetIndexOffset().isPresent()) {
            shapes.add(EncodedShape.OFFSET_INDEX);
        }
        if (chunk.bloomFilterOffset().isPresent()) {
            shapes.add(EncodedShape.BLOOM_FILTER);
        }
        if (path.numParts() > 1) {
            shapes.add(EncodedShape.NESTED_LEAF);
        }
        if (chunk.geoExtent().isPresent()) {
            shapes.add(EncodedShape.GEO_BBOX);
        }
        if (chunk.minValue().isEmpty() && chunk.maxValue().isEmpty()) {
            shapes.add(EncodedShape.ABSENT_STATISTICS);
        }
    }

    // --- shared malformed-footer setup ---

    /**
     * A footer whose only row group has lost one of its column chunks, with the ordinal of the leaf left uncovered.
     *
     * @param compact the encoded footer
     * @param droppedLeaf ordinal of the leaf with no chunk, which for row group 0 is also its chunk-table index
     */
    private record FooterMissingAColumn(CompactFooter compact, int droppedLeaf) {}

    private FooterMissingAColumn encodeWithoutOneColumn() {
        Path file = TestCorpus.extractFile(ALLTYPES_PLAIN, tempDir);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            FileMetaData wire = ParquetFormat.readFooter(source);
            ParquetSchema schema = ParquetFileReader.open(source).schema();
            RowGroup first = wire.rowGroups().getFirst();
            ColumnChunk dropped = first.columns().getFirst();
            List<ColumnChunk> remaining =
                    first.columns().subList(1, first.columns().size());
            LeafIndex leaves = LeafIndex.of(schema);
            CompactFooter compact = CompactFooter.encode(withRowGroup(wire, rowGroupOf(first, remaining)), leaves);
            return new FooterMissingAColumn(compact, leaves.ordinalOf(pathOf(dropped)));
        }
    }

    // --- wire-record helpers ---

    private static ColumnPath pathOf(ColumnChunk chunk) {
        return ColumnPath.of(chunk.metaData().orElseThrow().pathInSchema());
    }

    /** Chunks with a native geospatial bounding box, which is one 64-byte geo entry each. */
    private static long geoBboxCount(FileMetaData wire) {
        long count = 0;
        for (RowGroup group : wire.rowGroups()) {
            for (ColumnChunk chunk : group.columns()) {
                if (geoBboxOf(chunk).isPresent()) {
                    count++;
                }
            }
        }
        return count;
    }

    /** Statistics min/max bytes across every chunk, after the min/max precedence rule: the size of the arena. */
    private static long statisticsByteCount(FileMetaData wire) {
        long bytes = 0;
        for (RowGroup group : wire.rowGroups()) {
            for (ColumnChunk chunk : group.columns()) {
                Optional<Statistics> stats = chunk.metaData().orElseThrow().statistics();
                if (stats.isPresent()) {
                    Statistics recorded = stats.orElseThrow();
                    bytes += recorded.preferredMin().byteSize();
                    bytes += recorded.preferredMax().byteSize();
                }
            }
        }
        return bytes;
    }

    private static Optional<BoundingBox> geoBboxOf(ColumnChunk chunk) {
        return chunk.metaData().orElseThrow().geospatialStatistics().flatMap(GeospatialStatistics::bbox);
    }

    private static Map<ColumnPath, ColumnMetaData> metaDataByPath(RowGroup group) {
        Map<ColumnPath, ColumnMetaData> byPath = new HashMap<>();
        for (ColumnChunk chunk : group.columns()) {
            ColumnMetaData meta = chunk.metaData().orElseThrow();
            byPath.put(ColumnPath.of(meta.pathInSchema()), meta);
        }
        return byPath;
    }

    private static Map<ColumnPath, ColumnChunk> chunkByPath(RowGroup group) {
        Map<ColumnPath, ColumnChunk> byPath = new HashMap<>();
        for (ColumnChunk chunk : group.columns()) {
            ColumnMetaData meta = chunk.metaData().orElseThrow();
            byPath.put(ColumnPath.of(meta.pathInSchema()), chunk);
        }
        return byPath;
    }

    /** The dictionary page offset acted on by the read path: a non-positive wire value points at no dictionary page. */
    private static OptionalLong realDictionaryPageOffset(ColumnMetaData meta) {
        long offset = meta.dictionaryPageOffset().orElse(0L);
        return offset > 0 ? OptionalLong.of(offset) : OptionalLong.empty();
    }

    private static FileMetaData withRowGroup(FileMetaData footer, RowGroup replacement) {
        return FileMetaData.builder()
                .version(footer.version())
                .schema(footer.schema())
                .numRows(footer.numRows())
                .rowGroups(List.of(replacement))
                .keyValueMetadata(footer.keyValueMetadata())
                .createdBy(footer.createdBy())
                .build();
    }

    private static RowGroup rowGroupOf(RowGroup template, List<ColumnChunk> columns) {
        return RowGroup.builder()
                .columns(columns)
                .numRows(template.numRows())
                .totalByteSize(template.totalByteSize())
                .totalCompressedSize(template.totalCompressedSize())
                .build();
    }

    private static ColumnChunk chunkOf(ColumnMetaData meta) {
        return ColumnChunk.builder().metaData(Optional.of(meta)).build();
    }

    /** A one-row-group, one-geometry-column footer whose only chunk records {@code bbox} and nothing else. */
    private static FileMetaData geometryFooter(BoundingBox bbox) {
        GeospatialStatistics geospatial =
                GeospatialStatistics.builder().bbox(Optional.of(bbox)).build();
        ColumnMetaData meta = ColumnMetaData.builder()
                .type(PhysicalType.BYTE_ARRAY)
                .codec(CompressionCodec.UNCOMPRESSED)
                .pathInSchema(List.of("geometry"))
                .numValues(1L)
                .totalCompressedSize(1L)
                .dataPageOffset(4L)
                .geospatialStatistics(Optional.of(geospatial))
                .build();
        RowGroup rowGroup = RowGroup.builder().columns(List.of(chunkOf(meta))).build();
        return FileMetaData.builder().version(1).rowGroups(List.of(rowGroup)).build();
    }

    /** The schema that {@link #geometryFooter} is encoded against: one binary geometry leaf at ordinal 0. */
    private static ParquetSchema geometrySchema() {
        SchemaNode.Primitive geometry = new SchemaNode.Primitive(
                "geometry", Repetition.OPTIONAL, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        return new ParquetSchema(
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(geometry), Optional.empty(), -1));
    }

    private static ColumnMetaData chunkMetaData(String dottedPath, long totalCompressedSize) {
        return ColumnMetaData.builder()
                .type(PhysicalType.INT32)
                .codec(CompressionCodec.UNCOMPRESSED)
                .pathInSchema(List.of(dottedPath.split("\\.")))
                .numValues(1L)
                .totalCompressedSize(totalCompressedSize)
                .dataPageOffset(4L)
                .build();
    }
}
