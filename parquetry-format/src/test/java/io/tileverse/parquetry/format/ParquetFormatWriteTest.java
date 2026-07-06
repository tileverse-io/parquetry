/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.format;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;

import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.codec.ParquetFormatDeserializer;
import io.tileverse.parquetry.schema.geo.ParquetCrs;
import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem;
import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystems;
import io.tileverse.parquetry.schema.geo.projjson.GeographicCRS;

/**
 * Round-trip tests for the write path: build a non-default sample of every Thrift record, encode it through
 * {@link ParquetFormat} / {@link ParquetFormatDeserializer}, and verify the parsed record equals the original.
 *
 * <p>Nested records ({@code SchemaElement}, {@code ColumnChunk}, {@code ColumnMetaData}, {@code Statistics},
 * {@code EncodingStats}, {@code SizeStatistics}, {@code KeyValue}, {@code LogicalType}, {@code SortingColumn},
 * {@code ColumnOrder}, {@code BloomFilterHeader}, {@code PageLocation}) are exercised through the top-level wrappers
 * (FileMetaData / OffsetIndex / ColumnIndex / PageHeader); a few of the leaf records are also driven directly via the
 * {@code BloomFilterHeader} helper or by inlining them in dedicated focused tests.
 *
 * <p>Records that hold {@link MemorySegment} fields compare contents (not reference identity) via a custom recursive
 * comparison configuration registered in {@link #recursiveCompare()}.
 */
class ParquetFormatWriteTest {

    private static RecursiveComparisonConfiguration recursiveCompare() {
        // MemorySegment uses reference equality; compare contents byte-for-byte instead.
        RecursiveComparisonConfiguration cfg = new RecursiveComparisonConfiguration();
        cfg.registerEqualsForType(
                (MemorySegment a, MemorySegment b) -> {
                    if (a == b) {
                        return true;
                    }
                    if (a == null || b == null) {
                        return false;
                    }
                    // The "field absent" sentinel is a zero-byte segment regardless of whether the caller used
                    // MemorySegment.NULL, an empty heap segment, or the deserializer's fresh empty native segment.
                    // Treat all zero-byte segments as equivalent; byte-content comparison handles the populated case.
                    if (a.byteSize() != b.byteSize()) {
                        return false;
                    }
                    byte[] ab = a.toArray(JAVA_BYTE);
                    byte[] bb = b.toArray(JAVA_BYTE);
                    for (int i = 0; i < ab.length; i++) {
                        if (ab[i] != bb[i]) {
                            return false;
                        }
                    }
                    return true;
                },
                MemorySegment.class);
        return cfg;
    }

    // -- top-level: FileMetaData --------------------------------------------------------------------

    @Test
    void fileMetaDataRoundTrips() {
        FileMetaData original = sampleFileMetaData();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ParquetFormat.writeFooter(out, original);

        FileMetaData parsed = ParquetFormatDeserializer.readFileMetaData(new ByteArrayInputStream(out.toByteArray()));
        assertThat(parsed).usingRecursiveComparison(recursiveCompare()).isEqualTo(original);
    }

    @Test
    void toBytesProducesSameOutputAsWriteFooter() {
        FileMetaData original = sampleFileMetaData();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ParquetFormat.writeFooter(out, original);

        byte[] viaStream = out.toByteArray();
        byte[] viaConvenience = ParquetFormat.toBytes(original);
        assertThat(viaConvenience).containsExactly(viaStream);
    }

    // -- top-level: PageHeader (V1, V2, dictionary, index) ------------------------------------------

    @Test
    void pageHeaderRoundTripsV1() {
        PageHeader original = new PageHeader(
                PageType.DATA_PAGE,
                4096,
                1024,
                OptionalInt.of(0xdeadbeef),
                Optional.of(sampleDataPageHeader()),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ParquetFormat.writePageHeader(out, original);
        PageHeader parsed = ParquetFormat.readPageHeader(new ByteArrayInputStream(out.toByteArray()));
        assertThat(parsed).usingRecursiveComparison(recursiveCompare()).isEqualTo(original);
    }

    @Test
    void pageHeaderRoundTripsV2() {
        PageHeader original = new PageHeader(
                PageType.DATA_PAGE_V2,
                8192,
                2048,
                OptionalInt.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(sampleDataPageHeaderV2()));

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ParquetFormat.writePageHeader(out, original);
        PageHeader parsed = ParquetFormat.readPageHeader(new ByteArrayInputStream(out.toByteArray()));
        assertThat(parsed).usingRecursiveComparison(recursiveCompare()).isEqualTo(original);
    }

    @Test
    void pageHeaderRoundTripsDictionary() {
        PageHeader original = new PageHeader(
                PageType.DICTIONARY_PAGE,
                512,
                256,
                OptionalInt.of(7),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new DictionaryPageHeader(128, Encoding.PLAIN, true)),
                Optional.empty());

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ParquetFormat.writePageHeader(out, original);
        PageHeader parsed = ParquetFormat.readPageHeader(new ByteArrayInputStream(out.toByteArray()));
        assertThat(parsed).usingRecursiveComparison(recursiveCompare()).isEqualTo(original);
    }

    @Test
    void pageHeaderRoundTripsIndex() {
        PageHeader original = new PageHeader(
                PageType.INDEX_PAGE,
                64,
                32,
                OptionalInt.empty(),
                Optional.empty(),
                Optional.of(new IndexPageHeader()),
                Optional.empty(),
                Optional.empty());

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ParquetFormat.writePageHeader(out, original);
        PageHeader parsed = ParquetFormat.readPageHeader(new ByteArrayInputStream(out.toByteArray()));
        assertThat(parsed).usingRecursiveComparison(recursiveCompare()).isEqualTo(original);
    }

    // -- top-level: ColumnIndex / OffsetIndex -------------------------------------------------------

    @Test
    void columnIndexRoundTrips() {
        ColumnIndex original = sampleColumnIndex();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ParquetFormat.writeColumnIndex(out, original);

        ColumnIndex parsed = ParquetFormatDeserializer.readColumnIndex(new ByteArrayInputStream(out.toByteArray()));
        assertThat(parsed).usingRecursiveComparison(recursiveCompare()).isEqualTo(original);
    }

    @Test
    void offsetIndexRoundTrips() {
        OffsetIndex original = sampleOffsetIndex();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ParquetFormat.writeOffsetIndex(out, original);

        OffsetIndex parsed = ParquetFormatDeserializer.readOffsetIndex(new ByteArrayInputStream(out.toByteArray()));
        assertThat(parsed).usingRecursiveComparison(recursiveCompare()).isEqualTo(original);
    }

    // BloomFilterHeader has no public ParquetFormat.write surface; its round-trip coverage lives in
    // {@code SerializerRoundTripTest} where the package-private serializer is reachable directly.

    // -- focused round-trips that exercise nested-record edge cases ---------------------------------

    @Test
    void geospatialStatisticsRoundTrips() {
        // Drive through FileMetaData so we exercise GeospatialStatistics inside ColumnMetaData.
        GeospatialStatistics gs = new GeospatialStatistics(
                Optional.of(new BoundingBox(
                        -180.0,
                        180.0,
                        -85.0,
                        85.0,
                        OptionalDouble.of(0.0),
                        OptionalDouble.of(10000.0),
                        OptionalDouble.of(1.0),
                        OptionalDouble.of(2.0))),
                Optional.of(List.of(1, 2, 3, 4, 5, 6, 7)));

        FileMetaData original = footerWithGeospatialStatistics(gs);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ParquetFormat.writeFooter(out, original);

        FileMetaData parsed = ParquetFormatDeserializer.readFileMetaData(new ByteArrayInputStream(out.toByteArray()));
        assertThat(parsed).usingRecursiveComparison(recursiveCompare()).isEqualTo(original);
    }

    @Test
    void statisticsRoundTripsAllOptionalsPresent() {
        // Drive through PageHeader / DataPageHeader so the Statistics serializer exercises every optional field.
        Statistics stats = new Statistics(
                heapSegment(new byte[] {1, 2, 3}),
                heapSegment(new byte[] {0, 0, 1}),
                OptionalLong.of(7L),
                OptionalLong.of(15L),
                heapSegment(new byte[] {2, 3, 4}),
                heapSegment(new byte[] {0, 1, 2}),
                true,
                true);
        PageHeader original = new PageHeader(
                PageType.DATA_PAGE,
                4096,
                1024,
                OptionalInt.of(42),
                Optional.of(new DataPageHeader(
                        1000, Encoding.PLAIN_DICTIONARY, Encoding.RLE, Encoding.BIT_PACKED, Optional.of(stats))),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ParquetFormat.writePageHeader(out, original);
        PageHeader parsed = ParquetFormat.readPageHeader(new ByteArrayInputStream(out.toByteArray()));
        assertThat(parsed).usingRecursiveComparison(recursiveCompare()).isEqualTo(original);
    }

    @Test
    void logicalTypeRoundTripsAllPayloadlessVariants() {
        // Wrap each LogicalType case in a SchemaElement -> FileMetaData; verify the union decodes back identically.
        List<LogicalType> variants = List.of(
                new LogicalType.StringType(),
                new LogicalType.MapType(),
                new LogicalType.ListType(),
                new LogicalType.EnumType(),
                new LogicalType.DateType(),
                new LogicalType.UnknownType(),
                new LogicalType.JsonType(),
                new LogicalType.BsonType(),
                new LogicalType.UuidType(),
                new LogicalType.Float16Type(),
                new LogicalType.Variant());

        for (LogicalType lt : variants) {
            FileMetaData original = footerWithLogicalType(lt);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ParquetFormat.writeFooter(out, original);
            FileMetaData parsed =
                    ParquetFormatDeserializer.readFileMetaData(new ByteArrayInputStream(out.toByteArray()));
            assertThat(parsed).usingRecursiveComparison(recursiveCompare()).isEqualTo(original);
        }
    }

    @Test
    void logicalTypeRoundTripsPayloadVariants() {
        List<LogicalType> variants = List.of(
                new LogicalType.Decimal(2, 18),
                new LogicalType.Time(true, LogicalType.TimeUnit.MICROS),
                new LogicalType.Timestamp(false, LogicalType.TimeUnit.NANOS),
                new LogicalType.IntType((byte) 32, true),
                // Geometry / Geography with empty CRS exercise the field-absent path on the CRS field.
                new LogicalType.Geometry(Optional.empty()),
                new LogicalType.Geography(Optional.empty(), Optional.of(EdgeInterpolationAlgorithm.KARNEY)));

        for (LogicalType lt : variants) {
            FileMetaData original = footerWithLogicalType(lt);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ParquetFormat.writeFooter(out, original);
            FileMetaData parsed =
                    ParquetFormatDeserializer.readFileMetaData(new ByteArrayInputStream(out.toByteArray()));
            assertThat(parsed).usingRecursiveComparison(recursiveCompare()).isEqualTo(original);
        }
    }

    @Test
    void logicalTypeGeometryRoundTripsWithBundledCrs() {
        // OGC:CRS84 deserialised from the bundle round-trips through the LogicalType.Geometry CRS field. This exercises
        // the symmetric PROJJSON serializer landed alongside the CRS facade work - the earlier serializer caveat
        // ("concrete CRS records degrade
        // to Unknown on read-back") no longer applies.
        CoordinateReferenceSystem ogcCrs84 = CoordinateReferenceSystems.ogcCrs84();
        LogicalType.Geometry lt = new LogicalType.Geometry(Optional.of(ParquetCrs.inline(ogcCrs84)));

        FileMetaData original = footerWithLogicalType(lt);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ParquetFormat.writeFooter(out, original);
        FileMetaData parsed = ParquetFormatDeserializer.readFileMetaData(new ByteArrayInputStream(out.toByteArray()));

        assertThat(parsed).usingRecursiveComparison(recursiveCompare()).isEqualTo(original);
        SchemaElement leaf = parsed.schema().get(1);
        assertThat(leaf.logicalType())
                .hasValueSatisfying(logical -> assertThat(logical)
                        .isInstanceOfSatisfying(
                                LogicalType.Geometry.class,
                                g -> assertThat(g.crs())
                                        .hasValueSatisfying(crs -> assertThat(crs)
                                                .as("CRS reads back as inline PROJJSON, not Unknown")
                                                .isInstanceOfSatisfying(
                                                        ParquetCrs.Inline.class,
                                                        inline -> assertThat(inline.projjson())
                                                                .isInstanceOf(GeographicCRS.class)))));
    }

    @Test
    void logicalTypeGeographyRoundTripsWithBundledCrs() {
        // EPSG:4326 from the bundle exercises the Geography logical type's CRS round-trip alongside its algorithm.
        CoordinateReferenceSystem wgs84 =
                CoordinateReferenceSystems.forEpsg(4326).orElseThrow();
        LogicalType.Geography lt = new LogicalType.Geography(
                Optional.of(ParquetCrs.inline(wgs84)), Optional.of(EdgeInterpolationAlgorithm.SPHERICAL));

        FileMetaData original = footerWithLogicalType(lt);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ParquetFormat.writeFooter(out, original);
        FileMetaData parsed = ParquetFormatDeserializer.readFileMetaData(new ByteArrayInputStream(out.toByteArray()));

        assertThat(parsed).usingRecursiveComparison(recursiveCompare()).isEqualTo(original);
    }

    @Test
    void logicalTypeGeometryRoundTripsNativeCrsReferenceForms() {
        // The native Parquet crs property may be an authority:code or srid: reference (GeoParquet 2.0). Confirm both
        // persist to the footer and parse back to the same ParquetCrs record, not just inline PROJJSON.
        LogicalType.Geometry authorityCode =
                new LogicalType.Geometry(Optional.of(new ParquetCrs.AuthorityCode("EPSG", "3857")));
        assertNativeCrsRoundTrips(authorityCode, new ParquetCrs.AuthorityCode("EPSG", "3857"));

        LogicalType.Geometry srid = new LogicalType.Geometry(Optional.of(new ParquetCrs.Srid(0)));
        assertNativeCrsRoundTrips(srid, new ParquetCrs.Srid(0));
    }

    private static void assertNativeCrsRoundTrips(LogicalType.Geometry lt, ParquetCrs expected) {
        FileMetaData original = footerWithLogicalType(lt);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ParquetFormat.writeFooter(out, original);
        FileMetaData parsed = ParquetFormatDeserializer.readFileMetaData(new ByteArrayInputStream(out.toByteArray()));

        SchemaElement leaf = parsed.schema().get(1);
        assertThat(leaf.logicalType())
                .hasValueSatisfying(logical -> assertThat(logical)
                        .isInstanceOfSatisfying(
                                LogicalType.Geometry.class,
                                g -> assertThat(g.crs()).hasValue(expected)));
    }

    @Test
    void schemaElementWithAllOptionalsRoundTrips() {
        // The default leaf/root helpers leave typeLength (2), convertedType (6), scale (7), precision (8) empty.
        // A DECIMAL column exercises all four together, plus logicalType (10) and fieldId (11).
        SchemaElement decimalLeaf = new SchemaElement(
                Optional.of(PhysicalType.FIXED_LEN_BYTE_ARRAY),
                OptionalInt.of(16),
                Optional.of(FieldRepetitionType.REQUIRED),
                "amount",
                OptionalInt.empty(),
                Optional.of(ConvertedType.DECIMAL),
                OptionalInt.of(2),
                OptionalInt.of(18),
                Optional.of(new LogicalType.Decimal(2, 18)),
                OptionalInt.of(42));

        FileMetaData original = FileMetaData.builder()
                .version(2)
                .schema(List.of(rootSchema(1), decimalLeaf))
                .numRows(0L)
                .rowGroups(List.of())
                .keyValueMetadata(List.of())
                .build();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ParquetFormat.writeFooter(out, original);
        FileMetaData parsed = ParquetFormatDeserializer.readFileMetaData(new ByteArrayInputStream(out.toByteArray()));
        assertThat(parsed).usingRecursiveComparison(recursiveCompare()).isEqualTo(original);

        // Spot-check the decimal leaf survived intact.
        SchemaElement parsedLeaf = parsed.schema().get(1);
        assertThat(parsedLeaf.typeLength()).hasValue(16);
        assertThat(parsedLeaf.convertedType()).contains(ConvertedType.DECIMAL);
        assertThat(parsedLeaf.scale()).hasValue(2);
        assertThat(parsedLeaf.precision()).hasValue(18);
    }

    @Test
    void columnChunkWithEncryptedColumnMetadataRoundTrips() {
        // ColumnChunk.encryptedColumnMetadata (Thrift field 9, BINARY) is conditionally emitted only when non-NULL.
        // The default fixture leaves it at MemorySegment.NULL; this test exercises the populated path.
        byte[] encrypted = new byte[] {(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF, 0x01, 0x02, 0x03};

        ColumnMetaData cmd = ColumnMetaData.builder()
                .type(PhysicalType.INT32)
                .encodings(List.of(Encoding.PLAIN))
                .pathInSchema(List.of("c1"))
                .codec(CompressionCodec.UNCOMPRESSED)
                .numValues(0L)
                .totalUncompressedSize(0L)
                .totalCompressedSize(0L)
                .keyValueMetadata(List.of())
                .dataPageOffset(4L)
                .encodingStats(List.of())
                .build();

        ColumnChunk chunk = ColumnChunk.builder()
                .filePath(Optional.empty())
                .fileOffset(0L)
                .metaData(Optional.of(cmd))
                .encryptedColumnMetadata(heapSegment(encrypted))
                .build();

        RowGroup rg = RowGroup.builder()
                .columns(List.of(chunk))
                .totalByteSize(0L)
                .numRows(0L)
                .build();

        FileMetaData original = FileMetaData.builder()
                .version(2)
                .schema(List.of(rootSchema(1), leafSchema("c1", PhysicalType.INT32)))
                .numRows(0L)
                .rowGroups(List.of(rg))
                .keyValueMetadata(List.of())
                .build();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ParquetFormat.writeFooter(out, original);
        FileMetaData parsed = ParquetFormatDeserializer.readFileMetaData(new ByteArrayInputStream(out.toByteArray()));
        assertThat(parsed).usingRecursiveComparison(recursiveCompare()).isEqualTo(original);

        // Spot-check the encrypted blob made the round trip.
        MemorySegment parsedBlob =
                parsed.rowGroups().getFirst().columns().getFirst().encryptedColumnMetadata();
        assertThat(parsedBlob.toArray(JAVA_BYTE)).isEqualTo(encrypted);
    }

    @Test
    void columnOrderRoundTrips() {
        // ColumnOrder is reached through FileMetaData.columnOrders; exercise the only case (TypeDefined).
        FileMetaData original = FileMetaData.builder()
                .version(2)
                .schema(List.of(rootSchema(1), leafSchema("c1", PhysicalType.INT64)))
                .numRows(0L)
                .rowGroups(List.of())
                .keyValueMetadata(List.of())
                .columnOrders(Optional.of(List.of(new ColumnOrder.TypeDefined())))
                .build();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ParquetFormat.writeFooter(out, original);
        FileMetaData parsed = ParquetFormatDeserializer.readFileMetaData(new ByteArrayInputStream(out.toByteArray()));
        assertThat(parsed).usingRecursiveComparison(recursiveCompare()).isEqualTo(original);
    }

    // -- sample builders ---------------------------------------------------------------------------

    /**
     * Builds a realistic, every-field-populated {@link FileMetaData} that exercises both the field-id-delta
     * optimisation (consecutive ids) and the explicit-id fallback (skipped fields). The schema has a 1-leaf shape;
     * column metadata covers Statistics, EncodingStats, SizeStatistics, GeospatialStatistics, and key/value entries.
     */
    private static FileMetaData sampleFileMetaData() {
        ColumnMetaData cmd = ColumnMetaData.builder()
                .type(PhysicalType.BYTE_ARRAY)
                .encodings(List.of(Encoding.PLAIN, Encoding.RLE_DICTIONARY))
                .pathInSchema(List.of("col1"))
                .codec(CompressionCodec.SNAPPY)
                .numValues(1000L)
                .totalUncompressedSize(8192L)
                .totalCompressedSize(2048L)
                .keyValueMetadata(List.of(new KeyValue("k1", Optional.of("v1")), new KeyValue("k2", Optional.empty())))
                .dataPageOffset(4L)
                .indexPageOffset(OptionalLong.of(2000L))
                .dictionaryPageOffset(OptionalLong.of(8L))
                .statistics(Optional.of(sampleStatistics()))
                .encodingStats(List.of(
                        new EncodingStats(PageType.DATA_PAGE, Encoding.RLE_DICTIONARY, 10),
                        new EncodingStats(PageType.DICTIONARY_PAGE, Encoding.PLAIN, 1)))
                .bloomFilterOffset(OptionalLong.of(3000L))
                .bloomFilterLength(OptionalLong.of(512L))
                .sizeStatistics(Optional.of(new SizeStatistics(
                        OptionalLong.of(50000L), Optional.of(List.of(0L, 0L, 5L)), Optional.of(List.of(1L, 2L, 3L)))))
                .build();

        ColumnChunk chunk = ColumnChunk.builder()
                .filePath(Optional.of("external.parquet"))
                .fileOffset(0L)
                .metaData(Optional.of(cmd))
                .offsetIndexOffset(OptionalLong.of(4096L))
                .offsetIndexLength(OptionalInt.of(64))
                .columnIndexOffset(OptionalLong.of(4160L))
                .columnIndexLength(OptionalInt.of(96))
                .build();

        RowGroup rg = RowGroup.builder()
                .columns(List.of(chunk))
                .totalByteSize(8192L)
                .numRows(1000L)
                .sortingColumns(Optional.of(List.of(new RowGroup.SortingColumn(0, false, true))))
                .fileOffset(OptionalLong.of(4L))
                .totalCompressedSize(OptionalLong.of(2048L))
                .ordinal(OptionalInt.of(0))
                .build();

        return FileMetaData.builder()
                .version(2)
                .schema(List.of(rootSchema(1), leafSchema("col1", PhysicalType.BYTE_ARRAY)))
                .numRows(1000L)
                .rowGroups(List.of(rg))
                .keyValueMetadata(List.of(new KeyValue("created.by", Optional.of("parquetry-test"))))
                .createdBy(Optional.of("parquetry-format write test"))
                .columnOrders(Optional.of(List.of(new ColumnOrder.TypeDefined())))
                .build();
    }

    private static Statistics sampleStatistics() {
        return new Statistics(
                heapSegment(new byte[] {7, 7, 7}),
                heapSegment(new byte[] {1}),
                OptionalLong.of(42L),
                OptionalLong.of(900L),
                heapSegment(new byte[] {8}),
                heapSegment(new byte[] {2}),
                true,
                false);
    }

    private static DataPageHeader sampleDataPageHeader() {
        return new DataPageHeader(
                1000,
                Encoding.PLAIN_DICTIONARY,
                Encoding.RLE,
                Encoding.BIT_PACKED,
                Optional.of(new Statistics(
                        MemorySegment.NULL,
                        MemorySegment.NULL,
                        OptionalLong.of(2L),
                        OptionalLong.empty(),
                        heapSegment(new byte[] {9, 9}),
                        heapSegment(new byte[] {0, 0}),
                        true,
                        false)));
    }

    private static DataPageHeaderV2 sampleDataPageHeaderV2() {
        return new DataPageHeaderV2(
                1024,
                12,
                512,
                Encoding.DELTA_BINARY_PACKED,
                32,
                16,
                false,
                Optional.of(new Statistics(
                        MemorySegment.NULL,
                        MemorySegment.NULL,
                        OptionalLong.of(0L),
                        OptionalLong.of(1024L),
                        heapSegment(new byte[] {1, 2}),
                        heapSegment(new byte[] {0, 1}),
                        false,
                        true)));
    }

    private static ColumnIndex sampleColumnIndex() {
        return new ColumnIndex(
                List.of(false, false, true, false),
                List.of(
                        heapSegment(new byte[] {0, 0, 0, 1}),
                        heapSegment(new byte[] {0, 0, 0, 2}),
                        heapSegment(new byte[0]),
                        heapSegment(new byte[] {0, 0, 0, 4})),
                List.of(
                        heapSegment(new byte[] {0, 0, 1, 0}),
                        heapSegment(new byte[] {0, 0, 2, 0}),
                        heapSegment(new byte[0]),
                        heapSegment(new byte[] {0, 0, 4, 0})),
                BoundaryOrder.ASCENDING,
                Optional.of(List.of(0L, 1L, 10L, 0L)),
                Optional.of(List.of(0L, 0L, 0L, 0L, 1L, 2L, 3L, 4L)),
                Optional.of(List.of(0L, 1L, 1L, 2L, 2L, 3L, 3L, 4L)));
    }

    private static OffsetIndex sampleOffsetIndex() {
        return new OffsetIndex(
                List.of(
                        new PageLocation(4L, 1024, 0L),
                        new PageLocation(1028L, 1024, 100L),
                        new PageLocation(2052L, 512, 200L)),
                Optional.of(List.of(8192L, 8192L, 4096L)));
    }

    /**
     * Wraps a single {@link LogicalType} in a SchemaElement-bearing FileMetaData so the round-trip exercises the
     * codec's union-encoding path.
     */
    private static FileMetaData footerWithLogicalType(LogicalType lt) {
        return FileMetaData.builder()
                .version(2)
                .schema(List.of(
                        rootSchema(1),
                        new SchemaElement(
                                Optional.of(PhysicalType.BYTE_ARRAY),
                                OptionalInt.empty(),
                                Optional.of(FieldRepetitionType.OPTIONAL),
                                "leaf",
                                OptionalInt.empty(),
                                Optional.empty(),
                                OptionalInt.empty(),
                                OptionalInt.empty(),
                                Optional.of(lt),
                                OptionalInt.of(7))))
                .numRows(0L)
                .rowGroups(List.of())
                .build();
    }

    /** Wraps a {@link GeospatialStatistics} inside a full column-metadata footer for end-to-end round-tripping. */
    private static FileMetaData footerWithGeospatialStatistics(GeospatialStatistics gs) {
        ColumnMetaData cmd = ColumnMetaData.builder()
                .type(PhysicalType.BYTE_ARRAY)
                .encodings(List.of(Encoding.PLAIN))
                .pathInSchema(List.of("geom"))
                .codec(CompressionCodec.UNCOMPRESSED)
                .numValues(10L)
                .totalUncompressedSize(1024L)
                .totalCompressedSize(1024L)
                .dataPageOffset(4L)
                .geospatialStatistics(Optional.of(gs))
                .build();
        ColumnChunk chunk =
                ColumnChunk.builder().fileOffset(0L).metaData(Optional.of(cmd)).build();
        RowGroup rg = RowGroup.builder()
                .columns(List.of(chunk))
                .totalByteSize(1024L)
                .numRows(10L)
                .build();
        return FileMetaData.builder()
                .version(2)
                .schema(List.of(rootSchema(1), leafSchema("geom", PhysicalType.BYTE_ARRAY)))
                .numRows(10L)
                .rowGroups(List.of(rg))
                .build();
    }

    private static SchemaElement rootSchema(int numChildren) {
        return new SchemaElement(
                Optional.empty(),
                OptionalInt.empty(),
                Optional.empty(),
                "root",
                OptionalInt.of(numChildren),
                Optional.empty(),
                OptionalInt.empty(),
                OptionalInt.empty(),
                Optional.empty(),
                OptionalInt.empty());
    }

    private static SchemaElement leafSchema(String name, PhysicalType type) {
        return new SchemaElement(
                Optional.of(type),
                OptionalInt.empty(),
                Optional.of(FieldRepetitionType.OPTIONAL),
                name,
                OptionalInt.empty(),
                Optional.empty(),
                OptionalInt.empty(),
                OptionalInt.empty(),
                Optional.empty(),
                OptionalInt.empty());
    }

    private static MemorySegment heapSegment(byte[] bytes) {
        return MemorySegment.ofArray(bytes).asReadOnly();
    }
}
