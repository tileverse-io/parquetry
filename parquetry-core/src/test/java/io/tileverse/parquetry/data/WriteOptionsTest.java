/*
 * Copyright (c) 2026 Tileverse.io
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.tileverse.parquetry.data.WriteOptions.BloomFilterConfig;
import io.tileverse.parquetry.data.WriteOptions.EncodingPolicy;
import io.tileverse.parquetry.data.WriteOptions.GeoParquetMetadataMode;
import io.tileverse.parquetry.data.WriteOptions.ParquetVersion;
import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.DataPageHeader;
import io.tileverse.parquetry.format.DataPageHeaderV2;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.PageHeader;
import io.tileverse.parquetry.format.PageType;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem;
import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystems;

/**
 * Coverage for {@link WriteOptions} and the five value types it nests: {@link ParquetVersion},
 * {@link GeoParquetMetadataMode}, {@link RowGroupSize}, {@link EncodingPolicy}, {@link BloomFilterConfig}.
 *
 * <ul>
 *   <li>Top-level tests cover the {@link WriteOptions} record, its {@link WriteOptions.Builder}, and builder
 *       normalization rules.
 *   <li>{@link ParquetVersionTests}, {@link GeoParquetMetadataModeTests}, {@link RowGroupSizeTests},
 *       {@link BloomFilterConfigTests} cover the nested value types' shape, validation, and equality.
 *   <li>{@link EncodingPolicyTests} covers the sealed-interface singleton constants and the writer's actual encoding
 *       behavior end-to-end against the page-level binary output.
 * </ul>
 */
class WriteOptionsTest {

    @Test
    void defaultsAreV2ZstdAdaptive() {
        WriteOptions o = WriteOptions.defaults();

        assertThat(o.parquetVersion()).isEqualTo(ParquetVersion.V2_0);
        assertThat(o.geoParquetMetadata()).isEqualTo(GeoParquetMetadataMode.DUAL_V1_1_AND_V2_0);
        assertThat(o.defaultCompression()).isInstanceOf(Compression.Zstd.class);
        assertThat(((Compression.Zstd) o.defaultCompression()).level()).isEqualTo(3);
        assertThat(o.rowGroupSize()).isInstanceOf(RowGroupSize.Adaptive.class);
        assertThat(o.expectedRowCount()).isEmpty();
        assertThat(o.dictionaryByteLimit()).isEqualTo(1 << 20);
        assertThat(o.pageByteLimit()).isEqualTo(64 * 1024);
        assertThat(o.pageValueLimit()).isEqualTo(8_192);
        assertThat(o.progressListener()).isEmpty();
        assertThat(o.progressListenerCadenceRows()).isEqualTo(100_000L);
        assertThat(o.tempDir()).isEqualTo(Path.of(System.getProperty("java.io.tmpdir")));
        assertThat(o.compression()).isEmpty();
        assertThat(o.encodingPolicies()).isEmpty();
        assertThat(o.indexedColumns()).isEmpty();
        assertThat(o.bloomFilters()).isEmpty();
        assertThat(o.crs()).isEmpty();
        assertThat(o.keyValueMetadata()).isEmpty();
    }

    @Test
    void v1ModeForcesV1_1_ONLYGeoMetadata() {
        WriteOptions o = WriteOptions.builder()
                .parquetVersion(ParquetVersion.V1_1)
                .geoParquetMetadata(GeoParquetMetadataMode.DUAL_V1_1_AND_V2_0)
                .build();

        assertThat(o.geoParquetMetadata()).isEqualTo(GeoParquetMetadataMode.V1_1_ONLY);
    }

    @Test
    void v1ModeOverridesV2_0_ONLY() {
        WriteOptions o = WriteOptions.builder()
                .geoParquetMetadata(GeoParquetMetadataMode.V2_0_ONLY)
                .parquetVersion(ParquetVersion.V1_1)
                .build();

        assertThat(o.geoParquetMetadata()).isEqualTo(GeoParquetMetadataMode.V1_1_ONLY);
    }

    @Test
    void crsEpsgThrowsForUnknownCode() {
        WriteOptions.Builder b = WriteOptions.builder();

        assertThatThrownBy(() -> b.crsEpsg("geom", 99_999_999))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("99999999");
    }

    @Test
    void crsEpsgPopulatesFromBundle() {
        WriteOptions o = WriteOptions.builder().crsEpsg("geom", 4326).build();

        assertThat(o.crs()).containsKey("geom");
        assertThat(o.crs().get("geom")).isNotNull();
    }

    @Test
    void crsStringEagerlyValidatesProjjson() {
        WriteOptions.Builder b = WriteOptions.builder();

        assertThatThrownBy(() -> b.crs("geom", "not json"))
                .isInstanceOfAny(IllegalArgumentException.class, RuntimeException.class);
    }

    @Test
    void crsStringPopulatesFromValidProjjson() {
        String projjson = CoordinateReferenceSystems.toProjjson(CoordinateReferenceSystems.ogcCrs84());

        WriteOptions o = WriteOptions.builder().crs("geom", projjson).build();

        assertThat(o.crs()).containsKey("geom");
    }

    @Test
    void crsTypedAcceptsCoordinateReferenceSystemInstance() {
        CoordinateReferenceSystem crs84 = CoordinateReferenceSystems.ogcCrs84();

        WriteOptions o = WriteOptions.builder().crs("geom", crs84).build();

        assertThat(o.crs()).containsEntry("geom", crs84);
    }

    @Test
    void perColumnCompressionOverridesDefault() {
        WriteOptions o = WriteOptions.builder()
                .defaultCompression(Compression.zstd(3))
                .compression("geometry", Compression.lz4Raw())
                .build();

        assertThat(o.compression()).containsEntry("geometry", new Compression.Lz4Raw());
    }

    @Test
    void indexedColumnsAccumulatesAcrossCalls() {
        WriteOptions o = WriteOptions.builder()
                .indexedColumns("a", "b")
                .indexedColumns("c", "d")
                .build();

        assertThat(o.indexedColumns()).containsExactlyInAnyOrder("a", "b", "c", "d");
    }

    @Test
    void invalidPrimitiveLimitsRejected() {
        WriteOptions.Builder b = WriteOptions.builder();

        assertThatThrownBy(() -> b.pageValueLimit(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> b.pageValueLimit(-1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> b.pageByteLimit(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> b.pageByteLimit(-1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> b.dictionaryByteLimit(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> b.dictionaryByteLimit(-1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> b.progressListenerCadenceRows(0L)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> b.progressListenerCadenceRows(-1L)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void expectedRowCountAcceptsZero() {
        WriteOptions o = WriteOptions.builder().expectedRowCount(0L).build();

        assertThat(o.expectedRowCount()).hasValue(0L);
    }

    @Test
    void expectedRowCountRejectsNegative() {
        WriteOptions.Builder b = WriteOptions.builder();

        assertThatThrownBy(() -> b.expectedRowCount(-1L)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void buildIsImmutable() {
        WriteOptions o = WriteOptions.builder()
                .compression("a", Compression.snappy())
                .encodingPolicy("b", EncodingPolicy.FORCE_PLAIN)
                .indexedColumns("c")
                .bloomFilter("d", BloomFilterConfig.defaults())
                .crs("geom", CoordinateReferenceSystems.ogcCrs84())
                .build();

        Map<String, Compression> compression = o.compression();
        Compression gzip = Compression.gzip();
        assertThatThrownBy(() -> compression.put("x", gzip)).isInstanceOf(UnsupportedOperationException.class);
        Map<String, EncodingPolicy> encodingPolicies = o.encodingPolicies();
        EncodingPolicy auto = EncodingPolicy.AUTO;
        assertThatThrownBy(() -> encodingPolicies.put("x", auto)).isInstanceOf(UnsupportedOperationException.class);
        Set<String> indexedColumns = o.indexedColumns();
        assertThatThrownBy(() -> indexedColumns.add("x")).isInstanceOf(UnsupportedOperationException.class);
        Map<String, BloomFilterConfig> bloomFilters = o.bloomFilters();
        BloomFilterConfig bloomDefaults = BloomFilterConfig.defaults();
        assertThatThrownBy(() -> bloomFilters.put("x", bloomDefaults))
                .isInstanceOf(UnsupportedOperationException.class);
        Map<String, CoordinateReferenceSystem> crs = o.crs();
        CoordinateReferenceSystem crs84 = CoordinateReferenceSystems.ogcCrs84();
        assertThatThrownBy(() -> crs.put("x", crs84)).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void builderMutationsAfterBuildDoNotAffectBuiltInstance() {
        WriteOptions.Builder b = WriteOptions.builder().compression("a", Compression.snappy());
        WriteOptions snapshot = b.build();

        b.compression("z", Compression.gzip());

        assertThat(snapshot.compression()).containsOnlyKeys("a");
    }

    @Test
    void perColumnSettersOverwriteOnRepeatedKey() {
        WriteOptions o = WriteOptions.builder()
                .compression("col", Compression.snappy())
                .compression("col", Compression.gzip())
                .build();

        assertThat(o.compression()).containsEntry("col", new Compression.Gzip());
    }

    @Test
    void progressListenerCarriesThroughBuild() {
        WriteProgressListener listener = new WriteProgressListener() {};

        WriteOptions o = WriteOptions.builder()
                .progressListener(listener)
                .progressListenerCadenceRows(10_000L)
                .build();

        assertThat(o.progressListener()).containsSame(listener);
        assertThat(o.progressListenerCadenceRows()).isEqualTo(10_000L);
    }

    @Test
    void tempDirCarriesThroughBuild(@TempDir Path tmp) {
        WriteOptions o = WriteOptions.builder().tempDir(tmp).build();

        assertThat(o.tempDir()).isEqualTo(tmp);
    }

    @Test
    void keyValueMetadataCarriesThroughBuild() {
        WriteOptions o = WriteOptions.builder()
                .keyValueMetadata("pandas", "{}")
                .keyValueMetadata("author", "tileverse")
                .build();

        assertThat(o.keyValueMetadata()).containsEntry("pandas", "{}").containsEntry("author", "tileverse");
    }

    @Test
    void keyValueMetadataMapAccumulatesWithSingleEntries() {
        WriteOptions o = WriteOptions.builder()
                .keyValueMetadata("a", "1")
                .keyValueMetadata(Map.of("b", "2", "c", "3"))
                .build();

        assertThat(o.keyValueMetadata()).containsOnlyKeys("a", "b", "c");
    }

    @Test
    void keyValueMetadataRejectsReservedGeoKey() {
        WriteOptions.Builder b = WriteOptions.builder();

        assertThatThrownBy(() -> b.keyValueMetadata("geo", "{}"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("geo");
    }

    @Test
    void keyValueMetadataMapRejectsReservedGeoKey() {
        WriteOptions.Builder b = WriteOptions.builder();
        Map<String, String> withGeo = Map.of("geo", "{}");

        assertThatThrownBy(() -> b.keyValueMetadata(withGeo))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("geo");
    }

    @Test
    void keyValueMetadataIsImmutableAfterBuild() {
        WriteOptions o = WriteOptions.builder().keyValueMetadata("k", "v").build();

        Map<String, String> kv = o.keyValueMetadata();
        assertThatThrownBy(() -> kv.put("x", "y")).isInstanceOf(UnsupportedOperationException.class);
    }

    @Nested
    class ParquetVersionTests {

        @Test
        void exposesExactlyV2AndV1() {
            assertThat(ParquetVersion.values()).containsExactly(ParquetVersion.V2_0, ParquetVersion.V1_1);
        }

        @Test
        void valueOfRoundTripsByName() {
            assertThat(ParquetVersion.valueOf("V2_0")).isSameAs(ParquetVersion.V2_0);
            assertThat(ParquetVersion.valueOf("V1_1")).isSameAs(ParquetVersion.V1_1);
        }
    }

    @Nested
    class GeoParquetMetadataModeTests {

        @Test
        void exposesExactlyDualV2OnlyV1Only() {
            assertThat(GeoParquetMetadataMode.values())
                    .containsExactly(
                            GeoParquetMetadataMode.DUAL_V1_1_AND_V2_0,
                            GeoParquetMetadataMode.V2_0_ONLY,
                            GeoParquetMetadataMode.V1_1_ONLY);
        }

        @Test
        void valueOfRoundTripsByName() {
            assertThat(GeoParquetMetadataMode.valueOf("DUAL_V1_1_AND_V2_0"))
                    .isSameAs(GeoParquetMetadataMode.DUAL_V1_1_AND_V2_0);
            assertThat(GeoParquetMetadataMode.valueOf("V2_0_ONLY")).isSameAs(GeoParquetMetadataMode.V2_0_ONLY);
            assertThat(GeoParquetMetadataMode.valueOf("V1_1_ONLY")).isSameAs(GeoParquetMetadataMode.V1_1_ONLY);
        }
    }

    @Nested
    class RowGroupSizeTests {

        @Test
        void adaptiveReturnsAdaptiveRecord() {
            RowGroupSize size = RowGroupSize.adaptive();

            assertThat(size).isInstanceOf(RowGroupSize.Adaptive.class).isEqualTo(new RowGroupSize.Adaptive());
        }

        @Test
        void bytesCarriesThreshold() {
            RowGroupSize size = RowGroupSize.bytes(128 * 1024 * 1024L);

            assertThat(size).isInstanceOf(RowGroupSize.Bytes.class);
            RowGroupSize.Bytes bytes = (RowGroupSize.Bytes) size;
            assertThat(bytes.uncompressedByteThreshold()).isEqualTo(128 * 1024 * 1024L);
        }

        @Test
        void rowsCarriesThreshold() {
            RowGroupSize size = RowGroupSize.rows(1_000_000L);

            assertThat(size).isInstanceOf(RowGroupSize.Rows.class);
            RowGroupSize.Rows rows = (RowGroupSize.Rows) size;
            assertThat(rows.rowThreshold()).isEqualTo(1_000_000L);
        }

        @ParameterizedTest
        @ValueSource(longs = {0L, -1L, Long.MIN_VALUE})
        void bytesRejectsNonPositive(long bad) {
            assertThatThrownBy(() -> RowGroupSize.bytes(bad))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(Long.toString(bad));
        }

        @ParameterizedTest
        @ValueSource(longs = {0L, -1L, Long.MIN_VALUE})
        void rowsRejectsNonPositive(long bad) {
            assertThatThrownBy(() -> RowGroupSize.rows(bad))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(Long.toString(bad));
        }

        @Test
        void sealedPermitsClosure() {
            Class<?>[] permitted = RowGroupSize.class.getPermittedSubclasses();

            assertThat(permitted)
                    .containsExactlyInAnyOrder(
                            RowGroupSize.Adaptive.class, RowGroupSize.Bytes.class, RowGroupSize.Rows.class);
            assertThat(RowGroupSize.class.isSealed()).isTrue();
        }
    }

    @Nested
    class BloomFilterConfigTests {

        @Test
        void defaultsAre1PercentFppAnd1MillionNdv() {
            BloomFilterConfig cfg = BloomFilterConfig.defaults();

            assertThat(cfg.fpp()).isEqualTo(0.01);
            assertThat(cfg.ndv()).isEqualTo(1_000_000L);
        }

        @ParameterizedTest
        @ValueSource(doubles = {0.0, -0.01, -1.0, 1.0, 1.01, 2.0, Double.POSITIVE_INFINITY})
        void rejectsFppOutsideOpenUnitInterval(double bad) {
            assertThatThrownBy(() -> new BloomFilterConfig(bad, 1_000L))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("fpp");
        }

        @ParameterizedTest
        @ValueSource(longs = {0L, -1L, Long.MIN_VALUE})
        void rejectsNonPositiveNdv(long bad) {
            assertThatThrownBy(() -> new BloomFilterConfig(0.01, bad))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("ndv");
        }

        @Test
        void acceptsBoundaryFppNearOpenInterval() {
            assertThat(new BloomFilterConfig(Double.MIN_VALUE, 1L).fpp()).isEqualTo(Double.MIN_VALUE);
            assertThat(new BloomFilterConfig(0.9999999999, 1L).fpp()).isEqualTo(0.9999999999);
        }

        @Test
        void acceptsNdvOne() {
            assertThat(new BloomFilterConfig(0.01, 1L).ndv()).isEqualTo(1L);
        }
    }

    @Nested
    class EncodingPolicyTests {

        @TempDir
        Path tempDir;

        @Test
        void autoConstantIsAutoRecord() {
            assertThat(EncodingPolicy.AUTO)
                    .isInstanceOf(EncodingPolicy.Auto.class)
                    .isEqualTo(new EncodingPolicy.Auto());
        }

        @Test
        void forcePlainConstantIsForcePlainRecord() {
            assertThat(EncodingPolicy.FORCE_PLAIN)
                    .isInstanceOf(EncodingPolicy.ForcePlain.class)
                    .isEqualTo(new EncodingPolicy.ForcePlain());
        }

        @Test
        void forceDictionaryConstantIsForceDictionaryRecord() {
            assertThat(EncodingPolicy.FORCE_DICTIONARY)
                    .isInstanceOf(EncodingPolicy.ForceDictionary.class)
                    .isEqualTo(new EncodingPolicy.ForceDictionary());
        }

        @Test
        void forceDeltaConstantIsForceDeltaRecord() {
            assertThat(EncodingPolicy.FORCE_DELTA)
                    .isInstanceOf(EncodingPolicy.ForceDelta.class)
                    .isEqualTo(new EncodingPolicy.ForceDelta());
        }

        @Test
        void forceByteStreamSplitConstantIsForceByteStreamSplitRecord() {
            assertThat(EncodingPolicy.FORCE_BYTE_STREAM_SPLIT)
                    .isInstanceOf(EncodingPolicy.ForceByteStreamSplit.class)
                    .isEqualTo(new EncodingPolicy.ForceByteStreamSplit());
        }

        @Test
        void sealedPermitsClosure() {
            Class<?>[] permitted = EncodingPolicy.class.getPermittedSubclasses();

            assertThat(permitted)
                    .containsExactlyInAnyOrder(
                            EncodingPolicy.Auto.class,
                            EncodingPolicy.ForcePlain.class,
                            EncodingPolicy.ForceDictionary.class,
                            EncodingPolicy.ForceDelta.class,
                            EncodingPolicy.ForceByteStreamSplit.class);
            assertThat(EncodingPolicy.class.isSealed()).isTrue();
        }

        @Test
        void autoPolicyOnLowCardinalityIntEmitsDictionaryPagesInV2() throws Exception {
            Path file = writeIntColumn("value", 200, value -> value % 8, ParquetVersion.V2_0, EncodingPolicy.AUTO);

            List<Encoding> dataPageEncodings = readDataPageEncodings(file, "value");
            assertThat(dataPageEncodings)
                    .as("V2 dictionary attempt should emit RLE_DICTIONARY data pages on low cardinality")
                    .isNotEmpty()
                    .allMatch(e -> e == Encoding.RLE_DICTIONARY);
            assertThat(columnEncodings(file, "value")).contains(Encoding.RLE_DICTIONARY, Encoding.PLAIN);
        }

        @Test
        void autoPolicyOnLowCardinalityIntEmitsDictionaryPagesInV1() throws Exception {
            Path file = writeIntColumn("value", 200, value -> value % 8, ParquetVersion.V1_1, EncodingPolicy.AUTO);

            List<Encoding> dataPageEncodings = readDataPageEncodings(file, "value");
            assertThat(dataPageEncodings)
                    .as("V1 dictionary attempt should emit PLAIN_DICTIONARY data pages on low cardinality")
                    .isNotEmpty()
                    .allMatch(e -> e == Encoding.PLAIN_DICTIONARY);
        }

        @Test
        void autoPolicyOnHighCardinalityIntFallsBackToPlain() throws Exception {
            int rows = 5_000;
            WriteOptions options = options()
                    .parquetVersion(ParquetVersion.V2_0)
                    .dictionaryByteLimit(64)
                    .pageValueLimit(500)
                    .build();
            Path file = writeIntColumnWith(options, "id", rows, i -> i);

            List<Encoding> encodings = readDataPageEncodings(file, "id");
            assertThat(encodings)
                    .as("dictionary attempt should overflow well before %d distinct values fit a 64-byte budget", rows)
                    .contains(Encoding.PLAIN);
        }

        @Test
        void forcePlainSkipsDictionaryAttemptEntirely() throws Exception {
            WriteOptions options = options()
                    .parquetVersion(ParquetVersion.V2_0)
                    .encodingPolicy("value", EncodingPolicy.FORCE_PLAIN)
                    .build();
            Path file = writeIntColumnWith(options, "value", 200, i -> i % 8);

            List<Encoding> encodings = readDataPageEncodings(file, "value");
            assertThat(encodings).isNotEmpty().allMatch(e -> e == Encoding.PLAIN);

            ColumnMetaData meta = columnMetadata(file, "value");
            assertThat(meta.dictionaryPageOffset())
                    .as("forcePlain must not emit a dictionary page")
                    .isEmpty();
        }

        @Test
        void geometryColumnUnderForcePlainEmitsPlainPages() throws Exception {
            ParquetSchema schema = flatSchema(requiredBinary("geometry"));
            WriteOptions options = options()
                    .parquetVersion(ParquetVersion.V2_0)
                    .crsEpsg("geometry", 4326)
                    .encodingPolicy("geometry", EncodingPolicy.FORCE_PLAIN)
                    .build();
            Path file = tempDir.resolve("geom-plain.parquet");
            try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(file), schema, options)) {
                for (int i = 0; i < 32; i++) {
                    writer.write(rowOf(Map.of(
                            ColumnPath.of("geometry"),
                            MemorySegment.ofArray(wkbPoint(i * 0.1, i * 0.2)).asReadOnly())));
                }
            }

            List<Encoding> encodings = readDataPageEncodings(file, "geometry");
            assertThat(encodings).isNotEmpty().allMatch(e -> e == Encoding.PLAIN);

            ColumnMetaData meta = columnMetadata(file, "geometry");
            assertThat(meta.dictionaryPageOffset()).isEmpty();
        }

        // --- helpers ---

        private WriteOptions.Builder options() {
            return WriteOptions.builder().tempDir(tempDir);
        }

        private Path writeIntColumn(
                String column,
                int rows,
                java.util.function.IntUnaryOperator valueAt,
                ParquetVersion version,
                EncodingPolicy policy)
                throws Exception {
            WriteOptions.Builder builder = options().parquetVersion(version);
            if (policy != null) {
                builder.encodingPolicy(column, policy);
            }
            return writeIntColumnWith(builder.build(), column, rows, valueAt);
        }

        private Path writeIntColumnWith(
                WriteOptions options, String column, int rows, java.util.function.IntUnaryOperator valueAt)
                throws Exception {
            ParquetSchema schema = flatSchema(requiredInt32(column));
            Path file = tempDir.resolve(column + "-" + System.nanoTime() + ".parquet");
            try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(file), schema, options)) {
                for (int i = 0; i < rows; i++) {
                    writer.write(rowOf(Map.of(ColumnPath.of(column), valueAt.applyAsInt(i))));
                }
            }
            return file;
        }

        private ParquetSchema flatSchema(SchemaNode.Primitive... leaves) {
            List<SchemaNode> children =
                    Stream.of(leaves).map(f -> (SchemaNode) f).toList();
            SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, children, Optional.empty(), -1);
            return new ParquetSchema(root);
        }

        private SchemaNode.Primitive requiredInt32(String name) {
            return new SchemaNode.Primitive(
                    name, Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        }

        private SchemaNode.Primitive requiredBinary(String name) {
            return new SchemaNode.Primitive(
                    name, Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        }

        private WriteRow rowOf(Map<ColumnPath, Object> values) {
            Map<ColumnPath, Object> copy = new HashMap<>(values);
            return copy::get;
        }

        private byte[] wkbPoint(double x, double y) {
            ByteBuffer bb = ByteBuffer.allocate(21).order(java.nio.ByteOrder.LITTLE_ENDIAN);
            bb.put((byte) 1);
            bb.putInt(1);
            bb.putDouble(x);
            bb.putDouble(y);
            return bb.array();
        }

        private List<Encoding> readDataPageEncodings(Path file, String columnName) throws Exception {
            List<Encoding> out = new ArrayList<>();
            try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
                FileMetaData footer = ParquetFormat.readFooter(source);
                for (RowGroup rg : footer.rowGroups()) {
                    ColumnMetaData meta = findColumnInRowGroup(rg, columnName);
                    if (meta == null) {
                        continue;
                    }
                    collectPageEncodings(source, meta, out);
                }
            }
            return out;
        }

        private void collectPageEncodings(ByteRangeSource source, ColumnMetaData meta, List<Encoding> out)
                throws java.io.IOException {
            long chunkStart = meta.dictionaryPageOffset().orElse(meta.dataPageOffset());
            long chunkSize = meta.totalCompressedSize();
            if (chunkSize <= 0 || chunkSize > Integer.MAX_VALUE) {
                return;
            }
            byte[] chunkBytes = new byte[(int) chunkSize];
            source.readFully(chunkStart, MemorySegment.ofArray(chunkBytes));
            InputStream in = new ByteArrayInputStream(chunkBytes);
            long consumed = 0L;
            while (consumed < chunkSize) {
                in.mark(Integer.MAX_VALUE);
                int availableBefore = in.available();
                PageHeader header = ParquetFormat.readPageHeader(in);
                int headerBytes = availableBefore - in.available();
                if (header.type() == PageType.DATA_PAGE) {
                    DataPageHeader v1 = header.dataPageHeader().orElseThrow();
                    out.add(v1.encoding());
                } else if (header.type() == PageType.DATA_PAGE_V2) {
                    DataPageHeaderV2 v2 = header.dataPageHeaderV2().orElseThrow();
                    out.add(v2.encoding());
                }
                long pagePayloadBytes = header.compressedPageSize();
                long skipped = in.skip(pagePayloadBytes);
                if (skipped != pagePayloadBytes) {
                    throw new java.io.IOException(
                            "expected to skip " + pagePayloadBytes + " bytes, skipped " + skipped);
                }
                consumed += headerBytes + pagePayloadBytes;
            }
        }

        private List<Encoding> columnEncodings(Path file, String columnName) throws Exception {
            return columnMetadata(file, columnName).encodings();
        }

        private ColumnMetaData columnMetadata(Path file, String columnName) throws Exception {
            try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
                FileMetaData footer = ParquetFormat.readFooter(source);
                for (RowGroup rg : footer.rowGroups()) {
                    ColumnMetaData meta = findColumnInRowGroup(rg, columnName);
                    if (meta != null) {
                        return meta;
                    }
                }
                throw new AssertionError("column " + columnName + " not found");
            }
        }

        private ColumnMetaData findColumnInRowGroup(RowGroup rg, String columnName) {
            for (ColumnChunk chunk : rg.columns()) {
                ColumnMetaData meta = chunk.metaData().orElse(null);
                if (meta == null) {
                    continue;
                }
                if (meta.pathInSchema().size() == 1
                        && columnName.equals(meta.pathInSchema().get(0))) {
                    return meta;
                }
            }
            return null;
        }
    }
}
