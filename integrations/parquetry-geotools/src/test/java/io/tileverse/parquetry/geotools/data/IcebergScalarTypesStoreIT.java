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
package io.tileverse.parquetry.geotools.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import org.geotools.api.data.DataStore;
import org.geotools.api.data.Query;
import org.geotools.api.data.SimpleFeatureSource;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.filter.Filter;
import org.geotools.api.filter.FilterFactory;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.filter.text.ecql.ECQL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.geotools.data.FeatureTypeMapper.Mapping;
import io.tileverse.parquetry.geotools.iceberg.IcebergDataStoreFactory;
import io.tileverse.parquetry.iceberg.IcebergOptions;
import io.tileverse.parquetry.iceberg.IcebergTableCatalog;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Reads the {@code scalars} Iceberg fixture through the GeoTools store: every scalar column binds to its Java type, a
 * row's values come back reconstructed, one query per type returns the expected rows, and the same filters translate
 * with no residual, which proves they were pushed. Every expected value is the fixture's formula for the row id; the
 * live rows are ids 1, 3, 4, 5, 6, 8, 9, 10, 11, 12.
 */
class IcebergScalarTypesStoreIT {

    private static final String TABLE = "scalars";
    private static final String ID = "id";
    private static final FilterFactory FF = CommonFactoryFinder.getFilterFactory();

    /** The {@code ts} value of the first partition, and the base instant of the nanosecond columns. */
    private static final LocalDateTime P0 = LocalDateTime.of(2024, 1, 1, 0, 0, 0);

    /** The {@code ts} value of the second partition, at the microsecond precision of its column. */
    private static final LocalDateTime P1 = LocalDateTime.of(2024, 6, 15, 12, 30, 45, 123_456_000);

    @TempDir
    static Path tempDir;

    private static Path table;

    private DataStore store;
    private FilterToPredicate translator;

    @BeforeAll
    static void extractTable() {
        table = TestCorpus.extractDirectory("iceberg-scalar-types/" + TABLE, tempDir.resolve(TABLE));
    }

    @BeforeEach
    void openStore() throws IOException {
        store = new IcebergDataStoreFactory()
                .createDataStore(Map.of("iceberg", table.toUri().toString()));
        translator = new FilterToPredicate(mapTableSchema(table), null);
    }

    @AfterEach
    void disposeStore() {
        if (store != null) {
            store.dispose();
        }
    }

    @Test
    void bindsEveryScalarColumn() throws IOException {
        SimpleFeatureType schema = store.getSchema(TABLE);
        assertThat(binding(schema, ID)).isEqualTo(Long.class);
        assertThat(binding(schema, "ts")).isEqualTo(LocalDateTime.class);
        assertThat(binding(schema, "tstz")).isEqualTo(Instant.class);
        assertThat(binding(schema, "ts_ns")).isEqualTo(LocalDateTime.class);
        assertThat(binding(schema, "tstz_ns")).isEqualTo(Instant.class);
        assertThat(binding(schema, "t")).isEqualTo(LocalTime.class);
        assertThat(binding(schema, "dec")).isEqualTo(BigDecimal.class);
        assertThat(binding(schema, "bigdec")).isEqualTo(BigDecimal.class);
        assertThat(binding(schema, "u")).isEqualTo(UUID.class);
        assertThat(binding(schema, "fx")).isEqualTo(byte[].class);
        assertThat(binding(schema, "bin")).isEqualTo(byte[].class);
        assertThat(binding(schema, "d")).isEqualTo(LocalDate.class);
        assertThat(binding(schema, "label")).isEqualTo(String.class);
    }

    @Test
    void readsEveryLiveRow() throws IOException {
        assertThat(ids(Filter.INCLUDE)).containsExactly(1L, 3L, 4L, 5L, 6L, 8L, 9L, 10L, 11L, 12L);
    }

    @Test
    void readsEveryScalarValueOfARow() throws IOException {
        SimpleFeature row = onlyFeature(idEquals(1L));
        assertThat(row.getAttribute("ts")).isEqualTo(P0);
        assertThat(row.getAttribute("tstz")).isEqualTo(P0.plusMinutes(1).toInstant(ZoneOffset.UTC));
        assertThat(row.getAttribute("ts_ns")).isEqualTo(P0.plusNanos(1));
        assertThat(row.getAttribute("tstz_ns")).isEqualTo(P0.plusNanos(1).toInstant(ZoneOffset.UTC));
        assertThat(row.getAttribute("t")).isEqualTo(LocalTime.of(1, 30, 15, 250_000_000));
        assertThat((BigDecimal) row.getAttribute("dec")).isEqualByComparingTo("-6.25");
        assertThat((BigDecimal) row.getAttribute("bigdec")).isEqualByComparingTo("123456789012345.678");
        assertThat(row.getAttribute("u")).isEqualTo(uuidOf(1L));
        assertThat((byte[]) row.getAttribute("fx")).isEqualTo(new byte[] {1, 2, 3, 4});
        assertThat((byte[]) row.getAttribute("bin")).isEqualTo("bin-1".getBytes(StandardCharsets.UTF_8));
        assertThat(row.getAttribute("d")).isEqualTo(LocalDate.of(2024, 1, 2));
        assertThat(row.getAttribute("label")).isEqualTo("row-1");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("queries")
    void queriesEveryScalarTypeThroughTheStore(String name, Filter filter, List<Long> expectedIds) throws IOException {
        assertThat(ids(filter)).containsExactlyElementsOf(expectedIds);
        assertThat(translator.translate(filter).residual()).isEqualTo(Filter.INCLUDE);
    }

    private static Stream<Arguments> queries() throws Exception {
        return Stream.of(
                Arguments.of("decimal gt", ECQL.toFilter("dec > 2.00"), List.of(8L, 9L, 10L, 11L, 12L)),
                Arguments.of("decimal eq at another scale", ECQL.toFilter("dec = -6.250"), List.of(1L)),
                Arguments.of("fixed-length decimal lt", ECQL.toFilter("bigdec < 500000000000000"), List.of(1L, 3L, 4L)),
                Arguments.of("date eq", ECQL.toFilter("d = '2024-01-05'"), List.of(4L)),
                Arguments.of(
                        "date between", ECQL.toFilter("d BETWEEN '2024-01-04' AND '2024-01-06'"), List.of(3L, 4L, 5L)),
                Arguments.of(
                        "time gt",
                        FF.greater(FF.property("t"), FF.literal(LocalTime.of(5, 30, 15, 250_000_000))),
                        List.of(6L, 8L, 9L, 10L, 11L, 12L)),
                Arguments.of(
                        "local timestamp eq", ECQL.toFilter("ts = '2024-06-15T12:30:45.123456'"), List.of(5L, 6L, 8L)),
                Arguments.of(
                        "utc timestamp after",
                        ECQL.toFilter("tstz AFTER 2024-06-15T12:37:00Z"),
                        List.of(8L, 9L, 10L, 11L, 12L)),
                Arguments.of(
                        "local timestamp during",
                        ECQL.toFilter("ts DURING 2024-06-01T00:00:00Z/2025-01-01T00:00:00Z"),
                        List.of(5L, 6L, 8L)),
                Arguments.of(
                        "nanosecond timestamp gt",
                        FF.greater(FF.property("ts_ns"), FF.literal(P0.plusNanos(5))),
                        List.of(6L, 8L, 9L, 10L, 11L, 12L)),
                Arguments.of(
                        "utc nanosecond timestamp ltEq",
                        FF.lessOrEqual(
                                FF.property("tstz_ns"),
                                FF.literal(P0.plusNanos(4).toInstant(ZoneOffset.UTC))),
                        List.of(1L, 3L, 4L)),
                Arguments.of("uuid eq", ECQL.toFilter("u = '" + uuidOf(3L) + "'"), List.of(3L)),
                Arguments.of(
                        "binary eq",
                        FF.equals(FF.property("bin"), FF.literal("bin-4".getBytes(StandardCharsets.UTF_8))),
                        List.of(4L)),
                Arguments.of(
                        "fixed eq", FF.equals(FF.property("fx"), FF.literal(new byte[] {5, 10, 15, 20})), List.of(5L)),
                Arguments.of("string eq", ECQL.toFilter("label = 'row-9'"), List.of(9L)));
    }

    /**
     * A literal finer than the column unit is not pushed in either direction, and the store still answers exactly.
     * Truncating the literal to microseconds would drop rows 5, 6 and 8 from the {@code <} answer, which is the row set
     * that proves the literal was not truncated. The residual also shows in the count: a query with a residual answers
     * {@code -1}, the documented "iterate to find out", while a fully pushed one counts from the statistics.
     */
    @Test
    void aLiteralFinerThanTheColumnStaysResidualAndStillFiltersExactly() throws Exception {
        LocalDateTime finerThanTheColumn = P1.withNano(123_456_789);
        Filter greater = FF.greater(FF.property("ts"), FF.literal(finerThanTheColumn));
        assertThat(translator.translate(greater).residual()).isEqualTo(greater);
        assertThat(ids(greater)).containsExactly(9L, 10L, 11L, 12L);

        Filter less = FF.less(FF.property("ts"), FF.literal(finerThanTheColumn));
        assertThat(translator.translate(less).residual()).isEqualTo(less);
        assertThat(ids(less)).containsExactly(1L, 3L, 4L, 5L, 6L, 8L);

        assertThat(count(greater)).isEqualTo(-1);
        assertThat(count(ECQL.toFilter("dec > 2.00"))).isEqualTo(5);
    }

    /** The store's feature-type mapping for the table, built from the Iceberg schema presented by the store. */
    private static Mapping mapTableSchema(Path tableDir) {
        try (IcebergTableCatalog catalog = IcebergTableCatalog.openLocal(tableDir, IcebergOptions.defaults())) {
            List<String> datasets = catalog.datasets();
            assertThat(datasets).hasSize(1);
            ParquetSchema schema = catalog.dataset(datasets.get(0)).schema();
            return FeatureTypeMapper.map(TABLE, null, schema, Optional.empty(), null);
        }
    }

    /** Equality on the id column, built programmatically because ECQL reserves {@code id} for its own predicate. */
    private static Filter idEquals(long id) {
        return FF.equals(FF.property(ID), FF.literal(id));
    }

    /** The fixture's uuid for a row: the id in the high half and a fixed multiple of it in the low half. */
    private static UUID uuidOf(long id) {
        return new UUID(id, id * 1_000_003L);
    }

    private static Class<?> binding(SimpleFeatureType schema, String attribute) {
        return schema.getDescriptor(attribute).getType().getBinding();
    }

    /** The one feature matching {@code filter}, failing when the query returns any other number of rows. */
    private SimpleFeature onlyFeature(Filter filter) throws IOException {
        List<SimpleFeature> matches = features(filter);
        assertThat(matches).hasSize(1);
        return matches.get(0);
    }

    /** The ids of the features matching {@code filter}, ascending. */
    private List<Long> ids(Filter filter) throws IOException {
        List<Long> ids = new ArrayList<>();
        for (SimpleFeature feature : features(filter)) {
            ids.add(idOf(feature));
        }
        return ids;
    }

    /** The count that the store answers for {@code filter}, which is -1 when a residual filter remains. */
    private int count(Filter filter) throws IOException {
        SimpleFeatureSource source = store.getFeatureSource(TABLE);
        return source.getCount(new Query(TABLE, filter));
    }

    /** The features matching {@code filter}, ordered by id, which lets an expectation list them in one order. */
    private List<SimpleFeature> features(Filter filter) throws IOException {
        SimpleFeatureSource source = store.getFeatureSource(TABLE);
        SimpleFeatureCollection matches = source.getFeatures(filter);
        List<SimpleFeature> collected = new ArrayList<>();
        try (SimpleFeatureIterator rows = matches.features()) {
            while (rows.hasNext()) {
                collected.add(rows.next());
            }
        }
        collected.sort(Comparator.comparingLong(IcebergScalarTypesStoreIT::idOf));
        return collected;
    }

    private static long idOf(SimpleFeature feature) {
        return (Long) feature.getAttribute(ID);
    }
}
