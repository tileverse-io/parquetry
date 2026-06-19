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
package io.tileverse.parquetry.geotools;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.filter.Filter;
import org.geotools.api.filter.FilterFactory;
import org.geotools.api.filter.MultiValuedFilter.MatchAction;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.filter.text.ecql.ECQL;
import org.geotools.referencing.CRS;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import io.tileverse.parquetry.catalog.FilesetCatalog;
import io.tileverse.parquetry.dataset.GeoParquetDataset;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.geotools.GeoParquetSchemaMapper.AttributeMapping;
import io.tileverse.parquetry.geotools.GeoParquetSchemaMapper.Mapping;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ResolvedColumn;

class FilterToPredicateTest {

    private static final FilterFactory FF = CommonFactoryFinder.getFilterFactory();

    private static Mapping mapping() {
        SimpleFeatureTypeBuilder b = new SimpleFeatureTypeBuilder();
        b.setName("t");
        b.add("geom", Point.class);
        b.add("name", String.class);
        b.add("pop", Long.class);
        b.add("score", Double.class);
        b.add("active", Boolean.class);
        b.add("guid", UUID.class);
        SimpleFeatureType ft = b.buildFeatureType();
        List<AttributeMapping> attrs = List.of(
                new AttributeMapping("geom", ColumnPath.of("geometry"), true, Point.class),
                new AttributeMapping("name", ColumnPath.of("name"), false, String.class),
                new AttributeMapping("pop", ColumnPath.of("pop"), false, Long.class),
                new AttributeMapping("score", ColumnPath.of("score"), false, Double.class),
                new AttributeMapping("active", ColumnPath.of("active"), false, Boolean.class),
                new AttributeMapping("guid", ColumnPath.of("guid"), false, UUID.class));
        return new Mapping(ft, attrs);
    }

    private static FilterToPredicate translator() {
        Mapping m = mapping();
        return new FilterToPredicate(m, m.featureType().getCoordinateReferenceSystem());
    }

    @Test
    void numericGreaterThanTranslatesToPredicate() {
        Filter f = FF.greater(FF.property("pop"), FF.literal(1000));
        FilterToPredicate.Result r = translator().translate(f);
        assertThat(r.predicate()).isEqualTo(Pred.col(ColumnPath.of("pop")).gt(1000L));
        assertThat(r.residual()).isEqualTo(Filter.INCLUDE);
    }

    @Test
    void stringEqualsTranslates() {
        Filter f = FF.equals(FF.property("name"), FF.literal("AR"));
        FilterToPredicate.Result r = translator().translate(f);
        assertThat(r.predicate()).isEqualTo(Pred.col(ColumnPath.of("name")).eq("AR"));
        assertThat(r.residual()).isEqualTo(Filter.INCLUDE);
    }

    @Test
    void uuidEqualsTranslates() {
        UUID id = UUID.fromString("00000000-0000-0000-0000-0000000000ab");
        Filter f = FF.equals(FF.property("guid"), FF.literal(id.toString()));
        FilterToPredicate.Result r = translator().translate(f);
        assertThat(r.predicate()).isEqualTo(new Predicate.Eq(ColumnPath.of("guid"), new Value.UuidVal(id)));
        assertThat(r.residual()).isEqualTo(Filter.INCLUDE);
    }

    // A UUID column pushes only equality with a parseable literal. An uncoercible literal or any non-EQ comparison
    // cannot push (the unsigned byte order has no sound ordered/notEq push) and falls to the residual filter.
    @ParameterizedTest(name = "{0}")
    @MethodSource("unpushableUuidFilters")
    void unpushableUuidComparisonIsResidual(String name, Filter f) {
        FilterToPredicate.Result r = translator().translate(f);
        assertThat(r.predicate()).isEqualTo((Predicate) Predicate.ALWAYS_TRUE);
        assertThat(r.residual()).isEqualTo(f);
    }

    private static Stream<Arguments> unpushableUuidFilters() {
        String validUuid = "00000000-0000-0000-0000-0000000000ab";
        return Stream.of(
                Arguments.of("uncoercible literal on EQ", FF.equals(FF.property("guid"), FF.literal("not-a-uuid"))),
                Arguments.of("valid literal on non-EQ", FF.notEqual(FF.property("guid"), FF.literal(validUuid))));
    }

    @Test
    void stringLessThanIsNotPushableAndBecomesResidual() {
        Filter f = FF.less(FF.property("name"), FF.literal("M"));
        FilterToPredicate.Result r = translator().translate(f);
        assertThat(r.predicate()).isEqualTo((Predicate) Predicate.ALWAYS_TRUE);
        assertThat(r.residual()).isEqualTo(f);
    }

    @Test
    void betweenTranslatesToConjunction() {
        Filter f = FF.between(FF.property("score"), FF.literal(1.0), FF.literal(9.0));
        FilterToPredicate.Result r = translator().translate(f);
        ColumnPath p = ColumnPath.of("score");
        assertThat(r.predicate())
                .isEqualTo(Pred.and(Pred.col(p).gtEq(1.0), Pred.col(p).ltEq(9.0)));
    }

    @Test
    void isNullTranslates() {
        Filter f = FF.isNull(FF.property("name"));
        FilterToPredicate.Result r = translator().translate(f);
        assertThat(r.predicate()).isEqualTo(Pred.col(ColumnPath.of("name")).isNull());
    }

    @Test
    void booleanNotEqualIsNotPushable() {
        Filter f = FF.notEqual(FF.property("active"), FF.literal(true));
        FilterToPredicate.Result r = translator().translate(f);
        assertThat(r.predicate()).isEqualTo((Predicate) Predicate.ALWAYS_TRUE);
        assertThat(r.residual()).isEqualTo(f);
    }

    @Test
    void comparisonOnGeometryAttributeIsResidual() {
        Filter f = FF.equals(FF.property("geom"), FF.literal("anything"));
        FilterToPredicate.Result r = translator().translate(f);
        assertThat(r.predicate()).isEqualTo((Predicate) Predicate.ALWAYS_TRUE);
        assertThat(r.residual()).isEqualTo(f);
    }

    @Test
    void comparisonOnUnknownPropertyIsResidual() {
        Filter f = FF.greater(FF.property("does_not_exist"), FF.literal(1));
        FilterToPredicate.Result r = translator().translate(f);
        assertThat(r.predicate()).isEqualTo((Predicate) Predicate.ALWAYS_TRUE);
        assertThat(r.residual()).isEqualTo(f);
    }

    @Test
    void uncoercibleLiteralIsResidual() {
        // "abc" cannot convert to the Long binding of "pop"; the leaf must fall to residual, never a bogus predicate.
        Filter f = FF.equals(FF.property("pop"), FF.literal("abc"));
        FilterToPredicate.Result r = translator().translate(f);
        assertThat(r.predicate()).isEqualTo((Predicate) Predicate.ALWAYS_TRUE);
        assertThat(r.residual()).isEqualTo(f);
    }

    @Test
    void fractionalLiteralOnIntegerColumnIsResidual() {
        // pop is Long; pop < 1.9 must NOT push pop < 1 (that would drop pop == 1). It falls to residual.
        Filter f = FF.less(FF.property("pop"), FF.literal(1.9));
        FilterToPredicate.Result r = translator().translate(f);
        assertThat(r.predicate()).isEqualTo((Predicate) Predicate.ALWAYS_TRUE);
        assertThat(r.residual()).isEqualTo(f);
    }

    @Test
    void integralLiteralOnIntegerColumnStillPushes() {
        Filter f = FF.less(FF.property("pop"), FF.literal(1000));
        FilterToPredicate.Result r = translator().translate(f);
        assertThat(r.predicate()).isEqualTo(Pred.col(ColumnPath.of("pop")).lt(1000L));
    }

    @Test
    void fractionalLiteralOnDoubleColumnStillPushes() {
        Filter f = FF.less(FF.property("score"), FF.literal(1.9));
        FilterToPredicate.Result r = translator().translate(f);
        assertThat(r.predicate()).isEqualTo(Pred.col(ColumnPath.of("score")).lt(1.9));
    }

    @Test
    void pushedBetweenAndIsNullHaveIncludeResidual() {
        assertThat(translator()
                        .translate(FF.between(FF.property("score"), FF.literal(1.0), FF.literal(9.0)))
                        .residual())
                .isEqualTo(Filter.INCLUDE);
        assertThat(translator().translate(FF.isNull(FF.property("name"))).residual())
                .isEqualTo(Filter.INCLUDE);
    }

    private static final GeometryFactory GF = new GeometryFactory();

    private static Polygon box(double minx, double miny, double maxx, double maxy) {
        return GF.createPolygon(new Coordinate[] {
            new Coordinate(minx, miny),
            new Coordinate(maxx, miny),
            new Coordinate(maxx, maxy),
            new Coordinate(minx, maxy),
            new Coordinate(minx, miny)
        });
    }

    @Test
    void bboxTranslatesToBboxIntersects() {
        Filter f = FF.bbox("geom", 0, 0, 10, 10, null);
        FilterToPredicate.Result r = translator().translate(f);
        assertThat(r.predicate())
                .isEqualTo(new Predicate.Spatial.BboxIntersects(
                        ColumnPath.of("geometry"), io.tileverse.parquetry.filter.Bbox.of2d(0, 0, 10, 10)));
        assertThat(r.residual()).isEqualTo(Filter.INCLUDE);
    }

    @Test
    void intersectsTranslatesToGeometryFilter() {
        Polygon q = box(0, 0, 10, 10);
        Filter f = FF.intersects(FF.property("geom"), FF.literal(q));
        FilterToPredicate.Result r = translator().translate(f);
        assertThat(r.predicate()).isInstanceOf(Predicate.GeometryFilterPredicate.class);
        Predicate.GeometryFilterPredicate gfp = (Predicate.GeometryFilterPredicate) r.predicate();
        assertThat(gfp.filter().column()).isEqualTo(ColumnPath.of("geometry"));
        assertThat(r.residual()).isEqualTo(Filter.INCLUDE);
    }

    @Test
    void spatialFilterOnNonGeometryPropertyIsResidual() {
        Filter f = FF.intersects(FF.property("name"), FF.literal(box(0, 0, 1, 1)));
        FilterToPredicate.Result r = translator().translate(f);
        assertThat(r.predicate()).isEqualTo((Predicate) Predicate.ALWAYS_TRUE);
        assertThat(r.residual()).isEqualTo(f);
    }

    private static FilterToPredicate translatorWithCrs(CoordinateReferenceSystem crs) {
        Mapping base = mapping();
        SimpleFeatureTypeBuilder b = new SimpleFeatureTypeBuilder();
        b.init(base.featureType());
        b.setCRS(crs);
        SimpleFeatureType ft = b.buildFeatureType();
        return new FilterToPredicate(new Mapping(ft, base.attributes()), crs);
    }

    @Test
    void bboxWithMismatchedCrsThrows() throws Exception {
        CoordinateReferenceSystem native4326 = CRS.decode("EPSG:4326");
        FilterToPredicate t = translatorWithCrs(native4326);
        Filter f = FF.bbox("geom", 0, 0, 10, 10, "EPSG:3857");
        assertThatThrownBy(() -> t.translate(f)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void bboxWithMatchingCrsPushes() throws Exception {
        CoordinateReferenceSystem native4326 = CRS.decode("EPSG:4326");
        FilterToPredicate t = translatorWithCrs(native4326);
        Filter f = FF.bbox("geom", 0, 0, 10, 10, "EPSG:4326");
        FilterToPredicate.Result r = t.translate(f);
        assertThat(r.predicate()).isInstanceOf(Predicate.Spatial.BboxIntersects.class);
        assertThat(r.residual()).isEqualTo(Filter.INCLUDE);
    }

    @Test
    void andPushesTranslatableChildAndKeepsResidual() {
        Filter pushable = FF.greater(FF.property("pop"), FF.literal(1000));
        Filter residualOnly = FF.like(FF.property("name"), "A%");
        Filter f = FF.and(pushable, residualOnly);
        FilterToPredicate.Result r = translator().translate(f);
        assertThat(r.predicate()).isEqualTo(Pred.col(ColumnPath.of("pop")).gt(1000L));
        assertThat(r.residual()).isEqualTo(residualOnly);
    }

    @Test
    void orWithOneUnpushableChildIsAtomicResidual() {
        Filter f = FF.or(FF.greater(FF.property("pop"), FF.literal(1000)), FF.like(FF.property("name"), "A%"));
        FilterToPredicate.Result r = translator().translate(f);
        assertThat(r.predicate()).isEqualTo((Predicate) Predicate.ALWAYS_TRUE);
        assertThat(r.residual()).isEqualTo(f);
    }

    @Test
    void orWithAllPushableChildrenTranslates() {
        Filter f = FF.or(
                FF.greater(FF.property("pop"), FF.literal(1000)), FF.equals(FF.property("name"), FF.literal("AR")));
        FilterToPredicate.Result r = translator().translate(f);
        assertThat(r.predicate())
                .isEqualTo(Pred.or(
                        Pred.col(ColumnPath.of("pop")).gt(1000L),
                        Pred.col(ColumnPath.of("name")).eq("AR")));
        assertThat(r.residual()).isEqualTo(Filter.INCLUDE);
    }

    @Test
    void notOfPushableTranslates() {
        Filter f = FF.not(FF.equals(FF.property("name"), FF.literal("AR")));
        FilterToPredicate.Result r = translator().translate(f);
        assertThat(r.predicate())
                .isEqualTo(Pred.not(Pred.col(ColumnPath.of("name")).eq("AR")));
        assertThat(r.residual()).isEqualTo(Filter.INCLUDE);
    }

    @Test
    void notOfUnpushableIsAtomicResidual() {
        Filter f = FF.not(FF.like(FF.property("name"), "A%"));
        FilterToPredicate.Result r = translator().translate(f);
        assertThat(r.predicate()).isEqualTo((Predicate) Predicate.ALWAYS_TRUE);
        assertThat(r.residual()).isEqualTo(f);
    }

    @Test
    void includeAndExcludeFold() {
        assertThat(translator().translate(Filter.INCLUDE).predicate()).isEqualTo((Predicate) Predicate.ALWAYS_TRUE);
        assertThat(translator().translate(Filter.EXCLUDE).predicate()).isEqualTo((Predicate) Predicate.ALWAYS_FALSE);
    }

    @Test
    void orOfTwoBboxesPushesAsDisjunctionOfBboxIntersects() {
        // The renderer splits a cross-dateline query into two disjoint boxes joined by OR; both must push so the
        // region between them is never scanned.
        Filter f = FF.or(FF.bbox("geom", 0, 0, 10, 10, null), FF.bbox("geom", 100, 0, 110, 10, null));
        FilterToPredicate.Result r = translator().translate(f);
        ColumnPath geom = ColumnPath.of("geometry");
        assertThat(r.predicate())
                .isEqualTo(Pred.or(
                        new Predicate.Spatial.BboxIntersects(geom, Bbox.of2d(0, 0, 10, 10)),
                        new Predicate.Spatial.BboxIntersects(geom, Bbox.of2d(100, 0, 110, 10))));
        assertThat(r.residual()).isEqualTo(Filter.INCLUDE);
    }

    private static FilterToPredicate nestedTranslator(Path dir) throws Exception {
        Path file = dir.resolve("nested.parquet");
        NestedFixtures.writeSample(file);
        try (FilesetCatalog catalog = NestedFixtures.openCatalog(file)) {
            GeoParquetDataset dataset = (GeoParquetDataset) catalog.dataset("nested");
            Mapping mapping = GeoParquetSchemaMapper.map("nested", null, dataset.schema(), dataset.geoMetadata(), null);
            CoordinateReferenceSystem crs = mapping.featureType().getCoordinateReferenceSystem();
            ColumnPath physicalLocality = dataset.schema()
                    .resolve(ColumnPath.of("addresses", "locality"))
                    .map(ResolvedColumn::physical)
                    .orElseThrow();
            lastPhysicalLocality = physicalLocality;
            return new FilterToPredicate(mapping, crs);
        }
    }

    private static ColumnPath lastPhysicalLocality;

    @Test
    void nestedEqualsTranslatesToAnyQuantifiedOnPhysicalLeaf(@TempDir Path dir) throws Exception {
        FilterToPredicate t = nestedTranslator(dir);
        Filter f = ECQL.toFilter("addresses.locality = 'NYC'");

        FilterToPredicate.Result r = t.translate(f);

        assertThat(r.residual()).isEqualTo(Filter.INCLUDE);
        assertThat(r.predicate()).isInstanceOf(Predicate.Quantified.class);
        Predicate.Quantified quantified = (Predicate.Quantified) r.predicate();
        assertThat(quantified.match()).isEqualTo(io.tileverse.parquetry.filter.MatchAction.ANY);
        assertThat(quantified.leaf()).isEqualTo(Pred.col(lastPhysicalLocality).eq("NYC"));
    }

    @Test
    void nestedEqualsHonorsAllMatchAction(@TempDir Path dir) throws Exception {
        FilterToPredicate t = nestedTranslator(dir);
        Filter f = FF.equal(FF.property("addresses/locality"), FF.literal("NYC"), true, MatchAction.ALL);

        FilterToPredicate.Result r = t.translate(f);

        assertThat(r.predicate()).isInstanceOf(Predicate.Quantified.class);
        assertThat(((Predicate.Quantified) r.predicate()).match())
                .isEqualTo(io.tileverse.parquetry.filter.MatchAction.ALL);
    }

    @Test
    void nestedEqualsHonorsOneMatchAction(@TempDir Path dir) throws Exception {
        FilterToPredicate t = nestedTranslator(dir);
        Filter f = FF.equal(FF.property("addresses/locality"), FF.literal("NYC"), true, MatchAction.ONE);

        FilterToPredicate.Result r = t.translate(f);

        assertThat(r.predicate()).isInstanceOf(Predicate.Quantified.class);
        assertThat(((Predicate.Quantified) r.predicate()).match())
                .isEqualTo(io.tileverse.parquetry.filter.MatchAction.ONE);
    }

    @Test
    void unresolvableNestedPathIsResidual(@TempDir Path dir) throws Exception {
        FilterToPredicate t = nestedTranslator(dir);
        Filter f = FF.equals(FF.property("nonsense/x"), FF.literal("y"));

        FilterToPredicate.Result r = t.translate(f);

        assertThat(r.predicate()).isEqualTo((Predicate) Predicate.ALWAYS_TRUE);
        assertThat(r.residual()).isEqualTo(f);
    }

    @Test
    void topLevelScalarStillTranslatesWithoutQuantifier(@TempDir Path dir) throws Exception {
        FilterToPredicate t = nestedTranslator(dir);
        Filter f = FF.equals(FF.property("id"), FF.literal(1));

        FilterToPredicate.Result r = t.translate(f);

        assertThat(r.predicate()).isNotInstanceOf(Predicate.Quantified.class);
        assertThat(r.predicate()).isEqualTo(Pred.col(ColumnPath.of("id")).eq(1));
        assertThat(r.residual()).isEqualTo(Filter.INCLUDE);
    }

    @Test
    void unresolvableNestedPathDoesNotThrow(@TempDir Path dir) throws Exception {
        FilterToPredicate t = nestedTranslator(dir);
        assertThatCode(() -> t.translate(ECQL.toFilter("a.b.c = 'z'"))).doesNotThrowAnyException();
    }
}
