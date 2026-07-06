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
package io.tileverse.parquetry.geotools.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.geotools.api.data.Query;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.filter.Filter;
import org.geotools.api.filter.FilterFactory;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Point;

import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.geotools.data.FeatureTypeMapper.AttributeMapping;
import io.tileverse.parquetry.geotools.data.FeatureTypeMapper.Mapping;
import io.tileverse.parquetry.schema.ColumnPath;

class QueryTranslatorTest {

    private static final FilterFactory FF = CommonFactoryFinder.getFilterFactory();

    private static Mapping mapping() {
        SimpleFeatureTypeBuilder b = new SimpleFeatureTypeBuilder();
        b.setName("t");
        b.add("geom", Point.class);
        b.add("name", String.class);
        b.add("pop", Long.class);
        SimpleFeatureType ft = b.buildFeatureType();
        List<AttributeMapping> attrs = List.of(
                new AttributeMapping("geom", ColumnPath.of("geometry"), true, Point.class),
                new AttributeMapping("name", ColumnPath.of("name"), false, String.class),
                new AttributeMapping("pop", ColumnPath.of("pop"), false, Long.class));
        return new Mapping(ft, attrs);
    }

    private static Mapping mappingWithFid() {
        return mappingWithFid("name");
    }

    private static Mapping mappingWithFid(String column) {
        Mapping base = mapping();
        AttributeMapping fid = base.attributes().stream()
                .filter(a -> a.name().equals(column))
                .findFirst()
                .orElseThrow();
        return new Mapping(base.featureType(), base.attributes(), Optional.of(fid));
    }

    private static Mapping mappingWithUuidFid() {
        SimpleFeatureTypeBuilder b = new SimpleFeatureTypeBuilder();
        b.setName("t");
        b.add("geom", Point.class);
        b.add("guid", UUID.class);
        SimpleFeatureType ft = b.buildFeatureType();
        AttributeMapping fid = new AttributeMapping("guid", ColumnPath.of("guid"), false, UUID.class);
        List<AttributeMapping> attrs =
                List.of(new AttributeMapping("geom", ColumnPath.of("geometry"), true, Point.class), fid);
        return new Mapping(ft, attrs, Optional.of(fid));
    }

    @Test
    void idFilterWithoutFeatureIdColumnIsRejected() {
        QueryTranslator translator = new QueryTranslator(mapping());
        Query q = new Query("t", FF.id(Set.of(FF.featureId("x"))));
        assertThatThrownBy(() -> translator.translate(q)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void idFilterPushesInPredicateOnTheFeatureIdColumnAndKeepsResidual() {
        Filter idFilter = FF.id(Set.of(FF.featureId("x")));
        Query q = new Query("t", idFilter);
        QueryTranslator.TranslatedQuery t = new QueryTranslator(mappingWithFid()).translate(q);
        assertThat(t.predicate()).isEqualTo(new Predicate.In(ColumnPath.of("name"), List.of(new Value.StringVal("x"))));
        assertThat(t.postFilter()).isEqualTo(idFilter);
    }

    @Test
    void idFilterWithNumericFeatureIdColumnPushesLongValues() {
        Filter idFilter = FF.id(Set.of(FF.featureId("10"), FF.featureId("20")));
        QueryTranslator.TranslatedQuery t =
                new QueryTranslator(mappingWithFid("pop")).translate(new Query("t", idFilter));
        assertThat(t.predicate()).isInstanceOf(Predicate.In.class);
        Predicate.In in = (Predicate.In) t.predicate();
        assertThat(in.col()).isEqualTo(ColumnPath.of("pop"));
        assertThat(in.values()).containsExactlyInAnyOrder(new Value.LongVal(10), new Value.LongVal(20));
    }

    @Test
    void idFilterTreatsTheFeatureIdAsAPureValue() {
        // The store deals in bare ids; a dotted id is a literal value, not a typeName-prefixed one to unwind.
        Filter idFilter = FF.id(Set.of(FF.featureId("a.b")));
        QueryTranslator.TranslatedQuery t = new QueryTranslator(mappingWithFid()).translate(new Query("t", idFilter));
        assertThat(t.predicate())
                .isEqualTo(new Predicate.In(ColumnPath.of("name"), List.of(new Value.StringVal("a.b"))));
    }

    @Test
    void idFilterWithNoFeatureIdMatchingTheColumnTypeIsAlwaysFalse() {
        Filter idFilter = FF.id(Set.of(FF.featureId("not-a-number")));
        QueryTranslator.TranslatedQuery t =
                new QueryTranslator(mappingWithFid("pop")).translate(new Query("t", idFilter));
        assertThat(t.predicate()).isEqualTo((Predicate) Predicate.ALWAYS_FALSE);
    }

    @Test
    void idFilterWithUuidFeatureIdColumnPushesUuidValues() {
        UUID u1 = UUID.fromString("00000000-0000-0000-0000-000000000001");
        UUID u2 = UUID.fromString("00000000-0000-0000-0000-000000000002");
        Filter idFilter = FF.id(Set.of(FF.featureId(u1.toString()), FF.featureId(u2.toString())));
        QueryTranslator.TranslatedQuery t =
                new QueryTranslator(mappingWithUuidFid()).translate(new Query("t", idFilter));
        assertThat(t.predicate()).isInstanceOf(Predicate.In.class);
        Predicate.In in = (Predicate.In) t.predicate();
        assertThat(in.col()).isEqualTo(ColumnPath.of("guid"));
        assertThat(in.values()).containsExactlyInAnyOrder(new Value.UuidVal(u1), new Value.UuidVal(u2));
        assertThat(t.postFilter()).isEqualTo(idFilter);
    }

    @Test
    void idFilterWithUuidColumnDropsUnparseableIds() {
        UUID valid = UUID.fromString("00000000-0000-0000-0000-000000000001");
        Filter idFilter = FF.id(Set.of(FF.featureId(valid.toString()), FF.featureId("not-a-uuid")));
        QueryTranslator.TranslatedQuery t =
                new QueryTranslator(mappingWithUuidFid()).translate(new Query("t", idFilter));
        assertThat(t.predicate()).isInstanceOf(Predicate.In.class);
        Predicate.In in = (Predicate.In) t.predicate();
        assertThat(in.values()).containsExactly(new Value.UuidVal(valid));
    }

    @Test
    void idFilterWithAllUnparseableUuidIdsIsAlwaysFalse() {
        Filter idFilter = FF.id(Set.of(FF.featureId("not-a-uuid")));
        QueryTranslator.TranslatedQuery t =
                new QueryTranslator(mappingWithUuidFid()).translate(new Query("t", idFilter));
        assertThat(t.predicate()).isEqualTo((Predicate) Predicate.ALWAYS_FALSE);
    }

    @Test
    void projectionIncludesFeatureIdColumn() {
        Query q = new Query("t", Filter.INCLUDE, new String[] {"geom"});
        QueryTranslator.TranslatedQuery t = new QueryTranslator(mappingWithFid()).translate(q);
        assertThat(t.readProjection())
                .isEqualTo(Projection.ofPhysical(Set.of(ColumnPath.of("geometry"), ColumnPath.of("name"))));
    }

    @Test
    void fullyPushableFilterHasNoResidual() {
        Query q = new Query("t", FF.greater(FF.property("pop"), FF.literal(10)));
        QueryTranslator.TranslatedQuery t = new QueryTranslator(mapping()).translate(q);
        assertThat(t.predicate()).isEqualTo(Pred.col(ColumnPath.of("pop")).gt(10L));
        assertThat(t.postFilter()).isEqualTo(Filter.INCLUDE);
    }

    @Test
    void mixedFilterSplitsPreAndPost() {
        Filter mixed = FF.and(FF.greater(FF.property("pop"), FF.literal(10)), FF.like(FF.property("name"), "A%"));
        Query q = new Query("t", mixed);
        QueryTranslator.TranslatedQuery t = new QueryTranslator(mapping()).translate(q);
        assertThat(t.predicate()).isEqualTo(Pred.col(ColumnPath.of("pop")).gt(10L));
        assertThat(t.postFilter()).isEqualTo(FF.like(FF.property("name"), "A%"));
    }

    @Test
    void projectionIncludesPostFilterColumnsButNotPrePredicateColumns() {
        Filter mixed = FF.and(FF.greater(FF.property("pop"), FF.literal(10)), FF.like(FF.property("name"), "A%"));
        Query q = new Query("t", mixed, new String[] {"geom"});
        QueryTranslator.TranslatedQuery t = new QueryTranslator(mapping()).translate(q);
        // geometry (output) + name (post filter); NOT pop (pushed predicate -> parquetry decodes it itself)
        assertThat(t.readProjection())
                .isEqualTo(Projection.ofPhysical(Set.of(ColumnPath.of("geometry"), ColumnPath.of("name"))));
    }

    @Test
    void allPropertiesGivesProjectionAll() {
        Query q = new Query("t", Filter.INCLUDE);
        QueryTranslator.TranslatedQuery t = new QueryTranslator(mapping()).translate(q);
        assertThat(t.readProjection()).isEqualTo(Projection.ALL);
        assertThat(t.predicate()).isEqualTo((Predicate) Predicate.ALWAYS_TRUE);
    }
}
