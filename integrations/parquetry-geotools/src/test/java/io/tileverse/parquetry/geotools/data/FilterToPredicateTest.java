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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.stream.Stream;

import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.filter.Filter;
import org.geotools.api.filter.FilterFactory;
import org.geotools.api.filter.FilterVisitor;
import org.geotools.api.filter.MultiValuedFilter.MatchAction;
import org.geotools.api.filter.expression.Expression;
import org.geotools.api.filter.temporal.BinaryTemporalOperator;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.api.temporal.Period;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.filter.Capabilities;
import org.geotools.filter.text.ecql.ECQL;
import org.geotools.referencing.CRS;
import org.geotools.temporal.object.DefaultInstant;
import org.geotools.temporal.object.DefaultPeriod;
import org.geotools.temporal.object.DefaultPosition;
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
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.LogicalType.TimeUnit;
import io.tileverse.parquetry.geotools.data.FeatureTypeMapper.AttributeMapping;
import io.tileverse.parquetry.geotools.data.FeatureTypeMapper.Mapping;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.ResolvedColumn;
import io.tileverse.parquetry.schema.SchemaNode;

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
        b.add("amount", BigDecimal.class);
        b.add("bigamount", BigDecimal.class);
        b.add("day", LocalDate.class);
        b.add("stamp", Instant.class);
        b.add("local", LocalDateTime.class);
        b.add("stamp96", Instant.class);
        b.add("clock", LocalTime.class);
        b.add("clock32", LocalTime.class);
        b.add("blob", byte[].class);
        SimpleFeatureType ft = b.buildFeatureType();
        List<AttributeMapping> attrs = List.of(
                new AttributeMapping("geom", ColumnPath.of("geometry"), true, Point.class),
                new AttributeMapping("name", ColumnPath.of("name"), false, String.class),
                new AttributeMapping("pop", ColumnPath.of("pop"), false, Long.class),
                new AttributeMapping("score", ColumnPath.of("score"), false, Double.class),
                new AttributeMapping("active", ColumnPath.of("active"), false, Boolean.class),
                new AttributeMapping("guid", ColumnPath.of("guid"), false, UUID.class),
                new AttributeMapping(
                        "amount",
                        ColumnPath.of("amount"),
                        false,
                        BigDecimal.class,
                        null,
                        Optional.of(new LogicalType.Decimal(2, 9)),
                        PrimitiveKind.INT32),
                new AttributeMapping(
                        "bigamount",
                        ColumnPath.of("bigamount"),
                        false,
                        BigDecimal.class,
                        null,
                        Optional.of(new LogicalType.Decimal(3, 20)),
                        PrimitiveKind.FIXED_LEN_BYTE_ARRAY),
                new AttributeMapping(
                        "day",
                        ColumnPath.of("day"),
                        false,
                        LocalDate.class,
                        null,
                        Optional.of(new LogicalType.DateType()),
                        PrimitiveKind.INT32),
                new AttributeMapping(
                        "stamp",
                        ColumnPath.of("stamp"),
                        false,
                        Instant.class,
                        null,
                        Optional.of(new LogicalType.Timestamp(true, TimeUnit.MICROS)),
                        PrimitiveKind.INT64),
                new AttributeMapping(
                        "local",
                        ColumnPath.of("local"),
                        false,
                        LocalDateTime.class,
                        null,
                        Optional.of(new LogicalType.Timestamp(false, TimeUnit.MILLIS)),
                        PrimitiveKind.INT64),
                new AttributeMapping(
                        "stamp96",
                        ColumnPath.of("stamp96"),
                        false,
                        Instant.class,
                        null,
                        Optional.of(new LogicalType.Timestamp(true, TimeUnit.MICROS)),
                        PrimitiveKind.INT96),
                new AttributeMapping(
                        "clock",
                        ColumnPath.of("clock"),
                        false,
                        LocalTime.class,
                        null,
                        Optional.of(new LogicalType.Time(false, TimeUnit.MICROS)),
                        PrimitiveKind.INT64),
                new AttributeMapping(
                        "clock32",
                        ColumnPath.of("clock32"),
                        false,
                        LocalTime.class,
                        null,
                        Optional.of(new LogicalType.Time(false, TimeUnit.MILLIS)),
                        PrimitiveKind.INT32),
                new AttributeMapping(
                        "blob",
                        ColumnPath.of("blob"),
                        false,
                        byte[].class,
                        null,
                        Optional.empty(),
                        PrimitiveKind.BYTE_ARRAY));
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

    /** A filter that does not push keeps an always-true predicate and rides the residual whole. */
    @ParameterizedTest(name = "{0}")
    @MethodSource("residualFilters")
    void keepsAnUnpushableFilterResidual(String name, Filter f) {
        FilterToPredicate.Result r = translator().translate(f);
        assertThat(r.predicate()).isEqualTo((Predicate) Predicate.ALWAYS_TRUE);
        assertThat(r.residual()).isEqualTo(f);
    }

    /** Every filter that stays residual: the uuid, the scalar comparison, and the temporal operator cases. */
    private static Stream<Arguments> residualFilters() {
        return Stream.of(unpushableUuidFilters(), residualScalarComparisons(), residualTemporalOperators())
                .flatMap(vectors -> vectors);
    }

    // A UUID column pushes only equality with a parseable literal. An uncoercible literal or any non-EQ comparison
    // cannot push (the unsigned byte order has no sound ordered/notEq push) and falls to the residual filter.
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

    /** A filter that pushes whole translates to its predicate and leaves nothing for GeoTools to re-check. */
    @ParameterizedTest(name = "{0}")
    @MethodSource("pushedFilters")
    void pushesTheFilter(String name, Filter f, Predicate expected) {
        FilterToPredicate.Result r = translator().translate(f);
        assertThat(r.predicate()).isEqualTo(expected);
        assertThat(r.residual()).isEqualTo(Filter.INCLUDE);
    }

    /** Every filter that pushes whole: the scalar comparison and the temporal operator cases. */
    private static Stream<Arguments> pushedFilters() {
        return Stream.of(pushedScalarComparisons(), temporalOperators()).flatMap(vectors -> vectors);
    }

    private static Stream<Arguments> pushedScalarComparisons() {
        LocalDateTime noon = LocalDateTime.of(2024, 6, 15, 12, 30, 45, 123_000_000);
        Instant noonUtc = noon.toInstant(ZoneOffset.UTC);
        return Stream.of(
                Arguments.of(
                        "decimal gt from a double literal",
                        FF.greater(FF.property("amount"), FF.literal(2.5)),
                        new Predicate.Gt(ColumnPath.of("amount"), new Value.DecimalVal(new BigDecimal("2.5")))),
                Arguments.of(
                        "decimal eq from a text literal at another scale",
                        FF.equals(FF.property("amount"), FF.literal("1.250")),
                        new Predicate.Eq(ColumnPath.of("amount"), new Value.DecimalVal(new BigDecimal("1.250")))),
                Arguments.of(
                        "fixed-length decimal lt from a BigDecimal literal",
                        FF.less(FF.property("bigamount"), FF.literal(new BigDecimal("123456789012345.678"))),
                        new Predicate.Lt(
                                ColumnPath.of("bigamount"),
                                new Value.DecimalVal(new BigDecimal("123456789012345.678")))),
                Arguments.of(
                        "date gtEq from a text literal",
                        FF.greaterOrEqual(FF.property("day"), FF.literal("2024-01-05")),
                        new Predicate.GtEq(ColumnPath.of("day"), new Value.DateVal(LocalDate.of(2024, 1, 5)))),
                Arguments.of(
                        "date eq from a Date literal",
                        FF.equals(FF.property("day"), FF.literal(Date.from(noonUtc))),
                        new Predicate.Eq(ColumnPath.of("day"), new Value.DateVal(LocalDate.of(2024, 6, 15)))),
                Arguments.of(
                        "utc timestamp gt from an Instant literal",
                        FF.greater(FF.property("stamp"), FF.literal(noonUtc)),
                        new Predicate.Gt(ColumnPath.of("stamp"), new Value.TimestampVal(noon, true))),
                Arguments.of(
                        "utc timestamp lt from a Date literal",
                        FF.less(FF.property("stamp"), FF.literal(Date.from(noonUtc))),
                        new Predicate.Lt(ColumnPath.of("stamp"), new Value.TimestampVal(noon, true))),
                Arguments.of(
                        "utc timestamp eq from a zone-less LocalDateTime literal read at UTC",
                        FF.equals(FF.property("stamp"), FF.literal(noon)),
                        new Predicate.Eq(ColumnPath.of("stamp"), new Value.TimestampVal(noon, true))),
                Arguments.of(
                        "local timestamp gtEq from an Instant literal read at UTC",
                        FF.greaterOrEqual(FF.property("local"), FF.literal(noonUtc)),
                        new Predicate.GtEq(ColumnPath.of("local"), new Value.TimestampVal(noon, false))),
                Arguments.of(
                        "local timestamp notEq from an offset text literal",
                        FF.notEqual(FF.property("local"), FF.literal("2024-06-15T14:30:45.123+02:00")),
                        new Predicate.NotEq(ColumnPath.of("local"), new Value.TimestampVal(noon, false))),
                Arguments.of(
                        "time ltEq from a LocalTime literal",
                        FF.lessOrEqual(FF.property("clock"), FF.literal(LocalTime.of(5, 30, 15, 250_000_000))),
                        new Predicate.LtEq(
                                ColumnPath.of("clock"), new Value.TimeVal(LocalTime.of(5, 30, 15, 250_000_000)))),
                Arguments.of(
                        "time gt from a text literal",
                        FF.greater(FF.property("clock"), FF.literal("05:30:15.25")),
                        new Predicate.Gt(
                                ColumnPath.of("clock"), new Value.TimeVal(LocalTime.of(5, 30, 15, 250_000_000)))));
    }

    /**
     * A binary column pushes equality and inequality alone, and the pushed value holds a copy of the literal's bytes.
     * {@link Value.BinaryVal} wraps a {@link MemorySegment}, whose equality is backing-array identity rather than
     * content, hence the assertion on the bytes themselves.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("pushedBinaryComparisons")
    void pushesEqualityOnABinaryColumn(String name, Filter f, Class<? extends Predicate> expectedComparison) {
        FilterToPredicate.Result r = translator().translate(f);
        assertThat(r.residual()).isEqualTo(Filter.INCLUDE);
        assertThat(r.predicate()).isInstanceOf(expectedComparison);
        BinaryComparison pushed = binaryComparisonOf(r.predicate());
        assertThat(pushed.column()).isEqualTo(ColumnPath.of("blob"));
        assertThat(pushed.value().toArray(ValueLayout.JAVA_BYTE)).containsExactly(new byte[] {1, 2, 3});
    }

    private static Stream<Arguments> pushedBinaryComparisons() {
        byte[] bytes = {1, 2, 3};
        return Stream.of(
                Arguments.of("binary eq", FF.equals(FF.property("blob"), FF.literal(bytes)), Predicate.Eq.class),
                Arguments.of(
                        "binary notEq", FF.notEqual(FF.property("blob"), FF.literal(bytes)), Predicate.NotEq.class));
    }

    /** The column and the literal bytes of a pushed binary comparison. */
    private record BinaryComparison(ColumnPath column, MemorySegment value) {}

    private static BinaryComparison binaryComparisonOf(Predicate predicate) {
        return switch (predicate) {
            case Predicate.Eq(ColumnPath column, Value.BinaryVal(MemorySegment bytes)) ->
                new BinaryComparison(column, bytes);
            case Predicate.NotEq(ColumnPath column, Value.BinaryVal(MemorySegment bytes)) ->
                new BinaryComparison(column, bytes);
            default -> throw new AssertionError("not a binary equality comparison: " + predicate);
        };
    }

    private static Stream<Arguments> residualScalarComparisons() {
        return Stream.of(
                Arguments.of(
                        "timestamp literal finer than the micros column",
                        FF.greater(
                                FF.property("stamp"),
                                FF.literal(LocalDateTime.of(2024, 6, 15, 12, 30, 45, 123_456_789)))),
                Arguments.of(
                        "timestamp literal finer than the millis column",
                        FF.greater(
                                FF.property("local"),
                                FF.literal(LocalDateTime.of(2024, 6, 15, 12, 30, 45, 123_456_000)))),
                Arguments.of(
                        "time literal finer than the micros column",
                        FF.greater(FF.property("clock"), FF.literal(LocalTime.of(5, 30, 15, 250_000_001)))),
                Arguments.of(
                        "time on an INT32 column", FF.greater(FF.property("clock32"), FF.literal(LocalTime.of(5, 30)))),
                Arguments.of(
                        "timestamp on an INT96 column",
                        FF.greater(FF.property("stamp96"), FF.literal(LocalDateTime.of(2024, 6, 15, 12, 30)))),
                Arguments.of("unparseable date text", FF.equals(FF.property("day"), FF.literal("yesterday"))),
                Arguments.of("unparseable decimal text", FF.equals(FF.property("amount"), FF.literal("twelve"))),
                Arguments.of("binary ordering", FF.less(FF.property("blob"), FF.literal(new byte[] {1}))),
                Arguments.of("binary from a text literal", FF.equals(FF.property("blob"), FF.literal("010203"))));
    }

    @Test
    void betweenOnADateColumnPushesBothBounds() {
        Filter f = FF.between(FF.property("day"), FF.literal("2024-01-04"), FF.literal("2024-01-06"));
        FilterToPredicate.Result r = translator().translate(f);
        assertThat(r.predicate())
                .isEqualTo(Pred.and(
                        new Predicate.GtEq(ColumnPath.of("day"), new Value.DateVal(LocalDate.of(2024, 1, 4))),
                        new Predicate.LtEq(ColumnPath.of("day"), new Value.DateVal(LocalDate.of(2024, 1, 6)))));
        assertThat(r.residual()).isEqualTo(Filter.INCLUDE);
    }

    private static final LocalDateTime B = LocalDateTime.of(2024, 6, 1, 0, 0);
    private static final LocalDateTime E = LocalDateTime.of(2024, 7, 1, 0, 0);
    private static final LocalDateTime L = LocalDateTime.of(2024, 6, 15, 12, 0);
    private static final ColumnPath STAMP = ColumnPath.of("stamp");

    private static org.geotools.api.temporal.Instant gtInstant(LocalDateTime at) {
        return new DefaultInstant(new DefaultPosition(Date.from(at.toInstant(ZoneOffset.UTC))));
    }

    private static Period gtPeriod(LocalDateTime begin, LocalDateTime end) {
        return new DefaultPeriod(gtInstant(begin), gtInstant(end));
    }

    /** The value held by a comparison on {@code column}, adjusted to UTC when the mapped attribute binds to Instant. */
    private static Value.TimestampVal timestampValue(ColumnPath column, LocalDateTime at) {
        return new Value.TimestampVal(at, bindingOf(column) == Instant.class);
    }

    private static Class<?> bindingOf(ColumnPath column) {
        for (AttributeMapping attribute : mapping().attributes()) {
            if (attribute.path().equals(column)) {
                return attribute.binding();
            }
        }
        throw new AssertionError("no attribute is mapped to column " + column);
    }

    private static Predicate eq(ColumnPath column, LocalDateTime at) {
        return new Predicate.Eq(column, timestampValue(column, at));
    }

    private static Predicate lt(ColumnPath column, LocalDateTime at) {
        return new Predicate.Lt(column, timestampValue(column, at));
    }

    private static Predicate lte(ColumnPath column, LocalDateTime at) {
        return new Predicate.LtEq(column, timestampValue(column, at));
    }

    private static Predicate gt(ColumnPath column, LocalDateTime at) {
        return new Predicate.Gt(column, timestampValue(column, at));
    }

    private static Predicate gte(ColumnPath column, LocalDateTime at) {
        return new Predicate.GtEq(column, timestampValue(column, at));
    }

    /**
     * Every temporal operator on the {@code stamp} attribute, against an instant literal at {@code L} and against a
     * period literal bounded by {@code B} and {@code E}, with the attribute on either side. An operator in a position
     * never taken by an instant matches no row.
     */
    private static Stream<Arguments> temporalOperators() {
        Expression attr = FF.property("stamp");
        Expression instant = FF.literal(gtInstant(L));
        Expression period = FF.literal(gtPeriod(B, E));
        Predicate never = Predicate.ALWAYS_FALSE;
        Predicate inside = Pred.and(gt(STAMP, B), lt(STAMP, E));
        Predicate touching = Pred.and(gte(STAMP, B), lte(STAMP, E));
        return Stream.of(
                // attribute first, instant literal
                Arguments.of("after instant", FF.after(attr, instant), gt(STAMP, L)),
                Arguments.of("before instant", FF.before(attr, instant), lt(STAMP, L)),
                Arguments.of("tequals instant", FF.tequals(attr, instant), eq(STAMP, L)),
                Arguments.of("anyInteracts instant", FF.anyInteracts(attr, instant), eq(STAMP, L)),
                Arguments.of("during instant is never", FF.during(attr, instant), never),
                Arguments.of("begins instant is never", FF.begins(attr, instant), never),
                Arguments.of("ends instant is never", FF.ends(attr, instant), never),
                Arguments.of("begunBy instant is never", FF.begunBy(attr, instant), never),
                Arguments.of("endedBy instant is never", FF.endedBy(attr, instant), never),
                Arguments.of("tcontains instant is never", FF.tcontains(attr, instant), never),
                Arguments.of("meets instant is never", FF.meets(attr, instant), never),
                Arguments.of("metBy instant is never", FF.metBy(attr, instant), never),
                Arguments.of("overlappedBy instant is never", FF.overlappedBy(attr, instant), never),
                Arguments.of("toverlaps instant is never", FF.toverlaps(attr, instant), never),
                // attribute first, period literal
                Arguments.of("after period", FF.after(attr, period), gt(STAMP, E)),
                Arguments.of("before period", FF.before(attr, period), lt(STAMP, B)),
                Arguments.of("during period", FF.during(attr, period), inside),
                Arguments.of("begins period", FF.begins(attr, period), eq(STAMP, B)),
                Arguments.of("ends period", FF.ends(attr, period), eq(STAMP, E)),
                Arguments.of("anyInteracts period", FF.anyInteracts(attr, period), touching),
                Arguments.of("tequals period is never", FF.tequals(attr, period), never),
                Arguments.of("begunBy period is never", FF.begunBy(attr, period), never),
                Arguments.of("endedBy period is never", FF.endedBy(attr, period), never),
                Arguments.of("tcontains period is never", FF.tcontains(attr, period), never),
                Arguments.of("meets period is never", FF.meets(attr, period), never),
                Arguments.of("metBy period is never", FF.metBy(attr, period), never),
                Arguments.of("overlappedBy period is never", FF.overlappedBy(attr, period), never),
                Arguments.of("toverlaps period is never", FF.toverlaps(attr, period), never),
                // literal first, instant literal
                Arguments.of("instant after attribute", FF.after(instant, attr), lt(STAMP, L)),
                Arguments.of("instant before attribute", FF.before(instant, attr), gt(STAMP, L)),
                Arguments.of("instant tequals attribute", FF.tequals(instant, attr), eq(STAMP, L)),
                Arguments.of("instant anyInteracts attribute", FF.anyInteracts(instant, attr), eq(STAMP, L)),
                Arguments.of("instant during attribute is never", FF.during(instant, attr), never),
                Arguments.of("instant begins attribute is never", FF.begins(instant, attr), never),
                Arguments.of("instant ends attribute is never", FF.ends(instant, attr), never),
                Arguments.of("instant begunBy attribute is never", FF.begunBy(instant, attr), never),
                Arguments.of("instant endedBy attribute is never", FF.endedBy(instant, attr), never),
                Arguments.of("instant tcontains attribute is never", FF.tcontains(instant, attr), never),
                Arguments.of("instant meets attribute is never", FF.meets(instant, attr), never),
                Arguments.of("instant metBy attribute is never", FF.metBy(instant, attr), never),
                Arguments.of("instant overlappedBy attribute is never", FF.overlappedBy(instant, attr), never),
                Arguments.of("instant toverlaps attribute is never", FF.toverlaps(instant, attr), never),
                // literal first, period literal
                Arguments.of("period after attribute", FF.after(period, attr), lt(STAMP, B)),
                Arguments.of("period before attribute", FF.before(period, attr), gt(STAMP, E)),
                Arguments.of("period tcontains attribute", FF.tcontains(period, attr), inside),
                Arguments.of("period begunBy attribute", FF.begunBy(period, attr), eq(STAMP, B)),
                Arguments.of("period endedBy attribute", FF.endedBy(period, attr), eq(STAMP, E)),
                Arguments.of("period anyInteracts attribute", FF.anyInteracts(period, attr), touching),
                Arguments.of("period during attribute is never", FF.during(period, attr), never),
                Arguments.of("period begins attribute is never", FF.begins(period, attr), never),
                Arguments.of("period ends attribute is never", FF.ends(period, attr), never),
                Arguments.of("period tequals attribute is never", FF.tequals(period, attr), never),
                Arguments.of("period meets attribute is never", FF.meets(period, attr), never),
                Arguments.of("period metBy attribute is never", FF.metBy(period, attr), never),
                Arguments.of("period overlappedBy attribute is never", FF.overlappedBy(period, attr), never),
                Arguments.of("period toverlaps attribute is never", FF.toverlaps(period, attr), never),
                // other literal shapes
                Arguments.of(
                        "after a Date literal",
                        FF.after(attr, FF.literal(Date.from(L.toInstant(ZoneOffset.UTC)))),
                        gt(STAMP, L)),
                Arguments.of("after a text literal", FF.after(attr, FF.literal("2024-06-15T12:00:00Z")), gt(STAMP, L)),
                Arguments.of("after a LocalDateTime literal", FF.after(attr, FF.literal(L)), gt(STAMP, L)),
                Arguments.of(
                        "local column during period",
                        FF.during(FF.property("local"), period),
                        Pred.and(
                                new Predicate.Gt(ColumnPath.of("local"), new Value.TimestampVal(B, false)),
                                new Predicate.Lt(ColumnPath.of("local"), new Value.TimestampVal(E, false)))));
    }

    private static Stream<Arguments> residualTemporalOperators() {
        Expression period = FF.literal(gtPeriod(B, E));
        return Stream.of(
                Arguments.of("date attribute", FF.after(FF.property("day"), FF.literal(gtInstant(L)))),
                Arguments.of("INT96 timestamp attribute", FF.after(FF.property("stamp96"), FF.literal(gtInstant(L)))),
                Arguments.of("time attribute", FF.before(FF.property("clock"), FF.literal(gtInstant(L)))),
                Arguments.of("non-temporal attribute", FF.after(FF.property("pop"), FF.literal(gtInstant(L)))),
                Arguments.of("unconvertible literal", FF.after(FF.property("stamp"), FF.literal("someday"))),
                Arguments.of(
                        "period bound with no date",
                        FF.during(FF.property("stamp"), FF.literal(periodWithoutABeginningDate()))),
                Arguments.of("two properties", FF.after(FF.property("stamp"), FF.property("local"))),
                Arguments.of("two literals", FF.after(FF.literal(gtInstant(L)), period)));
    }

    /**
     * A period whose beginning holds a position with no date. {@link DefaultPeriod} validates its bounds on
     * construction, hence the valid pair first and the replacement after.
     */
    private static Period periodWithoutABeginningDate() {
        DefaultPeriod period = new DefaultPeriod(gtInstant(B), gtInstant(E));
        period.setBegining(new DefaultInstant(new DefaultPosition((Date) null)));
        return period;
    }

    /**
     * ECQL parses {@code DURING} to a period literal, {@code AFTER} to a {@link Date} literal, and {@code BEFORE OR
     * DURING} to a disjunction of a {@code BEFORE} on the period's beginning and the {@code DURING}, which the
     * translator pushes whole.
     */
    @Test
    void ecqlTemporalPredicatesTranslate() throws Exception {
        Predicate inside = Pred.and(gt(STAMP, B), lt(STAMP, E));

        FilterToPredicate.Result during =
                translator().translate(ECQL.toFilter("stamp DURING 2024-06-01T00:00:00Z/2024-07-01T00:00:00Z"));
        assertThat(during.predicate()).isEqualTo(inside);
        assertThat(during.residual()).isEqualTo(Filter.INCLUDE);

        FilterToPredicate.Result after = translator().translate(ECQL.toFilter("stamp AFTER 2024-06-15T12:00:00Z"));
        assertThat(after.predicate()).isEqualTo(gt(STAMP, L));
        assertThat(after.residual()).isEqualTo(Filter.INCLUDE);

        FilterToPredicate.Result beforeOrDuring = translator()
                .translate(ECQL.toFilter("stamp BEFORE OR DURING 2024-06-01T00:00:00Z/2024-07-01T00:00:00Z"));
        assertThat(beforeOrDuring.predicate()).isEqualTo(Pred.or(lt(STAMP, B), inside));
        assertThat(beforeOrDuring.residual()).isEqualTo(Filter.INCLUDE);
    }

    /**
     * A temporal operator outside the fourteen declared types. Its position rules are unknown to the translator, which
     * must leave it to GeoTools instead of guessing a relation that could match no row.
     */
    private record UnknownTemporalOperator(Expression getExpression1, Expression getExpression2)
            implements BinaryTemporalOperator {

        @Override
        public MatchAction getMatchAction() {
            return MatchAction.ANY;
        }

        @Override
        public boolean evaluate(Object object) {
            return false;
        }

        @Override
        public Object accept(FilterVisitor visitor, Object extraData) {
            return extraData;
        }
    }

    @Test
    void keepsAnUndeclaredTemporalOperatorResidual() {
        Filter f = new UnknownTemporalOperator(FF.property("stamp"), FF.literal(gtInstant(L)));

        FilterToPredicate.Result r = translator().translate(f);

        assertThat(r.predicate()).isEqualTo((Predicate) Predicate.ALWAYS_TRUE);
        assertThat(r.residual()).isEqualTo(f);
    }

    @Test
    void keepsAnUndeclaredTemporalOperatorOnAPeriodResidual() {
        Filter f = new UnknownTemporalOperator(FF.property("stamp"), FF.literal(gtPeriod(B, E)));

        FilterToPredicate.Result r = translator().translate(f);

        assertThat(r.predicate()).isEqualTo((Predicate) Predicate.ALWAYS_TRUE);
        assertThat(r.residual()).isEqualTo(f);
    }

    /**
     * A translator over a schema whose only filterable leaf is a list of UTC timestamps, reachable at the logical path
     * {@code events.at}: root -> events LIST -> list (repeated) -> element -> at INT64 Timestamp(true, MICROS).
     */
    private static FilterToPredicate repeatedTimestampTranslator() {
        SchemaNode.Primitive at = new SchemaNode.Primitive(
                "at",
                Repetition.OPTIONAL,
                PrimitiveKind.INT64,
                OptionalInt.empty(),
                Optional.of(new LogicalType.Timestamp(true, TimeUnit.MICROS)),
                -1);
        SchemaNode.Group element =
                new SchemaNode.Group("element", Repetition.OPTIONAL, List.of(at), Optional.empty(), -1);
        SchemaNode.Group list =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        SchemaNode.Group events =
                new SchemaNode.Group("events", Repetition.OPTIONAL, List.of(list), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(events), Optional.empty(), -1);
        SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
        builder.setName("events");
        builder.add("id", Long.class);
        Mapping m =
                new Mapping(builder.buildFeatureType(), new ParquetSchema(root), List.of(), Optional.empty(), Map.of());
        return new FilterToPredicate(m, null);
    }

    /**
     * Proves the repeated timestamp leaf resolves and is recognized as multi-valued, which is what makes the temporal
     * residual assertions below meaningful rather than vacuous.
     */
    @Test
    void repeatedTimestampLeafPushesAPlainComparisonAsQuantified() {
        Filter f = FF.equals(FF.property("events/at"), FF.literal(Date.from(L.toInstant(ZoneOffset.UTC))));

        FilterToPredicate.Result r = repeatedTimestampTranslator().translate(f);

        assertThat(r.residual()).isEqualTo(Filter.INCLUDE);
        assertThat(r.predicate()).isInstanceOf(Predicate.Quantified.class);
        Predicate.Quantified quantified = (Predicate.Quantified) r.predicate();
        assertThat(quantified.leaf())
                .isEqualTo(new Predicate.Eq(
                        ColumnPath.of("events", "list", "element", "at"), new Value.TimestampVal(L, true)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("repeatedLeafTemporalOperators")
    void keepsATemporalOperatorOnARepeatedLeafResidual(String name, Filter f) {
        FilterToPredicate.Result r = repeatedTimestampTranslator().translate(f);
        assertThat(r.predicate()).isEqualTo((Predicate) Predicate.ALWAYS_TRUE);
        assertThat(r.residual()).isEqualTo(f);
    }

    private static Stream<Arguments> repeatedLeafTemporalOperators() {
        Expression events = FF.property("events/at");
        return Stream.of(
                Arguments.of("after an instant literal", FF.after(events, FF.literal(gtInstant(L)))),
                Arguments.of(
                        "during a period literal with ALL",
                        FF.during(events, FF.literal(gtPeriod(B, E)), MatchAction.ALL)),
                Arguments.of("meets a period literal", FF.meets(events, FF.literal(gtPeriod(B, E)))));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("declaredTemporalOperators")
    void capabilitiesDeclareTheTemporalOperator(String name, Filter f) {
        Capabilities capabilities = PushdownFilterCapabilities.create();
        assertThat(capabilities.supports(f)).isTrue();
    }

    /** The fourteen OGC temporal operators, each against the literal shape required by its relation. */
    private static Stream<Arguments> declaredTemporalOperators() {
        Expression attr = FF.property("stamp");
        Expression instant = FF.literal(gtInstant(L));
        Expression period = FF.literal(gtPeriod(B, E));
        return Stream.of(
                Arguments.of("after", FF.after(attr, instant)),
                Arguments.of("anyInteracts", FF.anyInteracts(attr, period)),
                Arguments.of("before", FF.before(attr, instant)),
                Arguments.of("begins", FF.begins(attr, period)),
                Arguments.of("begunBy", FF.begunBy(attr, period)),
                Arguments.of("during", FF.during(attr, period)),
                Arguments.of("endedBy", FF.endedBy(attr, period)),
                Arguments.of("ends", FF.ends(attr, period)),
                Arguments.of("meets", FF.meets(attr, period)),
                Arguments.of("metBy", FF.metBy(attr, period)),
                Arguments.of("overlappedBy", FF.overlappedBy(attr, period)),
                Arguments.of("tcontains", FF.tcontains(attr, period)),
                Arguments.of("tequals", FF.tequals(attr, instant)),
                Arguments.of("toverlaps", FF.toverlaps(attr, period)));
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
            Mapping mapping = FeatureTypeMapper.map("nested", null, dataset.schema(), dataset.geoMetadata(), null);
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
