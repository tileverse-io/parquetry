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
package io.tileverse.parquetry.schema.geo;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.tileverse.parquetry.schema.geo.projjson.GeographicCRS;
import io.tileverse.parquetry.schema.geo.projjson.Identifier;

/**
 * Covers {@link ParquetCrs#reference(String)} classification of the native Parquet {@code crs} reference forms and the
 * {@link ParquetCrs#epsgCode()} derivation. Inline PROJJSON is classified by the deserializer, not here.
 */
class ParquetCrsTest {

    static Stream<Arguments> referenceForms() {
        return Stream.of(
                Arguments.of("EPSG:3857", new ParquetCrs.AuthorityCode("EPSG", "3857")),
                Arguments.of("OGC:CRS84", new ParquetCrs.AuthorityCode("OGC", "CRS84")),
                Arguments.of("  EPSG:4326  ", new ParquetCrs.AuthorityCode("EPSG", "4326")),
                Arguments.of("srid:0", new ParquetCrs.Srid(0)),
                Arguments.of("srid:4326", new ParquetCrs.Srid(4326)),
                Arguments.of("SRID:3857", new ParquetCrs.Srid(3857)),
                Arguments.of("projjson:my-key", new ParquetCrs.ProjjsonKey("my-key")),
                Arguments.of("PROJJSON:my-key", new ParquetCrs.ProjjsonKey("my-key")));
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @MethodSource("referenceForms")
    void classifiesReferenceForms(String raw, ParquetCrs expected) {
        assertThat(ParquetCrs.reference(raw)).contains(expected);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "   ", "not-a-reference", "srid:notanumber", "projjson:", "foo:", ":bar"})
    void unclassifiableStringsDegradeToEmpty(String raw) {
        assertThat(ParquetCrs.reference(raw)).isEmpty();
    }

    @Test
    void epsgCodeIsExposedOnlyWhenDerivableWithoutRegistry() {
        assertThat(new ParquetCrs.AuthorityCode("EPSG", "3857").epsgCode()).hasValue(3857);
        assertThat(new ParquetCrs.AuthorityCode("epsg", "4326").epsgCode()).hasValue(4326);
        assertThat(new ParquetCrs.AuthorityCode("OGC", "CRS84").epsgCode()).isEmpty();
        assertThat(new ParquetCrs.AuthorityCode("EPSG", "not-a-number").epsgCode())
                .isEmpty();
        assertThat(new ParquetCrs.Srid(3857).epsgCode()).hasValue(3857);
        assertThat(new ParquetCrs.Srid(0).epsgCode()).isEmpty();
        assertThat(new ParquetCrs.ProjjsonKey("k").epsgCode()).isEmpty();
    }

    @Test
    void inlineEpsgCodeComesFromTheProjjsonIdentifier() {
        ParquetCrs withEpsg = ParquetCrs.inline(new GeographicCRS(
                Optional.of("WGS 84"),
                Optional.of(new Identifier("EPSG", "4326", Optional.empty(), Optional.empty())),
                Optional.empty(),
                Optional.empty()));
        assertThat(withEpsg.epsgCode()).hasValue(4326);

        ParquetCrs withoutId = ParquetCrs.inline(
                new GeographicCRS(Optional.of("WGS 84"), Optional.empty(), Optional.empty(), Optional.empty()));
        assertThat(withoutId.epsgCode()).isEqualTo(OptionalInt.empty());
    }

    @Test
    void inlineFactoryRejectsNull() {
        assertThat(catchThrowable(() -> ParquetCrs.inline(null))).isInstanceOf(NullPointerException.class);
    }

    private static Throwable catchThrowable(Runnable action) {
        try {
            action.run();
            return null;
        } catch (Throwable thrown) {
            return thrown;
        }
    }
}
