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
package io.tileverse.parquetry.cli.expr;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class FilterSyntaxTest {

    @Test
    void referenceListsScalarLogicalAndSetOperators() {
        String reference = FilterSyntax.reference();
        assertThat(reference)
                .contains("= != <> < <= > >=")
                .contains("AND, OR, NOT")
                .contains("IN")
                .contains("BETWEEN")
                .contains("IS NULL")
                .contains("IS NOT NULL");
    }

    @Test
    void referenceListsEverySpatialFunctionTheTranslatorRecognizes() {
        String reference = FilterSyntax.reference();
        for (String relation : SpatialFilterTranslator.relationFunctionNames()) {
            assertThat(reference).as("relation %s listed", relation).contains(relation);
        }
        for (String constructor : SpatialFilterTranslator.queryConstructorNames()) {
            assertThat(reference).as("constructor %s listed", constructor).contains(constructor);
        }
    }

    @Test
    void referenceCoversTheKnownSpatialFunctions() {
        assertThat(SpatialFilterTranslator.relationFunctionNames())
                .contains("ST_Intersects", "ST_Contains", "ST_Within", "ST_DWithin", "ST_Equals");
        assertThat(SpatialFilterTranslator.queryConstructorNames())
                .containsExactly("ST_GeomFromText", "ST_MakeEnvelope");
    }
}
