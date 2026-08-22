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
package io.tileverse.parquetry.cli.expr;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

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

    @Test
    void referenceListsEveryExtentFunctionTheTranslatorRecognizes() {
        String reference = FilterSyntax.reference();
        for (String extent : SpatialFilterTranslator.extentFunctionNames()) {
            assertThat(reference).as("extent %s listed", extent).contains(extent);
        }
    }

    @Test
    void referenceExplainsWhatTheExtentFormMeans() {
        String reference = FilterSyntax.reference();
        assertThat(reference)
                .contains("compares bounding boxes")
                .contains("ST_Intersects, ST_Covers, ST_CoveredBy, ST_Equals and ST_Disjoint")
                .contains("the other six are rejected")
                .contains("ST_MakeEnvelope(...), ST_Extent(...) or ST_Envelope(...)")
                .contains("Edges are inclusive");
    }

    @Test
    void theExtentNoteBreaksAtItsSentences() {
        List<String> extentNote = FilterSyntax.reference()
                .lines()
                .filter(line -> line.contains("ST_Extent(<geometry column>)") || line.startsWith("    "))
                .toList();

        assertThat(extentNote)
                .as("one sentence per line keeps the note off a terminal's soft wrap")
                .hasSize(4)
                .allSatisfy(line -> assertThat(line).hasSizeLessThan(150));
    }

    @Test
    void extentFunctionNamesAreCanonicalFirst() {
        assertThat(SpatialFilterTranslator.extentFunctionNames()).containsExactly("ST_Extent", "ST_Envelope");
    }
}
