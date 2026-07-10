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
package io.tileverse.parquetry.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class IcebergNameMappingTest {

    @Test
    void resolvesEveryAliasToItsFieldId() {
        IcebergNameMapping mapping = IcebergNameMapping.fromJson("[{\"field-id\": 2, \"names\": [\"n\", \"count\"]}]");

        assertThat(mapping.idFor("n")).hasValue(2);
        assertThat(mapping.idFor("count")).hasValue(2);
        assertThat(mapping.idFor("missing")).isEmpty();
    }

    @Test
    void skipsAnEntryWithoutAFieldId() {
        IcebergNameMapping mapping = IcebergNameMapping.fromJson("[{\"names\": [\"orphan\"]}]");

        assertThat(mapping.idFor("orphan")).isEmpty();
    }

    @Test
    void anEmptyNamesListMatchesNothing() {
        IcebergNameMapping mapping = IcebergNameMapping.fromJson("[{\"field-id\": 1, \"names\": []}]");

        assertThat(mapping.idFor("anything")).isEmpty();
    }

    @Test
    void aLaterEntryWinsADuplicateName() {
        IcebergNameMapping mapping = IcebergNameMapping.fromJson(
                "[{\"field-id\": 1, \"names\": [\"x\"]}, {\"field-id\": 2, \"names\": [\"x\"]}]");

        assertThat(mapping.idFor("x")).hasValue(2);
    }

    @Test
    void toleratesNestedFieldsEntriesWithoutFlatteningThem() {
        IcebergNameMapping mapping = IcebergNameMapping.fromJson(
                "[{\"field-id\": 3, \"names\": [\"location\"], \"fields\": [{\"field-id\": 4, \"names\": [\"lat\"]}]}]");

        assertThat(mapping.idFor("location")).hasValue(3);
        assertThat(mapping.idFor("lat")).isEmpty();
    }

    @Test
    void derivesTheImplicitMappingFromTheSchemaFields() {
        IcebergNameMapping mapping = IcebergNameMapping.fromSchema(
                List.of(new IcebergField(1, "id", "string", false), new IcebergField(2, "n", "int", false)));

        assertThat(mapping.idFor("id")).hasValue(1);
        assertThat(mapping.idFor("n")).hasValue(2);
        assertThat(mapping.idFor("count")).isEmpty();
    }

    @Test
    void emptyMappingResolvesNothing() {
        assertThat(IcebergNameMapping.empty().idFor("id")).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("malformedDocuments")
    void rejectsAMalformedDocument(String json) {
        assertThatThrownBy(() -> IcebergNameMapping.fromJson(json))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("schema.name-mapping.default");
    }

    static Stream<String> malformedDocuments() {
        return Stream.of(
                "{ not json",
                "{\"field-id\": 1}",
                "[42]",
                "[{\"field-id\": 1}]",
                "[{\"field-id\": 1, \"names\": \"id\"}]",
                "[{\"field-id\": 1, \"names\": [7]}]",
                "[{\"field-id\": \"one\", \"names\": [\"id\"]}]");
    }
}
