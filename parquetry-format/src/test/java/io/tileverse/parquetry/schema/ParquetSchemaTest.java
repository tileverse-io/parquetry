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
package io.tileverse.parquetry.schema;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.LogicalType;

class SchemaTest {

    private static final ParquetSchema FLAT = new ParquetSchema(new Field.Group(
            "root",
            Repetition.REQUIRED,
            List.of(
                    new Field.Primitive(
                            "year",
                            Repetition.REQUIRED,
                            PrimitiveKind.INT32,
                            OptionalInt.empty(),
                            Optional.empty(),
                            -1),
                    new Field.Primitive(
                            "country",
                            Repetition.REQUIRED,
                            PrimitiveKind.BYTE_ARRAY,
                            OptionalInt.empty(),
                            Optional.of(new LogicalType.StringType()),
                            -1),
                    new Field.Primitive(
                            "geometry",
                            Repetition.OPTIONAL,
                            PrimitiveKind.BYTE_ARRAY,
                            OptionalInt.empty(),
                            Optional.empty(),
                            -1)),
            Optional.empty(),
            -1));

    @Test
    void leafColumnsListsAllPrimitives() {
        assertThat(FLAT.leafColumns())
                .containsExactly(ColumnPath.of("year"), ColumnPath.of("country"), ColumnPath.of("geometry"));
    }

    @Test
    void findReturnsFieldByPath() {
        assertThat(FLAT.find(ColumnPath.of("year"))).isPresent();
        assertThat(FLAT.find(ColumnPath.of("nonexistent"))).isEmpty();
    }

    @Test
    void projectionDropsUnreferencedColumns() {
        ParquetSchema projected = FLAT.project(Set.of(ColumnPath.of("year"), ColumnPath.of("geometry")));
        assertThat(projected.leafColumns()).containsExactly(ColumnPath.of("year"), ColumnPath.of("geometry"));
    }

    @Test
    void columnPathDotPrintsHumanReadable() {
        assertThat(ColumnPath.of("a", "b", "c").dot()).isEqualTo("a.b.c");
    }
}
