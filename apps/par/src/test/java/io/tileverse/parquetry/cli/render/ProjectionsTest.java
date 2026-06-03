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
package io.tileverse.parquetry.cli.render;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.cli.support.Fixtures;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

class ProjectionsTest {

    private final ParquetSchema schema = Fixtures.citiesSchema();

    @Test
    void emptyColumnsMeansAll() {
        Projections.Resolved resolved = Projections.resolve(List.of(), schema);
        assertThat(resolved.projection()).isEqualTo(Projection.ALL);
        assertThat(resolved.keptLeaves()).isEqualTo(schema.leafColumns());
    }

    @Test
    void namedColumnsBecomeProjectionColumns() {
        Projections.Resolved resolved = Projections.resolve(List.of("name", "pop"), schema);
        assertThat(resolved.keptLeaves()).containsExactly(ColumnPath.of("name"), ColumnPath.of("pop"));
        assertThat(resolved.projection()).isInstanceOf(Projection.Columns.class);
    }

    @Test
    void unknownColumnIsRejected() {
        assertThatThrownBy(() -> Projections.resolve(List.of("nope"), schema)).hasMessageContaining("unknown column");
    }
}
