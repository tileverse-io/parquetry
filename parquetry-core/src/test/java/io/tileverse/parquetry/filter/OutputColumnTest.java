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
package io.tileverse.parquetry.filter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.PrimitiveKind;

class OutputColumnTest {

    @Test
    void sourcesExposeNameAndSource() {
        ColumnPath count = ColumnPath.of("count");
        ColumnPath n = ColumnPath.of("n");
        assertThat(new OutputColumn.Physical(count, n).name()).isEqualTo(count);
        assertThat(new OutputColumn.Physical(count, n).source()).isEqualTo(n);
        assertThat(new OutputColumn.Constant(ColumnPath.of("label"), new Value.StringVal("x")).name())
                .isEqualTo(ColumnPath.of("label"));
        OutputColumn.Promoted promoted = new OutputColumn.Promoted(ColumnPath.of("big"), n, PrimitiveKind.INT64);
        assertThat(promoted.target()).isEqualTo(PrimitiveKind.INT64);
        assertThat(promoted.source()).isEqualTo(n);
        OutputColumn.Null nullColumn = new OutputColumn.Null(ColumnPath.of("label"), new Value.StringVal("x"));
        assertThat(nullColumn.name()).isEqualTo(ColumnPath.of("label"));
        assertThat(nullColumn.typeOf()).isEqualTo(new Value.StringVal("x"));
    }

    @Test
    void nullChecksHold() {
        ColumnPath n = ColumnPath.of("n");
        assertThatThrownBy(() -> new OutputColumn.Physical(null, n)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new OutputColumn.Constant(ColumnPath.of("x"), null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new OutputColumn.Promoted(ColumnPath.of("x"), n, null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new OutputColumn.Null(ColumnPath.of("x"), null))
                .isInstanceOf(NullPointerException.class);
    }
}
