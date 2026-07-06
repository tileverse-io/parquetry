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
package io.tileverse.parquetry.filter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.schema.ColumnPath;

class ConstantColumnTest {

    @Test
    void holdsPathAndValue() {
        ConstantColumn column = new ConstantColumn(ColumnPath.of("year"), new Value.IntVal(2024));
        assertThat(column.path()).isEqualTo(ColumnPath.of("year"));
        assertThat(column.value()).isEqualTo(new Value.IntVal(2024));
    }

    @Test
    void rejectsNulls() {
        Value intValue = new Value.IntVal(1);
        ColumnPath y = ColumnPath.of("y");
        assertThatThrownBy(() -> new ConstantColumn(null, intValue)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new ConstantColumn(y, null)).isInstanceOf(NullPointerException.class);
    }
}
