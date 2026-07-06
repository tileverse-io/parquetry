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
package io.tileverse.parquetry.data;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.internal.read.BatchForm;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Pins the row read's batch-form decision: the levels form only when a per-row filter runs. A null {@code recordFilter}
 * (the pushdown-only path, where every decoded row is emitted) takes the assembled form; a non-null filter takes the
 * levels form. This is the exact condition the row read uses when it hands a filter to the batch pipeline.
 */
class RowsFormTest {

    @Test
    void aNullRecordFilterTakesTheAssembledForm() {
        assertThat(ParquetFileReader.rowsForm(null)).isEqualTo(BatchForm.ASSEMBLED);
    }

    @Test
    void aNonNullRecordFilterTakesTheLevelsForm() {
        Predicate filter = new Predicate.Eq(ColumnPath.of("id"), new Value.IntVal(1));
        assertThat(ParquetFileReader.rowsForm(filter)).isEqualTo(BatchForm.LEVELS);
    }
}
