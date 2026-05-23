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
package io.tileverse.parquetry.batch;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.Arena;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class BatchMaterializerTest {

    @Test
    void defaultBatchMaterializerReturnsBatchAsIs() {
        BatchMaterializer<ParquetRecordBatch> m = BatchMaterializer.defaultBatch();
        ParquetSchema schema = minimalSchema();
        try (ParquetRecordBatch batch = new DefaultParquetRecordBatch(schema, Map.of(), 0, Arena.ofConfined())) {
            assertThat(m.materialize(schema, batch)).isSameAs(batch);
        }
    }

    /**
     * Returns a minimal one-column schema sufficient for tests that don't exercise schema content. Uses the same
     * construction pattern as ParquetRecordBatchTest.minimalSchema().
     */
    private static ParquetSchema minimalSchema() {
        return new ParquetSchema(new SchemaNode.Group(
                "root",
                Repetition.REQUIRED,
                List.of(new SchemaNode.Primitive(
                        "value", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1)),
                Optional.empty(),
                -1));
    }
}
