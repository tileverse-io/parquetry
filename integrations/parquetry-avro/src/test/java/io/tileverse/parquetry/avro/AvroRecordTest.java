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
package io.tileverse.parquetry.avro;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.Test;

class AvroRecordTest {

    private static AvroRecord sample() {
        AvroSchema.Record schema = new AvroSchema.Record(
                "r",
                null,
                List.of(),
                List.of(
                        AvroSchema.Field.of("status", 0, AvroSchema.INT),
                        AvroSchema.Field.of("path", 1, AvroSchema.STRING)));
        return new AvroRecord(schema, new Object[] {1, "a.parquet"});
    }

    @Test
    void readsByPosition() {
        assertThat(sample().get(0)).isEqualTo(1);
        assertThat(sample().get(1)).isEqualTo("a.parquet");
    }

    @Test
    void readsByName() {
        assertThat(sample().get("status")).isEqualTo(1);
        assertThat(sample().get("path")).isEqualTo("a.parquet");
    }

    @Test
    void exposesSchemaAndFieldCount() {
        assertThat(sample().schema().name()).isEqualTo("r");
        assertThat(sample().size()).isEqualTo(2);
    }

    @Test
    void rejectsUnknownField() {
        AvroRecord sampleRecord = sample();
        assertThatThrownBy(() -> sampleRecord.get("nope")).isInstanceOf(IllegalArgumentException.class);
    }
}
