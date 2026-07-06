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
package io.tileverse.parquetry.avro;

/**
 * A decoded Avro record: positional and by-name access over the values for one row, paired with its
 * {@link AvroSchema.Record}. Value mapping: record -> {@code AvroRecord}, array -> {@code List}, map -> {@code Map},
 * int -> {@code Integer}, long -> {@code Long}, float -> {@code Float}, double -> {@code Double}, boolean ->
 * {@code Boolean}, string -> {@code String}, bytes/fixed -> read-only {@code MemorySegment}, null union branch ->
 * {@code null}. Logical-type annotations produce typed values: decimal -> {@code BigDecimal}, uuid -> {@code UUID},
 * date -> {@code LocalDate}, time-millis/micros -> {@code LocalTime}, timestamp-millis/micros -> {@code Instant},
 * local-timestamp-millis/micros -> {@code LocalDateTime}, duration -> {@link AvroDuration}; unknown logical types
 * decode as the underlying type.
 */
public final class AvroRecord {

    private final AvroSchema.Record schema;
    private final Object[] values;

    AvroRecord(AvroSchema.Record schema, Object[] values) {
        this.schema = schema;
        this.values = values;
    }

    public AvroSchema.Record schema() {
        return schema;
    }

    public int size() {
        return values.length;
    }

    public Object get(int position) {
        return values[position];
    }

    public Object get(String fieldName) {
        for (int i = 0; i < schema.fields().size(); i++) {
            if (schema.fields().get(i).name().equals(fieldName)) {
                return values[i];
            }
        }
        throw new IllegalArgumentException("No such field: " + fieldName);
    }
}
