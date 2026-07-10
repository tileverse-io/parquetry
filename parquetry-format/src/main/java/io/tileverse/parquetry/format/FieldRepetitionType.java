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
package io.tileverse.parquetry.format;

/**
 * Repetition kind of a schema field; mirror of {@code FieldRepetitionType} in {@code parquet.thrift}.
 *
 * <p>Carried by {@link SchemaElement#repetitionType()}. {@link #REQUIRED} fields are always present, {@link #OPTIONAL}
 * fields may be null, and {@link #REPEATED} fields encode the list-like (zero-or-more) shape that Parquet's repetition
 * levels track.
 *
 * <p>Each constant carries its Thrift wire code in {@link #value()}; resolve incoming i32 codes via
 * {@link #valueOf(int)}.
 */
public enum FieldRepetitionType {
    REQUIRED(0),
    OPTIONAL(1),
    REPEATED(2);

    private final int value;

    FieldRepetitionType(int value) {
        this.value = value;
    }

    /** Thrift wire code for this constant, matching the value defined in {@code parquet.thrift}. */
    public int value() {
        return value;
    }

    /** @throws UnknownCodeException if no defined case carries that code */
    public static FieldRepetitionType valueOf(int code) {
        return switch (code) {
            case 0 -> REQUIRED;
            case 1 -> OPTIONAL;
            case 2 -> REPEATED;
            default -> throw new UnknownCodeException("Unknown FieldRepetitionType wire code: " + code);
        };
    }
}
