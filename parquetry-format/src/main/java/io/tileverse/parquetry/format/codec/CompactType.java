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
package io.tileverse.parquetry.format.codec;

/**
 * Thrift Compact Protocol type IDs as encoded on the wire (4 bits in the type byte).
 */
public enum CompactType {
    STOP(0),
    BOOLEAN_TRUE(1),
    BOOLEAN_FALSE(2),
    BYTE(3),
    I16(4),
    I32(5),
    I64(6),
    DOUBLE(7),
    BINARY(8),
    LIST(9),
    SET(10),
    MAP(11),
    STRUCT(12);

    public final int code;

    CompactType(int code) {
        this.code = code;
    }

    public static CompactType of(int code) {
        for (CompactType t : values()) {
            if (t.code == code) return t;
        }
        throw new IllegalArgumentException("Unknown CompactType code: " + code);
    }
}
