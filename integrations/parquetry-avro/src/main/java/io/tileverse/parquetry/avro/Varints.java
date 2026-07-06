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

/** Avro variable-length integer helpers. */
final class Varints {

    private Varints() {}

    /** Decodes a zigzag-encoded unsigned varint value into its signed form. */
    static long decodeZigZag(long encoded) {
        return (encoded >>> 1) ^ -(encoded & 1);
    }

    /** Encodes a signed value into its zigzag form (the inverse of {@link #decodeZigZag}). */
    static long encodeZigZag(long value) {
        return (value << 1) ^ (value >> 63);
    }
}
