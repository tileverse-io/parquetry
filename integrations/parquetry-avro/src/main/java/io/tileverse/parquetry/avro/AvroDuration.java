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

/**
 * An Avro {@code duration} logical value: three unsigned little-endian 32-bit counts over fixed(12). The components are
 * independent calendar units (a month is not a fixed number of days); java.time has no equivalent type.
 */
public record AvroDuration(long months, long days, long millis) {
    public AvroDuration {
        requireUnsignedInt(months, "months");
        requireUnsignedInt(days, "days");
        requireUnsignedInt(millis, "millis");
    }

    private static void requireUnsignedInt(long value, String component) {
        if (value < 0 || value > 0xFFFF_FFFFL) {
            throw new IllegalArgumentException(component + " out of unsigned-int range: " + value);
        }
    }
}
