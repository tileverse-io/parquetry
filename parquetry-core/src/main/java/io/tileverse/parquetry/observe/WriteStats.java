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
package io.tileverse.parquetry.observe;

import java.time.Duration;

/** Mergeable file-level write summary. */
public record WriteStats(
        long totalRows, int rowGroups, long compressedBytes, long uncompressedBytes, Duration wallClock) {

    public double compressionRatio() {
        if (compressedBytes == 0) {
            return 0.0;
        }
        return (double) uncompressedBytes / (double) compressedBytes;
    }

    public WriteStats combine(WriteStats other) {
        return new WriteStats(
                totalRows + other.totalRows,
                rowGroups + other.rowGroups,
                compressedBytes + other.compressedBytes,
                uncompressedBytes + other.uncompressedBytes,
                wallClock.plus(other.wallClock));
    }
}
