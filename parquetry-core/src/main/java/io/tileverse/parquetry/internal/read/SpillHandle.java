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
package io.tileverse.parquetry.internal.read;

/** Locates one spilled batch within a {@link BatchSpillStore}'s file: a byte offset and the serialized length. */
record SpillHandle(long offset, long length) {

    SpillHandle {
        if (offset < 0L) {
            throw new IllegalArgumentException("offset must be >= 0, got " + offset);
        }
        if (length <= 0L) {
            throw new IllegalArgumentException("length must be > 0, got " + length);
        }
    }
}
