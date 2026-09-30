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
package io.tileverse.parquetry.internal.read;

import io.tileverse.parquetry.observe.FetchPurpose;

import lombok.NonNull;

/**
 * Where one index section lies in the file and what its bytes count as, named ahead of the read that fetches it.
 *
 * @param purpose which tally counts the section's bytes
 * @param fileOffset absolute offset of the section's first byte
 * @param length the section's byte length
 */
public record IndexSectionRange(@NonNull FetchPurpose purpose, long fileOffset, int length) {

    public IndexSectionRange {
        if (fileOffset < 0) {
            throw new IllegalArgumentException("Index section offset must be non-negative, got " + fileOffset);
        }
        if (length <= 0) {
            throw new IllegalArgumentException("Index section length must be positive, got " + length);
        }
    }
}
