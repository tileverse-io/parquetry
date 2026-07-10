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
package io.tileverse.parquetry.dataset;

import java.util.List;

import io.tileverse.parquetry.io.ByteRangeSource;

/** Shared {@link FilesetReader} builders for the dataset tests. */
final class TestFilesets {

    private TestFilesets() {}

    /** A {@link FilesetReader} over {@code sources}, indexed in list order. */
    static FilesetReader of(List<ByteRangeSource> sources) {
        return new FilesetReader() {
            @Override
            public ByteRangeSource openFile(int index) {
                return sources.get(index);
            }

            @Override
            public int fileCount() {
                return sources.size();
            }
        };
    }
}
