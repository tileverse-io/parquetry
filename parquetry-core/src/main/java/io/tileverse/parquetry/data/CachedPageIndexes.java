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
package io.tileverse.parquetry.data;

import java.util.Optional;

import io.tileverse.parquetry.internal.filter.LearnedPages;
import io.tileverse.parquetry.internal.read.LearnedPageIndexes;
import io.tileverse.parquetry.io.ByteRangeSource;

/**
 * The page indexes learned for one file, held by the {@link FooterMetadataCache}. The file is named by its source and
 * measured by its length, as for its footer entry. Reads of the same file share the entries, across threads and
 * readers.
 */
final class CachedPageIndexes implements LearnedPageIndexes {

    private final String sourceId;
    private final long fileLength;

    private CachedPageIndexes(String sourceId, long fileLength) {
        this.sourceId = sourceId;
        this.fileLength = fileLength;
    }

    /**
     * The learned page indexes of the file read through {@code source}, or {@link LearnedPageIndexes#NONE}. An unnamed
     * source has no stable identity to key on. An encrypted file learns nothing, because what its page headers tell
     * must not outlive the reader holding its key. A file with an offset index on each column chunk has no chunk to
     * learn, and its reads look nothing up. A metadata pool with a budget of 0 keeps nothing, and a read would parse
     * page headers for nothing.
     *
     * @param footer the forms derived from the footer of the file
     */
    static LearnedPageIndexes of(ByteRangeSource source, FooterMetadata footer, boolean encrypted) {
        Optional<String> sourceId = source.sourceIdentifier();
        if (sourceId.isEmpty() || encrypted || footer.offsetIndexOnEachChunk() || cacheTurnedOff()) {
            return LearnedPageIndexes.NONE;
        }
        return new CachedPageIndexes(sourceId.orElseThrow(), source.size());
    }

    private static boolean cacheTurnedOff() {
        return FooterMetadataCache.keepsNothing();
    }

    @Override
    public Optional<LearnedPages> get(int rowGroup, int column) {
        return FooterMetadataCache.learnedPages(sourceId, fileLength, rowGroup, column);
    }

    @Override
    public void putIfAbsent(int rowGroup, int column, LearnedPages pages) {
        FooterMetadataCache.learnedPagesIfAbsent(sourceId, fileLength, rowGroup, column, pages);
    }
}
