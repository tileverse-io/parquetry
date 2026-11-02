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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.tileverse.parquetry.internal.filter.LearnedPages;

/**
 * Learned page indexes held in a map, for tests on one thread. The first entry put for a chunk stays, as in the
 * metadata cache.
 */
final class InMemoryLearnedPageIndexes implements LearnedPageIndexes {

    private final Map<Chunk, LearnedPages> entries = new HashMap<>();

    @Override
    public Optional<LearnedPages> get(int rowGroup, int column) {
        return Optional.ofNullable(entries.get(new Chunk(rowGroup, column)));
    }

    @Override
    public void putIfAbsent(int rowGroup, int column, LearnedPages pages) {
        entries.putIfAbsent(new Chunk(rowGroup, column), pages);
    }

    boolean isEmpty() {
        return entries.isEmpty();
    }

    /** The leaf ordinals of the columns holding an entry in row group {@code rowGroup}. */
    List<Integer> columnsOf(int rowGroup) {
        return entries.keySet().stream()
                .filter(chunk -> chunk.rowGroup() == rowGroup)
                .map(Chunk::column)
                .toList();
    }

    /** A column chunk, named by the ordinal of its row group and the leaf ordinal of its column. */
    private record Chunk(int rowGroup, int column) {}
}
