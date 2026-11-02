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

import java.util.Optional;

import io.tileverse.parquetry.internal.filter.LearnedPages;

/**
 * The page indexes learned for the column chunks of one file. A chunk without an offset index in the footer takes its
 * pages from here, once a read has learned them. A chunk is named by the ordinal of its row group and the leaf ordinal
 * of its column in the file schema.
 *
 * <p>Implementations are safe for use by several threads at once. Fetch threads store entries while the consumer thread
 * of another read looks them up.
 */
public interface LearnedPageIndexes {

    /** The entry learned for the chunk of column {@code column} in row group {@code rowGroup}, or empty. */
    Optional<LearnedPages> get(int rowGroup, int column);

    /**
     * Keeps {@code pages} as the entry of the chunk of column {@code column} in row group {@code rowGroup}. An entry
     * already kept for the chunk stays, and {@code pages} is dropped.
     */
    void putIfAbsent(int rowGroup, int column, LearnedPages pages);

    /** Page indexes holding no entry. They drop the entries handed to them. */
    LearnedPageIndexes NONE = new LearnedPageIndexes() {

        @Override
        public Optional<LearnedPages> get(int rowGroup, int column) {
            return Optional.empty();
        }

        @Override
        public void putIfAbsent(int rowGroup, int column, LearnedPages pages) {
            // keeps nothing
        }
    };
}
