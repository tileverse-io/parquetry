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

import java.lang.foreign.MemorySegment;

import lombok.NonNull;

/**
 * Where {@link ColumnChunkSlicer} finds the dictionary page of a column chunk. The source follows from the bytes read
 * by the fetch. The room of a prefix left unread holds earlier bytes of the pool, and such a prefix is never a source.
 */
sealed interface DictionarySource {

    /**
     * The chunk's first bytes, fetched as their own range.
     *
     * @param bytes a read-only view of the chunk's bytes up to its first data page
     */
    record Prefix(@NonNull MemorySegment bytes) implements DictionarySource {

        public Prefix {
            bytes = bytes.asReadOnly();
        }
    }

    /**
     * The head of the first run. The chunk has no bytes fetched before its first run, as in a whole chunk fetched in
     * one unit. The slicer reads the first page header there to find out whether the chunk has a dictionary page.
     */
    record FirstRun() implements DictionarySource {}

    /** No fetched bytes. The fetch left the dictionary out because no fetched page uses it, and nothing is sniffed. */
    record LeftOut() implements DictionarySource {}
}
