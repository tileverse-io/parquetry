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
import java.util.List;

import io.tileverse.parquetry.internal.footer.ChunkMeta;
import io.tileverse.parquetry.schema.ColumnPath;

import lombok.NonNull;

/**
 * The learner of a read. It learns the pages of the chunks fetched whole by the read, from their page headers. A later
 * read of the same row group prunes pages with the entries kept by the learner.
 *
 * <p>Implementations are safe for use by several threads at once. The fetch threads of a read call {@link #learn} for
 * the fetched row groups, prefetched ones included.
 */
public interface PageIndexLearner {

    /**
     * A column chunk fetched whole, from its first page to its end.
     *
     * @param path the leaf column of the chunk
     * @param meta the chunk in the footer
     * @param bytes the bytes of the chunk
     */
    record WholeChunk(
            @NonNull ColumnPath path,
            @NonNull ChunkMeta meta,
            @NonNull MemorySegment bytes) {}

    /**
     * Whether row group {@code rowGroup} is allowed to the learner. A fetch can skip listing the whole chunks of a row
     * group not allowed to it.
     */
    boolean learns(int rowGroup);

    /**
     * Learns the pages of {@code chunks}, the chunks of {@code survivor} fetched whole. A row group not allowed to the
     * learner is left alone. A failure of the pass, malformed page headers included, leaves the row group unlearned,
     * and the call returns normally.
     */
    void learn(RowGroupSurvivor survivor, List<WholeChunk> chunks);

    /** A learner leaving each row group alone. */
    PageIndexLearner NONE = new PageIndexLearner() {

        @Override
        public boolean learns(int rowGroup) {
            return false;
        }

        @Override
        public void learn(RowGroupSurvivor survivor, List<WholeChunk> chunks) {
            // learns nothing
        }
    };
}
