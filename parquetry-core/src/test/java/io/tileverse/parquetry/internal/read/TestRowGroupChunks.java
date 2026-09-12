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

import java.util.ArrayList;
import java.util.List;

import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.internal.footer.CompactFooter;
import io.tileverse.parquetry.internal.footer.LeafIndex;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * Builds {@link RowGroupChunks} views for tests that start from a wire footer, packing it into the compact planning
 * form built by the reader at open. A test holding a {@link FileMetaData} asks for one row group's view, or for every
 * row group's view in file order.
 */
final class TestRowGroupChunks {

    private TestRowGroupChunks() {}

    static RowGroupChunks of(
            FileMetaData footer, int rowGroupIndex, ParquetSchema fileSchema, IndexSectionLoader loader) {
        LeafIndex leaves = LeafIndex.of(fileSchema);
        return RowGroupChunks.of(CompactFooter.encode(footer, leaves), rowGroupIndex, leaves, fileSchema, loader);
    }

    /** Builds the view over a row group assembled by hand, with no file around it. */
    static RowGroupChunks of(RowGroup rowGroup, ParquetSchema fileSchema, IndexSectionLoader loader) {
        FileMetaData footer =
                FileMetaData.builder().rowGroups(List.of(rowGroup)).build();
        return of(footer, 0, fileSchema, loader);
    }

    static List<RowGroupChunks> allOf(FileMetaData footer, ParquetSchema fileSchema, IndexSectionLoader loader) {
        LeafIndex leaves = LeafIndex.of(fileSchema);
        CompactFooter compact = CompactFooter.encode(footer, leaves);
        List<RowGroupChunks> views = new ArrayList<>(compact.rowGroupCount());
        for (int rowGroupIndex = 0; rowGroupIndex < compact.rowGroupCount(); rowGroupIndex++) {
            views.add(RowGroupChunks.of(compact, rowGroupIndex, leaves, fileSchema, loader));
        }
        return views;
    }
}
