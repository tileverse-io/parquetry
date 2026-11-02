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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.WriteOptions.ParquetVersion;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.internal.filter.LearnedPages;
import io.tileverse.parquetry.internal.read.LearnedPageIndexes;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;

import lombok.NonNull;

/**
 * The rules deciding which files learn page indexes. The footers come from two small files of one INT64 column. The
 * first is written with Parquet 1 page headers, and the writer declares no page index for it. The second is written
 * with Parquet 2 page headers and an offset index for each chunk.
 */
class CachedPageIndexesTest {

    private static final String FILE_NAME = "mem://cached-page-indexes.parquet";
    private static final long FILE_LENGTH = 4096L;
    private static final ColumnPath ID = ColumnPath.of("id");

    @TempDir
    static Path tempDir;

    private static FooterMetadata unindexed;
    private static FooterMetadata indexed;

    @BeforeAll
    static void readTheFooters() throws IOException {
        unindexed = footerOf(writeIds("unindexed.parquet", ParquetVersion.V1_1));
        indexed = footerOf(writeIds("indexed.parquet", ParquetVersion.V2_0));
    }

    @Test
    void unnamedSourceLearnsNothing() {
        ByteRangeSource unnamed = new SizedSource(Optional.empty(), FILE_LENGTH);

        assertThat(CachedPageIndexes.of(unnamed, unindexed, false)).isSameAs(LearnedPageIndexes.NONE);
    }

    @Test
    void encryptedFileLearnsNothing() {
        ByteRangeSource named = new SizedSource(Optional.of(FILE_NAME), FILE_LENGTH);

        assertThat(CachedPageIndexes.of(named, unindexed, true)).isSameAs(LearnedPageIndexes.NONE);
    }

    // Only the footer differs between the two calls.
    @Test
    void indexedFileLearnsNothing() {
        ByteRangeSource named = new SizedSource(Optional.of(FILE_NAME), FILE_LENGTH);

        assertThat(CachedPageIndexes.of(named, unindexed, false)).isNotSameAs(LearnedPageIndexes.NONE);
        assertThat(CachedPageIndexes.of(named, indexed, false)).isSameAs(LearnedPageIndexes.NONE);
    }

    @Test
    void cacheTurnedOffLearnsNothing() {
        ByteRangeSource named = new SizedSource(Optional.of(FILE_NAME), FILE_LENGTH);
        System.setProperty(FooterMetadataCache.MAX_HEAP_PERCENT_PROPERTY, "0");
        try {
            assertThat(CachedPageIndexes.of(named, unindexed, false)).isSameAs(LearnedPageIndexes.NONE);
        } finally {
            System.clearProperty(FooterMetadataCache.MAX_HEAP_PERCENT_PROPERTY);
        }
    }

    // A later read of the file finds the entry, and a read of the file rewritten to another length does not.
    @Test
    void entriesKeyOnTheFile() {
        LearnedPages pages = onePage();
        ByteRangeSource named = new SizedSource(Optional.of(FILE_NAME), FILE_LENGTH);
        CachedPageIndexes.of(named, unindexed, false).putIfAbsent(1, 2, pages);

        LearnedPageIndexes sameFile = CachedPageIndexes.of(named, unindexed, false);
        ByteRangeSource longer = new SizedSource(Optional.of(FILE_NAME), 2 * FILE_LENGTH);
        LearnedPageIndexes rewritten = CachedPageIndexes.of(longer, unindexed, false);

        assertThat(sameFile.get(1, 2)).containsSame(pages);
        assertThat(sameFile.get(1, 3)).as("another column of the row group").isEmpty();
        assertThat(rewritten.get(1, 2)).isEmpty();
    }

    private static Path writeIds(String fileName, ParquetVersion version) throws IOException {
        ParquetSchema schema = WriteFixtures.schemaOf(WriteFixtures.requiredLeaf("id", PrimitiveKind.INT64));
        WriteOptions options =
                WriteOptions.builder().tempDir(tempDir).parquetVersion(version).build();
        List<Map<ColumnPath, Object>> rows = new ArrayList<>();
        for (long id = 0; id < 10; id++) {
            rows.add(Map.of(ID, id));
        }
        return WriteFixtures.writeRows(tempDir.resolve(fileName), schema, options, rows);
    }

    private static FooterMetadata footerOf(Path file) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            return FooterMetadata.parse(source);
        }
    }

    private static LearnedPages onePage() {
        LearnedPages.Builder builder = LearnedPages.builder(PrimitiveKind.INT64, Optional.empty(), 1);
        builder.add(0, 64, 0, 8, Encoding.PLAIN, Optional.empty());
        return builder.build();
    }

    /** A source of {@code size} bytes named {@code name}. These checks read none of its bytes. */
    private record SizedSource(@NonNull Optional<String> name, long size) implements ByteRangeSource {

        @Override
        public Optional<String> sourceIdentifier() {
            return name;
        }

        @Override
        public int read(long offset, MemorySegment dst) {
            throw new UnsupportedOperationException("the learned page indexes read no byte of the file");
        }

        @Override
        public void close() {
            // the source holds nothing
        }
    }
}
