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
import static org.assertj.core.api.Assertions.assertThatCode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.observe.FetchAccumulator;
import io.tileverse.parquetry.testsupport.CorpusFixtures;

/**
 * The page indexes and the bloom filters of a file lie outside its column chunks, and the reader refuses a section
 * declared longer than the bytes left there. This suite reads the sections of the {@code apache/parquet-testing} files,
 * written by many writers, under that rule: a writer declaring chunk sizes above the bytes of its chunks would lose its
 * page indexes without a word.
 */
class IndexSectionsOfTheCorpusIT {

    private static final Path DATA_DIR = CorpusFixtures.parquetTestingData();
    private static final int LENGTH_FROM_FILTER_HEADER = 0;

    // Guards the parameterized test against a corpus without such files, which would pass without running.
    @Test
    void corpusHoldsFilesWithSections() throws IOException {
        assertThat(filesWithSections()).hasSizeGreaterThan(20);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("filesWithSections")
    void sectionsOfTheFileAreRead(String fileName) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(DATA_DIR.resolve(fileName))) {
            FileMetaData footer = ParquetFormat.readFooter(source);
            long columnChunkBytes = FooterMetadata.columnChunkBytes(footer);
            FooterIndexSectionLoader loader =
                    new FooterIndexSectionLoader(source, FetchAccumulator.NONE, columnChunkBytes);

            for (ColumnChunk chunk : chunksOf(footer)) {
                assertThatCode(() -> readSections(loader, chunk))
                        .as("sections of column %s", pathOf(chunk))
                        .doesNotThrowAnyException();
            }
        }
    }

    static List<String> filesWithSections() throws IOException {
        List<String> names = new ArrayList<>();
        for (Path file : corpusFiles()) {
            if (declaresSections(file)) {
                names.add(file.getFileName().toString());
            }
        }
        return names;
    }

    private static List<Path> corpusFiles() throws IOException {
        try (Stream<Path> files = Files.list(DATA_DIR)) {
            return files.filter(file -> file.getFileName().toString().endsWith(".parquet"))
                    .sorted()
                    .toList();
        }
    }

    /** Whether the footer of {@code file} is readable and declares a page index or a bloom filter. */
    private static boolean declaresSections(Path file) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            FileMetaData footer = ParquetFormat.readFooter(source);
            return chunksOf(footer).stream().anyMatch(IndexSectionsOfTheCorpusIT::declaresSections);
        } catch (RuntimeException _) {
            return false;
        }
    }

    private static boolean declaresSections(ColumnChunk chunk) {
        boolean bloomFilter = chunk.metaData()
                .map(metaData -> metaData.bloomFilterOffset().isPresent())
                .orElse(false);
        return chunk.columnIndexOffset().isPresent()
                || chunk.offsetIndexOffset().isPresent()
                || bloomFilter;
    }

    private static void readSections(FooterIndexSectionLoader loader, ColumnChunk chunk) {
        if (chunk.columnIndexOffset().isPresent()) {
            long offset = chunk.columnIndexOffset().getAsLong();
            loader.readColumnIndex(offset, chunk.columnIndexLength().orElseThrow());
        }
        if (chunk.offsetIndexOffset().isPresent()) {
            long offset = chunk.offsetIndexOffset().getAsLong();
            loader.readOffsetIndex(offset, chunk.offsetIndexLength().orElseThrow());
        }
        ColumnMetaData metaData = chunk.metaData().orElseThrow();
        if (metaData.bloomFilterOffset().isPresent()) {
            long offset = metaData.bloomFilterOffset().getAsLong();
            long length = metaData.bloomFilterLength().orElse(LENGTH_FROM_FILTER_HEADER);
            loader.readBloom(offset, Math.toIntExact(length));
        }
    }

    private static String pathOf(ColumnChunk chunk) {
        return chunk.metaData()
                .map(metaData -> String.join(".", metaData.pathInSchema()))
                .orElse("?");
    }

    private static List<ColumnChunk> chunksOf(FileMetaData footer) {
        List<ColumnChunk> chunks = new ArrayList<>();
        for (RowGroup rowGroup : footer.rowGroups()) {
            chunks.addAll(rowGroup.columns());
        }
        return chunks;
    }
}
