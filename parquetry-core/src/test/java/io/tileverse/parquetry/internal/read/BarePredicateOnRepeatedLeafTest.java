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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.runtime.ParquetRuntime;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchemaException;
import io.tileverse.parquetry.testsupport.CorpusFixtures;

/**
 * A comparison over a leaf inside a LIST is expressed as {@link Predicate.Quantified}. The bare form names a
 * multi-valued column, which every read entry point rejects with a {@link ParquetSchemaException} naming the column
 * when it validates the predicate against the file schema.
 */
class BarePredicateOnRepeatedLeafTest {

    private static final ColumnPath LIST_ITEM = ColumnPath.parse("int64_list.list.item");

    private static final Predicate BARE_ON_LIST_ITEM = new Predicate.IsNotNull(LIST_ITEM);

    @Test
    void readRejectsBarePredicateOnListDescendantLeaf() {
        withListColumnsReader(reader -> assertThatThrownBy(() -> drainRows(reader))
                .isInstanceOf(ParquetSchemaException.class)
                .hasMessageContaining(LIST_ITEM.dot()));
    }

    @Test
    void readBatchesRejectsBarePredicateOnListDescendantLeaf() {
        withListColumnsReader(reader -> assertThatThrownBy(() -> drainBatches(reader))
                .isInstanceOf(ParquetSchemaException.class)
                .hasMessageContaining(LIST_ITEM.dot()));
    }

    @Test
    void countRejectsBarePredicateOnListDescendantLeaf() {
        withListColumnsReader(reader -> assertThatThrownBy(() -> reader.count(BARE_ON_LIST_ITEM, ReadOptions.DEFAULTS))
                .isInstanceOf(ParquetSchemaException.class)
                .hasMessageContaining(LIST_ITEM.dot()));
    }

    private static void drainRows(ParquetFileReader reader) {
        try (Stream<ParquetRecord> rows = reader.read(BARE_ON_LIST_ITEM, Projection.ALL, ReadOptions.DEFAULTS)) {
            rows.forEach(row -> {});
        }
    }

    private static void drainBatches(ParquetFileReader reader) {
        try (Stream<ParquetRecordBatch> batches =
                reader.readBatches(BARE_ON_LIST_ITEM, Projection.ALL, ReadOptions.DEFAULTS)) {
            batches.forEach(batch -> {});
        }
    }

    private static void withListColumnsReader(Consumer<ParquetFileReader> assertion) {
        Path file = CorpusFixtures.parquetTestingData().resolve("list_columns.parquet");
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader =
                    ParquetFileReader.open(source, ParquetRuntime.defaultRuntime(), Optional.empty());
            assertion.accept(reader);
        }
    }
}
