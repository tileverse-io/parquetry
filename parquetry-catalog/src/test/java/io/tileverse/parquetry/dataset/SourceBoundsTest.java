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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import com.google.errorprone.annotations.MustBeClosed;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ParquetSchema;

/** The shared read contract defaults {@code bounds} to empty for a reader that does not override it. */
class SourceBoundsTest {

    @Test
    void readerWithoutOverrideReturnsEmpty() {
        ParquetReader withoutBounds = readerThrowingOnEveryRead();

        assertThat(withoutBounds.bounds(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS))
                .as("the shared read contract defaults bounds to empty")
                .isEmpty();
    }

    /**
     * A {@link ParquetReader} whose every abstract read method throws and which adds no {@code bounds} override,
     * isolating the interface default for the empty-by-default assertion.
     */
    private static ParquetReader readerThrowingOnEveryRead() {
        return new ParquetReader() {
            @Override
            public ParquetSchema schema() {
                throw new UnsupportedOperationException();
            }

            @Override
            @MustBeClosed
            public Stream<ParquetRecord> read(Predicate predicate, Projection projection, ReadOptions options) {
                throw new UnsupportedOperationException();
            }

            @Override
            @MustBeClosed
            public <T> Stream<T> read(
                    Predicate predicate, Projection projection, Materializer<T> materializer, ReadOptions options) {
                throw new UnsupportedOperationException();
            }

            @Override
            @MustBeClosed
            public Stream<ParquetRecordBatch> readBatches(
                    Predicate predicate, Projection projection, ReadOptions options) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long count(Predicate predicate, ReadOptions options) {
                throw new UnsupportedOperationException();
            }
        };
    }
}
