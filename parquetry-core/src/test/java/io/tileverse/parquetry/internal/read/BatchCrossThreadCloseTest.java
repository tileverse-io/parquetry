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

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.Arena;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.DefaultParquetRecordBatch;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class BatchCrossThreadCloseTest {

    @Test
    void sharedArenaBatchClosesOnAnotherThread() throws Exception {
        ParquetSchema schema = minimalSchema();
        ParquetRecordBatch batch = new DefaultParquetRecordBatch(schema, Map.of(), 0, Arena.ofShared());

        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread closer = new Thread(() -> {
            try {
                batch.close();
            } catch (Throwable t) {
                failure.set(t);
            }
        });
        closer.start();
        closer.join();

        assertThat(failure.get())
                .as("a shared-arena batch must be closable on a thread other than the one that built it")
                .isNull();
    }

    @Test
    void confinedArenaBatchWouldFailAcrossThreads() throws Exception {
        ParquetSchema schema = minimalSchema();
        ParquetRecordBatch batch = new DefaultParquetRecordBatch(schema, Map.of(), 0, Arena.ofConfined());

        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread closer = new Thread(() -> {
            try {
                batch.close();
            } catch (Throwable t) {
                failure.set(t);
            }
        });
        closer.start();
        closer.join();

        assertThat(failure.get())
                .as("documents why nextBatch must not use a confined arena once decode runs off the consumer thread")
                .isInstanceOf(WrongThreadException.class);
    }

    private static ParquetSchema minimalSchema() {
        return new ParquetSchema(new SchemaNode.Group(
                "root",
                Repetition.REQUIRED,
                List.of(new SchemaNode.Primitive(
                        "value", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1)),
                Optional.empty(),
                -1));
    }
}
