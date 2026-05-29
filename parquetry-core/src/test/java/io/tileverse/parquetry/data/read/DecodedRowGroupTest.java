/*
 * Copyright (c) 2026 Multivers.io
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
package io.tileverse.parquetry.data.read;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.Arena;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.batch.DefaultParquetRecordBatch;
import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class DecodedRowGroupTest {

    @Test
    void emitsBatchesInOrder() {
        Arena arena0 = Arena.ofConfined();
        Arena arena1 = Arena.ofConfined();
        ParquetRecordBatch b0 = batch(arena0);
        ParquetRecordBatch b1 = batch(arena1);
        DecodedRowGroup rowGroup = new DecodedRowGroup(List.of(b0, b1));

        assertThat(rowGroup.hasNext()).isTrue();
        assertThat(rowGroup.next()).isSameAs(b0);
        assertThat(rowGroup.next()).isSameAs(b1);
        assertThat(rowGroup.hasNext()).isFalse();

        b0.close();
        b1.close();
    }

    @Test
    void closeClosesOnlyUnemittedBatches() {
        Arena arenaEmitted = Arena.ofConfined();
        Arena arenaUnemitted = Arena.ofConfined();
        ParquetRecordBatch emitted = batch(arenaEmitted);
        ParquetRecordBatch unemitted = batch(arenaUnemitted);
        DecodedRowGroup rowGroup = new DecodedRowGroup(List.of(emitted, unemitted));

        rowGroup.next(); // emit the first; the consumer now owns it
        rowGroup.close();

        // The unemitted batch's arena must be closed by DecodedRowGroup.close().
        assertThatThrownBy(() -> arenaUnemitted.allocate(8)).isInstanceOf(IllegalStateException.class);

        // The emitted batch's arena must still be open - its consumer owns it.
        assertThat(arenaEmitted.allocate(8)).isNotNull();

        emitted.close();
    }

    @Test
    void closeIsIdempotent() {
        Arena arena = Arena.ofConfined();
        ParquetRecordBatch only = batch(arena);
        DecodedRowGroup rowGroup = new DecodedRowGroup(List.of(only));
        rowGroup.close();
        rowGroup.close();
        // Arena must be closed exactly once (idempotent close does not throw).
        assertThatThrownBy(() -> arena.allocate(8)).isInstanceOf(IllegalStateException.class);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static ParquetRecordBatch batch(Arena arena) {
        return new DefaultParquetRecordBatch(minimalSchema(), Map.of(), 0, arena);
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
