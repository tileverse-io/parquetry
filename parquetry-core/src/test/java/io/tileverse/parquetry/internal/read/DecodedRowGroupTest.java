/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.Arena;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;

class DecodedRowGroupTest {

    @Test
    void emitsBatchesInOrder() {
        Arena arena0 = Arena.ofConfined();
        Arena arena1 = Arena.ofConfined();
        ParquetRecordBatch b0 = batch(arena0);
        ParquetRecordBatch b1 = batch(arena1);
        DecodedRowGroup rowGroup =
                new DecodedRowGroup(new InlineBatchSource(new ListRowGroupDriver(List.of(b0, b1))), true);

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
        DecodedRowGroup rowGroup =
                new DecodedRowGroup(new InlineBatchSource(new ListRowGroupDriver(List.of(emitted, unemitted))), true);

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
        DecodedRowGroup rowGroup =
                new DecodedRowGroup(new InlineBatchSource(new ListRowGroupDriver(List.of(only))), true);
        rowGroup.close();
        rowGroup.close();
        // Arena must be closed exactly once (idempotent close does not throw).
        assertThatThrownBy(() -> arena.allocate(8)).isInstanceOf(IllegalStateException.class);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static ParquetRecordBatch batch(Arena arena) {
        return TestBatches.emptyBatch(arena);
    }
}
