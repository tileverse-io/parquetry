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

import io.tileverse.parquetry.batch.ParquetRecordBatch;

import lombok.NonNull;

/**
 * Streams one row group two-phase: on first use, evaluates the predicate to a {@link Selection} (phase 1), then pulls
 * the output batches for the matching rows one at a time (phase 2). Phase 1 runs lazily on the decoding thread, not at
 * construction.
 */
final class LateMaterializedRowGroupDriver implements RowGroupBatchDriver {

    private final RowGroupFetch fetch;
    private final LateMaterializingRowGroupReader reader;

    private boolean phase1Done;
    private BatchRowGroupReader outputReader;

    LateMaterializedRowGroupDriver(@NonNull RowGroupFetch fetch, @NonNull LateMaterializingRowGroupReader reader) {
        this.fetch = fetch;
        this.reader = reader;
    }

    @Override
    public boolean hasMore() {
        ensurePhase1();
        return outputReader != null && outputReader.hasMore();
    }

    @Override
    @SuppressWarnings("MustBeClosed") // the caller owns and closes the returned batch
    public ParquetRecordBatch nextBatch() {
        ensurePhase1();
        return outputReader.nextBatch();
    }

    private void ensurePhase1() {
        if (phase1Done) {
            return;
        }
        phase1Done = true;
        Selection selection = reader.selectMatching();
        if (!selection.isEmpty()) {
            outputReader = reader.outputReader(selection);
        }
    }

    @Override
    public void close() {
        try {
            if (outputReader != null) {
                outputReader.close();
            }
        } finally {
            fetch.close();
        }
    }
}
