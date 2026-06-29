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
package io.tileverse.parquetry.internal.read;

import java.util.BitSet;
import java.util.Objects;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.BinaryView;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.SpatialReadProbe;
import io.tileverse.parquetry.filter.SpatialReadProbe.Decision;
import io.tileverse.parquetry.internal.filter.spatial.WkbEnvelope;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Decimates a decoded batch's post-filter survivors by consulting a {@link SpatialReadProbe} with each survivor's
 * geometry envelope, in ascending physical-row order. A {@code Skip} clears the survivor bit; {@code Keep} and
 * {@code Descend} leave it set. A {@code Replace} decision is rejected: substituting a probe-supplied representative
 * geometry for a row's decoded geometry is not yet supported on the read path.
 *
 * <p>The probe is stateful and single-threaded: it accumulates paint state across rows (for example, which pixels a
 * renderer has already covered), which is why rows are visited in physical-row order on one thread. One instance serves
 * one read; there is no synchronization.
 */
public final class SpatialDecimationGate {

    private static final BinaryView<Bbox> ENVELOPE =
            (backing, offset, length) -> WkbEnvelope.compute(backing.asSlice(offset, length));

    private final ColumnPath geometryColumn;
    private final SpatialReadProbe probe;

    public SpatialDecimationGate(ColumnPath geometryColumn, SpatialReadProbe probe) {
        this.geometryColumn = Objects.requireNonNull(geometryColumn, "geometryColumn");
        this.probe = Objects.requireNonNull(probe, "probe");
    }

    /**
     * Narrows {@code survivors} in place against {@code batch}, clearing the bit of every row the probe skips. The
     * caller owns {@code survivors}; this mutates that same bitset (matching the in-place style of the read pipeline's
     * filter sites) rather than allocating a copy. A row whose geometry column is absent or not binary contributes no
     * envelope and stays set.
     */
    public void narrow(ParquetRecordBatch batch, BitSet survivors) {
        ColumnVector column = batch.columns().get(geometryColumn);
        if (!(column instanceof BinaryVector wkb)) {
            return;
        }
        decimate(wkb, survivors);
    }

    private void decimate(BinaryVector wkb, BitSet survivors) {
        for (int row = survivors.nextSetBit(0); row >= 0; row = survivors.nextSetBit(row + 1)) {
            Bbox box = wkb.read(row, ENVELOPE);
            if (box == null) {
                continue;
            }
            Decision decision = probe.probe(box.minX(), box.minY(), box.maxX(), box.maxY());
            apply(decision, row, survivors);
        }
    }

    private void apply(Decision decision, int row, BitSet survivors) {
        switch (decision) {
            case Decision.Skip _ -> survivors.clear(row);
            case Decision.Keep _, Decision.Descend _ -> {
                // Both leave the survivor bit set; Descend behaves as Keep at the leaf, the finest level.
            }
            case Decision.Replace _ ->
                throw new UnsupportedOperationException(
                        "spatial Replace output substitution is not yet supported on the read path");
        }
    }
}
