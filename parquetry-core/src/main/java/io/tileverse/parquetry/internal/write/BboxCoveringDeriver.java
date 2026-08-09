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
package io.tileverse.parquetry.internal.write;

import java.lang.foreign.MemorySegment;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.Map;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DoubleVector;
import io.tileverse.parquetry.columnar.FloatVector;
import io.tileverse.parquetry.columnar.StructVector;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.internal.filter.spatial.WkbEnvelope;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Derives a per-row {@code bbox} covering struct from a geometry WKB column: each row becomes that geometry's exact 2D
 * envelope, rounded to the covering precision. A null or empty geometry yields a null covering row. A FLOAT covering
 * rounds each bound OUTWARD (min toward negative infinity, max toward positive infinity) to guarantee the covering
 * always encloses the exact envelope and never drops a match.
 */
public final class BboxCoveringDeriver {

    private static final ColumnPath XMIN = ColumnPath.of("xmin");
    private static final ColumnPath XMAX = ColumnPath.of("xmax");
    private static final ColumnPath YMIN = ColumnPath.of("ymin");
    private static final ColumnPath YMAX = ColumnPath.of("ymax");

    private BboxCoveringDeriver() {}

    /** The float nearest {@code d} that does not exceed it (round toward negative infinity). */
    public static float floatFloor(double d) {
        float f = (float) d;
        return f > d ? Math.nextDown(f) : f;
    }

    /** The float nearest {@code d} that is not below it (round toward positive infinity). */
    public static float floatCeil(double d) {
        float f = (float) d;
        return f < d ? Math.nextUp(f) : f;
    }

    /**
     * Builds the REQUIRED {@code bbox} struct for {@code geometry}; children are OPTIONAL, null for null/empty rows.
     */
    public static StructVector derive(BinaryVector geometry, int rowCount, boolean useFloat) {
        double[] xmin = new double[rowCount];
        double[] xmax = new double[rowCount];
        double[] ymin = new double[rowCount];
        double[] ymax = new double[rowCount];
        BitSet present = new BitSet(rowCount);
        for (int row = 0; row < rowCount; row++) {
            Bbox envelope = usableEnvelopeOrNull(geometry.get(row));
            if (envelope == null) {
                continue;
            }
            present.set(row);
            xmin[row] = envelope.minX();
            xmax[row] = envelope.maxX();
            ymin[row] = envelope.minY();
            ymax[row] = envelope.maxY();
        }
        Validity validity = Validity.of(present, rowCount);
        Map<ColumnPath, ColumnVector> children = new LinkedHashMap<>();
        children.put(XMIN, coveringLeaf(xmin, validity, useFloat, BboxCoveringDeriver::floatFloor));
        children.put(XMAX, coveringLeaf(xmax, validity, useFloat, BboxCoveringDeriver::floatCeil));
        children.put(YMIN, coveringLeaf(ymin, validity, useFloat, BboxCoveringDeriver::floatFloor));
        children.put(YMAX, coveringLeaf(ymax, validity, useFloat, BboxCoveringDeriver::floatCeil));
        return new StructVector(children, Validity.allValid(rowCount), rowCount);
    }

    /**
     * The 2D envelope of one WKB value, or null when the row has nothing to cover: a null geometry, or one whose
     * envelope is empty.
     */
    private static Bbox usableEnvelopeOrNull(MemorySegment wkb) {
        if (wkb == null) {
            return null;
        }
        Bbox envelope = WkbEnvelope.compute(wkb);
        return isEmptyEnvelope(envelope) ? null : envelope;
    }

    /**
     * An inverted running envelope means no real vertex ever widened it: the geometry was empty. Requiring {@code minX
     * <= maxX && minY <= maxY} also rejects a NaN ordinate, which fails every comparison.
     */
    private static boolean isEmptyEnvelope(Bbox envelope) {
        return !(envelope.minX() <= envelope.maxX() && envelope.minY() <= envelope.maxY());
    }

    /**
     * One covering column: a DOUBLE covering keeps each bound exact, a FLOAT covering rounds it outward via
     * {@code rounding} to keep the covering enclosing the exact envelope.
     */
    private static ColumnVector coveringLeaf(
            double[] values, Validity validity, boolean useFloat, OutwardRounding rounding) {
        if (!useFloat) {
            return DoubleVector.materialized(values, validity);
        }
        float[] rounded = new float[values.length];
        for (int i = 0; i < values.length; i++) {
            rounded[i] = rounding.round(values[i]);
        }
        return FloatVector.materialized(rounded, validity);
    }

    /**
     * Rounds a bound to the float on the outward side of the envelope: {@link #floatFloor} for a lower bound,
     * {@link #floatCeil} for an upper one.
     */
    @FunctionalInterface
    private interface OutwardRounding {
        float round(double bound);
    }
}
