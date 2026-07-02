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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DefaultParquetRecordBatch;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.filter.SpatialReadProbe;
import io.tileverse.parquetry.filter.SpatialReadProbe.Decision;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class SpatialDecimationGateTest {

    private static final ColumnPath GEOM = ColumnPath.of("geometry");

    @Test
    void narrowsSurvivorsBySkippingPaintedRows() {
        ParquetRecordBatch batch = pointBatch(new double[][] {{0, 0}, {5, 5}, {9, 9}}, "111");
        BitSet survivors = new BitSet();
        survivors.set(0, 3);

        SpatialReadProbe probe = (minX, minY, maxX, maxY) -> minX == 5.0 ? Decision.skip() : Decision.keep();

        SpatialDecimationGate gate = new SpatialDecimationGate(GEOM, probe);
        gate.narrow(batch, survivors);

        assertThat(survivors.get(0)).isTrue();
        assertThat(survivors.get(1)).isFalse();
        assertThat(survivors.get(2)).isTrue();
    }

    @Test
    void replaceIsRejectedUntilOutputSubstitutionIsSupported() {
        ParquetRecordBatch batch = pointBatch(new double[][] {{0, 0}}, "1");
        BitSet survivors = new BitSet();
        survivors.set(0);

        SpatialReadProbe probe = (minX, minY, maxX, maxY) -> Decision.replace(new Object());

        SpatialDecimationGate gate = new SpatialDecimationGate(GEOM, probe);

        assertThatThrownBy(() -> gate.narrow(batch, survivors))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Replace");
    }

    @Test
    void nullGeometryIsKeptAndPaintsNothing() {
        ParquetRecordBatch batch = pointBatch(new double[][] {{0, 0}, {0, 0}, {9, 9}}, "101");
        BitSet survivors = new BitSet();
        survivors.set(0, 3);

        SpatialReadProbe probe = (minX, minY, maxX, maxY) -> Decision.skip();

        SpatialDecimationGate gate = new SpatialDecimationGate(GEOM, probe);
        gate.narrow(batch, survivors);

        assertThat(survivors.get(0)).isFalse();
        assertThat(survivors.get(1)).isTrue();
        assertThat(survivors.get(2)).isFalse();
    }

    private static ParquetRecordBatch pointBatch(double[][] points, String validityBits) {
        MemorySegment[] values = new MemorySegment[points.length];
        for (int i = 0; i < points.length; i++) {
            values[i] = wkbPoint(points[i][0], points[i][1]);
        }
        BinaryVector vector = BinaryVector.materialized(values, validity(validityBits));
        Map<ColumnPath, ColumnVector> columns = Map.of(GEOM, vector);
        return new DefaultParquetRecordBatch(singleColumnSchema(GEOM), columns, points.length, Arena.ofConfined());
    }

    private static MemorySegment wkbPoint(double x, double y) {
        ByteBuffer buffer = ByteBuffer.allocate(21).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 1);
        buffer.putInt(1);
        buffer.putDouble(x);
        buffer.putDouble(y);
        return MemorySegment.ofArray(buffer.array());
    }

    private static Validity validity(String validityBits) {
        BitSet validity = new BitSet(validityBits.length());
        for (int i = 0; i < validityBits.length(); i++) {
            if (validityBits.charAt(i) == '1') {
                validity.set(i);
            }
        }
        return Validity.of(validity, validityBits.length());
    }

    private static ParquetSchema singleColumnSchema(ColumnPath col) {
        String leafName = col.name();
        SchemaNode.Primitive leaf = new SchemaNode.Primitive(
                leafName, Repetition.OPTIONAL, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        return new ParquetSchema(
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(leaf), Optional.empty(), -1));
    }
}
