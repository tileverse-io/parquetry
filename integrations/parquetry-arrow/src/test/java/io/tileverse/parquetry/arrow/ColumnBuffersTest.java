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
package io.tileverse.parquetry.arrow;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.batch.IntVector;

class ColumnBuffersTest {

    @Test
    void fixedWidthIntHasValidityThenLittleEndianData() {
        BitSet validity = new BitSet();
        validity.set(0);
        validity.set(2); // row 1 null
        IntVector vector = IntVector.materialized(new int[] {10, 0, 30}, validity);

        List<byte[]> buffers = ColumnBuffers.forVector(vector);

        assertThat(buffers).hasSize(2);
        // validity: bits 0 and 2 set -> 0b00000101 = 0x05
        assertThat(buffers.get(0)[0]).isEqualTo((byte) 0x05);
        assertThat(buffers.get(0).length % 8).isZero();
        ByteBuffer data = ByteBuffer.wrap(buffers.get(1)).order(ByteOrder.LITTLE_ENDIAN);
        assertThat(data.getInt(0)).isEqualTo(10);
        assertThat(data.getInt(8)).isEqualTo(30);
        assertThat(buffers.get(1).length % 8).isZero();
    }
}
