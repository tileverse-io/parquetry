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

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.arrow.columnar.ArrowBufferCodec;
import io.tileverse.parquetry.arrow.columnar.EncodedBatch;
import io.tileverse.parquetry.arrow.columnar.EncodedBuffer;
import io.tileverse.parquetry.arrow.columnar.EncodedNode;
import io.tileverse.parquetry.arrow.columnar.NodeEncoding;
import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.schema.ColumnPath;

class EncodedBatchSerializerTest {

    @Test
    void roundTripsAFixedWidthLeafBatch() {
        EncodedNode node = ArrowBufferCodec.encode(IntVector.materialized(new int[] {1, 2, 3}, Validity.allValid(3)));
        EncodedBatch original = new EncodedBatch(3, List.of(ColumnPath.of("n")), List.of(node));

        MemorySegment wire = EncodedBatchSerializer.serialize(original);
        EncodedBatch restored = EncodedBatchSerializer.deserialize(wire);

        assertEncodedBatchEquals(original, restored);
    }

    @Test
    void roundTripsADictionaryEncodedNode() {
        IntVector indices = IntVector.materialized(new int[] {0, 1, 0}, Validity.allValid(3));
        EncodedNode plain = ArrowBufferCodec.encode(indices);
        EncodedNode dict = new EncodedNode(2, 0, plain.buffers(), plain.children(), new NodeEncoding.FixedWidth(4));
        EncodedNode dictionaryNode =
                new EncodedNode(3, 0, plain.buffers(), plain.children(), new NodeEncoding.Dictionary(dict, 4));
        EncodedBatch original = new EncodedBatch(3, List.of(ColumnPath.of("d")), List.of(dictionaryNode));

        EncodedBatch restored = EncodedBatchSerializer.deserialize(EncodedBatchSerializer.serialize(original));

        assertEncodedBatchEquals(original, restored);
    }

    @Test
    void roundTripsAMultiSegmentColumnPath() {
        EncodedNode node = ArrowBufferCodec.encode(IntVector.materialized(new int[] {1, 2, 3}, Validity.allValid(3)));
        EncodedBatch original = new EncodedBatch(3, List.of(ColumnPath.of("bbox", "xmin")), List.of(node));

        EncodedBatch restored = EncodedBatchSerializer.deserialize(EncodedBatchSerializer.serialize(original));

        assertEncodedBatchEquals(original, restored);
    }

    private static void assertEncodedBatchEquals(EncodedBatch expected, EncodedBatch actual) {
        assertThat(actual.rowCount()).isEqualTo(expected.rowCount());
        assertThat(actual.columns()).isEqualTo(expected.columns());
        assertThat(actual.nodes()).hasSameSizeAs(expected.nodes());
        for (int i = 0; i < expected.nodes().size(); i++) {
            assertNodeEquals(expected.nodes().get(i), actual.nodes().get(i));
        }
    }

    private static void assertNodeEquals(EncodedNode expected, EncodedNode actual) {
        assertThat(actual.length()).isEqualTo(expected.length());
        assertThat(actual.nullCount()).isEqualTo(expected.nullCount());
        assertThat(actual.encoding().getClass()).isEqualTo(expected.encoding().getClass());
        assertThat(actual.buffers()).hasSameSizeAs(expected.buffers());
        for (int i = 0; i < expected.buffers().size(); i++) {
            EncodedBuffer e = expected.buffers().get(i);
            EncodedBuffer a = actual.buffers().get(i);
            assertThat(a.role()).isEqualTo(e.role());
            assertThat(a.bytes().toArray(ValueLayout.JAVA_BYTE))
                    .isEqualTo(e.bytes().toArray(ValueLayout.JAVA_BYTE));
        }
        assertThat(actual.children()).hasSameSizeAs(expected.children());
        for (int i = 0; i < expected.children().size(); i++) {
            assertNodeEquals(expected.children().get(i), actual.children().get(i));
        }
        if (expected.encoding() instanceof NodeEncoding.Dictionary(EncodedNode expectedDict, int _)) {
            NodeEncoding.Dictionary actualDict = (NodeEncoding.Dictionary) actual.encoding();
            assertNodeEquals(expectedDict, actualDict.dictionary());
        }
    }
}
