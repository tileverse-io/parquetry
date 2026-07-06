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
package io.tileverse.parquetry.arrow.ipc;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.RecordBatch;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DefaultParquetRecordBatch;
import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class ArrowBatchEncoderTest {

    @Test
    void encodesNodesBuffersAndBodyLength() {
        SchemaNode.Primitive id = new SchemaNode.Primitive(
                "id", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), 0);
        ParquetSchema schema =
                new ParquetSchema(new SchemaNode.Group("root", Repetition.REQUIRED, List.of(id), Optional.empty(), -1));
        BitSet validBits = new BitSet();
        validBits.set(0, 3);
        Validity validity = Validity.of(validBits, 3);
        Map<ColumnPath, ColumnVector> columns = new LinkedHashMap<>();
        columns.put(ColumnPath.of("id"), IntVector.materialized(new int[] {1, 2, 3}, validity));
        ParquetRecordBatch batch = new DefaultParquetRecordBatch(schema, columns, 3, Arena.ofShared());

        ArrowBatchEncoder.Encoded encoded = ArrowBatchEncoder.encode(batch);

        Message message = Message.getRootAsMessage(encoded.metadata());
        assertThat(message.headerType()).isEqualTo(MessageHeader.RecordBatch);
        RecordBatch recordBatch = (RecordBatch) message.header(new RecordBatch());
        assertThat(recordBatch.length()).isEqualTo(3L);
        assertThat(recordBatch.nodesLength()).isEqualTo(1);
        assertThat(recordBatch.nodes(0).length()).isEqualTo(3L);
        assertThat(recordBatch.nodes(0).nullCount()).isZero();
        assertThat(recordBatch.buffersLength()).isEqualTo(2); // validity + data
        // An all-valid column needs no bitmap: its validity buffer is length 0.
        assertThat(recordBatch.buffers(0).length()).isZero();
        assertThat(recordBatch.buffers(1).length()).isPositive();
        long bodyLength =
                encoded.body().stream().mapToLong(MemorySegment::byteSize).sum();
        assertThat(message.bodyLength()).isEqualTo(bodyLength);
    }

    @Test
    void emitsAValidityBitmapWhenAColumnHasNulls() {
        SchemaNode.Primitive id = new SchemaNode.Primitive(
                "id", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), 0);
        ParquetSchema schema =
                new ParquetSchema(new SchemaNode.Group("root", Repetition.REQUIRED, List.of(id), Optional.empty(), -1));
        BitSet validBits = new BitSet();
        validBits.set(0);
        validBits.set(2); // row 1 null
        Validity validity = Validity.of(validBits, 3);
        Map<ColumnPath, ColumnVector> columns = new LinkedHashMap<>();
        columns.put(ColumnPath.of("id"), IntVector.materialized(new int[] {10, 0, 30}, validity));
        ParquetRecordBatch batch = new DefaultParquetRecordBatch(schema, columns, 3, Arena.ofShared());

        ArrowBatchEncoder.Encoded encoded = ArrowBatchEncoder.encode(batch);

        Message message = Message.getRootAsMessage(encoded.metadata());
        RecordBatch recordBatch = (RecordBatch) message.header(new RecordBatch());
        assertThat(recordBatch.nodes(0).nullCount()).isEqualTo(1L);
        assertThat(recordBatch.buffersLength()).isEqualTo(2); // validity + data
        // A column with nulls emits a real (8-byte aligned) bitmap.
        assertThat(recordBatch.buffers(0).length()).isPositive();
        long bodyLength =
                encoded.body().stream().mapToLong(MemorySegment::byteSize).sum();
        assertThat(message.bodyLength()).isEqualTo(bodyLength);
    }
}
