/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.assembly;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.page.LevelDecoder;
import io.tileverse.parquetry.page.plain.PlainBinaryDecoder;
import io.tileverse.parquetry.page.plain.PlainInt32Decoder;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.Field;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.Schema;

class RecordAssemblerTest {

    @Test
    void flatRequiredSchemaThreeRows() {
        Schema schema = new Schema(new Field.Group(
                "root",
                Repetition.REQUIRED,
                List.of(
                        new Field.Primitive(
                                "id",
                                Repetition.REQUIRED,
                                PrimitiveKind.INT32,
                                OptionalInt.empty(),
                                Optional.empty(),
                                -1),
                        new Field.Primitive(
                                "name",
                                Repetition.REQUIRED,
                                PrimitiveKind.BYTE_ARRAY,
                                OptionalInt.empty(),
                                Optional.empty(),
                                -1)),
                Optional.empty(),
                -1));

        ByteBuffer idPage = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN);
        idPage.putInt(1).putInt(2).putInt(3).flip();
        PlainInt32Decoder idDecoder = new PlainInt32Decoder();
        idDecoder.load(idPage, 3);

        ByteBuffer namePage = ByteBuffer.allocate(64).order(ByteOrder.LITTLE_ENDIAN);
        for (String s : new String[] {"alice", "bob", "carol"}) {
            byte[] b = s.getBytes(StandardCharsets.UTF_8);
            namePage.putInt(b.length);
            namePage.put(b);
        }
        namePage.flip();
        PlainBinaryDecoder nameDecoder = new PlainBinaryDecoder();
        nameDecoder.load(namePage, 3);

        BasicColumnReader idReader = new BasicColumnReader(
                ColumnPath.of("id"), 0, 0, new LevelDecoder(0), new LevelDecoder(0), idDecoder, 3);
        BasicColumnReader nameReader = new BasicColumnReader(
                ColumnPath.of("name"), 0, 0, new LevelDecoder(0), new LevelDecoder(0), nameDecoder, 3);

        RecordAssembler assembler = new RecordAssembler(schema, List.of(idReader, nameReader));

        int[] expectedIds = {1, 2, 3};
        String[] expectedNames = {"alice", "bob", "carol"};
        for (int i = 0; i < 3; i++) {
            assertThat(assembler.hasNext()).isTrue();
            RowAccessor row = assembler.next();
            assertThat(row.get(ColumnPath.of("id"))).isEqualTo(expectedIds[i]);
            ByteBuffer nameBuf = (ByteBuffer) row.get(ColumnPath.of("name"));
            byte[] nameBytes = new byte[nameBuf.remaining()];
            nameBuf.get(nameBytes);
            assertThat(new String(nameBytes, StandardCharsets.UTF_8)).isEqualTo(expectedNames[i]);
        }
        assertThat(assembler.hasNext()).isFalse();
    }

    @Test
    void flatOptionalSchemaWithNull() {
        Schema schema = new Schema(new Field.Group(
                "root",
                Repetition.REQUIRED,
                List.of(
                        new Field.Primitive(
                                "id",
                                Repetition.REQUIRED,
                                PrimitiveKind.INT32,
                                OptionalInt.empty(),
                                Optional.empty(),
                                -1),
                        new Field.Primitive(
                                "nickname",
                                Repetition.OPTIONAL,
                                PrimitiveKind.BYTE_ARRAY,
                                OptionalInt.empty(),
                                Optional.empty(),
                                -1)),
                Optional.empty(),
                -1));

        ByteBuffer idPage = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN);
        idPage.putInt(1).putInt(2).putInt(3).flip();
        PlainInt32Decoder idDecoder = new PlainInt32Decoder();
        idDecoder.load(idPage, 3);

        // Only 2 non-null values; the second row (id=2) has nickname=null (def=0).
        ByteBuffer nicknamePage = ByteBuffer.allocate(64).order(ByteOrder.LITTLE_ENDIAN);
        for (String s : new String[] {"alice", "carol"}) {
            byte[] b = s.getBytes(StandardCharsets.UTF_8);
            nicknamePage.putInt(b.length);
            nicknamePage.put(b);
        }
        nicknamePage.flip();
        PlainBinaryDecoder nicknameDecoder = new PlainBinaryDecoder();
        nicknameDecoder.load(nicknamePage, 2);

        // def levels: [1, 0, 1] -- bit-packed at bitWidth=1, padded to 8 values per group.
        LevelDecoder defDecoder = new LevelDecoder(1);
        defDecoder.load(encodeBitPacked(new int[] {1, 0, 1, 0, 0, 0, 0, 0}, 1));

        BasicColumnReader idReader = new BasicColumnReader(
                ColumnPath.of("id"), 0, 0, new LevelDecoder(0), new LevelDecoder(0), idDecoder, 3);
        BasicColumnReader nicknameReader = new BasicColumnReader(
                ColumnPath.of("nickname"), 0, 1, new LevelDecoder(0), defDecoder, nicknameDecoder, 3);

        RecordAssembler assembler = new RecordAssembler(schema, List.of(idReader, nicknameReader));

        RowAccessor r0 = assembler.next();
        assertThat(r0.get(ColumnPath.of("id"))).isEqualTo(1);
        ByteBuffer name0 = (ByteBuffer) r0.get(ColumnPath.of("nickname"));
        byte[] name0Bytes = new byte[name0.remaining()];
        name0.get(name0Bytes);
        assertThat(new String(name0Bytes, StandardCharsets.UTF_8)).isEqualTo("alice");

        RowAccessor r1 = assembler.next();
        assertThat(r1.get(ColumnPath.of("id"))).isEqualTo(2);
        assertThat(r1.get(ColumnPath.of("nickname"))).isNull();

        RowAccessor r2 = assembler.next();
        assertThat(r2.get(ColumnPath.of("id"))).isEqualTo(3);
        ByteBuffer name2 = (ByteBuffer) r2.get(ColumnPath.of("nickname"));
        byte[] name2Bytes = new byte[name2.remaining()];
        name2.get(name2Bytes);
        assertThat(new String(name2Bytes, StandardCharsets.UTF_8)).isEqualTo("carol");

        assertThat(assembler.hasNext()).isFalse();
    }

    @Test
    void repeatedColumnRejectedAtConstruction() {
        Schema schema = new Schema(new Field.Group(
                "root",
                Repetition.REQUIRED,
                List.of(new Field.Primitive(
                        "tags", Repetition.REPEATED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1)),
                Optional.empty(),
                -1));

        PlainInt32Decoder dec = new PlainInt32Decoder();
        ByteBuffer page = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        page.putInt(1).flip();
        dec.load(page, 1);

        ColumnReader repeatedReader = new BasicColumnReader(
                ColumnPath.of("tags"),
                /* maxRep */ 1,
                /* maxDef */ 1,
                new LevelDecoder(1),
                new LevelDecoder(1),
                dec,
                1);

        assertThatThrownBy(() -> new RecordAssembler(schema, List.of(repeatedReader)))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("tags")
                .hasMessageContaining("maxRep=1");
    }

    /**
     * Encodes values as a single bit-packed group of 8. Header varint encodes (groups << 1) | 1 where groups=1 yields
     * header=3. Values are packed LSB-first.
     */
    private static ByteBuffer encodeBitPacked(int[] values, int bitWidth) {
        if (values.length != 8) {
            throw new IllegalArgumentException("Must supply exactly 8 values for one bit-packed group");
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        // Header: 1 group, bit-packed flag -> varint = (1 << 1) | 1 = 3
        long header = (1L << 1) | 1L;
        while ((header & ~0x7fL) != 0L) {
            out.write((int) ((header & 0x7f) | 0x80));
            header >>>= 7;
        }
        out.write((int) header);
        // Pack values LSB-first
        long buf = 0L;
        int bits = 0;
        for (int v : values) {
            buf |= ((long) v) << bits;
            bits += bitWidth;
            while (bits >= 8) {
                out.write((int) (buf & 0xff));
                buf >>>= 8;
                bits -= 8;
            }
        }
        if (bits > 0) {
            out.write((int) (buf & 0xff));
        }
        return ByteBuffer.wrap(out.toByteArray());
    }
}
