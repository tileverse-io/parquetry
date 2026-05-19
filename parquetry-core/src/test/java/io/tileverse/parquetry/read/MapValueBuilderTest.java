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
package io.tileverse.parquetry.read;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.materializer.RowAccessor;
import io.tileverse.parquetry.page.LevelDecoder;
import io.tileverse.parquetry.page.PlainBinaryDecoder;
import io.tileverse.parquetry.page.PlainInt32Decoder;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.Field;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;

/**
 * Direct unit coverage for the {@code MapValueBuilder} branch of the Dremel assembler. MAP shape support was added but
 * the corpus's two MAP fixtures use INT32 keys (so {@code parquet-avro} can't read them as the oracle); this fills the
 * gap by driving the assembler with hand-crafted column readers that produce a few canonical MAP layouts.
 *
 * <h2>Schema</h2>
 *
 * <pre>
 *   message root {
 *     optional group settings (MAP) {
 *       repeated group key_value {
 *         required binary key (UTF8);
 *         required int32 value;
 *       }
 *     }
 *   }
 * </pre>
 *
 * <p>The Dremel levels per leaf are then {@code maxRep=1, maxDef=2} (one optional ancestor + one repeated ancestor).
 */
class MapValueBuilderTest {

    @Test
    void singleEntryMapAssemblesToMap() {
        // Row 0: settings = {"locale" -> 1}. Triples (rep=0, def=2, val=...) for both key and value.
        Object value = readFirstRow(
                new String[] {"locale"},
                new int[] {1},
                new int[] {0, 0, 0, 0, 0, 0, 0, 0}, // rep levels for key leaf, 1 entry then padding
                new int[] {2, 0, 0, 0, 0, 0, 0, 0}, // def levels for key leaf
                new int[] {0, 0, 0, 0, 0, 0, 0, 0}, // rep levels for value leaf
                new int[] {2, 0, 0, 0, 0, 0, 0, 0}); // def levels for value leaf
        assertThat(value).isInstanceOf(Map.class);
        @SuppressWarnings("unchecked")
        Map<Object, Object> map = (Map<Object, Object>) value;
        assertThat(map).hasSize(1);
        Map.Entry<Object, Object> only = map.entrySet().iterator().next();
        assertThat(asString(only.getKey())).isEqualTo("locale");
        assertThat(only.getValue()).isEqualTo(1);
    }

    @Test
    void nullMapReadsAsNull() {
        // Row 0: settings = null. The leaf def=0 means even the optional MAP ancestor isn't present.
        Object value = readFirstRow(
                new String[] {},
                new int[] {},
                new int[] {0, 0, 0, 0, 0, 0, 0, 0},
                new int[] {0, 0, 0, 0, 0, 0, 0, 0},
                new int[] {0, 0, 0, 0, 0, 0, 0, 0},
                new int[] {0, 0, 0, 0, 0, 0, 0, 0});
        assertThat(value)
                .as("a null MAP must surface as null at the map's path")
                .isNull();
    }

    @Test
    void emptyMapReadsAsEmptyMap() {
        // Row 0: settings = {} (the optional MAP is present but has zero entries). Leaf def=1 (one less than maxDef=2)
        // signals the map exists but the key_value group has no elements.
        Object value = readFirstRow(
                new String[] {},
                new int[] {},
                new int[] {0, 0, 0, 0, 0, 0, 0, 0},
                new int[] {1, 0, 0, 0, 0, 0, 0, 0},
                new int[] {0, 0, 0, 0, 0, 0, 0, 0},
                new int[] {1, 0, 0, 0, 0, 0, 0, 0});
        assertThat(value).isInstanceOf(Map.class);
        @SuppressWarnings("unchecked")
        Map<Object, Object> map = (Map<Object, Object>) value;
        assertThat(map)
                .as("an empty MAP must surface as an empty Map (not null)")
                .isEmpty();
    }

    @Test
    void twoEntryMapPreservesInsertionOrder() {
        // Row 0: settings = {"a" -> 10, "b" -> 20}. First entry has rep=0, second has rep=1 (same map continues).
        Object value = readFirstRow(
                new String[] {"a", "b"},
                new int[] {10, 20},
                new int[] {0, 1, 0, 0, 0, 0, 0, 0},
                new int[] {2, 2, 0, 0, 0, 0, 0, 0},
                new int[] {0, 1, 0, 0, 0, 0, 0, 0},
                new int[] {2, 2, 0, 0, 0, 0, 0, 0});
        @SuppressWarnings("unchecked")
        Map<Object, Object> map = (Map<Object, Object>) value;
        assertThat(map).hasSize(2);
        List<String> keys =
                map.keySet().stream().map(MapValueBuilderTest::asString).toList();
        assertThat(keys)
                .as("MapValueBuilder must preserve key_value entry order")
                .containsExactly("a", "b");
        assertThat(map.values()).containsExactly(10, 20);
    }

    /**
     * Builds the MAP schema, wires column readers for the key + value leaves with the supplied triples, and runs the
     * assembler once to return the first row's value at {@code settings}. Keeps the test ergonomics close to the
     * existing assembler tests (8-element level arrays driven by {@link #encodeBitPacked}).
     */
    private static Object readFirstRow(
            String[] keyValues,
            int[] valueValues,
            int[] keyRepLevels,
            int[] keyDefLevels,
            int[] valueRepLevels,
            int[] valueDefLevels) {
        ParquetSchema schema = mapSchema();

        PlainBinaryDecoder keyDecoder = new PlainBinaryDecoder();
        keyDecoder.load(plainBinaryBytes(keyValues), keyValues.length);
        PlainInt32Decoder valueDecoder = plainInt32Decoder(valueValues);

        LevelDecoder keyRep = levelDecoder(1, keyRepLevels);
        LevelDecoder keyDef = levelDecoder(2, keyDefLevels);
        LevelDecoder valueRep = levelDecoder(1, valueRepLevels);
        LevelDecoder valueDef = levelDecoder(2, valueDefLevels);

        ColumnReader keyReader = new BasicColumnReader(
                ColumnPath.of("settings", "key_value", "key"),
                /* maxRep */ 1,
                /* maxDef */ 2,
                keyRep,
                keyDef,
                keyDecoder,
                /* totalTriples */ 8);
        ColumnReader valueReader = new BasicColumnReader(
                ColumnPath.of("settings", "key_value", "value"),
                /* maxRep */ 1,
                /* maxDef */ 2,
                valueRep,
                valueDef,
                valueDecoder,
                /* totalTriples */ 8);

        RecordAssembler assembler = new RecordAssembler(schema, List.of(keyReader, valueReader));
        RowAccessor row = assembler.next();
        return row.get(ColumnPath.of("settings"));
    }

    private static ParquetSchema mapSchema() {
        Field.Primitive key = new Field.Primitive(
                "key", Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        Field.Primitive value = new Field.Primitive(
                "value", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        Field.Group keyValue =
                new Field.Group("key_value", Repetition.REPEATED, List.of(key, value), Optional.empty(), -1);
        Field.Group settings = new Field.Group(
                "settings", Repetition.OPTIONAL, List.of(keyValue), Optional.of(new LogicalType.MapType()), -1);
        return new ParquetSchema(new Field.Group("root", Repetition.REQUIRED, List.of(settings), Optional.empty(), -1));
    }

    private static ByteBuffer plainBinaryBytes(String[] strings) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (String s : strings) {
            byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
            // Plain encoding: 4-byte little-endian length followed by raw bytes.
            out.write(bytes.length & 0xff);
            out.write((bytes.length >> 8) & 0xff);
            out.write((bytes.length >> 16) & 0xff);
            out.write((bytes.length >> 24) & 0xff);
            out.write(bytes, 0, bytes.length);
        }
        return ByteBuffer.wrap(out.toByteArray()).order(ByteOrder.LITTLE_ENDIAN);
    }

    private static PlainInt32Decoder plainInt32Decoder(int[] values) {
        ByteBuffer page = ByteBuffer.allocate(values.length * 4).order(ByteOrder.LITTLE_ENDIAN);
        for (int v : values) {
            page.putInt(v);
        }
        page.flip();
        PlainInt32Decoder decoder = new PlainInt32Decoder();
        decoder.load(page, values.length);
        return decoder;
    }

    private static LevelDecoder levelDecoder(int bitWidth, int[] levels) {
        LevelDecoder decoder = new LevelDecoder(bitWidth);
        decoder.load(encodeBitPacked(levels, bitWidth));
        return decoder;
    }

    private static String asString(Object value) {
        if (value instanceof ByteBuffer bb) {
            byte[] bytes = new byte[bb.remaining()];
            bb.duplicate().get(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return value.toString();
    }

    /**
     * Mirrors {@code RecordAssemblerTest#encodeBitPacked}: encodes 8 values as a single bit-packed group with header
     * varint (1 group, bit-packed flag).
     */
    private static ByteBuffer encodeBitPacked(int[] values, int bitWidth) {
        if (values.length != 8) {
            throw new IllegalArgumentException("Must supply exactly 8 values for one bit-packed group");
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        long header = (1L << 1) | 1L;
        while ((header & ~0x7fL) != 0L) {
            out.write((int) ((header & 0x7f) | 0x80));
            header >>>= 7;
        }
        out.write((int) header);
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
