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
package io.tileverse.parquetry.avro;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class AvroDatumDecoderTest {

    private static void varLong(ByteArrayOutputStream out, long signed) {
        long n = (signed << 1) ^ (signed >> 63);
        while ((n & ~0x7FL) != 0) {
            out.write((int) ((n & 0x7F) | 0x80));
            n >>>= 7;
        }
        out.write((int) n);
    }

    private static AvroBinaryDecoder decoder(byte[] bytes) {
        return new AvroBinaryDecoder(MemorySegment.ofArray(bytes).asReadOnly());
    }

    @Test
    void decodesRecordOfPrimitives() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        varLong(out, 7);
        byte[] path = "a.parquet".getBytes(StandardCharsets.UTF_8);
        varLong(out, path.length);
        out.writeBytes(path);

        AvroSchema.Record schema = new AvroSchema.Record(
                "r",
                null,
                List.of(),
                List.of(
                        AvroSchema.Field.of("id", 0, AvroSchema.LONG),
                        AvroSchema.Field.of("path", 1, AvroSchema.STRING)));

        AvroRecord decoded = (AvroRecord) new AvroDatumDecoder().decode(schema, decoder(out.toByteArray()));
        assertThat(decoded.get("id")).isEqualTo(7L);
        assertThat(decoded.get("path")).isEqualTo("a.parquet");
    }

    @Test
    void decodesNullableUnionBranches() {
        AvroSchema.Union nullableLong = new AvroSchema.Union(List.of(AvroSchema.NULL, AvroSchema.LONG));

        ByteArrayOutputStream present = new ByteArrayOutputStream();
        varLong(present, 1);
        varLong(present, 42);
        assertThat(new AvroDatumDecoder().decode(nullableLong, decoder(present.toByteArray())))
                .isEqualTo(42L);

        ByteArrayOutputStream absent = new ByteArrayOutputStream();
        varLong(absent, 0);
        assertThat(new AvroDatumDecoder().decode(nullableLong, decoder(absent.toByteArray())))
                .isNull();
    }

    @Test
    void decodesBinaryMapOfBounds() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        varLong(out, 1);
        byte[] key = "2".getBytes(StandardCharsets.UTF_8);
        varLong(out, key.length);
        out.writeBytes(key);
        varLong(out, 2);
        out.write(0xDE);
        out.write(0xAD);
        varLong(out, 0);

        AvroSchema.Map boundsMap = new AvroSchema.Map(AvroSchema.BYTES);
        @SuppressWarnings("unchecked")
        Map<String, Object> decoded =
                (Map<String, Object>) new AvroDatumDecoder().decode(boundsMap, decoder(out.toByteArray()));
        MemorySegment bound = (MemorySegment) decoded.get("2");
        assertThat(bound.byteSize()).isEqualTo(2);
        assertThat(bound.isReadOnly()).isTrue();
    }

    @Test
    void decodesArrayOfLongs() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        varLong(out, 2);
        varLong(out, 10);
        varLong(out, 20);
        varLong(out, 0);

        AvroSchema.Array longs = new AvroSchema.Array(AvroSchema.LONG);
        @SuppressWarnings("unchecked")
        List<Object> decoded = (List<Object>) new AvroDatumDecoder().decode(longs, decoder(out.toByteArray()));
        assertThat(decoded).containsExactly(10L, 20L);
    }

    @Test
    void decodesArrayWithNegativeBlockCountAndSizeHint() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        varLong(out, -2); // negative count: 2 items, followed by a byte-size hint
        varLong(out, 16); // skippable byte-size hint
        varLong(out, 100);
        varLong(out, 200);
        varLong(out, 0);

        AvroSchema.Array longs = new AvroSchema.Array(AvroSchema.LONG);
        @SuppressWarnings("unchecked")
        List<Object> decoded = (List<Object>) new AvroDatumDecoder().decode(longs, decoder(out.toByteArray()));
        assertThat(decoded).containsExactly(100L, 200L);
    }

    @Test
    void decodesEnumByIndex() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        varLong(out, 1); // index 1
        AvroSchema.Enum status = new AvroSchema.Enum("S", null, List.of(), List.of("ADDED", "DELETED"), null);
        assertThat(new AvroDatumDecoder().decode(status, decoder(out.toByteArray())))
                .isEqualTo("DELETED");
    }

    @Test
    void rejectsUnionIndexOutOfRange() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        varLong(out, 5); // only 2 branches exist
        AvroSchema.Union nullableLong = new AvroSchema.Union(List.of(AvroSchema.NULL, AvroSchema.LONG));
        AvroDatumDecoder datumDecoder = new AvroDatumDecoder();
        AvroBinaryDecoder in = decoder(out.toByteArray());
        assertThatThrownBy(() -> datumDecoder.decode(nullableLong, in)).isInstanceOf(AvroFormatException.class);
    }

    @Test
    void rejectsEnumIndexOutOfRange() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        varLong(out, 9); // only 2 symbols exist
        AvroSchema.Enum status = new AvroSchema.Enum("S", null, List.of(), List.of("ADDED", "DELETED"), null);
        AvroDatumDecoder datumDecoder = new AvroDatumDecoder();
        AvroBinaryDecoder in = decoder(out.toByteArray());
        assertThatThrownBy(() -> datumDecoder.decode(status, in)).isInstanceOf(AvroFormatException.class);
    }

    @Test
    void decodesNestedRecord() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        varLong(out, 0); // outer status = 0
        byte[] path = "x.parquet".getBytes(StandardCharsets.UTF_8);
        varLong(out, path.length);
        out.writeBytes(path);
        varLong(out, 99); // inner record_count

        AvroSchema.Record inner = new AvroSchema.Record(
                "data_file",
                null,
                List.of(),
                List.of(
                        AvroSchema.Field.of("file_path", 0, AvroSchema.STRING),
                        AvroSchema.Field.of("record_count", 1, AvroSchema.LONG)));
        AvroSchema.Record outer = new AvroSchema.Record(
                "manifest_entry",
                null,
                List.of(),
                List.of(AvroSchema.Field.of("status", 0, AvroSchema.INT), AvroSchema.Field.of("data_file", 1, inner)));

        AvroRecord entry = (AvroRecord) new AvroDatumDecoder().decode(outer, decoder(out.toByteArray()));
        assertThat(entry.get("status")).isEqualTo(0);
        AvroRecord dataFile = (AvroRecord) entry.get("data_file");
        assertThat(dataFile.get("file_path")).isEqualTo("x.parquet");
        assertThat(dataFile.get("record_count")).isEqualTo(99L);
    }

    @Test
    void decodesIntAsIntegerAndFloatAsFloat() {
        byte[] bytes = new AvroTestEncoder().intValue(-42).floatValue(1.5f).toBytes();

        AvroSchema.Record schema = new AvroSchema.Record(
                "r",
                null,
                List.of(),
                List.of(
                        AvroSchema.Field.of("count", 0, AvroSchema.INT),
                        AvroSchema.Field.of("ratio", 1, AvroSchema.FLOAT)));

        AvroRecord decoded = (AvroRecord) new AvroDatumDecoder().decode(schema, decoder(bytes));
        assertThat(decoded.get("count")).isEqualTo(-42);
        assertThat(decoded.get("ratio")).isEqualTo(1.5f);
    }

    @Test
    void decodesRecursiveStructures() {
        AvroSchema schema = AvroSchema.parse("""
                {"type":"record","name":"Node","fields":[
                  {"name":"value","type":"int"},
                  {"name":"next","type":["null","Node"]}]}
                """);
        // Node{1, Node{2, null}}: value 1, union branch 1, value 2, union branch 0
        byte[] bytes = new AvroTestEncoder()
                .intValue(1)
                .longValue(1)
                .intValue(2)
                .longValue(0)
                .toBytes();

        AvroRecord outer = (AvroRecord) new AvroDatumDecoder().decode(schema, decoder(bytes));
        assertThat(outer.get("value")).isEqualTo(1);
        AvroRecord inner = (AvroRecord) outer.get("next");
        assertThat(inner.get("value")).isEqualTo(2);
        assertThat(inner.get("next")).isNull();
    }

    @Test
    void rejectsArrayBlockCountBeyondRemainingInput() {
        // each long item costs at least one byte; a billion of them cannot fit in five bytes of input
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        varLong(out, 1_000_000_000L);

        AvroSchema.Array arrayOfLong = new AvroSchema.Array(AvroSchema.LONG);
        AvroDatumDecoder datumDecoder = new AvroDatumDecoder();
        AvroBinaryDecoder in = decoder(out.toByteArray());
        assertThatThrownBy(() -> datumDecoder.decode(arrayOfLong, in))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("exceeds remaining");
    }

    @Test
    void rejectsMapBlockCountBeyondRemainingInput() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        varLong(out, 1_000_000_000L);

        AvroSchema.Map mapOfNull = new AvroSchema.Map(AvroSchema.NULL);
        AvroDatumDecoder datumDecoder = new AvroDatumDecoder();
        AvroBinaryDecoder in = decoder(out.toByteArray());
        assertThatThrownBy(() -> datumDecoder.decode(mapOfNull, in))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("exceeds remaining");
    }

    @Test
    void rejectsUnrepresentableNegativeBlockCount() {
        // -Long.MIN_VALUE is Long.MIN_VALUE: left unchecked, the still-negative count would run zero
        // iterations and the data bytes would be misparsed as the next block count
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        varLong(out, Long.MIN_VALUE);
        varLong(out, 0); // the byte-size hint that follows any negative count

        AvroSchema.Array arrayOfLong = new AvroSchema.Array(AvroSchema.LONG);
        AvroDatumDecoder datumDecoder = new AvroDatumDecoder();
        AvroBinaryDecoder in = decoder(out.toByteArray());
        assertThatThrownBy(() -> datumDecoder.decode(arrayOfLong, in))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("not representable");
    }

    @Test
    void rejectsZeroByteItemBlockCountAboveCap() {
        // zero-byte items make the input length useless as a bound; the named cap keeps a few crafted
        // bytes from allocating a billion-element list
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        varLong(out, 1_000_000_000L);

        AvroSchema.Array arrayOfNull = new AvroSchema.Array(AvroSchema.NULL);
        AvroDatumDecoder datumDecoder = new AvroDatumDecoder();
        AvroBinaryDecoder in = decoder(out.toByteArray());
        assertThatThrownBy(() -> datumDecoder.decode(arrayOfNull, in))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining(String.valueOf(AvroDatumDecoder.MAX_ZERO_BYTE_ITEMS_PER_BLOCK));
    }

    @Test
    void decodesArrayOfNullsAsTheLastBytesOfInput() {
        // legal Avro: the count varint plus terminator encode any N of zero-byte items, even at end of input
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        varLong(out, 3);
        varLong(out, 0);

        AvroSchema.Array arrayOfNull = new AvroSchema.Array(AvroSchema.NULL);
        @SuppressWarnings("unchecked")
        List<Object> decoded = (List<Object>) new AvroDatumDecoder().decode(arrayOfNull, decoder(out.toByteArray()));
        assertThat(decoded).hasSize(3).containsOnlyNulls();
    }

    @Test
    void decodesThousandNullArrayBlockFromAFewBytes() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        varLong(out, 1000);
        varLong(out, 0);

        AvroSchema.Array arrayOfNull = new AvroSchema.Array(AvroSchema.NULL);
        @SuppressWarnings("unchecked")
        List<Object> decoded = (List<Object>) new AvroDatumDecoder().decode(arrayOfNull, decoder(out.toByteArray()));
        assertThat(decoded).hasSize(1000).containsOnlyNulls();
    }

    @Test
    void rejectsRunawayRecursionDepth() {
        AvroSchema schema = AvroSchema.parse("""
                {"type":"record","name":"Node","fields":[{"name":"next","type":["null","Node"]}]}
                """);
        byte[] deep = new byte[4096];
        Arrays.fill(deep, (byte) 0x02); // zig-zag varint 1 = union branch 1 = recurse, past any sane depth

        AvroDatumDecoder datumDecoder = new AvroDatumDecoder();
        AvroBinaryDecoder in = decoder(deep);
        assertThatThrownBy(() -> datumDecoder.decode(schema, in))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("depth");
    }
}
