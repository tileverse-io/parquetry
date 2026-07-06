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
package io.tileverse.parquetry.avro;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class DatumSkipperTest {

    private static AvroBinaryDecoder decoder(byte[] bytes) {
        return new AvroBinaryDecoder(MemorySegment.ofArray(bytes).asReadOnly());
    }

    @Test
    void skipsLeadingFieldsToTheExactValuePosition() {
        byte[] bytes =
                new AvroTestEncoder().intValue(7).string("hi").doubleValue(1.5).toBytes();

        AvroBinaryDecoder in = decoder(bytes);
        DatumSkipper skipper = new DatumSkipper();
        skipper.skip(AvroSchema.INT, in);
        skipper.skip(AvroSchema.STRING, in);

        assertThat(in.readDouble()).isEqualTo(1.5);
    }

    @Test
    void skipsBlockedArrayByItsByteSizeHint() {
        byte[] items = new AvroTestEncoder().longValue(100).longValue(200).toBytes();
        byte[] bytes = new AvroTestEncoder()
                .longValue(-2)
                .longValue(items.length)
                .raw(items)
                .longValue(0)
                .intValue(77)
                .toBytes();

        AvroBinaryDecoder in = decoder(bytes);
        new DatumSkipper().skip(new AvroSchema.Array(AvroSchema.LONG), in);

        assertThat(in.readInt()).isEqualTo(77);
    }

    @ParameterizedTest
    @MethodSource("schemaShapes")
    void skipsDatumAndLandsOnTheNextValue(AvroSchema schema, byte[] datum) {
        byte[] bytes = new AvroTestEncoder().raw(datum).intValue(7).toBytes();

        AvroBinaryDecoder in = decoder(bytes);
        new DatumSkipper().skip(schema, in);

        assertThat(in.readInt()).isEqualTo(7);
    }

    static Stream<Arguments> schemaShapes() {
        AvroSchema.Union nullableLong = new AvroSchema.Union(List.of(AvroSchema.NULL, AvroSchema.LONG));
        AvroSchema.Record nested = new AvroSchema.Record(
                "pair",
                null,
                List.of(),
                List.of(AvroSchema.Field.of("a", 0, AvroSchema.INT), AvroSchema.Field.of("b", 1, AvroSchema.STRING)));
        AvroSchema recursiveNode = AvroSchema.parse("""
                {"type":"record","name":"Node","fields":[
                  {"name":"value","type":"int"},
                  {"name":"next","type":["null","Node"]}]}
                """);
        return Stream.of(
                arguments(Named.of("null", AvroSchema.NULL), new byte[0]),
                arguments(
                        Named.of("boolean", AvroSchema.BOOLEAN),
                        new AvroTestEncoder().booleanValue(true).toBytes()),
                arguments(
                        Named.of("long", AvroSchema.LONG),
                        new AvroTestEncoder().longValue(123456789L).toBytes()),
                arguments(
                        Named.of("float", AvroSchema.FLOAT),
                        new AvroTestEncoder().floatValue(2.5f).toBytes()),
                arguments(
                        Named.of("bytes", AvroSchema.BYTES),
                        new AvroTestEncoder().bytes(new byte[] {1, 2, 3}).toBytes()),
                arguments(
                        Named.of("fixed(4)", new AvroSchema.Fixed("f4", null, List.of(), 4, null)),
                        new AvroTestEncoder().raw(new byte[] {9, 9, 9, 9}).toBytes()),
                arguments(
                        Named.of("enum", new AvroSchema.Enum("S", null, List.of(), List.of("ADDED", "DELETED"), null)),
                        new AvroTestEncoder().intValue(1).toBytes()),
                arguments(
                        Named.of("union[null,long] null branch", nullableLong),
                        new AvroTestEncoder().longValue(0).toBytes()),
                arguments(
                        Named.of("union[null,long] long branch", nullableLong),
                        new AvroTestEncoder().longValue(1).longValue(99).toBytes()),
                arguments(
                        Named.of("map<string>", new AvroSchema.Map(AvroSchema.STRING)),
                        new AvroTestEncoder()
                                .longValue(1)
                                .string("k")
                                .string("v")
                                .longValue(0)
                                .toBytes()),
                arguments(
                        Named.of("nested record", nested),
                        new AvroTestEncoder().intValue(5).string("x").toBytes()),
                arguments(
                        Named.of("recursive Ref depth 2", recursiveNode),
                        new AvroTestEncoder()
                                .intValue(1)
                                .longValue(1)
                                .intValue(2)
                                .longValue(0)
                                .toBytes()));
    }

    @Test
    void rejectsRunawayRecursionDepth() {
        AvroSchema schema = AvroSchema.parse("""
                {"type":"record","name":"Node","fields":[{"name":"next","type":["null","Node"]}]}
                """);
        byte[] deep = new byte[4096];
        Arrays.fill(deep, (byte) 0x02); // zig-zag varint 1 = union branch 1 = recurse, past any sane depth

        DatumSkipper skipper = new DatumSkipper();
        AvroBinaryDecoder in = decoder(deep);
        assertThatThrownBy(() -> skipper.skip(schema, in))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("depth");
    }

    @Test
    void rejectsArrayBlockCountBeyondRemainingInput() {
        // each long item costs at least one byte; a billion of them cannot fit in five bytes of input
        byte[] bytes = new AvroTestEncoder().longValue(1_000_000_000L).toBytes();

        DatumSkipper skipper = new DatumSkipper();
        AvroSchema.Array arrayOfLong = new AvroSchema.Array(AvroSchema.LONG);
        AvroBinaryDecoder in = decoder(bytes);
        assertThatThrownBy(() -> skipper.skip(arrayOfLong, in))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("exceeds remaining");
    }

    @Test
    void rejectsMapBlockCountBeyondRemainingInput() {
        byte[] bytes = new AvroTestEncoder().longValue(1_000_000_000L).toBytes();

        DatumSkipper skipper = new DatumSkipper();
        AvroSchema.Map mapOfNull = new AvroSchema.Map(AvroSchema.NULL);
        AvroBinaryDecoder in = decoder(bytes);
        assertThatThrownBy(() -> skipper.skip(mapOfNull, in))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("exceeds remaining");
    }

    @Test
    void skipsHugeZeroByteItemArrayWithoutIterating() {
        // legal Avro: the nulls occupy no bytes; a count no loop could finish proves the one-step pass
        byte[] bytes = new AvroTestEncoder()
                .longValue(Long.MAX_VALUE / 2)
                .longValue(0)
                .intValue(7)
                .toBytes();

        AvroBinaryDecoder in = decoder(bytes);
        new DatumSkipper().skip(new AvroSchema.Array(AvroSchema.NULL), in);

        assertThat(in.readInt()).isEqualTo(7);
    }

    @Test
    void skipsArrayOfNullsAsTheLastBytesOfInput() {
        byte[] bytes = new AvroTestEncoder().longValue(3).longValue(0).toBytes();

        AvroBinaryDecoder in = decoder(bytes);
        new DatumSkipper().skip(new AvroSchema.Array(AvroSchema.NULL), in);

        assertThat(in.hasRemaining()).isFalse();
    }

    @ParameterizedTest
    @MethodSource("minimumWireSizes")
    void computesMinimumWireSize(AvroSchema schema, int expected) {
        assertThat(DatumSkipper.minimumWireSize(schema)).isEqualTo(expected);
    }

    static Stream<Arguments> minimumWireSizes() {
        AvroSchema.Record emptyRecord = new AvroSchema.Record("empty", null, List.of(), List.of());
        AvroSchema.Record nullsRecord = new AvroSchema.Record(
                "nulls",
                null,
                List.of(),
                List.of(AvroSchema.Field.of("a", 0, AvroSchema.NULL), AvroSchema.Field.of("b", 1, AvroSchema.NULL)));
        AvroSchema.Record intDoubleRecord = new AvroSchema.Record(
                "pair",
                null,
                List.of(),
                List.of(AvroSchema.Field.of("i", 0, AvroSchema.INT), AvroSchema.Field.of("d", 1, AvroSchema.DOUBLE)));
        AvroSchema recursiveNode = AvroSchema.parse("""
                {"type":"record","name":"Node","fields":[
                  {"name":"value","type":"int"},
                  {"name":"next","type":["null","Node"]}]}
                """);
        // no union escape: this schema cannot encode any finite datum; the depth cap answers 1
        AvroSchema unboundedCycle = AvroSchema.parse("""
                {"type":"record","name":"Node","fields":[{"name":"next","type":"Node"}]}
                """);
        return Stream.of(
                arguments(Named.of("null", AvroSchema.NULL), 0),
                arguments(Named.of("boolean", AvroSchema.BOOLEAN), 1),
                arguments(Named.of("int", AvroSchema.INT), 1),
                arguments(Named.of("long", AvroSchema.LONG), 1),
                arguments(Named.of("float", AvroSchema.FLOAT), 4),
                arguments(Named.of("double", AvroSchema.DOUBLE), 8),
                arguments(Named.of("string", AvroSchema.STRING), 1),
                arguments(Named.of("bytes", AvroSchema.BYTES), 1),
                arguments(Named.of("fixed(0)", new AvroSchema.Fixed("f0", null, List.of(), 0, null)), 0),
                arguments(Named.of("fixed(16)", new AvroSchema.Fixed("f16", null, List.of(), 16, null)), 16),
                arguments(
                        Named.of("enum", new AvroSchema.Enum("S", null, List.of(), List.of("ADDED", "DELETED"), null)),
                        1),
                arguments(
                        Named.of("union[null,long]", new AvroSchema.Union(List.of(AvroSchema.NULL, AvroSchema.LONG))),
                        1),
                arguments(Named.of("array<long>", new AvroSchema.Array(AvroSchema.LONG)), 1),
                arguments(Named.of("map<string>", new AvroSchema.Map(AvroSchema.STRING)), 1),
                arguments(Named.of("empty record", emptyRecord), 0),
                arguments(Named.of("record of nulls", nullsRecord), 0),
                arguments(Named.of("record{int,double}", intDoubleRecord), 9),
                arguments(Named.of("recursive Node with union escape", recursiveNode), 2),
                arguments(Named.of("unbounded no-union cycle", unboundedCycle), 1));
    }
}
