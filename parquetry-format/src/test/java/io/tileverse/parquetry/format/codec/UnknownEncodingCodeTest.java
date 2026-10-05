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
package io.tileverse.parquetry.format.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.CompressionCodec;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.EncodingStats;
import io.tileverse.parquetry.format.PageType;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.format.UnsupportedFeatureException;

/**
 * Wire-level coverage for encoding codes unknown to parquetry, as written by a newer Parquet writer. The footer's
 * encodings list and encoding stats are informational and skip such codes; a page header naming one is unsupported,
 * because its page cannot be decoded.
 */
class UnknownEncodingCodeTest {

    private static final int UNKNOWN = 11;
    private static final int RLE = Encoding.RLE.value();
    private static final int PLAIN = Encoding.PLAIN.value();

    @Test
    void columnMetaDataLeavesOutUnknownEncodingsAndTheirStats() throws IOException {
        byte[] wire = columnMetaDataWire(List.of(RLE, UNKNOWN, PLAIN));

        ColumnMetaData metaData = ColumnMetaDataDeserializer.read(reader(wire));

        assertThat(metaData.encodings()).containsExactly(Encoding.RLE, Encoding.PLAIN);
        assertThat(metaData.encodingStats())
                .containsExactly(
                        new EncodingStats(PageType.DATA_PAGE, Encoding.RLE, 1),
                        new EncodingStats(PageType.DATA_PAGE, Encoding.PLAIN, 1));
        assertThat(metaData.dataPageOffset()).isEqualTo(4L);
    }

    @Test
    void alpReadsAsAKnownEncoding() throws IOException {
        byte[] wire = columnMetaDataWire(List.of(RLE, Encoding.ALP.value()));

        ColumnMetaData metaData = ColumnMetaDataDeserializer.read(reader(wire));

        assertThat(metaData.encodings()).containsExactly(Encoding.RLE, Encoding.ALP);
    }

    static Stream<Arguments> pageHeadersNamingAnUnknownEncoding() throws IOException {
        return Stream.of(
                Arguments.of("DataPageHeader.encoding", dataPageWire(UNKNOWN, RLE, RLE)),
                Arguments.of("DataPageHeader.definition_level_encoding", dataPageWire(PLAIN, UNKNOWN, RLE)),
                Arguments.of("DataPageHeader.repetition_level_encoding", dataPageWire(PLAIN, RLE, UNKNOWN)),
                Arguments.of("DataPageHeaderV2.encoding", dataPageV2Wire(UNKNOWN)),
                Arguments.of("DictionaryPageHeader.encoding", dictionaryPageWire(UNKNOWN)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("pageHeadersNamingAnUnknownEncoding")
    void pageHeaderNamingAnUnknownEncodingIsUnsupported(String field, byte[] wire) {
        assertThatThrownBy(() -> ParquetFormatDeserializer.readPageHeader(new ByteArrayInputStream(wire)))
                .isInstanceOf(UnsupportedFeatureException.class)
                .hasMessageContaining("wire code " + UNKNOWN)
                .hasMessageContaining(field);
    }

    /** A ColumnMetaData struct with the required fields, the given encodings and one stats entry per encoding. */
    private static byte[] columnMetaDataWire(List<Integer> encodings) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        CompactProtocolWriter w = new CompactProtocolWriter(bytes);
        w.writeStructBegin();
        w.writeI32Field((short) 1, PhysicalType.DOUBLE.value());
        w.writeListField((short) 2, CompactType.I32, encodings, CompactProtocolWriter::writeI32);
        w.writeListField((short) 3, CompactType.BINARY, List.of("x"), CompactProtocolWriter::writeString);
        w.writeI32Field((short) 4, CompressionCodec.UNCOMPRESSED.value());
        w.writeI64Field((short) 5, 5L);
        w.writeI64Field((short) 6, 40L);
        w.writeI64Field((short) 7, 40L);
        w.writeI64Field((short) 9, 4L);
        w.writeListField((short) 13, CompactType.STRUCT, encodings, UnknownEncodingCodeTest::writeDataPageStats);
        w.writeFieldStop();
        w.writeStructEnd();
        return bytes.toByteArray();
    }

    private static void writeDataPageStats(CompactProtocolWriter w, Integer encoding) throws IOException {
        w.writeStructBegin();
        w.writeI32Field((short) 1, PageType.DATA_PAGE.value());
        w.writeI32Field((short) 2, encoding);
        w.writeI32Field((short) 3, 1);
        w.writeFieldStop();
        w.writeStructEnd();
    }

    private static byte[] dataPageWire(int encoding, int definitionLevelEncoding, int repetitionLevelEncoding)
            throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        CompactProtocolWriter w = new CompactProtocolWriter(bytes);
        beginPageHeader(w, PageType.DATA_PAGE, (short) 5);
        w.writeStructBegin();
        w.writeI32Field((short) 1, 4);
        w.writeI32Field((short) 2, encoding);
        w.writeI32Field((short) 3, definitionLevelEncoding);
        w.writeI32Field((short) 4, repetitionLevelEncoding);
        endStruct(w);
        endStruct(w);
        return bytes.toByteArray();
    }

    private static byte[] dataPageV2Wire(int encoding) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        CompactProtocolWriter w = new CompactProtocolWriter(bytes);
        beginPageHeader(w, PageType.DATA_PAGE_V2, (short) 8);
        w.writeStructBegin();
        w.writeI32Field((short) 1, 4);
        w.writeI32Field((short) 2, 0);
        w.writeI32Field((short) 3, 4);
        w.writeI32Field((short) 4, encoding);
        w.writeI32Field((short) 5, 0);
        w.writeI32Field((short) 6, 0);
        endStruct(w);
        endStruct(w);
        return bytes.toByteArray();
    }

    private static byte[] dictionaryPageWire(int encoding) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        CompactProtocolWriter w = new CompactProtocolWriter(bytes);
        beginPageHeader(w, PageType.DICTIONARY_PAGE, (short) 7);
        w.writeStructBegin();
        w.writeI32Field((short) 1, 4);
        w.writeI32Field((short) 2, encoding);
        endStruct(w);
        endStruct(w);
        return bytes.toByteArray();
    }

    /** Opens a PageHeader struct of {@code type} and its sub-header field, ready for the sub-header struct. */
    private static void beginPageHeader(CompactProtocolWriter w, PageType type, short subHeaderField)
            throws IOException {
        w.writeStructBegin();
        w.writeI32Field((short) 1, type.value());
        w.writeI32Field((short) 2, 32);
        w.writeI32Field((short) 3, 32);
        w.writeFieldBegin(subHeaderField, CompactType.STRUCT);
    }

    private static void endStruct(CompactProtocolWriter w) throws IOException {
        w.writeFieldStop();
        w.writeStructEnd();
    }

    private static CompactProtocolReader reader(byte[] wire) {
        return new CompactProtocolReader(new ByteArrayInputStream(wire));
    }
}
