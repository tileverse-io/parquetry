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
package io.tileverse.parquetry.internal.read.page;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.data.Compression;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.DataPageHeader;
import io.tileverse.parquetry.format.DataPageHeaderV2;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.PageHeader;
import io.tileverse.parquetry.format.PageType;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaBuilder;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Decoder conformance test: for each fixture file, walks every column chunk page by page, decompresses via
 * {@link Compression}, dispatches via {@link DecoderFactory}, and asserts that the total decoded value count matches
 * the column's {@code numValues} metadata.
 *
 * <p>Scope: REQUIRED, non-nested scalar columns only. Optional or repeated columns are skipped with an explicit
 * {@link Assumptions#abort} message, because handling them correctly requires rep/def level decoding and Dremel record
 * assembly.
 *
 * <p>DataPage V1 level stream handling: per the Parquet spec, when max_rep=0 and max_def=0 (REQUIRED columns) the level
 * streams are completely absent - no prefix bytes, no data. The decompressed page payload starts directly with the
 * encoded values.
 *
 * <p>DataPage V2: level byte lengths are carried in the page header's {@code repetitionLevelsByteLength} and
 * {@code definitionLevelsByteLength} fields. Those uncompressed bytes precede the optionally-compressed value portion.
 */
class DecoderFactoryConformanceTest {

    @ParameterizedTest(name = "{0}")
    @MethodSource("fixtures")
    void decodesAllValuesPerColumn(String name, TestFixtureRef fix, @TempDir Path tmp) throws Exception {
        Path file = fix.generate(tmp);
        if (file == null) {
            // Fixture explicitly skipped (e.g. BYTE_STREAM_SPLIT not configurable).
            return;
        }

        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {

            FileMetaData footer = ParquetFormat.readFooter(source);
            ParquetSchema schema = SchemaBuilder.build(footer.schema());

            for (RowGroup rg : footer.rowGroups()) {
                int colIndex = 0;
                for (ColumnChunk col : rg.columns()) {
                    ColumnMetaData meta = col.metaData().orElseThrow();
                    String colName = String.join(".", meta.pathInSchema());

                    SchemaNode field = findLeafField(schema, meta.pathInSchema());
                    if (field == null) {
                        // Nested column path not directly resolvable as a simple leaf; skip.
                        Assumptions.abort("Cannot resolve field for path " + colName + " in " + name
                                + " - skipped (scalar-column scope)");
                    }

                    if (field.repetition() != Repetition.REQUIRED) {
                        // Non-required columns carry def-level prefixes that require Dremel assembly.
                        Assumptions.abort("Column " + colName + " is not REQUIRED in " + name
                                + " - skipped (scalar-column scope)");
                    }

                    PrimitiveKind kind = primitiveKindOf(meta.type());
                    OptionalInt typeLength = OptionalInt.empty();
                    if (field instanceof SchemaNode.Primitive prim) {
                        typeLength = prim.typeLength();
                    }

                    List<Object> values = decodeAllValues(source, meta, kind, typeLength, name + "/" + colName);

                    assertThat(values)
                            .as("column %s in %s (row-group %d)", colName, name, colIndex)
                            .hasSize((int) meta.numValues());
                    colIndex++;
                }
            }
        }
    }

    /**
     * Walks all pages in a column chunk, decompressing and decoding each data page.
     *
     * <p>Reads the entire column chunk byte range into memory, then iterates page headers and page payloads using the
     * running buffer position.
     */
    private static List<Object> decodeAllValues(
            ByteRangeSource source, ColumnMetaData meta, PrimitiveKind kind, OptionalInt typeLength, String debugLabel)
            throws Exception {

        // Determine the start of the page section. If a dictionary page exists its offset is
        // always earlier than the first data page.
        long chunkStart = meta.dictionaryPageOffset().isPresent()
                ? meta.dictionaryPageOffset().getAsLong()
                : meta.dataPageOffset();
        long chunkEnd = chunkStart + meta.totalCompressedSize();
        int chunkLen = (int) (chunkEnd - chunkStart);

        byte[] chunkArray = new byte[chunkLen];
        source.readFully(chunkStart, MemorySegment.ofArray(chunkArray));
        ByteBuffer chunkBuf = ByteBuffer.wrap(chunkArray);
        chunkBuf.position(chunkLen);
        chunkBuf.flip();

        Compression codec = Compression.forWireCodec(meta.codec());
        List<Object> values = new ArrayList<>((int) meta.numValues());
        Optional<Dictionary<?>> currentDict = Optional.empty();

        // Convert chunk bytes to an InputStream-backed stream for page header parsing.
        // We keep a separate offset tracker so we can extract compressed page bytes after
        // each header.
        int pos = 0;
        try (Arena arena = Arena.ofConfined()) {
            while (pos < chunkBuf.limit()) {
                // Wrap remaining bytes as an InputStream for the Thrift compact reader.
                byte[] chunkBytes = chunkBuf.array();
                int arrayOffset = chunkBuf.arrayOffset() + pos;
                int remaining = chunkBuf.limit() - pos;

                InputStream headerStream = new ByteArrayInputStream(chunkBytes, arrayOffset, remaining);
                int beforeHeaderRead = headerStream.available();
                PageHeader header = ParquetFormat.readPageHeader(headerStream);
                int headerLen = beforeHeaderRead - headerStream.available();
                pos += headerLen;

                int compressedSize = header.compressedPageSize();
                int uncompressedSize = header.uncompressedPageSize();

                // Slice the compressed payload from the chunk buffer.
                ByteBuffer compressedPayload =
                        ByteBuffer.wrap(chunkBytes, chunkBuf.arrayOffset() + pos, compressedSize);
                pos += compressedSize;

                PageType type = header.type();
                if (type == PageType.DICTIONARY_PAGE) {
                    // Decompress the full dictionary page.
                    MemorySegment dictPageSeg = arena.allocate(uncompressedSize);
                    codec.decompress(MemorySegment.ofBuffer(compressedPayload), dictPageSeg);
                    ByteBuffer dictPage = dictPageSeg.asByteBuffer().asReadOnlyBuffer();
                    dictPage.position(0);
                    dictPage.limit(uncompressedSize);
                    int dictValueCount = header.dictionaryPageHeader()
                            .orElseThrow(
                                    () -> new IllegalStateException("DICTIONARY_PAGE missing header for " + debugLabel))
                            .numValues();
                    Dictionary<?> dict =
                            DictionaryDecoder.read(MemorySegment.ofBuffer(dictPage), kind, dictValueCount, typeLength);
                    currentDict = Optional.of(dict);

                } else if (type == PageType.DATA_PAGE) {
                    // V1 data page.
                    DataPageHeader dpHeader = header.dataPageHeader()
                            .orElseThrow(() -> new IllegalStateException("DATA_PAGE missing header for " + debugLabel));
                    int numValues = dpHeader.numValues();
                    Encoding encoding = dpHeader.encoding();

                    // Decompress the full page.
                    MemorySegment pageSeg = arena.allocate(uncompressedSize);
                    codec.decompress(MemorySegment.ofBuffer(compressedPayload), pageSeg);
                    ByteBuffer pageBuf = pageSeg.asByteBuffer().asReadOnlyBuffer();
                    pageBuf.position(0);
                    pageBuf.limit(uncompressedSize);

                    // Parquet spec: for REQUIRED columns (max_rep=0, max_def=0) the level streams
                    // are completely absent - no bytes, no length prefix. The value bytes start at
                    // offset 0 of the decompressed page payload.
                    // We only get here for REQUIRED columns (non-REQUIRED are skipped above), so
                    // we pass the full decompressed payload directly to the value decoder.
                    ByteBuffer valuePayload = pageBuf.slice();
                    decodePageValues(encoding, kind, typeLength, currentDict, valuePayload, numValues, values);

                } else if (type == PageType.DATA_PAGE_V2) {
                    // V2 data page: level bytes are uncompressed and precede the value section.
                    DataPageHeaderV2 dpv2Header = header.dataPageHeaderV2()
                            .orElseThrow(
                                    () -> new IllegalStateException("DATA_PAGE_V2 missing header for " + debugLabel));
                    int numValues = dpv2Header.numValues();
                    Encoding encoding = dpv2Header.encoding();
                    int repLevelBytes = dpv2Header.repetitionLevelsByteLength();
                    int defLevelBytes = dpv2Header.definitionLevelsByteLength();

                    // Decompress the whole page payload, then skip the level sections.
                    MemorySegment pageSeg = arena.allocate(uncompressedSize);
                    codec.decompress(MemorySegment.ofBuffer(compressedPayload), pageSeg);
                    ByteBuffer pageBuf = pageSeg.asByteBuffer().asReadOnlyBuffer();
                    pageBuf.position(0);
                    pageBuf.limit(uncompressedSize);
                    pageBuf.position(pageBuf.position() + repLevelBytes + defLevelBytes);
                    ByteBuffer valuePayload = pageBuf.slice();
                    decodePageValues(encoding, kind, typeLength, currentDict, valuePayload, numValues, values);
                }
                // INDEX_PAGE and other types are skipped silently.
            }
        }
        return values;
    }

    /**
     * Decodes {@code numValues} values from {@code valuePayload} using the given encoding and appends them to
     * {@code out}.
     */
    private static void decodePageValues(
            Encoding encoding,
            PrimitiveKind kind,
            OptionalInt typeLength,
            Optional<Dictionary<?>> dictionary,
            ByteBuffer valuePayload,
            int numValues,
            List<Object> out) {

        PageDecoder<?> decoder = DecoderFactory.decoderFor(encoding, kind, typeLength, dictionary);
        decoder.load(MemorySegment.ofBuffer(valuePayload), numValues);
        for (int i = 0; i < numValues; i++) {
            out.add(decoder.next());
        }
    }

    /**
     * Resolves the {@link Field} at the given path within the schema.
     *
     * <p>Uses a depth-first walk rather than {@link ParquetSchema#find} because the conformance fixtures have flat
     * schemas; nested paths still work because we traverse groups.
     */
    private static SchemaNode findLeafField(ParquetSchema schema, List<String> pathParts) {
        SchemaNode current = schema.root();
        for (String part : pathParts) {
            if (!(current instanceof SchemaNode.Group g)) {
                return null;
            }
            SchemaNode next = null;
            for (SchemaNode child : g.children()) {
                if (child.name().equals(part)) {
                    next = child;
                    break;
                }
            }
            if (next == null) {
                return null;
            }
            current = next;
        }
        return current;
    }

    private static PrimitiveKind primitiveKindOf(PhysicalType type) {
        return switch (type) {
            case BOOLEAN -> PrimitiveKind.BOOLEAN;
            case INT32 -> PrimitiveKind.INT32;
            case INT64 -> PrimitiveKind.INT64;
            case INT96 -> PrimitiveKind.INT96;
            case FLOAT -> PrimitiveKind.FLOAT;
            case DOUBLE -> PrimitiveKind.DOUBLE;
            case BYTE_ARRAY -> PrimitiveKind.BYTE_ARRAY;
            case FIXED_LEN_BYTE_ARRAY -> PrimitiveKind.FIXED_LEN_BYTE_ARRAY;
        };
    }

    /** Functional interface matching the shape of the fixture generators in {@link TestFixtures}. */
    @FunctionalInterface
    interface TestFixtureRef {
        Path generate(Path tmpDir) throws Exception;
    }

    static Stream<Arguments> fixtures() {
        return Stream.of(
                // flatIntSnappy: 5 REQUIRED scalar columns (int, long, float, double, bool), Snappy, 100 rows.
                // All columns are REQUIRED so no def/rep level noise.
                Arguments.of("flat-int-snappy", (TestFixtureRef) DecoderFactoryConformanceTest::writeFlatIntSnappy),

                // multiRowGroup: REQUIRED int column, 3+ row groups. Exercises multi-row-group iteration.
                Arguments.of("multi-rowgroup", (TestFixtureRef) DecoderFactoryConformanceTest::writeMultiRowGroup),

                // deltaEncoded: REQUIRED int column with PARQUET_2_0 writer -> DELTA_BINARY_PACKED.
                Arguments.of("delta-encoded", (TestFixtureRef) DecoderFactoryConformanceTest::writeDeltaEncoded),

                // nestedList, nestedMap: non-REQUIRED columns - skipped via Assumptions.abort in the test body.
                // Listed to document the expected skip behaviour.
                Arguments.of("nested-list", (TestFixtureRef) DecoderFactoryConformanceTest::writeNestedList),
                Arguments.of("nested-map", (TestFixtureRef) DecoderFactoryConformanceTest::writeNestedMap));
    }

    // --- Inline fixture generators (mirrors parquetry-format TestFiles and parquetry-core TestFixtures) ---

    private static Path writeFlatIntSnappy(Path tmpDir) throws Exception {
        org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse("""
                {"type":"record","name":"FlatInt","fields":[
                  {"name":"col_int","type":"int"},
                  {"name":"col_long","type":"long"},
                  {"name":"col_float","type":"float"},
                  {"name":"col_double","type":"double"},
                  {"name":"col_bool","type":"boolean"}
                ]}""");
        Path out = tmpDir.resolve("flat-int-snappy.parquet");
        try (org.apache.parquet.hadoop.ParquetWriter<org.apache.avro.generic.GenericData.Record> writer =
                org.apache.parquet.avro.AvroParquetWriter.<org.apache.avro.generic.GenericData.Record>builder(
                                new org.apache.parquet.io.LocalOutputFile(out))
                        .withSchema(schema)
                        .withCompressionCodec(org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY)
                        .build()) {
            for (int i = 0; i < 100; i++) {
                org.apache.avro.generic.GenericData.Record rec = new org.apache.avro.generic.GenericData.Record(schema);
                rec.put("col_int", i);
                rec.put("col_long", (long) i * 1000);
                rec.put("col_float", (float) i * 0.5f);
                rec.put("col_double", (double) i * 0.25);
                rec.put("col_bool", i % 2 == 0);
                writer.write(rec);
            }
        }
        return out;
    }

    private static Path writeMultiRowGroup(Path tmpDir) throws Exception {
        org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse("""
                {"type":"record","name":"MultiRG","fields":[
                  {"name":"id","type":"int"}
                ]}""");
        Path out = tmpDir.resolve("multi-rowgroup.parquet");
        try (org.apache.parquet.hadoop.ParquetWriter<org.apache.avro.generic.GenericData.Record> writer =
                org.apache.parquet.avro.AvroParquetWriter.<org.apache.avro.generic.GenericData.Record>builder(
                                new org.apache.parquet.io.LocalOutputFile(out))
                        .withSchema(schema)
                        .withCompressionCodec(org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY)
                        .withRowGroupSize(4096L)
                        .build()) {
            for (int i = 0; i < 3000; i++) {
                org.apache.avro.generic.GenericData.Record rec = new org.apache.avro.generic.GenericData.Record(schema);
                rec.put("id", i);
                writer.write(rec);
            }
        }
        return out;
    }

    private static Path writeDeltaEncoded(Path tmpDir) throws Exception {
        org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse("""
                {"type":"record","name":"DeltaEncoded","fields":[
                  {"name":"seq","type":"int"}
                ]}""");
        Path out = tmpDir.resolve("delta-encoded.parquet");
        try (org.apache.parquet.hadoop.ParquetWriter<org.apache.avro.generic.GenericData.Record> writer =
                org.apache.parquet.avro.AvroParquetWriter.<org.apache.avro.generic.GenericData.Record>builder(
                                new org.apache.parquet.io.LocalOutputFile(out))
                        .withSchema(schema)
                        .withCompressionCodec(org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY)
                        .withWriterVersion(org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0)
                        .build()) {
            for (int i = 0; i < 1000; i++) {
                org.apache.avro.generic.GenericData.Record rec = new org.apache.avro.generic.GenericData.Record(schema);
                rec.put("seq", i);
                writer.write(rec);
            }
        }
        return out;
    }

    private static Path writeNestedList(Path tmpDir) throws Exception {
        org.apache.avro.Schema arraySchema =
                org.apache.avro.Schema.createArray(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT));
        org.apache.avro.Schema schema = org.apache.avro.Schema.createRecord("NestedList", null, null, false);
        schema.setFields(java.util.List.of(new org.apache.avro.Schema.Field("col_list", arraySchema, null, null)));
        Path out = tmpDir.resolve("nested-list.parquet");
        try (org.apache.parquet.hadoop.ParquetWriter<org.apache.avro.generic.GenericData.Record> writer =
                org.apache.parquet.avro.AvroParquetWriter.<org.apache.avro.generic.GenericData.Record>builder(
                                new org.apache.parquet.io.LocalOutputFile(out))
                        .withSchema(schema)
                        .withCompressionCodec(org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY)
                        .build()) {
            for (int i = 0; i < 10; i++) {
                org.apache.avro.generic.GenericData.Record rec = new org.apache.avro.generic.GenericData.Record(schema);
                rec.put("col_list", java.util.List.of(i, i + 1));
                writer.write(rec);
            }
        }
        return out;
    }

    private static Path writeNestedMap(Path tmpDir) throws Exception {
        org.apache.avro.Schema mapSchema =
                org.apache.avro.Schema.createMap(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT));
        org.apache.avro.Schema schema = org.apache.avro.Schema.createRecord("NestedMap", null, null, false);
        schema.setFields(java.util.List.of(new org.apache.avro.Schema.Field("col_map", mapSchema, null, null)));
        Path out = tmpDir.resolve("nested-map.parquet");
        try (org.apache.parquet.hadoop.ParquetWriter<org.apache.avro.generic.GenericData.Record> writer =
                org.apache.parquet.avro.AvroParquetWriter.<org.apache.avro.generic.GenericData.Record>builder(
                                new org.apache.parquet.io.LocalOutputFile(out))
                        .withSchema(schema)
                        .withCompressionCodec(org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY)
                        .build()) {
            for (int i = 0; i < 10; i++) {
                org.apache.avro.generic.GenericData.Record rec = new org.apache.avro.generic.GenericData.Record(schema);
                java.util.Map<String, Integer> map = new java.util.HashMap<>();
                map.put("key_a", i);
                rec.put("col_map", map);
                writer.write(rec);
            }
        }
        return out;
    }
}
