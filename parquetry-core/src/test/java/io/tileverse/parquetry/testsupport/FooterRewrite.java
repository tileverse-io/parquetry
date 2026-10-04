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
package io.tileverse.parquetry.testsupport;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;

import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.GeospatialStatistics;
import io.tileverse.parquetry.format.KeyValue;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.format.Statistics;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ColumnPath;

import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

/**
 * Copies a Parquet file with an edited footer, keeping the bytes ahead of the footer unchanged. Tests use it to give a
 * file footer metadata written by other writers and never by parquetry, such as a geometry box wrapping the
 * antimeridian.
 */
public final class FooterRewrite {

    private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.US_ASCII);
    private static final int TRAILER_BYTES = Integer.BYTES + MAGIC.length;
    private static final String GEO_KEY = "geo";
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private FooterRewrite() {}

    /** Writes {@code target}: the data of {@code source} followed by the footer of {@code source} changed by edit. */
    public static Path rewrite(Path source, Path target, UnaryOperator<FileMetaData> edit) throws IOException {
        byte[] bytes = Files.readAllBytes(source);
        FileMetaData footer = readFooter(source);
        byte[] editedFooter = ParquetFormat.toBytes(edit.apply(footer));
        try (OutputStream out = Files.newOutputStream(target)) {
            out.write(bytes, 0, footerOffset(bytes));
            out.write(editedFooter);
            out.write(littleEndian(editedFooter.length));
            out.write(MAGIC);
        }
        return target;
    }

    private static FileMetaData readFooter(Path file) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            return ParquetFormat.readFooter(source);
        }
    }

    private static int footerOffset(byte[] file) {
        int lengthOffset = file.length - TRAILER_BYTES;
        ByteBuffer trailer = ByteBuffer.wrap(file).order(ByteOrder.LITTLE_ENDIAN);
        return lengthOffset - trailer.getInt(lengthOffset);
    }

    private static byte[] littleEndian(int value) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(value);
        return buffer.array();
    }

    /** An edit setting the geospatial statistics bbox of each column chunk of the top-level {@code column}. */
    public static UnaryOperator<FileMetaData> geospatialBbox(String column, BoundingBox bbox) {
        return footer -> withRowGroups(footer, rowGroupsWithBbox(footer.rowGroups(), column, bbox));
    }

    private static List<RowGroup> rowGroupsWithBbox(List<RowGroup> rowGroups, String column, BoundingBox bbox) {
        List<RowGroup> edited = new ArrayList<>(rowGroups.size());
        for (RowGroup rowGroup : rowGroups) {
            edited.add(withColumns(rowGroup, chunksWithBbox(rowGroup.columns(), column, bbox)));
        }
        return edited;
    }

    private static List<ColumnChunk> chunksWithBbox(List<ColumnChunk> chunks, String column, BoundingBox bbox) {
        List<ColumnChunk> edited = new ArrayList<>(chunks.size());
        for (ColumnChunk chunk : chunks) {
            ColumnMetaData metaData = chunk.metaData().orElseThrow();
            if (metaData.pathInSchema().equals(List.of(column))) {
                edited.add(withMetaData(chunk, withBbox(metaData, bbox)));
            } else {
                edited.add(chunk);
            }
        }
        return edited;
    }

    private static ColumnMetaData withBbox(ColumnMetaData metaData, BoundingBox bbox) {
        Optional<List<Integer>> types = metaData.geospatialStatistics().flatMap(GeospatialStatistics::geospatialTypes);
        GeospatialStatistics statistics = new GeospatialStatistics(Optional.of(bbox), types);
        return new ColumnMetaData(
                metaData.type(),
                metaData.encodings(),
                metaData.pathInSchema(),
                metaData.codec(),
                metaData.numValues(),
                metaData.totalUncompressedSize(),
                metaData.totalCompressedSize(),
                metaData.keyValueMetadata(),
                metaData.dataPageOffset(),
                metaData.indexPageOffset(),
                metaData.dictionaryPageOffset(),
                metaData.statistics(),
                metaData.encodingStats(),
                metaData.bloomFilterOffset(),
                metaData.bloomFilterLength(),
                metaData.sizeStatistics(),
                Optional.of(statistics));
    }

    private static ColumnChunk withMetaData(ColumnChunk chunk, ColumnMetaData metaData) {
        return new ColumnChunk(
                chunk.filePath(),
                chunk.fileOffset(),
                Optional.of(metaData),
                chunk.offsetIndexOffset(),
                chunk.offsetIndexLength(),
                chunk.columnIndexOffset(),
                chunk.columnIndexLength(),
                chunk.cryptoMetadata(),
                chunk.encryptedColumnMetadata());
    }

    private static RowGroup withColumns(RowGroup rowGroup, List<ColumnChunk> columns) {
        return new RowGroup(
                columns,
                rowGroup.totalByteSize(),
                rowGroup.numRows(),
                rowGroup.sortingColumns(),
                rowGroup.fileOffset(),
                rowGroup.totalCompressedSize(),
                rowGroup.ordinal());
    }

    private static FileMetaData withRowGroups(FileMetaData footer, List<RowGroup> rowGroups) {
        return new FileMetaData(
                footer.version(),
                footer.schema(),
                footer.numRows(),
                rowGroups,
                footer.keyValueMetadata(),
                footer.createdBy(),
                footer.columnOrders(),
                footer.encryptionAlgorithm(),
                footer.footerSigningKeyMetadata());
    }

    /** An edit setting the 2D bbox declared for {@code column} by the GeoParquet {@code "geo"} metadata document. */
    public static UnaryOperator<FileMetaData> geoMetadataBbox(String column, BoundingBox bbox) {
        return footer -> withKeyValues(footer, keyValuesWithGeoBbox(footer.keyValueMetadata(), column, bbox));
    }

    private static List<KeyValue> keyValuesWithGeoBbox(List<KeyValue> keyValues, String column, BoundingBox bbox) {
        List<KeyValue> edited = new ArrayList<>(keyValues.size());
        for (KeyValue keyValue : keyValues) {
            if (GEO_KEY.equals(keyValue.key())) {
                String geo = keyValue.value().orElseThrow();
                edited.add(new KeyValue(GEO_KEY, Optional.of(withColumnBbox(geo, column, bbox))));
            } else {
                edited.add(keyValue);
            }
        }
        return edited;
    }

    private static String withColumnBbox(String geo, String column, BoundingBox bbox) {
        ObjectNode document = (ObjectNode) JSON.readTree(geo);
        ObjectNode columnNode = (ObjectNode) document.get("columns").get(column);
        ArrayNode corners = columnNode.putArray("bbox");
        corners.add(bbox.xmin());
        corners.add(bbox.ymin());
        corners.add(bbox.xmax());
        corners.add(bbox.ymax());
        return JSON.writeValueAsString(document);
    }

    private static FileMetaData withKeyValues(FileMetaData footer, List<KeyValue> keyValues) {
        return new FileMetaData(
                footer.version(),
                footer.schema(),
                footer.numRows(),
                footer.rowGroups(),
                keyValues,
                footer.createdBy(),
                footer.columnOrders(),
                footer.encryptionAlgorithm(),
                footer.footerSigningKeyMetadata());
    }

    /** An edit setting the PLAIN-encoded maximum statistic of {@code column} in the row group at {@code rowGroup}. */
    public static UnaryOperator<FileMetaData> statisticsMax(ColumnPath column, int rowGroup, MemorySegment max) {
        return footer -> withRowGroups(footer, rowGroupsWithMax(footer.rowGroups(), column, rowGroup, max));
    }

    private static List<RowGroup> rowGroupsWithMax(
            List<RowGroup> rowGroups, ColumnPath column, int rowGroup, MemorySegment max) {
        List<RowGroup> edited = new ArrayList<>(rowGroups);
        RowGroup target = rowGroups.get(rowGroup);
        edited.set(rowGroup, withColumns(target, chunksWithMax(target.columns(), column, max)));
        return edited;
    }

    private static List<ColumnChunk> chunksWithMax(List<ColumnChunk> chunks, ColumnPath column, MemorySegment max) {
        List<ColumnChunk> edited = new ArrayList<>(chunks.size());
        for (ColumnChunk chunk : chunks) {
            ColumnMetaData metaData = chunk.metaData().orElseThrow();
            if (ColumnPath.of(metaData.pathInSchema()).equals(column)) {
                edited.add(withMetaData(chunk, withMax(metaData, max)));
            } else {
                edited.add(chunk);
            }
        }
        return edited;
    }

    private static ColumnMetaData withMax(ColumnMetaData metaData, MemorySegment max) {
        Statistics stats = metaData.statistics().orElseThrow();
        Statistics edited = new Statistics(
                stats.max(),
                stats.min(),
                stats.nullCount(),
                stats.distinctCount(),
                max,
                stats.minValue(),
                stats.isMaxValueExact(),
                stats.isMinValueExact());
        return new ColumnMetaData(
                metaData.type(),
                metaData.encodings(),
                metaData.pathInSchema(),
                metaData.codec(),
                metaData.numValues(),
                metaData.totalUncompressedSize(),
                metaData.totalCompressedSize(),
                metaData.keyValueMetadata(),
                metaData.dataPageOffset(),
                metaData.indexPageOffset(),
                metaData.dictionaryPageOffset(),
                Optional.of(edited),
                metaData.encodingStats(),
                metaData.bloomFilterOffset(),
                metaData.bloomFilterLength(),
                metaData.sizeStatistics(),
                metaData.geospatialStatistics());
    }
}
