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
package io.tileverse.parquetry.cli.cmd;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import io.tileverse.parquetry.cli.DstStorageOptions;
import io.tileverse.parquetry.cli.GlobalOptions;
import io.tileverse.parquetry.cli.StorageOptions;
import io.tileverse.parquetry.cli.UriResolver;
import io.tileverse.parquetry.cli.expr.FilterParser;
import io.tileverse.parquetry.cli.render.Projections;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.dataset.ParquetSource;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Query;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoColumn;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;
import io.tileverse.parquetry.schema.geo.geoparquet.GeometryColumns;
import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem;
import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystems;

import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "cp", description = "Read src, apply projection/filter, write a new Parquet file at dst.")
public final class CpCmd implements Callable<Integer> {

    @Parameters(index = "0", paramLabel = "<src>", description = "Source Parquet file path or URI.")
    private String src;

    @Parameters(
            index = "1",
            paramLabel = "<dst>",
            description = "Destination Parquet file path or URI (local or cloud).")
    private String dst;

    @Option(
            names = {"-f", "--overwrite"},
            description = "Overwrite an existing destination.")
    private boolean overwrite;

    @Mixin
    private GlobalOptions options;

    @ArgGroup(validate = false, heading = StorageOptions.HEADING)
    private StorageOptions storage = new StorageOptions();

    @ArgGroup(validate = false, heading = DstStorageOptions.HEADING)
    private DstStorageOptions dstStorage = new DstStorageOptions();

    @ArgGroup(exclusive = true, multiplicity = "0..1", heading = "%nRow group sizing:%n")
    private RowGroupSizing rowGroupSizing;

    @Override
    public Integer call() throws Exception {
        String sourceFileName = sourceFileName();
        refuseWritingOntoSource(sourceFileName);
        try (UriResolver.OpenFile open = UriResolver.open(src, storage.toProperties())) {
            ParquetSource source = ParquetSource.open(open.source());
            ParquetSchema sourceSchema = source.schema();
            Projections.Resolved projection = Projections.resolve(options.columns, sourceSchema);
            ParquetSchema writeSchema = buildWriteSchema(sourceSchema, projection);
            CpWritable.requireWritable(writeSchema);
            Set<ColumnPath> geometryColumns = GeometryColumns.resolve(sourceSchema, source.keyValueMetadata());
            Predicate predicate = buildPredicate(sourceSchema, geometryColumns);
            writeAll(source, writeSchema, projection, predicate, sourceFileName, source.keyValueMetadata());
        }
        return 0;
    }

    private ParquetSchema buildWriteSchema(ParquetSchema sourceSchema, Projections.Resolved projection) {
        if (projection.projection() == Projection.ALL) {
            return sourceSchema;
        }
        return sourceSchema.project(Set.copyOf(projection.keptLeaves()));
    }

    private Predicate buildPredicate(ParquetSchema schema, Set<ColumnPath> geometryColumns) {
        if (options.filter == null) {
            return Predicate.ALWAYS_TRUE;
        }
        return FilterParser.parse(options.filter, schema, geometryColumns);
    }

    private String sourceFileName() {
        URI sourceUri = UriResolver.normalizeToUri(src);
        if ("file".equals(sourceUri.getScheme())) {
            return Path.of(sourceUri).getFileName().toString();
        }
        String path = sourceUri.getPath();
        if (path == null || path.isEmpty() || path.endsWith("/")) {
            throw new IllegalArgumentException("source must point to a file, not a directory prefix: " + src);
        }
        return path.substring(path.lastIndexOf('/') + 1);
    }

    private void refuseWritingOntoSource(String sourceFileName) {
        URI sourceUri = UriResolver.normalizeToUri(src);
        URI destinationUri = UriResolver.resolvedUri(dst, sourceFileName);
        if (sourceUri.equals(destinationUri)) {
            throw new IllegalArgumentException("cp refuses to write onto the source file: " + dst);
        }
    }

    private void writeAll(
            ParquetSource source,
            ParquetSchema writeSchema,
            Projections.Resolved projection,
            Predicate predicate,
            String sourceFileName,
            Map<String, String> sourceKeyValue)
            throws IOException {
        WriteOptions.RowGroupSize rowGroupSize = resolveRowGroupSize();
        WriteOptions writeOptions =
                buildWriteOptions(writeSchema, writerTempDir(sourceFileName), sourceKeyValue, rowGroupSize);
        long limit = options.limit == null ? Long.MAX_VALUE : options.limit;
        try (UriResolver.OpenSink sink =
                UriResolver.openForWrite(dst, sourceFileName, overwrite, dstStorage.toProperties())) {
            writeAndFinalize(source, writeSchema, projection, predicate, writeOptions, rowGroupSize, limit, sink);
            sink.commit();
        }
    }

    /**
     * Streams the matching rows into the writer as columnar batches and finalizes the file. The writer is closed (which
     * writes the footer) before {@code writeAll} commits the destination. A failure anywhere in here leaves the sink
     * uncommitted and the try-with-resources aborts it; a failed copy never leaves a visible footerless destination.
     *
     * <p>The read applies the predicate and the row limit exactly: each batch it emits already holds only the rows to
     * write (narrowed to the projection, windowed by the limit), in the assembled shape the writer consumes. Struct,
     * list, map and Variant columns reproduce faithfully because the batch passes straight to
     * {@link ParquetFileWriter#writeBatch}.
     */
    private void writeAndFinalize(
            ParquetSource source,
            ParquetSchema writeSchema,
            Projections.Resolved projection,
            Predicate predicate,
            WriteOptions writeOptions,
            WriteOptions.RowGroupSize rowGroupSize,
            long limit,
            UriResolver.OpenSink sink) {
        Query query = buildQuery(predicate, projection, limit);
        try (ParquetFileWriter writer = ParquetFileWriter.create(sink.out(), writeSchema, writeOptions);
                Stream<ParquetRecordBatch> batches = source.readBatches(query, pumpReadOptions(rowGroupSize))) {
            batches.forEach(batch -> {
                try (batch) {
                    writer.writeBatch(batch);
                }
            });
        }
    }

    /**
     * Builds the read query. {@code Long.MAX_VALUE} marks an absent {@code --limit} and reads the identity window; a
     * present limit bounds the read to that many matching rows.
     */
    private static Query buildQuery(Predicate predicate, Projections.Resolved projection, long limit) {
        if (limit == Long.MAX_VALUE) {
            return Query.of(predicate, projection.projection());
        }
        return Query.builder(predicate, projection.projection()).limit(limit).build();
    }

    /**
     * Read options for the pump. A row-count row-group target caps each emitted batch to that target (bounded by
     * {@link #MAX_COPY_BATCH_ROWS}), which lets the writer seal a row group at exactly the requested count; the writer
     * seals a row group only at a batch boundary. A byte target or the adaptive default leaves the natural batch size,
     * and the writer re-chunks row groups by byte budget across batches.
     */
    private static ReadOptions pumpReadOptions(WriteOptions.RowGroupSize rowGroupSize) {
        if (rowGroupSize instanceof WriteOptions.RowGroupSize.Rows(long rows)) {
            int batchRows = (int) Math.min(rows, MAX_COPY_BATCH_ROWS);
            return ReadOptions.builder().batchSize(batchRows).build();
        }
        return ReadOptions.DEFAULTS;
    }

    /**
     * Where the writer spills working files. A local destination uses its own directory; a remote destination has no
     * local parent, and the system temporary directory is used instead.
     */
    private Path writerTempDir(String sourceFileName) {
        URI destinationUri = UriResolver.resolvedUri(dst, sourceFileName);
        if ("file".equals(destinationUri.getScheme())) {
            return Path.of(destinationUri).getParent();
        }
        return Path.of(System.getProperty("java.io.tmpdir"));
    }

    private static WriteOptions buildWriteOptions(
            ParquetSchema writeSchema,
            Path tempDir,
            Map<String, String> sourceKeyValue,
            WriteOptions.RowGroupSize rowGroupSize) {
        WriteOptions.Builder builder = WriteOptions.builder().tempDir(tempDir);
        if (rowGroupSize != null) {
            builder.rowGroupSize(rowGroupSize);
        }
        for (ColumnPath leaf : writeSchema.leafColumns()) {
            SchemaNode node = writeSchema.find(leaf).orElseThrow();
            SchemaNode.Primitive prim = (SchemaNode.Primitive) node;
            if (prim.kind() == PrimitiveKind.FIXED_LEN_BYTE_ARRAY) {
                builder.encodingPolicy(prim.name(), WriteOptions.EncodingPolicy.FORCE_PLAIN);
            }
        }
        forwardOpaqueMetadata(builder, sourceKeyValue);
        collectGeometryColumns(builder, writeSchema, sourceKeyValue);
        return builder.build();
    }

    /**
     * Resolves the row-group sizing override from the CLI flags, or {@code null} to keep the writer's adaptive default.
     */
    private WriteOptions.RowGroupSize resolveRowGroupSize() {
        if (rowGroupSizing == null) {
            return null;
        }
        if (rowGroupSizing.rows != null) {
            return WriteOptions.RowGroupSize.rows(rowGroupSizing.rows);
        }
        return WriteOptions.RowGroupSize.bytes(rowGroupSizing.bytes);
    }

    private static long parseByteSize(String text) {
        Matcher matcher = BYTE_SIZE_PATTERN.matcher(text);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(
                    "invalid size '" + text + "'; use a byte count or a value like 256MB (units K, M, G are binary)");
        }
        long value = Long.parseLong(matcher.group(1));
        long multiplier = byteUnitMultiplier(matcher.group(2).toUpperCase(Locale.ROOT));
        return Math.multiplyExact(value, multiplier);
    }

    private static long byteUnitMultiplier(String unit) {
        return switch (unit) {
            case "", "B" -> 1L;
            case "K", "KB", "KIB" -> 1024L;
            case "M", "MB", "MIB" -> 1024L * 1024L;
            case "G", "GB", "GIB" -> 1024L * 1024L * 1024L;
            default ->
                throw new IllegalArgumentException("unsupported size unit '" + unit + "'; use K, M, or G (binary)");
        };
    }

    /** Maximum rows the copy appender buffers per batch; also bounds where a row-count target seals a row group. */
    private static final int MAX_COPY_BATCH_ROWS = 8192;

    private static final Pattern BYTE_SIZE_PATTERN = Pattern.compile("\\s*(\\d+)\\s*([A-Za-z]*)\\s*");

    /** Parses {@code --row-group-bytes} values like {@code 256MB} into an uncompressed byte count. */
    static final class ByteSizeConverter implements CommandLine.ITypeConverter<Long> {
        @Override
        public Long convert(String value) {
            return parseByteSize(value);
        }
    }

    /** Mutually-exclusive row-group sizing overrides; the writer's adaptive byte budget applies when unset. */
    static final class RowGroupSizing {

        @Option(
                names = "--row-group-rows",
                paramLabel = "<n>",
                description = "Seal each row group after N rows. Overrides the default byte-budget sizing.")
        Long rows;

        @Option(
                names = "--row-group-bytes",
                paramLabel = "<size>",
                converter = ByteSizeConverter.class,
                description = "Seal each row group near SIZE of uncompressed data, e.g. 256MB or 268435456. "
                        + "Overrides the default sizing. Units K, M, G are binary.")
        Long bytes;
    }

    /**
     * Forwards the source's file-level key-value metadata into the copy, minus the reserved GeoParquet {@code geo}
     * block, which the writer regenerates from the geometry columns. This preserves opaque metadata the CLI does not
     * interpret (pandas, custom application keys) across a copy.
     */
    private static void forwardOpaqueMetadata(WriteOptions.Builder builder, Map<String, String> sourceKeyValue) {
        Map<String, String> opaque = new LinkedHashMap<>(sourceKeyValue);
        opaque.remove(GEO_METADATA_KEY);
        if (!opaque.isEmpty()) {
            builder.keyValueMetadata(opaque);
        }
    }

    /**
     * Re-declares the source's geometry columns that survive the projection, letting the writer regenerate the
     * GeoParquet footer block for the output. The block must be regenerated rather than copied: its bounding box and
     * geometry types are derived from the rows actually written (which a filter or projection may narrow), and a
     * geometry column dropped by the projection must leave no dangling entry behind.
     */
    private static void collectGeometryColumns(
            WriteOptions.Builder builder, ParquetSchema writeSchema, Map<String, String> sourceKeyValue) {
        String geoJson = sourceKeyValue.get(GEO_METADATA_KEY);
        if (geoJson == null) {
            return;
        }
        GeoParquetMetadata geo = GeoParquetMetadata.parse(geoJson);
        for (Map.Entry<String, GeoColumn> column : geo.columns().entrySet()) {
            String columnName = column.getKey();
            if (!survivesProjection(writeSchema, columnName)) {
                continue;
            }
            // An absent CRS means the GeoParquet default (OGC:CRS84); declare it explicitly so the column is treated
            // as geometry.
            CoordinateReferenceSystem crs = column.getValue().crs().orElseGet(CoordinateReferenceSystems::ogcCrs84);
            builder.crs(columnName, crs);
        }
    }

    private static boolean survivesProjection(ParquetSchema writeSchema, String columnName) {
        ColumnPath path = ColumnPath.of(columnName.split("\\."));
        return writeSchema.find(path).isPresent();
    }

    /**
     * The GeoParquet file-level key-value metadata key; the writer owns and regenerates it from the geometry columns.
     */
    private static final String GEO_METADATA_KEY = "geo";
}
