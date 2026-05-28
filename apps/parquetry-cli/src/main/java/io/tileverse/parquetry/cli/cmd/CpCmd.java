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
package io.tileverse.parquetry.cli.cmd;

import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

import io.tileverse.parquetry.cli.GlobalOptions;
import io.tileverse.parquetry.cli.StorageOptions;
import io.tileverse.parquetry.cli.UriResolver;
import io.tileverse.parquetry.cli.expr.FilterParser;
import io.tileverse.parquetry.cli.render.Projections;
import io.tileverse.parquetry.cli.render.RecordToWriteRow;
import io.tileverse.parquetry.data.ParquetDataset;
import io.tileverse.parquetry.data.ParquetWriter;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoColumn;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;
import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem;
import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystems;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "cp", description = "Read src, apply projection/filter, write a new Parquet file at dst.")
public final class CpCmd implements Callable<Integer> {

    @Parameters(index = "0", paramLabel = "<src>", description = "Source Parquet file path or URI.")
    private String src;

    @Parameters(index = "1", paramLabel = "<dst>", description = "Destination Parquet file path (local).")
    private String dst;

    @Option(
            names = {"-f", "--overwrite"},
            description = "Overwrite an existing destination.")
    private boolean overwrite;

    @Mixin
    private GlobalOptions options;

    @Mixin
    private StorageOptions storage;

    @Override
    public Integer call() throws Exception {
        Path destination = Path.of(dst).toAbsolutePath().normalize();
        checkDestination(destination);
        try (UriResolver.OpenFile open = UriResolver.open(src, storage.toProperties())) {
            ParquetDataset dataset = ParquetDataset.open(open.source());
            ParquetSchema sourceSchema = dataset.schema();
            Projections.Resolved projection = Projections.resolve(options.columns, sourceSchema);
            ParquetSchema writeSchema = buildWriteSchema(sourceSchema, projection);
            RecordToWriteRow.requireWritable(writeSchema);
            Predicate predicate = buildPredicate(sourceSchema);
            writeAll(dataset, writeSchema, projection, predicate, destination, dataset.keyValueMetadata());
        }
        return 0;
    }

    private ParquetSchema buildWriteSchema(ParquetSchema sourceSchema, Projections.Resolved projection) {
        if (projection.projection() == Projection.ALL) {
            return sourceSchema;
        }
        return sourceSchema.project(Set.copyOf(projection.keptLeaves()));
    }

    private Predicate buildPredicate(ParquetSchema schema) {
        if (options.filter == null) {
            return Predicate.ALWAYS_TRUE;
        }
        return FilterParser.parse(options.filter, schema);
    }

    private void checkDestination(Path destination) {
        Path source = sourceAsLocalPath();
        if (source != null && source.equals(destination)) {
            throw new IllegalArgumentException("cp refuses to write onto the source file: " + destination);
        }
        if (Files.exists(destination) && !overwrite) {
            throw new IllegalArgumentException("destination exists (use -f to overwrite): " + destination);
        }
    }

    private Path sourceAsLocalPath() {
        try {
            URI uri = new URI(src);
            if (uri.getScheme() == null) {
                return Path.of(src).toAbsolutePath().normalize();
            }
            if ("file".equals(uri.getScheme())) {
                return Path.of(uri).toAbsolutePath().normalize();
            }
        } catch (URISyntaxException ignored) {
            // non-URI source: treat as a local path
            return Path.of(src).toAbsolutePath().normalize();
        } catch (RuntimeException ignored) {
            // non-file source cannot collide with a local destination
        }
        return null;
    }

    private void writeAll(
            ParquetDataset dataset,
            ParquetSchema writeSchema,
            Projections.Resolved projection,
            Predicate predicate,
            Path destination,
            Map<String, String> sourceKeyValue)
            throws Exception {
        WriteOptions writeOptions = buildWriteOptions(writeSchema, destination.getParent(), sourceKeyValue);
        long limit = options.limit == null ? Long.MAX_VALUE : options.limit;
        try (OutputStream out = Files.newOutputStream(destination);
                ParquetWriter writer = ParquetWriter.create(out, writeSchema, writeOptions);
                Stream<ParquetRecord> rows = dataset.read(predicate, projection.projection(), ReadOptions.DEFAULTS)) {
            long written = 0;
            Iterator<ParquetRecord> it = rows.iterator();
            while (it.hasNext() && written < limit) {
                writer.write(RecordToWriteRow.adapt(it.next()));
                written++;
            }
        }
    }

    private static WriteOptions buildWriteOptions(
            ParquetSchema writeSchema, Path tempDir, Map<String, String> sourceKeyValue) {
        WriteOptions.Builder builder = WriteOptions.builder().tempDir(tempDir);
        for (ColumnPath leaf : writeSchema.leafColumns()) {
            SchemaNode node = writeSchema.find(leaf).orElseThrow();
            SchemaNode.Primitive prim = (SchemaNode.Primitive) node;
            if (prim.kind() == PrimitiveKind.FIXED_LEN_BYTE_ARRAY) {
                builder.encodingPolicy(prim.name(), WriteOptions.EncodingPolicy.FORCE_PLAIN);
            }
        }
        forwardOpaqueMetadata(builder, sourceKeyValue);
        carryGeometryColumns(builder, writeSchema, sourceKeyValue);
        return builder.build();
    }

    /**
     * Carries the source's file-level key-value metadata into the copy, minus the reserved GeoParquet {@code geo}
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
     * Re-declares the source's geometry columns that survive the projection so the writer regenerates the GeoParquet
     * footer block for the output. The block must be regenerated rather than copied: its bounding box and geometry
     * types are derived from the rows actually written (which a filter or projection may narrow), and a geometry column
     * dropped by the projection must leave no dangling entry behind.
     */
    private static void carryGeometryColumns(
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
