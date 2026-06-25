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

import java.io.PrintWriter;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import io.tileverse.parquetry.cli.DatasetResolver;
import io.tileverse.parquetry.cli.GlobalOptions;
import io.tileverse.parquetry.cli.StorageOptions;
import io.tileverse.parquetry.cli.arrow.ArrowOutput;
import io.tileverse.parquetry.cli.arrow.ArrowOutputRequest;
import io.tileverse.parquetry.cli.expr.FilterParser;
import io.tileverse.parquetry.cli.expr.GeometryColumns;
import io.tileverse.parquetry.cli.render.Projections;
import io.tileverse.parquetry.cli.render.RecordRenderer;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.Dataset;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

@Command(name = "cat", description = "Decode rows of a file, directory, or Iceberg table to stdout (jsonl by default).")
public class CatCmd implements Callable<Integer> {

    @Parameters(index = "0", paramLabel = "<uri>", description = "Parquet file path or URI.")
    private String uri;

    @Mixin
    private GlobalOptions options;

    @ArgGroup(validate = false, heading = StorageOptions.HEADING)
    private StorageOptions storage = new StorageOptions();

    @Spec
    private CommandSpec spec;

    // S3516: success is always 0; failures are thrown and mapped to exit codes by the exception handler.
    @SuppressWarnings("java:S3516")
    @Override
    public Integer call() throws Exception {
        try (DatasetResolver.OpenDataset open = DatasetResolver.open(uri, storage.toProperties())) {
            Dataset dataset = open.dataset();
            ParquetSchema schema = dataset.schema();
            Projections.Resolved projection = Projections.resolve(options.columns, schema);
            Set<ColumnPath> geometryColumns = GeometryColumns.resolve(schema, DatasetResolver.geoMetadataOf(dataset));
            Predicate predicate = buildPredicate(schema, geometryColumns);
            long limit = options.limit == null ? defaultLimit() : options.limit;
            if (options.format == GlobalOptions.Format.ARROW) {
                writeArrow(dataset, schema, projection, predicate, limit);
                return 0;
            }
            RecordRenderer.Mode mode = resolveMode();
            PrintWriter out = spec.commandLine().getOut();
            ParquetSchema projectedSchema = projectedSchema(schema, projection);
            RecordRenderer renderer = new RecordRenderer(mode, projectedSchema, out);
            renderer.begin();
            emitRows(dataset, predicate, projection, limit, renderer);
            renderer.end();
            return 0;
        }
    }

    // S106: binary Arrow output is written to the real process stdout, bypassing the text writer.
    @SuppressWarnings("java:S106")
    private void writeArrow(
            Dataset dataset, ParquetSchema schema, Projections.Resolved projection, Predicate predicate, long limit) {
        ParquetSchema projectedSchema = projectedSchema(schema, projection);
        Optional<GeoParquetMetadata> geo = DatasetResolver.geoMetadataOf(dataset);
        ArrowOutputRequest request =
                new ArrowOutputRequest(predicate, projection.projection(), options.filter != null, limit);
        ArrowOutput.write(dataset, projectedSchema, geo, request, System.out);
    }

    private ParquetSchema projectedSchema(ParquetSchema schema, Projections.Resolved projection) {
        if (projection.projection() == Projection.ALL) {
            return schema;
        }
        return schema.project(Set.copyOf(projection.keptLeaves()));
    }

    private Predicate buildPredicate(ParquetSchema schema, Set<ColumnPath> geometryColumns) {
        if (options.filter == null) {
            return Predicate.ALWAYS_TRUE;
        }
        return FilterParser.parse(options.filter, schema, geometryColumns);
    }

    private void emitRows(
            Dataset dataset,
            Predicate predicate,
            Projections.Resolved projection,
            long limit,
            RecordRenderer renderer) {
        AtomicLong emitted = new AtomicLong();
        try (Stream<ParquetRecord> rows = dataset.read(predicate, projection.projection(), ReadOptions.DEFAULTS)) {
            rows.takeWhile(r -> emitted.getAndIncrement() < limit).forEach(renderer::row);
        }
    }

    /**
     * Returns the row cap used when the caller passes no --limit flag. Subcommands can override this to impose a finite
     * default (e.g. head limits to 10 rows).
     */
    protected long defaultLimit() {
        return Long.MAX_VALUE;
    }

    private RecordRenderer.Mode resolveMode() {
        if (options.format == null) {
            return RecordRenderer.Mode.JSONL;
        }
        return switch (options.format) {
            case JSONL -> RecordRenderer.Mode.JSONL;
            case CSV -> RecordRenderer.Mode.CSV;
            case TSV -> RecordRenderer.Mode.TSV;
            case TEXT -> RecordRenderer.Mode.TEXT;
            case JSON ->
                throw new ParameterException(spec.commandLine(), "cat does not support --format json; use jsonl");
            case ARROW -> throw new IllegalStateException("arrow output is handled before resolveMode");
        };
    }
}
