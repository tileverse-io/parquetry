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
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import io.tileverse.parquetry.cli.GlobalOptions;
import io.tileverse.parquetry.cli.StorageOptions;
import io.tileverse.parquetry.cli.UriResolver;
import io.tileverse.parquetry.cli.expr.FilterParser;
import io.tileverse.parquetry.cli.render.Projections;
import io.tileverse.parquetry.cli.render.RecordRenderer;
import io.tileverse.parquetry.data.ParquetDataset;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ParquetSchema;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

@Command(name = "cat", description = "Decode rows to stdout (jsonl by default).")
public class CatCmd implements Callable<Integer> {

    @Parameters(index = "0", paramLabel = "<uri>", description = "Parquet file path or URI.")
    private String uri;

    @Mixin
    private GlobalOptions options;

    @Mixin
    private StorageOptions storage;

    @Spec
    private CommandSpec spec;

    @Override
    public Integer call() throws Exception {
        try (UriResolver.OpenFile open = UriResolver.open(uri, storage.toProperties())) {
            ParquetDataset dataset = ParquetDataset.open(open.source());
            ParquetSchema schema = dataset.schema();
            Projections.Resolved projection = Projections.resolve(options.columns, schema);
            Predicate predicate = buildPredicate(schema);
            RecordRenderer.Mode mode = resolveMode();
            PrintWriter out = spec.commandLine().getOut();
            RecordRenderer renderer = new RecordRenderer(mode, schema, projection.keptLeaves(), out);
            renderer.begin();
            long limit = options.limit == null ? defaultLimit() : options.limit;
            emitRows(dataset, predicate, projection, limit, renderer);
            renderer.end();
            return 0;
        }
    }

    private Predicate buildPredicate(ParquetSchema schema) {
        if (options.filter == null) {
            return Predicate.ALWAYS_TRUE;
        }
        return FilterParser.parse(options.filter, schema);
    }

    private void emitRows(
            ParquetDataset dataset,
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
        };
    }
}
