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

import java.util.concurrent.Callable;

import io.tileverse.parquetry.cli.GlobalOptions;
import io.tileverse.parquetry.cli.StorageOptions;
import io.tileverse.parquetry.cli.UriResolver;
import io.tileverse.parquetry.cli.expr.FilterParser;
import io.tileverse.parquetry.cli.render.Projections;
import io.tileverse.parquetry.data.ParquetDataset;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.ExplainPlan;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.schema.ParquetSchema;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

@Command(name = "explain", description = "Print the filter/scan plan for a Parquet file.")
public final class ExplainCmd implements Callable<Integer> {

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
            Predicate predicate = buildPredicate(schema);
            Projection projection = Projections.resolve(options.columns, schema).projection();
            ExplainPlan plan = dataset.explain(predicate, projection, ReadOptions.DEFAULTS);
            String rendered = renderPlan(plan);
            spec.commandLine().getOut().println(rendered);
            return 0;
        }
    }

    private Predicate buildPredicate(ParquetSchema schema) {
        if (options.filter == null) {
            return Predicate.ALWAYS_TRUE;
        }
        return FilterParser.parse(options.filter, schema);
    }

    private String renderPlan(ExplainPlan plan) {
        if (options.format == null || options.format == GlobalOptions.Format.TEXT) {
            return plan.toAsciiTable();
        }
        return switch (options.format) {
            case JSON -> plan.toJson();
            case JSONL, CSV, TSV ->
                throw new ParameterException(
                        spec.commandLine(),
                        "explain does not support --format "
                                + options.format.name().toLowerCase() + "; use text or json");
            // TEXT already handled above; TEXT listed here keeps the switch exhaustive
            case TEXT -> plan.toAsciiTable();
        };
    }
}
