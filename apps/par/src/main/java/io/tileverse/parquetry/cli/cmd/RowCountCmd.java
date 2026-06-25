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

import java.util.Set;
import java.util.concurrent.Callable;

import io.tileverse.parquetry.cli.DatasetResolver;
import io.tileverse.parquetry.cli.GlobalOptions;
import io.tileverse.parquetry.cli.StorageOptions;
import io.tileverse.parquetry.cli.expr.FilterParser;
import io.tileverse.parquetry.cli.expr.GeometryColumns;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.ParquetDataset;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

@Command(name = "row-count", description = "Print the number of rows in a Parquet file.")
public final class RowCountCmd implements Callable<Integer> {

    @Parameters(index = "0", paramLabel = "<uri>", description = "Parquet file path or URI.")
    private String uri;

    @Mixin
    private GlobalOptions options;

    @ArgGroup(validate = false, heading = StorageOptions.HEADING)
    private StorageOptions storage = new StorageOptions();

    @Spec
    private CommandSpec spec;

    @Override
    public Integer call() throws Exception {
        try (DatasetResolver.OpenDataset open = DatasetResolver.open(uri, storage.toProperties())) {
            ParquetDataset dataset = open.dataset();
            Predicate predicate = buildPredicate(dataset);
            long count = dataset.count(predicate, ReadOptions.DEFAULTS);
            spec.commandLine().getOut().println(count);
            return 0;
        }
    }

    private Predicate buildPredicate(ParquetDataset dataset) {
        if (options.filter == null) {
            return Predicate.ALWAYS_TRUE;
        }
        ParquetSchema schema = dataset.schema();
        Set<ColumnPath> geometryColumns = GeometryColumns.resolve(schema, DatasetResolver.geoMetadataOf(dataset));
        return FilterParser.parse(options.filter, schema, geometryColumns);
    }
}
