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
import java.util.stream.Stream;

import io.tileverse.parquetry.cli.GlobalOptions;
import io.tileverse.parquetry.cli.StorageOptions;
import io.tileverse.parquetry.cli.UriResolver;
import io.tileverse.parquetry.cli.expr.FilterParser;
import io.tileverse.parquetry.data.ParquetDataset;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ParquetSchema;

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

    @Mixin
    private StorageOptions storage;

    @Spec
    private CommandSpec spec;

    @Override
    public Integer call() throws Exception {
        try (UriResolver.OpenFile open = UriResolver.open(uri, storage.toProperties())) {
            long count;
            if (options.filter == null) {
                // Read only the footer: no data pages needed, one round trip.
                count = countAll(open.source());
            } else {
                count = countMatching(open.source());
            }
            spec.commandLine().getOut().println(count);
            return 0;
        }
    }

    /**
     * Returns the exact row count stored in the Parquet footer without scanning any data pages. This is the fast path
     * when no filter is given.
     */
    private long countAll(ByteRangeSource source) {
        FileMetaData footer = ParquetFormat.readFooter(source);
        return footer.numRows();
    }

    /**
     * Streams all rows that match the filter predicate and counts them. Requires a full column scan because the footer
     * row count includes rows that would be filtered out.
     */
    private long countMatching(ByteRangeSource source) {
        ParquetDataset dataset = ParquetDataset.open(source);
        ParquetSchema schema = dataset.schema();
        Predicate predicate = FilterParser.parse(options.filter, schema);
        try (Stream<ParquetRecord> rows = dataset.read(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
            return rows.count();
        }
    }
}
