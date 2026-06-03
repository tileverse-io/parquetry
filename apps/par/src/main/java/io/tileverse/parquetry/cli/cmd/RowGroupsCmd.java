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
import java.util.List;
import java.util.concurrent.Callable;

import io.tileverse.parquetry.cli.GlobalOptions;
import io.tileverse.parquetry.cli.StorageOptions;
import io.tileverse.parquetry.cli.UriResolver;
import io.tileverse.parquetry.cli.render.RowGroupsRenderer;
import io.tileverse.parquetry.data.ParquetDataset;
import io.tileverse.parquetry.data.RowGroupSummary;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

@Command(name = "row-groups", description = "List the file's row groups.")
public final class RowGroupsCmd implements Callable<Integer> {

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
        try (UriResolver.OpenFile open = UriResolver.open(uri, storage.toProperties())) {
            ParquetDataset dataset = ParquetDataset.open(open.source());
            List<RowGroupSummary> rowGroups = dataset.rowGroups();
            renderRowGroups(spec.commandLine().getOut(), rowGroups);
            return 0;
        }
    }

    private void renderRowGroups(PrintWriter out, List<RowGroupSummary> rowGroups) {
        // Null format means no -o flag was given, which defaults to text
        if (options.format == null || options.format == GlobalOptions.Format.TEXT) {
            RowGroupsRenderer.writeText(out, rowGroups);
            return;
        }
        switch (options.format) {
            case JSON -> RowGroupsRenderer.writeJson(out, rowGroups);
            case JSONL, CSV, TSV, ARROW ->
                throw new ParameterException(
                        spec.commandLine(),
                        "row-groups does not support --format "
                                + options.format.name().toLowerCase() + "; use text or json");
            // TEXT is handled above; listed here to keep the switch over the enum complete
            case TEXT -> RowGroupsRenderer.writeText(out, rowGroups);
        }
    }
}
