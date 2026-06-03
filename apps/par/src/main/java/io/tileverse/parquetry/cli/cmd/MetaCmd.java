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
import java.util.Map;
import java.util.concurrent.Callable;

import io.tileverse.parquetry.cli.GlobalOptions;
import io.tileverse.parquetry.cli.StorageOptions;
import io.tileverse.parquetry.cli.UriResolver;
import io.tileverse.parquetry.cli.render.MetaRenderer;
import io.tileverse.parquetry.data.ParquetDataset;
import io.tileverse.parquetry.data.RowGroupSummary;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.schema.ColumnPath;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

@Command(name = "meta", description = "Print a one-screen file summary.")
public final class MetaCmd implements Callable<Integer> {

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
            FileMetaData footer = ParquetFormat.readFooter(open.source());
            ParquetDataset dataset = ParquetDataset.open(open.source());
            List<RowGroupSummary> rowGroups = dataset.rowGroups();
            List<ColumnPath> leafColumns = dataset.schema().leafColumns();
            Map<String, String> keyValue = dataset.keyValueMetadata();
            PrintWriter out = spec.commandLine().getOut();
            if (options.format == GlobalOptions.Format.JSON) {
                MetaRenderer.writeJson(out, footer, rowGroups, leafColumns.size(), keyValue);
            } else {
                MetaRenderer.writeText(out, footer, rowGroups, leafColumns.size(), keyValue);
            }
            return 0;
        }
    }
}
