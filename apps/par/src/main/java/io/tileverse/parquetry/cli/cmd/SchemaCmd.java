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

import java.io.PrintWriter;
import java.util.concurrent.Callable;

import io.tileverse.parquetry.cli.DatasetResolver;
import io.tileverse.parquetry.cli.GlobalOptions;
import io.tileverse.parquetry.cli.StorageOptions;
import io.tileverse.parquetry.cli.render.SchemaRenderer;
import io.tileverse.parquetry.schema.ParquetSchema;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

@Command(name = "schema", description = "Print the Parquet message-type tree of a file, directory, or Iceberg table.")
public final class SchemaCmd implements Callable<Integer> {

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
            ParquetSchema schema = open.dataset().schema();
            PrintWriter out = spec.commandLine().getOut();
            if (options.format == GlobalOptions.Format.JSON) {
                SchemaRenderer.writeJson(out, schema);
            } else {
                SchemaRenderer.writeText(out, schema);
            }
            return 0;
        }
    }
}
