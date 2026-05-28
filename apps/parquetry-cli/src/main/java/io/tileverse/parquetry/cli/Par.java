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
package io.tileverse.parquetry.cli;

import io.tileverse.parquetry.cli.cmd.CatCmd;
import io.tileverse.parquetry.cli.cmd.CpCmd;
import io.tileverse.parquetry.cli.cmd.ExplainCmd;
import io.tileverse.parquetry.cli.cmd.HeadCmd;
import io.tileverse.parquetry.cli.cmd.MetaCmd;
import io.tileverse.parquetry.cli.cmd.RowCountCmd;
import io.tileverse.parquetry.cli.cmd.RowGroupsCmd;
import io.tileverse.parquetry.cli.cmd.SchemaCmd;
import io.tileverse.parquetry.cli.cmd.StatsCmd;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(
        name = "par",
        mixinStandardHelpOptions = true,
        version = "par (dev)",
        synopsisSubcommandLabel = "COMMAND",
        subcommands = {
            SchemaCmd.class,
            MetaCmd.class,
            CatCmd.class,
            HeadCmd.class,
            CpCmd.class,
            ExplainCmd.class,
            RowGroupsCmd.class,
            RowCountCmd.class,
            StatsCmd.class
        },
        description = "Inspect and transform Parquet files.")
public final class Par {

    public static CommandLine newCommandLine() {
        CommandLine commandLine = new CommandLine(new Par())
                .setExecutionExceptionHandler(new ExitCodeExceptionHandler())
                .setCaseInsensitiveEnumValuesAllowed(true);
        abbreviateSynopsis(commandLine);
        return commandLine;
    }

    private static void abbreviateSynopsis(CommandLine commandLine) {
        commandLine.getCommandSpec().usageMessage().abbreviateSynopsis(true);
        for (CommandLine subcommand : commandLine.getSubcommands().values()) {
            abbreviateSynopsis(subcommand);
        }
    }

    public static void main(String[] args) {
        System.exit(newCommandLine().execute(args));
    }
}
