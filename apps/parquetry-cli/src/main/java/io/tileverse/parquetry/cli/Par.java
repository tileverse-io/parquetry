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
import io.tileverse.parquetry.cli.expr.FilterSyntax;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParseResult;

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
                .setExecutionStrategy(Par::runOrShowFilterHelp)
                .setCaseInsensitiveEnumValuesAllowed(true);
        abbreviateSynopsis(commandLine);
        return commandLine;
    }

    /**
     * Intercepts {@code --filter-help} on any subcommand, printing the filter reference and exiting before the command
     * runs; otherwise hands off to the default run-last strategy.
     */
    private static int runOrShowFilterHelp(ParseResult parseResult) {
        CommandLine requestingCommand = commandRequestingFilterHelp(parseResult);
        if (requestingCommand != null) {
            requestingCommand.getOut().println(FilterSyntax.reference());
            requestingCommand.getOut().flush();
            return CliExitCode.OK;
        }
        return new CommandLine.RunLast().execute(parseResult);
    }

    private static CommandLine commandRequestingFilterHelp(ParseResult parseResult) {
        ParseResult current = parseResult;
        while (current != null) {
            if (current.hasMatchedOption("--filter-help")) {
                return current.commandSpec().commandLine();
            }
            current = current.subcommand();
        }
        return null;
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
