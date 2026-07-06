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
package io.tileverse.parquetry.cli;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.junit.jupiter.api.Test;

import picocli.CommandLine;

class ParCommandTest {

    @Test
    void usageHelpExitsZeroAndListsSubcommands() {
        StringWriter out = new StringWriter();
        CommandLine cmd = Par.newCommandLine();
        cmd.setOut(new PrintWriter(out));
        int code = cmd.execute("--help");
        assertThat(code).isZero();
        assertThat(out.toString()).contains("schema", "meta", "cat", "cp");
    }

    @Test
    void unknownCommandIsUsageError() {
        int code = Par.newCommandLine().execute("bogus");
        assertThat(code).isEqualTo(CliExitCode.USAGE);
    }

    @Test
    void filterHelpListsPredicatesAndExitsZeroWithoutAFile() {
        StringWriter out = new StringWriter();
        CommandLine cmd = Par.newCommandLine();
        cmd.setOut(new PrintWriter(out));
        int code = cmd.execute("cat", "--filter-help");
        assertThat(code).isZero();
        assertThat(out.toString())
                .contains("Supported --filter predicates")
                .contains("= != <> < <= > >=")
                .contains("ST_Intersects")
                .contains("ST_MakeEnvelope");
    }

    @Test
    void filterHelpIsAvailableOnHeadToo() {
        StringWriter out = new StringWriter();
        CommandLine cmd = Par.newCommandLine();
        cmd.setOut(new PrintWriter(out));
        int code = cmd.execute("head", "--filter-help");
        assertThat(code).isZero();
        assertThat(out.toString()).contains("Supported --filter predicates");
    }

    @Test
    void generateCompletionEmitsAShellCompletionScript() {
        StringWriter out = new StringWriter();
        CommandLine cmd = Par.newCommandLine();
        cmd.setOut(new PrintWriter(out));
        int code = cmd.execute("generate-completion");
        assertThat(code).isZero();
        assertThat(out.toString()).contains("_complete_par").contains("complete -F");
    }

    @Test
    void helpListsCommandOptionsBeforeTheStorageSections() {
        String help = Par.newCommandLine().getSubcommands().get("cp").getUsageMessage(CommandLine.Help.Ansi.OFF);
        assertThat(help).contains("Storage options:").contains("Destination storage options:");
        assertThat(help.indexOf("--overwrite"))
                .as("command options precede the storage section")
                .isLessThan(help.indexOf("Storage options:"));
        assertThat(help.indexOf("Storage options:"))
                .as("source storage precedes destination storage")
                .isLessThan(help.indexOf("Destination storage options:"));
    }
}
