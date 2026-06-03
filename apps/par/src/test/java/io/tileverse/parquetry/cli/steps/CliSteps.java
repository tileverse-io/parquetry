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
package io.tileverse.parquetry.cli.steps;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import io.tileverse.parquetry.cli.support.CliRunner;
import io.tileverse.parquetry.cli.support.Fixtures;

import io.cucumber.java.After;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

/** One instance per scenario; fields hold the per-scenario working dir and the last run's captured output. */
public class CliSteps {

    private Path workDir;
    private int exitCode;
    private String stdout = "";
    private String stderr = "";

    @Given("the cities fixture")
    public void theCitiesFixture() throws Exception {
        workDir = Files.createTempDirectory("par-cucumber-");
        Fixtures.writeCities(workDir.resolve("cities.parquet"));
    }

    @Given("the geoparquet fixture")
    public void theGeoparquetFixture() throws Exception {
        workDir = Files.createTempDirectory("par-cucumber-");
        Fixtures.writeGeoCities(workDir.resolve("geo.parquet"));
    }

    @Given("a directory {string}")
    public void aDirectory(String name) throws Exception {
        Files.createDirectories(workDir.resolve(name));
    }

    @When("I run: {}")
    public void iRun(String commandLine) {
        CliRunner.Result result = CliRunner.run(resolveArgs(commandLine));
        exitCode = result.exitCode();
        stdout = result.stdout();
        stderr = result.stderr();
    }

    @Then("the exit code is {int}")
    public void theExitCodeIs(int expected) {
        assertThat(exitCode).as("stderr was: %s", stderr).isEqualTo(expected);
    }

    @Then("stdout has {int} lines")
    public void stdoutHasLines(int expected) {
        int actual = stdout.strip().isEmpty() ? 0 : stdout.strip().split("\n").length;
        assertThat(actual).isEqualTo(expected);
    }

    @Then("stdout line {int} contains {string}")
    public void stdoutLineContains(int oneBasedLine, String needle) {
        String[] lines = stdout.strip().split("\n");
        assertThat(lines[oneBasedLine - 1]).contains(needle);
    }

    @Then("stdout contains {string}")
    public void stdoutContains(String needle) {
        assertThat(stdout).contains(needle);
    }

    @Then("stdout does not contain {string}")
    public void stdoutDoesNotContain(String needle) {
        assertThat(stdout).doesNotContain(needle);
    }

    @Then("stderr contains {string}")
    public void stderrContains(String needle) {
        assertThat(stderr).contains(needle);
    }

    @Then("the file {string} exists")
    public void theFileExists(String relativePath) {
        Path file = workDir.resolve(relativePath);
        assertThat(file).exists();
    }

    @Then("the file {string} has {int} rows")
    public void theFileHasRows(String relativePath, int expected) {
        Path file = workDir.resolve(relativePath);
        CliRunner.Result result = CliRunner.run("row-count", file.toString());
        assertThat(result.exitCode())
                .as("par row-count exit code; stderr was: %s", result.stderr())
                .isZero();
        assertThat(Integer.parseInt(result.stdout().strip())).isEqualTo(expected);
    }

    @After
    public void cleanup() throws Exception {
        if (workDir == null) {
            return;
        }
        try (Stream<Path> walk = Files.walk(workDir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }

    private String[] resolveArgs(String commandLine) {
        List<String> args = tokenize(commandLine);
        List<String> resolved = new ArrayList<>(args.size());
        for (String arg : args) {
            Path candidate = workDir.resolve(arg);
            boolean looksLikeFile = Files.exists(candidate) || arg.endsWith(".parquet");
            resolved.add(looksLikeFile ? candidate.toString() : arg);
        }
        return resolved.toArray(new String[0]);
    }

    private static List<String> tokenize(String line) {
        List<String> out = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        char quote = 0;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (quote != 0) {
                if (c == quote) {
                    quote = 0;
                } else {
                    current.append(c);
                }
            } else if (c == '\'' || c == '"') {
                quote = c;
            } else if (Character.isWhitespace(c)) {
                if (current.length() > 0) {
                    out.add(current.toString());
                    current.setLength(0);
                }
            } else {
                current.append(c);
            }
        }
        if (current.length() > 0) {
            out.add(current.toString());
        }
        return out;
    }
}
