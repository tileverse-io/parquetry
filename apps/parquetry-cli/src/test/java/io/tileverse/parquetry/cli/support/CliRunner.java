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
package io.tileverse.parquetry.cli.support;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.tileverse.parquetry.cli.Par;

import picocli.CommandLine;

/**
 * Runs {@code par} for tests, either in-process (the default, fast) or by executing a built native binary when the
 * {@code par.bin} system property points at one. The same scenarios then validate either the JVM command tree or the
 * shipped native executable, which catches process-level differences (real stdout flushing, exit codes) and
 * native-image runtime gaps that an in-process run cannot.
 */
public final class CliRunner {

    /** Outcome of one {@code par} invocation. */
    public record Result(int exitCode, String stdout, String stderr) {}

    private CliRunner() {}

    /** Absolute path to a native {@code par} binary from {@code -Dpar.bin}, or {@code null} to run in-process. */
    public static String nativeBinary() {
        String path = System.getProperty("par.bin");
        if (path == null || path.isBlank()) {
            return null;
        }
        return path;
    }

    public static boolean usesNativeBinary() {
        return nativeBinary() != null;
    }

    public static Result run(String... args) {
        String binary = nativeBinary();
        if (binary == null) {
            return runInProcess(args);
        }
        return runBinary(binary, args);
    }

    private static Result runInProcess(String[] args) {
        StringWriter out = new StringWriter();
        StringWriter err = new StringWriter();
        CommandLine cmd = Par.newCommandLine();
        cmd.setOut(new PrintWriter(out));
        cmd.setErr(new PrintWriter(err));
        int code = cmd.execute(args);
        return new Result(code, out.toString(), err.toString());
    }

    private static Result runBinary(String binary, String[] args) {
        File executable = new File(binary);
        if (!executable.canExecute()) {
            throw new IllegalStateException(
                    "par.bin points at a missing or non-executable binary: " + binary + " (build it with -Pnative)");
        }
        List<String> command = new ArrayList<>();
        command.add(binary);
        command.addAll(List.of(args));
        // Redirect to files so a chatty stderr cannot deadlock against a full stdout pipe buffer.
        try {
            File outFile = File.createTempFile("par-stdout", ".txt");
            File errFile = File.createTempFile("par-stderr", ".txt");
            try {
                Process process = new ProcessBuilder(command)
                        .redirectOutput(outFile)
                        .redirectError(errFile)
                        .start();
                boolean finished = process.waitFor(120, TimeUnit.SECONDS);
                if (!finished) {
                    process.destroyForcibly();
                    throw new IllegalStateException("par binary timed out: " + command);
                }
                String stdout = Files.readString(outFile.toPath(), StandardCharsets.UTF_8);
                String stderr = Files.readString(errFile.toPath(), StandardCharsets.UTF_8);
                return new Result(process.exitValue(), stdout, stderr);
            } finally {
                outFile.delete();
                errFile.delete();
            }
        } catch (IOException e) {
            throw new IllegalStateException("failed to run par binary: " + command, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted running par binary: " + command, e);
        }
    }
}
