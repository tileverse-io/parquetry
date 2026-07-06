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
package io.tileverse.parquetry.cli.support;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
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

    /** Outcome of one {@code par} invocation whose stdout is binary (for example Arrow IPC bytes). */
    public record BytesResult(int exitCode, byte[] stdout, String stderr) {}

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

    /**
     * Runs {@code par} like {@link #run(String...)} but captures stdout as raw bytes instead of decoding it as text.
     * Use this for commands that emit a binary stream (for example {@code cat -o arrow}), whose bytes decoding as text
     * would mangle.
     */
    public static BytesResult runCapturingBytes(String... args) {
        String binary = nativeBinary();
        if (binary == null) {
            return runInProcessCapturingBytes(args);
        }
        return runBinaryCapturingBytes(binary, args);
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

    /**
     * The Arrow output path writes to the JVM's real {@link System#out}, not to picocli's out writer, hence this method
     * redirects {@code System.out} to a byte buffer for the duration of the call and restores it afterwards.
     */
    private static BytesResult runInProcessCapturingBytes(String[] args) {
        ByteArrayOutputStream stdoutBuffer = new ByteArrayOutputStream();
        StringWriter err = new StringWriter();
        PrintStream originalOut = System.out;
        try (PrintStream capturingOut = new PrintStream(stdoutBuffer, true, StandardCharsets.UTF_8)) {
            System.setOut(capturingOut);
            CommandLine cmd = Par.newCommandLine();
            cmd.setErr(new PrintWriter(err));
            int code = cmd.execute(args);
            capturingOut.flush();
            return new BytesResult(code, stdoutBuffer.toByteArray(), err.toString());
        } finally {
            System.setOut(originalOut);
        }
    }

    private static Result runBinary(String binary, String[] args) {
        return runBinaryRedirectingToFiles(binary, args, (process, outFile, errFile) -> {
            String stdout = Files.readString(outFile.toPath(), StandardCharsets.UTF_8);
            String stderr = Files.readString(errFile.toPath(), StandardCharsets.UTF_8);
            return new Result(process.exitValue(), stdout, stderr);
        });
    }

    private static BytesResult runBinaryCapturingBytes(String binary, String[] args) {
        return runBinaryRedirectingToFiles(binary, args, (process, outFile, errFile) -> {
            byte[] stdout = Files.readAllBytes(outFile.toPath());
            String stderr = Files.readString(errFile.toPath(), StandardCharsets.UTF_8);
            return new BytesResult(process.exitValue(), stdout, stderr);
        });
    }

    /**
     * Reads the stdout/stderr redirect files once the process has exited; throws {@link IOException} on read errors.
     */
    @FunctionalInterface
    private interface RedirectReader<T> {
        T read(Process process, File outFile, File errFile) throws IOException;
    }

    private static <T> T runBinaryRedirectingToFiles(String binary, String[] args, RedirectReader<T> reader) {
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
            File outFile = File.createTempFile("par-stdout", ".bin");
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
                return reader.read(process, outFile, errFile);
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
