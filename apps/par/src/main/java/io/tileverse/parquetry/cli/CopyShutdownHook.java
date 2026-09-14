/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Turns a JVM shutdown during a copy (Ctrl-C, SIGTERM) into the copy's normal failure path. The hook interrupts the
 * copy thread, which makes the writer fail, delete its working directory, and abort the staged destination, and it
 * waits for the copy to report that unwinding done before the JVM halts. Joining the copy thread instead would never
 * return: that thread ends in {@code System.exit}, which blocks while shutdown hooks run. When the copy does not report
 * back in time, the hook aborts the destination itself.
 */
public final class CopyShutdownHook {

    private static final Duration UNWIND_TIMEOUT = Duration.ofSeconds(10);

    private final Thread hook;
    private final CountDownLatch unwound = new CountDownLatch(1);

    private CopyShutdownHook(Thread copyThread, UriResolver.OpenSink sink) {
        this.hook = new Thread(() -> unwind(copyThread, sink), "par-cp-shutdown");
    }

    /** Registers the hook; call {@link #copyUnwound()} once the copy has committed or failed and closed its sink. */
    public static CopyShutdownHook install(Thread copyThread, UriResolver.OpenSink sink) {
        CopyShutdownHook installed = new CopyShutdownHook(copyThread, sink);
        Runtime.getRuntime().addShutdownHook(installed.hook);
        return installed;
    }

    /** Reports that the copy is over and its sink closed, and unregisters the hook when no shutdown is under way. */
    public void copyUnwound() {
        unwound.countDown();
        try {
            Runtime.getRuntime().removeShutdownHook(hook);
        } catch (IllegalStateException _) {
            // shutdown in progress: the hook is running and returns now that the copy has unwound
        }
    }

    private void unwind(Thread copyThread, UriResolver.OpenSink sink) {
        copyThread.interrupt();
        boolean finished;
        try {
            finished = unwound.await(UNWIND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException _) {
            finished = false;
            Thread.currentThread().interrupt();
        }
        if (!finished) {
            sink.abort();
        }
    }
}
