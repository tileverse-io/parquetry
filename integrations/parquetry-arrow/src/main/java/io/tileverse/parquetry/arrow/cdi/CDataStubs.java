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
package io.tileverse.parquetry.arrow.cdi;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

/**
 * Builds the upcall stubs the C Data Interface exporters install as their callbacks. Every stub lives in one
 * process-lifetime arena shared across all exports: a consumer invokes a callback on its own schedule, after the
 * per-export data arena is gone, and the stub must outlive every export. The arena is {@link Arena#ofShared()} rather
 * than {@link Arena#global()} because global-arena native memory counts against {@code -XX:MaxDirectMemorySize}.
 *
 * <p>Each exporter passes its own {@link MethodHandles.Lookup} so the stub can bind a private static callback declared
 * in that exporter. A callback body must never let an exception cross the upcall boundary; an escaping exception
 * crashes the JVM, which is why every exporter wraps its callbacks in a catch-all.
 */
final class CDataStubs {

    private static final Arena STUB_ARENA = Arena.ofShared();

    private CDataStubs() {
        // utility
    }

    /**
     * Builds an upcall stub bound to the static method {@code methodName} of {@code lookup}'s own class, with the given
     * {@code type} and native {@code descriptor}. The stub lives in the shared process-lifetime arena.
     */
    static MemorySegment upcall(
            MethodHandles.Lookup lookup, String methodName, MethodType type, FunctionDescriptor descriptor) {
        try {
            MethodHandle handle = lookup.findStatic(lookup.lookupClass(), methodName, type);
            return Linker.nativeLinker().upcallStub(handle, descriptor, STUB_ARENA);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
}
