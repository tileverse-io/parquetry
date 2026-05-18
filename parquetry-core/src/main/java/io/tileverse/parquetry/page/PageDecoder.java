/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.page;

import java.nio.ByteBuffer;

/**
 * Contract every page decoder satisfies.
 *
 * <p>Lifecycle: {@link #load(ByteBuffer, int)} hands the decoder a page's compressed-then- decompressed bytes plus the
 * value count from the page header. The decoder then yields values via {@link #next()} (one per call) up to
 * {@code valueCount}. {@link #skip(int)} advances past values without materializing them (used for column-index page
 * skipping where some rows in a kept page aren't needed).
 *
 * <p>Thread-confined; create one decoder per column reader per page.
 *
 * @param <T> the primitive value type yielded; boxed primitive for Java compatibility, but implementations should still
 *     favor specialized internal storage.
 */
public interface PageDecoder<T> {

    /** Load a fresh page. Implementations may reset internal buffers. */
    void load(ByteBuffer page, int valueCount);

    /** Yield the next value. Caller must ensure they haven't exceeded {@code valueCount}. */
    T next();

    /** Skip {@code n} values without materializing them. */
    void skip(int n);
}
