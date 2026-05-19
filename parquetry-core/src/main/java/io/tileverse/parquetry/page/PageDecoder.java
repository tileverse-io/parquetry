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
 * Lazily materializes column values from one decompressed data-page payload, one value per {@link #next()} call.
 *
 * <p>{@link #skip(int)} advances the cursor without producing values (used when column-index narrowing rejects some
 * rows inside a kept page). Thread-confined; one decoder per column reader per page.
 *
 * <p>{@code valueCount} is the page header's {@code numValues} - the logical row count <em>including nulls</em>. PLAIN
 * encoding stores only the non-null values, so an all-null OPTIONAL page validly has {@code valueCount = N} and zero
 * value bytes; the column reader will not call {@link #next()} for those rows. Implementations must therefore not cap
 * the input buffer at {@code valueCount} (e.g. {@code asIntBuffer().limit(valueCount)}); the buffer's natural capacity
 * already reflects what was written. Over-consumption past that capacity should fail loudly.
 */
public interface PageDecoder<T> {

    void load(ByteBuffer page, int valueCount);

    T next();

    void skip(int n);
}
