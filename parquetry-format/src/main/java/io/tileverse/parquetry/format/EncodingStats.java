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
package io.tileverse.parquetry.format;

/**
 * Per-({@link PageType}, {@link Encoding}) page count for a column chunk; mirror of {@code PageEncodingStats} in
 * {@code parquet.thrift}.
 *
 * <p>Carried by {@link ColumnMetaData#encodingStats()}; lets a reader know how many pages of each kind use each
 * encoding without scanning every page header.
 *
 * @param pageType {@link PageType} (data, dictionary, etc.) being counted
 * @param encoding {@link Encoding} being counted
 * @param count number of pages in the column chunk of this {@code pageType} that use this {@code encoding}
 */
public record EncodingStats(PageType pageType, Encoding encoding, int count) {}
