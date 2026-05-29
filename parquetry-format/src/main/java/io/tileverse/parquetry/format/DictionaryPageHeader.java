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
 * Header for the optional dictionary page of a column chunk; mirror of {@code DictionaryPageHeader} in
 * {@code parquet.thrift}.
 *
 * <p>Located via {@link ColumnMetaData#dictionaryPageOffset()}. Declares the dictionary entry count, the
 * {@link Encoding} used for the dictionary values (typically {@link Encoding#PLAIN}), and whether entries are sorted.
 *
 * @param numValues number of entries in this dictionary
 * @param encoding {@link Encoding} of the dictionary values themselves (typically {@link Encoding#PLAIN})
 * @param isSorted whether the dictionary entries are sorted in ascending order; defaults to {@code false} when the
 *     writer did not record the flag (the conservative choice - consumers that rely on sorted order should look at the
 *     dictionary bytes themselves)
 */
public record DictionaryPageHeader(int numValues, Encoding encoding, boolean isSorted) {}
