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
package io.tileverse.parquetry.format;

import java.util.Optional;

/**
 * V1 data page header; mirror of {@code DataPageHeader} in {@code parquet.thrift}.
 *
 * <p>Carried by {@link PageHeader#dataPageHeader()}. Declares the page's value count, the value-{@link Encoding} and
 * the separate encodings used for definition and repetition levels (RLE/BIT_PACKED in practice), plus optional inline
 * {@link Statistics}.
 *
 * @param numValues number of values in this page, including nulls
 * @param encoding {@link Encoding} used for the value bytes of this page
 * @param definitionLevelEncoding {@link Encoding} used for definition levels (typically {@code RLE} or
 *     {@code BIT_PACKED})
 * @param repetitionLevelEncoding {@link Encoding} used for repetition levels (typically {@code RLE} or
 *     {@code BIT_PACKED})
 * @param statistics optional inline {@link Statistics} for this page's values; empty when not written
 */
public record DataPageHeader(
        int numValues,
        Encoding encoding,
        Encoding definitionLevelEncoding,
        Encoding repetitionLevelEncoding,
        Optional<Statistics> statistics) {}
