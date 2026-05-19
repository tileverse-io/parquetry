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
package io.tileverse.parquetry.format;

import java.util.Optional;

/**
 * V2 data page header; mirror of {@code DataPageHeaderV2} in {@code parquet.thrift}.
 *
 * <p>Differs from {@link DataPageHeader} in that the rep/def-level sections are stored uncompressed with explicit byte
 * lengths ({@link #repetitionLevelsByteLength()}, {@link #definitionLevelsByteLength()}), only the value section is
 * compressed (gated by {@link #isCompressed()}), and the header carries explicit {@link #numRows()} and
 * {@link #numNulls()} counts in addition to {@link #numValues()}.
 *
 * @param numValues number of values in this page, including nulls
 * @param numNulls number of null values in this page ({@code num_nulls} in the thrift schema)
 * @param numRows number of rows in this page ({@code num_rows} in the thrift schema); equal to {@code numValues} for
 *     non-repeated columns
 * @param encoding {@link Encoding} used for the value bytes of this page
 * @param definitionLevelsByteLength byte length of the uncompressed definition-levels section
 *     ({@code definition_levels_byte_length} in the thrift schema)
 * @param repetitionLevelsByteLength byte length of the uncompressed repetition-levels section
 *     ({@code repetition_levels_byte_length} in the thrift schema)
 * @param isCompressed whether the value section is compressed with the column chunk's {@link CompressionCodec}; empty
 *     defaults to {@code true} per the thrift schema
 * @param statistics optional inline {@link Statistics} for this page's values; empty when not written
 */
public record DataPageHeaderV2(
        int numValues,
        int numNulls,
        int numRows,
        Encoding encoding,
        int definitionLevelsByteLength,
        int repetitionLevelsByteLength,
        Optional<Boolean> isCompressed,
        Optional<Statistics> statistics) {}
