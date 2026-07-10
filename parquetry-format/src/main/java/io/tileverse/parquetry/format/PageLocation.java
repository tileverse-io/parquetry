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
package io.tileverse.parquetry.format;

/**
 * One entry in an {@link OffsetIndex}; mirror of {@code PageLocation} in {@code parquet.thrift}.
 *
 * <p>Points at one data page by file offset and compressed byte length, and records the row index of its first value so
 * a reader can pick which pages to fetch for a given row range.
 *
 * @param offset byte offset of the page (its {@link PageHeader}) from the beginning of the file
 * @param compressedPageSize compressed byte size of the page including its header ({@code compressed_page_size} in the
 *     thrift schema)
 * @param firstRowIndex zero-based row index, relative to the start of the {@link RowGroup}, of the first row in this
 *     page ({@code first_row_index} in the thrift schema)
 */
public record PageLocation(long offset, int compressedPageSize, long firstRowIndex) {}
