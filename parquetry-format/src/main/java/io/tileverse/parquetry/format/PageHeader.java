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

import java.util.Optional;
import java.util.OptionalInt;

/**
 * Header prefix before every page in a column chunk.
 *
 * <p>The Thrift {@code PageHeader} struct carries optional nested headers for each page type; we keep the same shape
 * with one {@code Optional<XHeader>} per page type. Exactly one is present (cross-validated by {@link #type()}).
 *
 * @param type page kind ({@link PageType#DATA_PAGE}, {@link PageType#DATA_PAGE_V2}, {@link PageType#DICTIONARY_PAGE},
 *     {@link PageType#INDEX_PAGE}); selects which of the four optional nested headers is populated
 * @param uncompressedPageSize size in bytes of the page after decompression but before decoding levels and values
 * @param compressedPageSize size in bytes of the on-disk page bytes (after the writer's {@link CompressionCodec})
 * @param crc optional CRC-32 of the compressed page bytes for end-to-end integrity checking; empty when the writer did
 *     not record one
 * @param dataPageHeader present when {@link #type} is {@link PageType#DATA_PAGE}; empty otherwise
 * @param indexPageHeader present when {@link #type} is {@link PageType#INDEX_PAGE}; empty otherwise
 * @param dictionaryPageHeader present when {@link #type} is {@link PageType#DICTIONARY_PAGE}; empty otherwise
 * @param dataPageHeaderV2 present when {@link #type} is {@link PageType#DATA_PAGE_V2}; empty otherwise
 * @see ParquetFormat#readPageHeader(java.io.InputStream)
 */
public record PageHeader(
        PageType type,
        int uncompressedPageSize,
        int compressedPageSize,
        OptionalInt crc,
        Optional<DataPageHeader> dataPageHeader,
        Optional<IndexPageHeader> indexPageHeader,
        Optional<DictionaryPageHeader> dictionaryPageHeader,
        Optional<DataPageHeaderV2> dataPageHeaderV2) {}
