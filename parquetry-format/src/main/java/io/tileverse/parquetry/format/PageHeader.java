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
import java.util.OptionalInt;

import io.tileverse.parquetry.format.enums.PageType;

/**
 * Header prefix before every page in a column chunk.
 *
 * <p>The Thrift {@code PageHeader} struct carries optional nested headers for each page type;
 * we keep the same shape with one {@code Optional<XHeader>} per page type. Exactly one is present
 * (cross-validated by {@link #type()}).
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
