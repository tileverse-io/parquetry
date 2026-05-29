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

/**
 * One entry in a key/value metadata list; mirror of {@code KeyValue} in {@code parquet.thrift}.
 *
 * <p>Used both at the file level ({@link FileMetaData#keyValueMetadata()}) and at the column-chunk level
 * ({@link ColumnMetaData#keyValueMetadata()}). The value is optional, mirroring the Thrift {@code optional string}.
 *
 * @param key required metadata key
 * @param value associated value; empty when the key was written without one
 */
public record KeyValue(String key, Optional<String> value) {}
