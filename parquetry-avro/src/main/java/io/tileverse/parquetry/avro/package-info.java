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
/**
 * A clean-room, read-only Avro Object Container File reader. Supports the full Avro schema language (namespaces,
 * recursive types, aliases, field defaults, logical-type annotations) and all six codecs named by the spec
 * ({@code null}, {@code deflate}, {@code snappy}, {@code zstandard}, {@code bzip2}, {@code xz}). Decoded values follow
 * the Java mapping documented on {@link AvroRecord}; reader-schema resolution (projection, defaults, promotions,
 * aliases) is available through {@link AvroDataFileReader#records(AvroSchema)}. Custom schema attributes (for example
 * Iceberg's {@code field-id}) are exposed verbatim for the caller to interpret. Iceberg-agnostic: it knows nothing
 * about manifest semantics. The pre-1.3 container format (magic {@code Obj} followed by a zero byte) is not supported.
 */
package io.tileverse.parquetry.avro;
