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
package io.tileverse.parquetry.filter.eval;

import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Accessor for a single assembled record's column values, as seen by the record-level filter evaluator. The boxed
 * return is one of: Boolean / Integer / Long / Float / Double / String / java.nio.ByteBuffer (for binary) /
 * java.time.LocalDate / java.time.LocalDateTime, or {@code null} when the column is NULL for this row.
 */
@FunctionalInterface
public interface RecordAccessor {

    Object value(ColumnPath path);
}
