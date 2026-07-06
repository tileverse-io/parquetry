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
package io.tileverse.parquetry.filter;

import java.util.Objects;

import io.tileverse.parquetry.schema.ColumnPath;

/**
 * One output column whose value is the same for every row of a read - a constant. Appended to the read result after the
 * physical columns; the leaf's physical kind and logical type are derived from {@link #value()}.
 */
public record ConstantColumn(ColumnPath path, Value value) {

    public ConstantColumn {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(value, "value");
    }
}
