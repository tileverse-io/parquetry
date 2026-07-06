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
package io.tileverse.parquetry.schema;

import java.util.Optional;

import io.tileverse.parquetry.format.LogicalType;

/**
 * The shape of a Parquet group node, classified from its annotation and repetition. The read path (record assembly) and
 * the write path (column accumulation) share this single classification.
 */
public enum GroupKind {
    STRUCT,
    LIST,
    MAP,
    VARIANT;

    /**
     * Classifies {@code group} by its logical-type annotation first, then by repetition. A Variant annotation is a
     * Variant; an explicit list or map annotation is a list or map; a bare repeated group with no annotation is a
     * legacy two-level list; anything else is a struct.
     */
    public static GroupKind of(SchemaNode.Group group) {
        Optional<LogicalType> annotation = group.logicalType();
        if (annotation.isPresent()) {
            LogicalType logicalType = annotation.get();
            if (logicalType instanceof LogicalType.Variant) {
                return VARIANT;
            }
            if (logicalType instanceof LogicalType.ListType) {
                return LIST;
            }
            if (logicalType instanceof LogicalType.MapType) {
                return MAP;
            }
        }
        if (group.repetition() == Repetition.REPEATED) {
            return LIST;
        }
        return STRUCT;
    }
}
