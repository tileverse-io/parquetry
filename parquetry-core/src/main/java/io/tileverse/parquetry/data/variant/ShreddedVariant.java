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
package io.tileverse.parquetry.data.variant;

import java.util.Map;

import io.tileverse.parquetry.schema.SchemaNode;

/** The shredded shape of a Parquet Variant column's typed_value subtree, classified once from the schema. */
public sealed interface ShreddedVariant
        permits ShreddedVariant.Scalar, ShreddedVariant.ShreddedObject, ShreddedVariant.ShreddedArray {

    /** A scalar shredding: the typed_value leaf maps to one Variant primitive type id. */
    record Scalar(SchemaNode.Primitive typedValue, int variantTypeId) implements ShreddedVariant {}

    /** An object shredding: one shredded field per entry, each a value/typed_value node. */
    record ShreddedObject(Map<String, Field> fields) implements ShreddedVariant {}

    /** An array shredding: the element is a value/typed_value node. */
    record ShreddedArray(Field element) implements ShreddedVariant {}

    /** One value/typed_value node: an optional unshredded leaf plus an optional nested shredding (each may be null). */
    record Field(SchemaNode.Primitive value, ShreddedVariant typedValue) {}

    static ShreddedVariant classify(SchemaNode.Group variantGroup) {
        return ShreddedVariantClassifier.classify(variantGroup);
    }
}
