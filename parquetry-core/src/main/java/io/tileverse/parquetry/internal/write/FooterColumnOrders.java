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
package io.tileverse.parquetry.internal.write;

import java.util.ArrayList;
import java.util.List;

import io.tileverse.parquetry.data.WriteOptions.FloatColumnOrder;
import io.tileverse.parquetry.format.ColumnOrder;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * The column orders declared in the footer of a written file: for each leaf column, the order of its min and max
 * statistics and of the page bounds of its column index. A reader disregards those statistics for a column without a
 * declared order.
 */
public final class FooterColumnOrders {

    private FooterColumnOrders() {}

    /**
     * The column orders of the leaves of {@code schema}, in schema order: IEEE 754 total order for the columns taking
     * it under {@code floatOrder}, the type-defined order for the others.
     */
    public static List<ColumnOrder> of(ParquetSchema schema, FloatColumnOrder floatOrder) {
        List<ColumnPath> leaves = schema.leafColumns();
        List<ColumnOrder> orders = new ArrayList<>(leaves.size());
        for (ColumnPath leaf : leaves) {
            SchemaNode.Primitive column = leafNode(schema, leaf);
            orders.add(columnOrderOf(column, floatOrder));
        }
        return orders;
    }

    private static ColumnOrder columnOrderOf(SchemaNode.Primitive leaf, FloatColumnOrder floatOrder) {
        if (BoundsOrder.of(leaf, floatOrder).isTotalOrder()) {
            return new ColumnOrder.Ieee754TotalOrder();
        }
        return new ColumnOrder.TypeDefined();
    }

    private static SchemaNode.Primitive leafNode(ParquetSchema schema, ColumnPath leaf) {
        if (schema.find(leaf).orElseThrow() instanceof SchemaNode.Primitive column) {
            return column;
        }
        throw new IllegalStateException(leaf.dot() + " is not a leaf column");
    }
}
