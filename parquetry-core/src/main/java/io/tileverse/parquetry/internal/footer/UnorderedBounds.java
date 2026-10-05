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
package io.tileverse.parquetry.internal.footer;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import io.tileverse.parquetry.format.ColumnOrder;
import io.tileverse.parquetry.format.ConvertedType;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.format.SchemaElement;

/**
 * Finds, from the footer alone, the leaf columns with statistics bounds in an order not applied by this reader: a
 * column order unknown to the reader, a column order defined for another physical type, and the legacy {@code INTERVAL}
 * annotation, with an order left undefined by the format. The format asks a reader to ignore the min and max of such a
 * column; the page bounds of its column index follow the same order.
 */
final class UnorderedBounds {

    private UnorderedBounds() {}

    /**
     * The ordinals of the leaf columns with bounds in an order not applied by this reader. Ordinals follow the schema's
     * leaf order, the order of the footer's column orders and of a {@link LeafIndex}. Empty for a footer declaring the
     * type-defined order throughout, the common case.
     */
    static BitSet leavesOf(FileMetaData footer) {
        List<SchemaElement> leaves = leafElements(footer.schema());
        List<ColumnOrder> orders = footer.columnOrders().orElse(List.of());
        BitSet unordered = new BitSet(leaves.size());
        for (int leaf = 0; leaf < leaves.size(); leaf++) {
            if (!boundsOrdered(leaves.get(leaf), orderOf(orders, leaf))) {
                unordered.set(leaf);
            }
        }
        return unordered;
    }

    /** The schema elements without children, in their depth-first order. */
    private static List<SchemaElement> leafElements(List<SchemaElement> schema) {
        List<SchemaElement> leaves = new ArrayList<>(schema.size());
        for (SchemaElement element : schema) {
            if (element.numChildren().isEmpty()) {
                leaves.add(element);
            }
        }
        return leaves;
    }

    /** A footer listing no order for a column leaves it with the type-defined order. */
    private static ColumnOrder orderOf(List<ColumnOrder> orders, int leaf) {
        if (leaf < orders.size()) {
            return orders.get(leaf);
        }
        return new ColumnOrder.TypeDefined();
    }

    private static boolean boundsOrdered(SchemaElement leaf, ColumnOrder order) {
        if (isInterval(leaf)) {
            return false;
        }
        PhysicalType type = leaf.type().orElse(null);
        return switch (order) {
            case ColumnOrder.TypeDefined _ -> true;
            case ColumnOrder.Ieee754TotalOrder _ ->
                type == PhysicalType.FLOAT || type == PhysicalType.DOUBLE || isHalfFloat(leaf);
            case ColumnOrder.Int96TimestampOrder _ -> type == PhysicalType.INT96;
            case ColumnOrder.Unknown _ -> false;
        };
    }

    private static boolean isInterval(SchemaElement leaf) {
        return leaf.convertedType().orElse(null) == ConvertedType.INTERVAL;
    }

    private static boolean isHalfFloat(SchemaElement leaf) {
        return leaf.logicalType().orElse(null) instanceof LogicalType.Float16Type;
    }
}
