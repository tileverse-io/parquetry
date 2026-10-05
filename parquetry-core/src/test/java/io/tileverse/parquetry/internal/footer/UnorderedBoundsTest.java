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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.ColumnOrder;
import io.tileverse.parquetry.format.CompressionCodec;
import io.tileverse.parquetry.format.FieldRepetitionType;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.format.SchemaElement;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.SchemaBuilder;

/**
 * Covers how the footer's column orders map to leaf columns of a nested schema. The orders follow the leaves in schema
 * order, skipping the groups, and land on the ordinals of a {@link LeafIndex}.
 */
class UnorderedBoundsTest {

    private static final ColumnPath CITY = ColumnPath.of("address", "city");
    private static final ColumnPath ZIP = ColumnPath.of("address", "zip");
    private static final ColumnPath PRICE = ColumnPath.of("price");

    /** {@code schema { address { city, zip }, price }}: two leaves inside a group, then a top-level leaf. */
    private static final List<SchemaElement> NESTED_SCHEMA = List.of(
            group("schema", 2),
            group("address", 2),
            leaf("city", PhysicalType.BYTE_ARRAY),
            leaf("zip", PhysicalType.INT32),
            leaf("price", PhysicalType.DOUBLE));

    @Test
    void unknownOrderOnALeafAfterAGroupMarksThatLeafAlone() {
        FileMetaData footer = footer(List.of(typeDefined(), typeDefined(), unknown()));
        LeafIndex leaves = LeafIndex.of(SchemaBuilder.build(NESTED_SCHEMA));

        BitSet unordered = UnorderedBounds.leavesOf(footer);

        assertThat(unordered.stream().boxed().toList()).containsExactly(leaves.ordinalOf(PRICE));
    }

    @Test
    void unknownOrderOnALeafInsideAGroupMarksThatLeafAlone() {
        FileMetaData footer = footer(List.of(typeDefined(), unknown(), typeDefined()));
        LeafIndex leaves = LeafIndex.of(SchemaBuilder.build(NESTED_SCHEMA));

        BitSet unordered = UnorderedBounds.leavesOf(footer);

        assertThat(unordered.stream().boxed().toList()).containsExactly(leaves.ordinalOf(ZIP));
    }

    @Test
    void encodedChunksReportTheOrderOfTheirOwnLeaf() {
        FileMetaData footer = footer(List.of(typeDefined(), typeDefined(), unknown()));
        LeafIndex leaves = LeafIndex.of(SchemaBuilder.build(NESTED_SCHEMA));

        CompactFooter compact = CompactFooter.encode(footer, leaves);

        assertThat(compact.chunk(0, leaves.ordinalOf(CITY)).boundsOrdered()).isTrue();
        assertThat(compact.chunk(0, leaves.ordinalOf(ZIP)).boundsOrdered()).isTrue();
        assertThat(compact.chunk(0, leaves.ordinalOf(PRICE)).boundsOrdered()).isFalse();
    }

    private static FileMetaData footer(List<ColumnOrder> orders) {
        List<ColumnChunk> chunks = new ArrayList<>();
        chunks.add(chunk(List.of("address", "city"), PhysicalType.BYTE_ARRAY));
        chunks.add(chunk(List.of("address", "zip"), PhysicalType.INT32));
        chunks.add(chunk(List.of("price"), PhysicalType.DOUBLE));
        RowGroup rowGroup = RowGroup.builder().columns(chunks).numRows(1L).build();
        return FileMetaData.builder()
                .version(2)
                .schema(NESTED_SCHEMA)
                .numRows(1L)
                .rowGroups(List.of(rowGroup))
                .columnOrders(Optional.of(orders))
                .build();
    }

    private static ColumnChunk chunk(List<String> pathInSchema, PhysicalType type) {
        ColumnMetaData meta = ColumnMetaData.builder()
                .type(type)
                .codec(CompressionCodec.UNCOMPRESSED)
                .pathInSchema(pathInSchema)
                .numValues(1L)
                .totalCompressedSize(1L)
                .dataPageOffset(4L)
                .build();
        return ColumnChunk.builder().metaData(Optional.of(meta)).build();
    }

    private static ColumnOrder typeDefined() {
        return new ColumnOrder.TypeDefined();
    }

    private static ColumnOrder unknown() {
        return new ColumnOrder.Unknown((short) 7);
    }

    private static SchemaElement group(String name, int children) {
        return new SchemaElement(
                Optional.empty(),
                OptionalInt.empty(),
                Optional.of(FieldRepetitionType.REQUIRED),
                name,
                OptionalInt.of(children),
                Optional.empty(),
                OptionalInt.empty(),
                OptionalInt.empty(),
                Optional.empty(),
                OptionalInt.empty());
    }

    private static SchemaElement leaf(String name, PhysicalType type) {
        return new SchemaElement(
                Optional.of(type),
                OptionalInt.empty(),
                Optional.of(FieldRepetitionType.OPTIONAL),
                name,
                OptionalInt.empty(),
                Optional.empty(),
                OptionalInt.empty(),
                OptionalInt.empty(),
                Optional.empty(),
                OptionalInt.empty());
    }
}
