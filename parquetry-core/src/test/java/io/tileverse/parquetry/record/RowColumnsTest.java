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
package io.tileverse.parquetry.record;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.IntVector;
import io.tileverse.parquetry.batch.Validity;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class RowColumnsTest {

    private static ParquetSchema schemaOf(String... topLevelInts) {
        List<SchemaNode> children = new ArrayList<>();
        for (String name : topLevelInts) {
            children.add(new SchemaNode.Primitive(
                    name, Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1));
        }
        return new ParquetSchema(new SchemaNode.Group("root", Repetition.REQUIRED, children, Optional.empty(), -1));
    }

    private static IntVector ints(int... values) {
        return IntVector.materialized(values, Validity.allValid(values.length));
    }

    @Test
    void ordersColumnsByGroupChildOrderNotMapOrder() {
        ParquetSchema schema = schemaOf("a", "b", "c");
        IntVector va = ints(1);
        IntVector vb = ints(2);
        IntVector vc = ints(3);
        // deliberately out-of-order map; ordering must come from the schema, not the map
        Map<ColumnPath, ColumnVector> columns = new LinkedHashMap<>();
        columns.put(ColumnPath.of("c"), vc);
        columns.put(ColumnPath.of("a"), va);
        columns.put(ColumnPath.of("b"), vb);

        RowColumns rc = RowColumns.of(schema, schema.root(), columns);

        assertThat(rc.columnCount()).isEqualTo(3);
        assertThat(rc.path(0)).isEqualTo(ColumnPath.of("a"));
        assertThat(rc.path(1)).isEqualTo(ColumnPath.of("b"));
        assertThat(rc.path(2)).isEqualTo(ColumnPath.of("c"));
        assertThat(rc.vector(0)).isSameAs(va);
        assertThat(rc.vector(2)).isSameAs(vc);
        assertThat(rc.indexOf(ColumnPath.of("b"))).isEqualTo(1);
        assertThat(rc.indexOf(ColumnPath.of("missing"))).isEqualTo(-1);
        assertThat(rc.schema()).isSameAs(schema);
        assertThat(rc.group()).isSameAs(schema.root());
    }

    @Test
    void absentProjectedColumnYieldsNullVector() {
        ParquetSchema schema = schemaOf("a", "b");
        IntVector va = ints(1);

        RowColumns rc = RowColumns.of(schema, schema.root(), Map.of(ColumnPath.of("a"), va));

        assertThat(rc.columnCount()).isEqualTo(2);
        assertThat(rc.vector(0)).isSameAs(va);
        assertThat(rc.vector(1))
                .as("projected-away or all-null child has no vector")
                .isNull();
        assertThat(rc.indexOf(ColumnPath.of("b"))).isEqualTo(1);
    }
}
