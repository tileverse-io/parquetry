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
package io.tileverse.parquetry.geotools.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;

import org.geotools.data.nested.NestedType;
import org.geotools.data.nested.NestedType.Field;
import org.geotools.data.nested.NestedType.ListType;
import org.geotools.data.nested.NestedType.MapType;
import org.geotools.data.nested.NestedType.ScalarType;
import org.geotools.data.nested.NestedType.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.catalog.FilesetCatalog;
import io.tileverse.parquetry.dataset.ParquetDataset;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Projects the DuckDB-generated nested GeoParquet fixture's top-level columns onto the {@link NestedType} ADT: scalar
 * leaves are flat, and {@code brand}/{@code addresses}/{@code tags} project to a struct, a list of struct, and a map,
 * with the synthetic {@code list}/{@code element} and {@code key_value}/{@code key}/{@code value} levels dropped.
 */
class NestedTypesTest {

    @Test
    void projectsTopLevelNestedColumns(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("nested.parquet");
        NestedFixtures.writeSample(file);

        try (FilesetCatalog catalog = NestedFixtures.openCatalog(file)) {
            ParquetDataset dataset = catalog.dataset("nested");
            List<SchemaNode> top = dataset.schema().root().children();

            assertScalarLeavesAreFlat(top);
            assertNestedColumnsAreNested(top);
            assertBrandProjection(top);
            assertAddressesProjection(top);
            assertTagsProjection(top);
        }
    }

    private void assertScalarLeavesAreFlat(List<SchemaNode> top) {
        assertThat(NestedTypes.isNested(node(top, "id"))).isFalse();
        assertThat(NestedTypes.isNested(node(top, "geometry"))).isFalse();
    }

    private void assertNestedColumnsAreNested(List<SchemaNode> top) {
        assertThat(NestedTypes.isNested(node(top, "brand"))).isTrue();
        assertThat(NestedTypes.isNested(node(top, "addresses"))).isTrue();
        assertThat(NestedTypes.isNested(node(top, "tags"))).isTrue();
    }

    private void assertBrandProjection(List<SchemaNode> top) {
        NestedType expected = new StructType(List.of(new Field("name", new ScalarType(String.class))));
        assertThat(NestedTypes.of(node(top, "brand"))).isEqualTo(expected);
    }

    private void assertAddressesProjection(List<SchemaNode> top) {
        NestedType expected = new ListType(new StructType(List.of(
                new Field("locality", new ScalarType(String.class)),
                new Field("postcode", new ScalarType(String.class)))));
        assertThat(NestedTypes.of(node(top, "addresses"))).isEqualTo(expected);
    }

    private void assertTagsProjection(List<SchemaNode> top) {
        NestedType expected = new MapType(new ScalarType(String.class), new ScalarType(String.class));
        assertThat(NestedTypes.of(node(top, "tags"))).isEqualTo(expected);
    }

    private SchemaNode node(List<SchemaNode> nodes, String name) {
        return nodes.stream()
                .filter(node -> node.name().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no top-level node named '" + name + "'"));
    }
}
