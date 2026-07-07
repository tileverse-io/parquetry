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
package io.tileverse.parquetry.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.geo.ParquetCrs;
import io.tileverse.parquetry.testkit.TestCorpus;

import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

class IcebergCrsPropagationIT {

    private static final String TABLE = "v3_geometry";

    @TempDir
    Path tempDir;

    @Test
    void bareGeometryTokenPresentsCrs84() {
        Path tableDir = corpusTable();
        Optional<LogicalType> geometryLogicalType = geometryLeafLogicalType(tableDir);
        assertThat(geometryLogicalType)
                .contains(new LogicalType.Geometry(Optional.of(new ParquetCrs.AuthorityCode("OGC", "CRS84"))));
    }

    @Test
    void parameterizedGeometryTokenOpensAndPresentsThatCrs() {
        Path tableDir = corpusTable();
        IcebergSchemaEvolution.evolveCurrentSchemaRaw(
                tableDir, List.of(rawField(1, "id", "string"), rawField(2, "geom", "geometry(EPSG:3857)")));

        Optional<LogicalType> geometryLogicalType = geometryLeafLogicalType(tableDir);
        assertThat(geometryLogicalType)
                .contains(new LogicalType.Geometry(Optional.of(new ParquetCrs.AuthorityCode("EPSG", "3857"))));
    }

    private Path corpusTable() {
        Path root = TestCorpus.extractDirectory("iceberg-geo-testbed", tempDir.resolve(TABLE));
        return root.resolve(TABLE);
    }

    private static Optional<LogicalType> geometryLeafLogicalType(Path tableDir) {
        try (IcebergTableCatalog catalog = IcebergTableCatalog.openLocal(tableDir, IcebergOptions.defaults())) {
            IcebergDataset dataset = (IcebergDataset) catalog.dataset(TABLE);
            SchemaNode.Primitive geometry = dataset.schema().root().children().stream()
                    .filter(child -> child.name().equals("geom"))
                    .map(SchemaNode.Primitive.class::cast)
                    .findFirst()
                    .orElseThrow();
            return geometry.logicalType();
        }
    }

    private static ObjectNode rawField(int id, String name, String type) {
        ObjectNode field = JsonMapper.shared().createObjectNode();
        field.put("id", id);
        field.put("name", name);
        field.put("required", false);
        field.put("type", type);
        return field;
    }
}
