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
package io.tileverse.parquetry.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.format.EdgeInterpolationAlgorithm;
import io.tileverse.parquetry.schema.geo.ParquetCrs;
import io.tileverse.parquetry.testkit.TestCorpus;

class IcebergTableMetadataTest {

    @TempDir
    Path tempDir;

    @Test
    void parsesCurrentSnapshotAndManifestList() throws Exception {
        Path root = TestCorpus.extractDirectory("iceberg-geo-testbed", tempDir);
        Path json = root.resolve("v2_flat_columns/metadata/v1.metadata.json");
        IcebergTableMetadata metadata = IcebergTableMetadata.read(Files.readString(json));

        assertThat(metadata.formatVersion()).isEqualTo(2);
        assertThat(metadata.tableLocation()).isEqualTo("file:///iceberg-geo-testbed/v2_flat_columns");
        assertThat(metadata.currentSnapshotId()).isEqualTo(1700000000000L);
        assertThat(metadata.currentSnapshotTimestampMs()).isEqualTo(1700000000000L);
        assertThat(metadata.manifestListLocation())
                .isEqualTo(
                        "file:///iceberg-geo-testbed/v2_flat_columns/metadata/snap-1700000000000-manifest-list.avro");
        assertThat(metadata.isPartitioned()).isFalse();
    }

    @Test
    void parsesCurrentSchemaFields() throws Exception {
        Path root = TestCorpus.extractDirectory("iceberg-geo-testbed", tempDir);
        Path json = root.resolve("v3_geometry/metadata/v1.metadata.json");
        IcebergTableMetadata metadata = IcebergTableMetadata.read(Files.readString(json));

        Optional<ParquetCrs> crs84 = ParquetCrs.reference("OGC:CRS84");
        assertThat(metadata.fields())
                .containsExactly(
                        new IcebergField(1, "id", "string", false),
                        new IcebergField(2, "geom", "geometry", false, Optional.empty(), crs84, Optional.empty()));
        assertThat(metadata.fields().get(1).isGeometry()).isTrue();
        assertThat(metadata.fields().get(1).isGeography()).isFalse();
    }

    @Test
    void parsesGeographyTokenWithAlgorithm() {
        String json = metadataWithFields("""
                {"id": 1, "name": "shape", "required": false, "type": "geography(OGC:CRS84, karney)"}
                """);

        IcebergTableMetadata metadata = IcebergTableMetadata.read(json);

        IcebergField shape = metadata.fields().get(0);
        assertThat(shape.type()).isEqualTo("geography");
        assertThat(shape.isGeography()).isTrue();
        assertThat(shape.crs()).isEqualTo(ParquetCrs.reference("OGC:CRS84"));
        assertThat(shape.geographyAlgorithm()).isEqualTo(Optional.of(EdgeInterpolationAlgorithm.KARNEY));
    }

    @Test
    void readsGeometryInitialDefaultAgainstTheBareKind() {
        String json = metadataWithFields("""
                {"id": 1, "name": "g", "required": false, "type": "geometry(EPSG:3857)", "initial-default": "x"}
                """);

        assertThatThrownBy(() -> IcebergTableMetadata.read(json))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("geometry")
                .hasMessageNotContaining("EPSG:3857");
    }

    @Test
    void parsesTopLevelSchemaShape() {
        String json = """
                {
                  "format-version": 1,
                  "location": "file:///t",
                  "current-snapshot-id": 1,
                  "snapshots": [
                    {"snapshot-id": 1, "timestamp-ms": 10, "manifest-list": "file:///t/m.avro"}
                  ],
                  "schema": {
                    "type": "struct",
                    "fields": [
                      {"id": 7, "name": "lon", "type": "double"}
                    ]
                  }
                }
                """;
        IcebergTableMetadata metadata = IcebergTableMetadata.read(json);
        assertThat(metadata.fields()).containsExactly(new IcebergField(7, "lon", "double", false));
    }

    @Test
    void skipsComplexTypedFields() {
        String json = """
                {
                  "format-version": 2,
                  "location": "file:///t",
                  "current-snapshot-id": 1,
                  "current-schema-id": 0,
                  "snapshots": [
                    {"snapshot-id": 1, "timestamp-ms": 10, "manifest-list": "file:///t/m.avro"}
                  ],
                  "schemas": [
                    {
                      "schema-id": 0,
                      "fields": [
                        {"id": 1, "name": "id", "type": "long"},
                        {"id": 2, "name": "nested", "type": {"type": "struct", "fields": []}}
                      ]
                    }
                  ]
                }
                """;
        IcebergTableMetadata metadata = IcebergTableMetadata.read(json);
        assertThat(metadata.fields()).containsExactly(new IcebergField(1, "id", "long", false));
    }

    @Test
    void nestedSchemaCapturesAStructColumn() {
        String json = metadataWithFields("""
                {"id": 1, "name": "id", "type": "string", "required": false},
                {"id": 2, "name": "bbox", "required": false,
                 "type": {"type": "struct", "fields": [
                   {"id": 5, "name": "xmin", "required": false, "type": "double"},
                   {"id": 6, "name": "ymin", "required": false, "type": "double"}
                 ]}}
                """);

        IcebergNestedSchema nested = IcebergTableMetadata.read(json).nestedSchema();

        assertThat(nested.isEmpty()).isFalse();
        assertThat(nested.path(5)).isEqualTo("bbox.xmin");
        assertThat(nested.idAt(2, "ymin")).isEqualTo(6);
    }

    @Test
    void nestedSchemaIsEmptyForAFlatTable() {
        String json = metadataWithFields("""
                {"id": 1, "name": "id", "type": "long", "required": true}
                """);

        assertThat(IcebergTableMetadata.read(json).nestedSchema().isEmpty()).isTrue();
    }

    @Test
    void readsRequiredNullability() {
        String json = """
                {
                  "format-version": 2,
                  "location": "file:///t",
                  "current-snapshot-id": 1,
                  "current-schema-id": 0,
                  "snapshots": [
                    {"snapshot-id": 1, "timestamp-ms": 10, "manifest-list": "file:///t/m.avro"}
                  ],
                  "schemas": [
                    {
                      "schema-id": 0,
                      "fields": [
                        {"id": 1, "name": "id", "type": "long", "required": true},
                        {"id": 2, "name": "name", "type": "string", "required": false},
                        {"id": 3, "name": "extra", "type": "double"}
                      ]
                    }
                  ]
                }
                """;
        IcebergTableMetadata metadata = IcebergTableMetadata.read(json);
        assertThat(metadata.fields())
                .containsExactly(
                        new IcebergField(1, "id", "long", true),
                        new IcebergField(2, "name", "string", false),
                        new IcebergField(3, "extra", "double", false));
    }

    @Test
    void parsesIdentityPartitionSpec() {
        String json = """
                {
                  "format-version": 2,
                  "location": "file:///t",
                  "current-snapshot-id": 1,
                  "current-schema-id": 0,
                  "snapshots": [
                    {"snapshot-id": 1, "timestamp-ms": 10, "manifest-list": "file:///t/m.avro"}
                  ],
                  "schemas": [
                    {
                      "schema-id": 0,
                      "fields": [
                        {"id": 1, "name": "id", "type": "long", "required": true},
                        {"id": 2, "name": "category", "type": "string", "required": true}
                      ]
                    }
                  ],
                  "partition-specs": [
                    {
                      "spec-id": 0,
                      "fields": [
                        {"source-id": 2, "field-id": 1000, "name": "category", "transform": "identity"}
                      ]
                    }
                  ]
                }
                """;
        IcebergTableMetadata metadata = IcebergTableMetadata.read(json);

        assertThat(metadata.isPartitioned()).isTrue();
        assertThat(metadata.partitionSpec().byPartitionFieldId(1000))
                .contains(new IcebergPartitionSpec.PartitionField(1000, 2, "category", "identity"));
        assertThat(metadata.partitionSpec().identitySourceFields())
                .containsEntry(2, new IcebergField(2, "category", "string", true));
    }

    @Test
    void parsesV1SingularPartitionSpecWithoutFieldId() {
        String json = """
                {
                  "format-version": 1,
                  "location": "file:///t",
                  "current-snapshot-id": 1,
                  "current-schema-id": 0,
                  "snapshots": [
                    {"snapshot-id": 1, "timestamp-ms": 10, "manifest-list": "file:///t/m.avro"}
                  ],
                  "schemas": [
                    {
                      "schema-id": 0,
                      "fields": [
                        {"id": 1, "name": "id", "type": "long", "required": true},
                        {"id": 2, "name": "category", "type": "string", "required": true}
                      ]
                    }
                  ],
                  "partition-spec": [
                    {"source-id": 2, "name": "category", "transform": "identity"}
                  ]
                }
                """;
        IcebergTableMetadata metadata = IcebergTableMetadata.read(json);

        assertThat(metadata.isPartitioned()).isTrue();
        assertThat(metadata.partitionSpec().byPartitionFieldId(1000))
                .contains(new IcebergPartitionSpec.PartitionField(1000, 2, "category", "identity"));
        assertThat(metadata.partitionSpec().identitySourceFields())
                .containsEntry(2, new IcebergField(2, "category", "string", true));
    }

    @Test
    void parsesColumnDefaultsFromSingleValueSerialization() {
        String json = metadataWithFields("""
                {"id": 1, "name": "id", "type": "long", "required": true},
                {"id": 2, "name": "label", "type": "string", "initial-default": "n/a"},
                {"id": 3, "name": "count", "type": "int", "initial-default": 7},
                {"id": 4, "name": "born", "type": "date", "initial-default": "2024-01-15"},
                {"id": 5, "name": "plain", "type": "string"}
                """);
        IcebergTableMetadata metadata = IcebergTableMetadata.read(json);

        assertThat(metadata.fields())
                .containsExactly(
                        new IcebergField(1, "id", "long", true),
                        new IcebergField(2, "label", "string", false, Optional.of(new Value.StringVal("n/a"))),
                        new IcebergField(3, "count", "int", false, Optional.of(new Value.IntVal(7))),
                        new IcebergField(
                                4, "born", "date", false, Optional.of(new Value.DateVal(LocalDate.of(2024, 1, 15)))),
                        new IcebergField(5, "plain", "string", false));
    }

    @Test
    void rejectsColumnDefaultOfUnsupportedType() {
        String json = metadataWithFields("""
                {"id": 1, "name": "amount", "type": "decimal", "initial-default": "1.50"}
                """);

        assertThatThrownBy(() -> IcebergTableMetadata.read(json))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("amount")
                .hasMessageContaining("decimal");
    }

    @Test
    void rejectsNonIsoDateColumnDefault() {
        String json = metadataWithFields("""
                {"id": 1, "name": "born", "type": "date", "initial-default": "15/01/2024"}
                """);

        assertThatThrownBy(() -> IcebergTableMetadata.read(json))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("born");
    }

    @Test
    void parsesTheNameMappingProperty() {
        String json = metadataWithProperties(
                "{\"schema.name-mapping.default\": \"[{\\\"field-id\\\": 1, \\\"names\\\": [\\\"id\\\"]}]\"}");

        IcebergTableMetadata metadata = IcebergTableMetadata.read(json);

        assertThat(metadata.nameMapping()).isPresent();
        assertThat(metadata.nameMapping().orElseThrow().idFor("id")).hasValue(1);
    }

    @Test
    void hasNoNameMappingWhenThePropertyIsAbsent() {
        String json = metadataWithFields("""
                {"id": 1, "name": "id", "type": "long", "required": true}
                """);

        assertThat(IcebergTableMetadata.read(json).nameMapping()).isEmpty();
    }

    @Test
    void hasNoNameMappingWhenThePropertyIsNull() {
        String json = metadataWithProperties("{\"schema.name-mapping.default\": null}");

        assertThat(IcebergTableMetadata.read(json).nameMapping()).isEmpty();
    }

    @Test
    void rejectsAMalformedNameMappingAtOpen() {
        String json = metadataWithProperties("{\"schema.name-mapping.default\": \"{ not json\"}");

        assertThatThrownBy(() -> IcebergTableMetadata.read(json))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("schema.name-mapping.default");
    }

    @Test
    void rejectsANonStringNameMappingProperty() {
        String json = metadataWithProperties("{\"schema.name-mapping.default\": 42}");

        assertThatThrownBy(() -> IcebergTableMetadata.read(json))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("schema.name-mapping.default");
    }

    private static String metadataWithFields(String fields) {
        return """
                {
                  "format-version": 3,
                  "location": "file:///t",
                  "current-snapshot-id": 1,
                  "current-schema-id": 0,
                  "snapshots": [
                    {"snapshot-id": 1, "timestamp-ms": 10, "manifest-list": "file:///t/m.avro"}
                  ],
                  "schemas": [
                    {
                      "schema-id": 0,
                      "fields": [
                        %s
                      ]
                    }
                  ]
                }
                """.formatted(fields);
    }

    private static String metadataWithProperties(String properties) {
        return """
                {
                  "format-version": 2,
                  "location": "file:///t",
                  "current-snapshot-id": 1,
                  "properties": %s,
                  "snapshots": [
                    {"snapshot-id": 1, "timestamp-ms": 10, "manifest-list": "file:///t/m.avro"}
                  ],
                  "current-schema-id": 0,
                  "schemas": [
                    {
                      "schema-id": 0,
                      "fields": [
                        {"id": 1, "name": "id", "type": "long", "required": true}
                      ]
                    }
                  ]
                }
                """.formatted(properties);
    }

    @Test
    void malformedJsonRejected() {
        assertThatThrownBy(() -> IcebergTableMetadata.read("{ not json")).isInstanceOf(IcebergFormatException.class);
    }

    @Test
    void wrongTypeFieldRejected() {
        assertThatThrownBy(() -> IcebergTableMetadata.read("{\"format-version\":\"two\"}"))
                .isInstanceOf(IcebergFormatException.class);
    }
}
