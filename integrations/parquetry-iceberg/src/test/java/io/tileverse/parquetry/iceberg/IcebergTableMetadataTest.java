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
package io.tileverse.parquetry.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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

        assertThat(metadata.fields())
                .containsExactly(new IcebergField(1, "id", "string"), new IcebergField(2, "geom", "geometry"));
        assertThat(metadata.fields().get(1).isGeometry()).isTrue();
        assertThat(metadata.fields().get(1).isGeography()).isFalse();
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
        assertThat(metadata.fields()).containsExactly(new IcebergField(7, "lon", "double"));
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
        assertThat(metadata.fields()).containsExactly(new IcebergField(1, "id", "long"));
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
