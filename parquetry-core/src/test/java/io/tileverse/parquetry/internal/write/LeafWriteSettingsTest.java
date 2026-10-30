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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.BloomFilterConfig;
import io.tileverse.parquetry.data.WriteOptions.EncodingPolicy;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaNode;

/** The per-column entries of the write options reach a leaf by its dotted path, never by its name alone. */
class LeafWriteSettingsTest {

    private static final ColumnPath NESTED = ColumnPath.of("s", "x");
    private static final SchemaNode.Primitive X = WriteFixtures.requiredLeaf("x", PrimitiveKind.INT32);
    private static final ColumnPath GEOMETRY = ColumnPath.of("geometry");
    private static final SchemaNode.Primitive WKB = WriteFixtures.requiredLeaf("geometry", PrimitiveKind.BYTE_ARRAY);

    @Test
    void policyByDottedPath() {
        WriteOptions options = WriteOptions.builder()
                .encodingPolicy("s.x", EncodingPolicy.FORCE_PLAIN)
                .build();

        LeafWriteSettings settings = LeafWriteSettings.of(options, NESTED, X, 8);

        assertThat(settings.encodingPolicy()).isEqualTo(EncodingPolicy.FORCE_PLAIN);
    }

    @Test
    void leafNameMissesNestedPolicy() {
        WriteOptions options = WriteOptions.builder()
                .encodingPolicy("x", EncodingPolicy.FORCE_PLAIN)
                .build();

        LeafWriteSettings settings = LeafWriteSettings.of(options, NESTED, X, 8);

        assertThat(settings.encodingPolicy()).isEqualTo(EncodingPolicy.AUTO);
    }

    @Test
    void bloomFilterByDottedPath() {
        BloomFilterConfig config = new BloomFilterConfig(0.01, 1_000);
        WriteOptions explicit =
                WriteOptions.builder().bloomFilter("s.x", config).build();
        WriteOptions indexed = WriteOptions.builder().indexedColumns("s.x").build();
        WriteOptions byName = WriteOptions.builder()
                .bloomFilter("x", config)
                .indexedColumns("x")
                .build();

        assertThat(LeafWriteSettings.of(explicit, NESTED, X, 8).bloomFilter()).contains(config);
        assertThat(LeafWriteSettings.of(indexed, NESTED, X, 8).bloomFilter()).contains(BloomFilterConfig.defaults());
        assertThat(LeafWriteSettings.of(byName, NESTED, X, 8).bloomFilter()).isEmpty();
    }

    // A GeoParquet 1.1-only write leaves the annotation out: the CRS entry alone names the geometry column.
    @Test
    void crsEntryMarksGeometry() {
        WriteOptions options = WriteOptions.builder().crsEpsg("geometry", 4326).build();

        LeafWriteSettings settings = LeafWriteSettings.of(options, GEOMETRY, WKB, 8);

        assertThat(settings.geometry()).isTrue();
        assertThat(settings.encodingPolicy()).isEqualTo(EncodingPolicy.FORCE_PLAIN);
        assertThat(settings.boundsOrder()).isEqualTo(BoundsOrder.UNDEFINED);
    }
}
