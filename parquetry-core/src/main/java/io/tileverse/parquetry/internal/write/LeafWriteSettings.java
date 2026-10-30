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

import java.util.Optional;

import io.tileverse.parquetry.data.Compression;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.BloomFilterConfig;
import io.tileverse.parquetry.data.WriteOptions.EncodingPolicy;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.SchemaNode;

import lombok.NonNull;

/**
 * The settings of one leaf column of a write. The per-column entries of {@link WriteOptions} name a leaf by its dotted
 * path, for example {@code s.x}. The name of a nested leaf alone, {@code x}, does not reach it.
 *
 * @param codec the codec of the pages of the leaf and of its column chunk in the footer
 * @param encodingPolicy the encoding policy of the leaf
 * @param bloomFilter the bloom filter of the leaf; empty for none
 * @param pageValueLimit the page cap of the leaf
 * @param boundsOrder the order of the statistics of the leaf
 * @param geometry whether the leaf holds the WKB of a geometry column
 */
record LeafWriteSettings(
        @NonNull Compression codec,
        @NonNull EncodingPolicy encodingPolicy,
        @NonNull Optional<BloomFilterConfig> bloomFilter,
        int pageValueLimit,
        @NonNull BoundsOrder boundsOrder,
        boolean geometry) {

    LeafWriteSettings {
        if (pageValueLimit <= 0) {
            throw new IllegalArgumentException("pageValueLimit must be positive: " + pageValueLimit);
        }
    }

    /**
     * Resolves the settings of {@code leaf} at {@code path}. A leaf named by {@link WriteOptions#crs()} is a geometry
     * column, also when the write leaves out its annotation, as a GeoParquet 1.1-only write does. A geometry column
     * gets no bounds: the byte order of its WKB means nothing to a spatial query.
     */
    static LeafWriteSettings of(
            @NonNull WriteOptions options,
            @NonNull ColumnPath path,
            @NonNull SchemaNode.Primitive leaf,
            int pageValueLimit) {
        String key = path.dot();
        boolean geometry = options.crs().containsKey(key) || hasGeometryAnnotation(leaf);
        Compression codec = options.compression().getOrDefault(key, options.defaultCompression());
        BoundsOrder boundsOrder = geometry ? BoundsOrder.UNDEFINED : BoundsOrder.of(leaf, options.floatColumnOrder());
        return new LeafWriteSettings(
                codec,
                encodingPolicy(options, key, geometry),
                bloomFilter(options, key),
                pageValueLimit,
                boundsOrder,
                geometry);
    }

    /** Whether {@code leaf} is annotated as a geometry or a geography column. */
    static boolean hasGeometryAnnotation(SchemaNode.Primitive leaf) {
        LogicalType logicalType = leaf.logicalType().orElse(null);
        return logicalType instanceof LogicalType.Geometry || logicalType instanceof LogicalType.Geography;
    }

    /**
     * An explicit policy wins. Absent one, a geometry column takes {@link EncodingPolicy#FORCE_PLAIN}: its WKB is
     * effectively unique per row, where a dictionary overflows and never pays for itself. Any other column takes
     * {@link EncodingPolicy#AUTO}.
     */
    private static EncodingPolicy encodingPolicy(WriteOptions options, String key, boolean geometry) {
        EncodingPolicy explicit = options.encodingPolicies().get(key);
        if (explicit != null) {
            return explicit;
        }
        return geometry ? EncodingPolicy.FORCE_PLAIN : EncodingPolicy.AUTO;
    }

    /** An explicit configuration wins. An indexed column without one takes the default configuration. */
    private static Optional<BloomFilterConfig> bloomFilter(WriteOptions options, String key) {
        BloomFilterConfig explicit = options.bloomFilters().get(key);
        if (explicit != null) {
            return Optional.of(explicit);
        }
        if (options.indexedColumns().contains(key)) {
            return Optional.of(BloomFilterConfig.defaults());
        }
        return Optional.empty();
    }
}
