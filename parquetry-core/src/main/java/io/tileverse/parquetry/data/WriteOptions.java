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
package io.tileverse.parquetry.data;

import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import io.tileverse.parquetry.observe.WriteObserver;
import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem;
import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystems;

import lombok.NonNull;

/**
 * Immutable configuration for the parquetry write engine.
 *
 * <p>Aggregates the typed configuration -- {@link ParquetVersion}, {@link RowGroupSize}, {@link Compression},
 * {@link EncodingPolicy}, {@link BloomFilterConfig}, {@link GeoParquetMetadataMode}, {@link WriteObserver} -- and
 * exposes per-column override maps for compression, encoding, bloom filters, and CRS.
 *
 * <p>The five write-only value types ({@link ParquetVersion}, {@link GeoParquetMetadataMode}, {@link RowGroupSize},
 * {@link EncodingPolicy}, {@link BloomFilterConfig}) are nested here to keep the {@code dataset} package surface
 * focused on the entry points. {@link Compression} stays at the package root because the read pipeline consumes it too.
 *
 * <p>Construct via {@link #defaults()} for the project defaults, or via {@link #builder()} to override individual
 * settings. The {@link Builder} returns an immutable record; subsequent mutations on the builder do not affect
 * previously built instances.
 *
 * @param parquetVersion page-format version selected for the file
 * @param geoParquetMetadata GeoParquet metadata emission mode
 * @param rowGroupSize row group sizing policy
 * @param expectedRowCount caller-supplied row count hint used by {@link RowGroupSize.Adaptive}
 * @param pageValueLimit per-page value-count cap
 * @param pageByteLimit per-page uncompressed-byte cap
 * @param dictionaryByteLimit per-column-chunk dictionary byte budget; overflow triggers PLAIN fallback
 * @param defaultCompression codec used for any column without a per-column override
 * @param compression per-column compression overrides keyed by dotted column path; unmodifiable
 * @param encodingPolicies per-column encoding policy overrides keyed by dotted column path; unmodifiable
 * @param indexedColumns columns flagged for bloom filter emission; unmodifiable
 * @param bloomFilters per-column explicit bloom filter configs; unmodifiable
 * @param crs per-column CRS overrides keyed by dotted column path; unmodifiable
 * @param keyValueMetadata caller-supplied file-level key/value metadata merged into the footer alongside the
 *     writer-managed GeoParquet entry; the reserved {@code "geo"} key is rejected; unmodifiable
 * @param tempDir working directory for column-chunk temp files
 * @param writeObserver write-observability callback target; defaults to {@link WriteObserver#NONE}
 * @param writeObserverCadenceRows row cadence between {@link WriteObserver#onRowsWritten(long)} callbacks
 */
public record WriteOptions(
        @NonNull ParquetVersion parquetVersion,
        @NonNull GeoParquetMetadataMode geoParquetMetadata,
        @NonNull RowGroupSize rowGroupSize,
        @NonNull OptionalLong expectedRowCount,
        int pageValueLimit,
        int pageByteLimit,
        int dictionaryByteLimit,
        @NonNull Compression defaultCompression,
        @NonNull Map<String, Compression> compression,
        @NonNull Map<String, EncodingPolicy> encodingPolicies,
        @NonNull Set<String> indexedColumns,
        @NonNull Map<String, BloomFilterConfig> bloomFilters,
        @NonNull Map<String, CoordinateReferenceSystem> crs,
        @NonNull Map<String, String> keyValueMetadata,
        @NonNull Path tempDir,
        @NonNull WriteObserver writeObserver,
        long writeObserverCadenceRows) {

    private static final String RESERVED_GEO_KEY = "geo";

    /**
     * Maximum accepted row-group byte size: 2^31 minus 2^20, below 2^31 with roughly 1 MiB of headroom for buffer slack
     * on top of the accumulated tally. Per-row-group byte counts and buffer sizes stay within an int.
     */
    public static final long MAX_ROW_GROUP_BYTES_LIMIT = (1L << 31) - (1L << 20);

    public WriteOptions {
        if (pageValueLimit <= 0) {
            throw new IllegalArgumentException("pageValueLimit must be positive: " + pageValueLimit);
        }
        if (pageByteLimit <= 0) {
            throw new IllegalArgumentException("pageByteLimit must be positive: " + pageByteLimit);
        }
        if (dictionaryByteLimit <= 0) {
            throw new IllegalArgumentException("dictionaryByteLimit must be positive: " + dictionaryByteLimit);
        }
        if (expectedRowCount.isPresent() && expectedRowCount.getAsLong() < 0) {
            throw new IllegalArgumentException(
                    "expectedRowCount must be non-negative: " + expectedRowCount.getAsLong());
        }
        if (writeObserverCadenceRows <= 0) {
            throw new IllegalArgumentException(
                    "writeObserverCadenceRows must be positive: " + writeObserverCadenceRows);
        }
        if (rowGroupSize instanceof RowGroupSize.Bytes(long uncompressedByteThreshold)
                && uncompressedByteThreshold > MAX_ROW_GROUP_BYTES_LIMIT) {
            throw new IllegalArgumentException("rowGroupSize bytes threshold " + uncompressedByteThreshold
                    + " exceeds the maximum row-group byte limit " + MAX_ROW_GROUP_BYTES_LIMIT);
        }
        // V1.1 page format cannot carry the GeoParquet 2.0 native logical types; coerce the metadata mode for
        // coherence.
        if (parquetVersion == ParquetVersion.V1_1 && geoParquetMetadata != GeoParquetMetadataMode.V1_1_ONLY) {
            geoParquetMetadata = GeoParquetMetadataMode.V1_1_ONLY;
        }
        compression = unmodifiableSnapshot(compression);
        encodingPolicies = unmodifiableSnapshot(encodingPolicies);
        bloomFilters = unmodifiableSnapshot(bloomFilters);
        crs = unmodifiableSnapshot(crs);
        indexedColumns = unmodifiableSnapshot(indexedColumns);
        rejectReservedKeys(keyValueMetadata);
        keyValueMetadata = unmodifiableSnapshot(keyValueMetadata);
    }

    /**
     * Returns the project-default options: Parquet 2.0 page format, ZSTD-3, 128 MB adaptive row groups, 64 KB pages,
     * DUAL GeoParquet metadata.
     */
    public static WriteOptions defaults() {
        return builder().build();
    }

    /** Returns a fresh builder pre-populated with the {@link #defaults()} values. */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Parquet page format version the writer emits.
     *
     * <p>{@link #V2_0} uses {@code DATA_PAGE_V2} headers with repetition and definition levels held outside the
     * compressed payload, native logical types ({@code TIMESTAMP}, {@code UUID}, {@code FLOAT16}), and the modern
     * GeoParquet 2.0 geometry encodings. {@link #V1_1} uses {@code DATA_PAGE} headers and the GeoParquet 1.1 WKB-only
     * metadata shape.
     *
     * <p>Default is {@link #V2_0}; V1 is provided for round-tripping against legacy readers.
     */
    public enum ParquetVersion {
        V2_0,
        V1_1
    }

    /**
     * Which GeoParquet metadata flavors the writer emits into the file footer.
     *
     * <p>
     *
     * <ul>
     *   <li>{@link #DUAL_V1_1_AND_V2_0} writes both for maximum compatibility, the default in V2 page mode.
     *   <li>{@link #V2_0_ONLY} writes only the native fields for clean modern files.
     *   <li>{@link #V1_1_ONLY} writes only the legacy GeoParquet 1.1 {@code "geo"} JSON metadata block; this is the
     *       only coherent mode under {@link ParquetVersion#V1_1}.
     * </ul>
     */
    public enum GeoParquetMetadataMode {
        DUAL_V1_1_AND_V2_0,
        V2_0_ONLY,
        V1_1_ONLY
    }

    /**
     * Row group sizing policy.
     *
     * <p>{@link Adaptive} is the default: the writer honours {@link WriteOptions#expectedRowCount()} when set,
     * otherwise targets 128 MB of uncompressed accumulated bytes per row group. {@link Bytes} pins the threshold to a
     * byte budget; {@link Rows} pins it to a row count.
     */
    public sealed interface RowGroupSize permits RowGroupSize.Adaptive, RowGroupSize.Bytes, RowGroupSize.Rows {

        /** Adaptive sizing: honours {@link WriteOptions#expectedRowCount()}; default falls back to 128 MB. */
        static RowGroupSize adaptive() {
            return new Adaptive();
        }

        /** Pin the row group to an uncompressed-byte threshold. */
        static RowGroupSize bytes(long uncompressedByteThreshold) {
            if (uncompressedByteThreshold <= 0) {
                throw new IllegalArgumentException(
                        "uncompressedByteThreshold must be positive: " + uncompressedByteThreshold);
            }
            return new Bytes(uncompressedByteThreshold);
        }

        /** Pin the row group to a row count threshold. */
        static RowGroupSize rows(long rowThreshold) {
            if (rowThreshold <= 0) {
                throw new IllegalArgumentException("rowThreshold must be positive: " + rowThreshold);
            }
            return new Rows(rowThreshold);
        }

        record Adaptive() implements RowGroupSize {}

        record Bytes(long uncompressedByteThreshold) implements RowGroupSize {}

        record Rows(long rowThreshold) implements RowGroupSize {}
    }

    /**
     * Per-column encoding selection policy.
     *
     * <p>{@link Auto} is the default: the writer attempts dictionary encoding and falls back to {@code PLAIN} when
     * cardinality exceeds the dictionary byte budget. The forced variants pin a specific encoder for the entire column:
     *
     * <ul>
     *   <li>{@link ForcePlain} -- never attempt dictionary; emit {@code PLAIN} from the first page (geometry columns
     *       get this implicitly unless overridden).
     *   <li>{@link ForceDictionary} -- never fall back; cardinality overflow surfaces as a write failure.
     *   <li>{@link ForceDelta} -- {@code DELTA_BINARY_PACKED} for integer types; {@code DELTA_LENGTH_BYTE_ARRAY} for
     *       binary types.
     *   <li>{@link ForceByteStreamSplit} -- valid for {@code FLOAT}/{@code DOUBLE} columns only.
     * </ul>
     *
     * <p>Each variant is currently a no-arg record exposed as a singleton constant ({@link #AUTO},
     * {@link #FORCE_PLAIN}, {@link #FORCE_DICTIONARY}, {@link #FORCE_DELTA}, {@link #FORCE_BYTE_STREAM_SPLIT}), which
     * gives the API an enum-like feel. The sealed interface shape is intentional so individual variants can grow
     * parameters later -- for example a future {@code ForceDictionary(int dictionaryByteBudget)} or
     * {@code ForceDelta(int blockSize, int miniBlockSize)} can coexist with the constants without breaking call sites
     * that only need the defaults.
     */
    public sealed interface EncodingPolicy
            permits EncodingPolicy.Auto,
                    EncodingPolicy.ForcePlain,
                    EncodingPolicy.ForceDictionary,
                    EncodingPolicy.ForceDelta,
                    EncodingPolicy.ForceByteStreamSplit {

        /** No-arg singleton. The writer attempts dictionary and falls back to PLAIN on cardinality overflow. */
        EncodingPolicy AUTO = new Auto();

        /** No-arg singleton. Never attempt dictionary; emit PLAIN from the first page. */
        EncodingPolicy FORCE_PLAIN = new ForcePlain();

        /** No-arg singleton. Never fall back from dictionary; cardinality overflow surfaces as a write failure. */
        EncodingPolicy FORCE_DICTIONARY = new ForceDictionary();

        /** No-arg singleton. DELTA_BINARY_PACKED for ints; DELTA_LENGTH_BYTE_ARRAY for binary. */
        EncodingPolicy FORCE_DELTA = new ForceDelta();

        /** No-arg singleton. Valid for FLOAT and DOUBLE columns only. */
        EncodingPolicy FORCE_BYTE_STREAM_SPLIT = new ForceByteStreamSplit();

        record Auto() implements EncodingPolicy {}

        record ForcePlain() implements EncodingPolicy {}

        record ForceDictionary() implements EncodingPolicy {}

        record ForceDelta() implements EncodingPolicy {}

        record ForceByteStreamSplit() implements EncodingPolicy {}
    }

    /**
     * Per-column bloom filter sizing config.
     *
     * @param fpp target false positive probability; must lie in the open interval {@code (0, 1)}
     * @param ndv expected number of distinct values; must be strictly positive
     */
    public record BloomFilterConfig(double fpp, long ndv) {

        public BloomFilterConfig {
            if (fpp <= 0.0 || fpp >= 1.0) {
                throw new IllegalArgumentException("fpp must be in (0, 1): " + fpp);
            }
            if (ndv <= 0) {
                throw new IllegalArgumentException("ndv must be positive: " + ndv);
            }
        }

        /** Defaults used when {@link WriteOptions#indexedColumns()} auto-enables a bloom without an explicit config. */
        public static BloomFilterConfig defaults() {
            return new BloomFilterConfig(0.01, 1_000_000L);
        }
    }

    /**
     * Fluent builder for {@link WriteOptions}.
     *
     * <p>Builder normalization rules:
     *
     * <ul>
     *   <li>{@link #parquetVersion(ParquetVersion)} with {@link ParquetVersion#V1_1} silently forces
     *       {@link GeoParquetMetadataMode#V1_1_ONLY} -- a coherence guarantee, since the GeoParquet 2.0 native logical
     *       types do not exist in V1 page mode. The pin is enforced regardless of call order: once V1_1 is selected,
     *       subsequent {@link #geoParquetMetadata(GeoParquetMetadataMode)} calls are silently coerced back to
     *       {@code V1_1_ONLY}.
     *   <li>{@link #crsEpsg(String, int)} eagerly resolves the code via the curated bundle and throws if missing.
     *   <li>{@link #crs(String, String)} eagerly parses raw PROJJSON so malformed input fails at builder time, not at
     *       write time.
     *   <li>All primitive-limit setters reject non-positive arguments; {@link #expectedRowCount(long)} accepts zero (it
     *       means "no rows expected").
     * </ul>
     */
    public static final class Builder {

        private ParquetVersion parquetVersion = ParquetVersion.V2_0;
        private GeoParquetMetadataMode geoParquetMetadata = GeoParquetMetadataMode.DUAL_V1_1_AND_V2_0;
        private RowGroupSize rowGroupSize = RowGroupSize.adaptive();
        private OptionalLong expectedRowCount = OptionalLong.empty();
        private int pageValueLimit = 8_192;
        private int pageByteLimit = 65_536;
        private int dictionaryByteLimit = 1_048_576;
        private Compression defaultCompression = Compression.zstd(3);
        private final Map<String, Compression> compression = new LinkedHashMap<>();
        private final Map<String, EncodingPolicy> encodingPolicies = new LinkedHashMap<>();
        private final Set<String> indexedColumns = new LinkedHashSet<>();
        private final Map<String, BloomFilterConfig> bloomFilters = new LinkedHashMap<>();
        private final Map<String, CoordinateReferenceSystem> crs = new LinkedHashMap<>();
        private final Map<String, String> keyValueMetadata = new LinkedHashMap<>();
        private Path tempDir = Path.of(System.getProperty("java.io.tmpdir"));
        private WriteObserver writeObserver = WriteObserver.NONE;
        private long writeObserverCadenceRows = 100_000L;

        private Builder() {}

        public Builder parquetVersion(@NonNull ParquetVersion v) {
            this.parquetVersion = v;
            if (v == ParquetVersion.V1_1) {
                this.geoParquetMetadata = GeoParquetMetadataMode.V1_1_ONLY;
            }
            return this;
        }

        public Builder geoParquetMetadata(@NonNull GeoParquetMetadataMode mode) {
            if (parquetVersion == ParquetVersion.V1_1) {
                this.geoParquetMetadata = GeoParquetMetadataMode.V1_1_ONLY;
            } else {
                this.geoParquetMetadata = mode;
            }
            return this;
        }

        public Builder rowGroupSize(@NonNull RowGroupSize size) {
            this.rowGroupSize = size;
            return this;
        }

        public Builder expectedRowCount(long n) {
            if (n < 0) {
                throw new IllegalArgumentException("expectedRowCount must be non-negative: " + n);
            }
            this.expectedRowCount = OptionalLong.of(n);
            return this;
        }

        public Builder pageValueLimit(int n) {
            this.pageValueLimit = requirePositive("pageValueLimit", n);
            return this;
        }

        public Builder pageByteLimit(int n) {
            this.pageByteLimit = requirePositive("pageByteLimit", n);
            return this;
        }

        public Builder dictionaryByteLimit(int n) {
            this.dictionaryByteLimit = requirePositive("dictionaryByteLimit", n);
            return this;
        }

        public Builder defaultCompression(@NonNull Compression codec) {
            this.defaultCompression = codec;
            return this;
        }

        public Builder compression(@NonNull String columnPath, @NonNull Compression codec) {
            this.compression.put(columnPath, codec);
            return this;
        }

        public Builder encodingPolicy(@NonNull String columnPath, @NonNull EncodingPolicy policy) {
            this.encodingPolicies.put(columnPath, policy);
            return this;
        }

        /** Add column paths to the indexed-columns set; accumulates across calls. */
        public Builder indexedColumns(@NonNull String... columnPaths) {
            for (String path : columnPaths) {
                this.indexedColumns.add(path);
            }
            return this;
        }

        public Builder bloomFilter(@NonNull String columnPath, @NonNull BloomFilterConfig cfg) {
            this.bloomFilters.put(columnPath, cfg);
            return this;
        }

        public Builder crs(@NonNull String columnPath, @NonNull CoordinateReferenceSystem crs) {
            this.crs.put(columnPath, crs);
            return this;
        }

        /** Eagerly parses {@code rawProjjson}; throws on malformed input. */
        public Builder crs(@NonNull String columnPath, @NonNull String rawProjjson) {
            CoordinateReferenceSystem parsed = CoordinateReferenceSystems.fromProjjson(rawProjjson);
            this.crs.put(columnPath, parsed);
            return this;
        }

        /** Eagerly resolves via {@code CoordinateReferenceSystems.forEpsg}; throws if the code is not in the bundle. */
        public Builder crsEpsg(@NonNull String columnPath, int epsgCode) {
            Optional<CoordinateReferenceSystem> bundled = CoordinateReferenceSystems.forEpsg(epsgCode);
            if (bundled.isEmpty()) {
                throw new IllegalArgumentException("""
                        EPSG:%d is not in the curated bundle; use crs(col, String) for raw PROJJSON or \
                        crs(col, CoordinateReferenceSystem) for a typed instance""".formatted(epsgCode));
            }
            this.crs.put(columnPath, bundled.get());
            return this;
        }

        /**
         * Adds a caller-managed file-level key/value metadata entry, merged into the footer alongside the
         * writer-managed GeoParquet block. Accumulates across calls; a repeated key overwrites. The reserved
         * {@code "geo"} key is rejected because the writer derives it from the geometry columns.
         */
        public Builder keyValueMetadata(@NonNull String key, @NonNull String value) {
            if (RESERVED_GEO_KEY.equals(key)) {
                throw reservedKeyError();
            }
            this.keyValueMetadata.put(key, value);
            return this;
        }

        /** Adds every entry of {@code entries}; same accumulation and reserved-key rules as the single-entry form. */
        public Builder keyValueMetadata(@NonNull Map<String, String> entries) {
            rejectReservedKeys(entries);
            this.keyValueMetadata.putAll(entries);
            return this;
        }

        public Builder tempDir(@NonNull Path dir) {
            this.tempDir = dir;
            return this;
        }

        public Builder writeObserver(@NonNull WriteObserver observer) {
            this.writeObserver = observer;
            return this;
        }

        public Builder writeObserverCadenceRows(long every) {
            if (every <= 0) {
                throw new IllegalArgumentException("writeObserverCadenceRows must be positive: " + every);
            }
            this.writeObserverCadenceRows = every;
            return this;
        }

        public WriteOptions build() {
            return new WriteOptions(
                    parquetVersion,
                    geoParquetMetadata,
                    rowGroupSize,
                    expectedRowCount,
                    pageValueLimit,
                    pageByteLimit,
                    dictionaryByteLimit,
                    defaultCompression,
                    compression,
                    encodingPolicies,
                    indexedColumns,
                    bloomFilters,
                    crs,
                    keyValueMetadata,
                    tempDir,
                    writeObserver,
                    writeObserverCadenceRows);
        }

        private static int requirePositive(String name, int value) {
            if (value <= 0) {
                throw new IllegalArgumentException(name + " must be positive: " + value);
            }
            return value;
        }
    }

    private static void rejectReservedKeys(Map<String, String> entries) {
        if (entries.containsKey(RESERVED_GEO_KEY)) {
            throw reservedKeyError();
        }
    }

    private static IllegalArgumentException reservedKeyError() {
        return new IllegalArgumentException(
                "'" + RESERVED_GEO_KEY
                        + "' is a reserved key managed by the writer; it is derived from the geometry columns and cannot be set explicitly");
    }

    private static <K, V> Map<K, V> unmodifiableSnapshot(Map<K, V> source) {
        if (source.isEmpty()) {
            return Map.of();
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(source));
    }

    private static <T> Set<T> unmodifiableSnapshot(Set<T> source) {
        if (source.isEmpty()) {
            return Set.of();
        }
        return Collections.unmodifiableSet(new LinkedHashSet<>(source));
    }
}
