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
package io.tileverse.parquetry.schema.geo;

import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem;
import io.tileverse.parquetry.schema.geo.projjson.Identifier;

/**
 * The {@code crs} property of a native Parquet {@code GEOMETRY} / {@code GEOGRAPHY} logical type.
 *
 * <p>GeoParquet 2.0 makes the native Parquet {@code crs} property the source of truth and lets it take several forms
 * (the GeoParquet {@code geo} file metadata {@code crs}, in contrast, is always inline PROJJSON or {@code null}). This
 * ADT models the forms a reader must interpret:
 *
 * <ul>
 *   <li>{@link Inline} - an embedded PROJJSON document.
 *   <li>{@link AuthorityCode} - an {@code <authority>:<code>} reference such as {@code EPSG:3857} or {@code OGC:CRS84}.
 *   <li>{@link Srid} - a {@code srid:<id>} reference; {@code srid:0} denotes an undefined CRS.
 *   <li>{@link ProjjsonKey} - a {@code projjson:<key>} reference into a registry the reader resolves out of band.
 * </ul>
 *
 * <p>Readers must interpret inline PROJJSON and {@code <authority>:<code>}; {@code srid:} and {@code projjson:} are
 * tolerated. A blank, malformed, or otherwise unclassifiable native {@code crs} string degrades to
 * {@link Optional#empty()} (see {@link #reference(String)}), leaving the column readable and the consumer free to fall
 * back to the GeoParquet spec default (typically OGC:CRS84).
 *
 * <p>{@link #epsgCode()} exposes the EPSG SRID derivable without a registry lookup; resolving an arbitrary authority or
 * {@code projjson:} key to a full coordinate reference system is left to a CRS engine in an integration module.
 */
public sealed interface ParquetCrs
        permits ParquetCrs.Inline, ParquetCrs.AuthorityCode, ParquetCrs.Srid, ParquetCrs.ProjjsonKey {

    String SRID_PREFIX = "srid:";

    String PROJJSON_PREFIX = "projjson:";

    /**
     * The EPSG code this CRS denotes when derivable without a registry lookup, otherwise empty. Empty for non-EPSG
     * authorities, {@code srid:0}, and {@code projjson:} references.
     */
    OptionalInt epsgCode();

    /** Wraps an inline PROJJSON coordinate reference system as a native Parquet {@code crs}. */
    static ParquetCrs inline(CoordinateReferenceSystem projjson) {
        return new Inline(projjson);
    }

    /**
     * Classifies a non-PROJJSON native {@code crs} string into its reference form. Returns {@link Optional#empty()} for
     * a string that matches no recognized form (the lenient degrade path); callers handle inline PROJJSON before
     * reaching here.
     */
    static Optional<ParquetCrs> reference(String raw) {
        String value = raw.strip();
        if (value.isEmpty()) {
            return Optional.empty();
        }
        String lower = value.toLowerCase(Locale.ROOT);
        if (lower.startsWith(SRID_PREFIX)) {
            return parseSrid(value.substring(SRID_PREFIX.length()));
        }
        if (lower.startsWith(PROJJSON_PREFIX)) {
            String key = value.substring(PROJJSON_PREFIX.length()).strip();
            return key.isEmpty() ? Optional.empty() : Optional.of(new ProjjsonKey(key));
        }
        int colon = value.indexOf(':');
        boolean hasAuthorityAndCode = colon > 0 && colon < value.length() - 1;
        if (hasAuthorityAndCode) {
            return Optional.of(new AuthorityCode(value.substring(0, colon), value.substring(colon + 1)));
        }
        return Optional.empty();
    }

    private static Optional<ParquetCrs> parseSrid(String digits) {
        try {
            return Optional.of(new Srid(Long.parseLong(digits.strip())));
        } catch (NumberFormatException notANumber) {
            return Optional.empty();
        }
    }

    /** An embedded PROJJSON coordinate reference system. */
    record Inline(CoordinateReferenceSystem projjson) implements ParquetCrs {

        public Inline {
            Objects.requireNonNull(projjson, "projjson");
        }

        @Override
        public OptionalInt epsgCode() {
            return projjson.id().map(Identifier::epsgCode).orElse(OptionalInt.empty());
        }
    }

    /** An {@code <authority>:<code>} reference, e.g. {@code EPSG:3857} or {@code OGC:CRS84}. */
    record AuthorityCode(String authority, String code) implements ParquetCrs {

        public AuthorityCode {
            Objects.requireNonNull(authority, "authority");
            Objects.requireNonNull(code, "code");
        }

        @Override
        public OptionalInt epsgCode() {
            if (!"EPSG".equalsIgnoreCase(authority)) {
                return OptionalInt.empty();
            }
            try {
                int parsed = Integer.parseInt(code.strip());
                return parsed >= 0 ? OptionalInt.of(parsed) : OptionalInt.empty();
            } catch (NumberFormatException notANumber) {
                return OptionalInt.empty();
            }
        }
    }

    /** A {@code srid:<id>} reference; {@code srid:0} denotes an undefined CRS. */
    record Srid(long id) implements ParquetCrs {

        @Override
        public OptionalInt epsgCode() {
            boolean usableAsEpsg = id > 0 && id <= Integer.MAX_VALUE;
            return usableAsEpsg ? OptionalInt.of((int) id) : OptionalInt.empty();
        }
    }

    /** A {@code projjson:<key>} reference into a registry the reader resolves out of band. */
    record ProjjsonKey(String key) implements ParquetCrs {

        public ProjjsonKey {
            Objects.requireNonNull(key, "key");
        }

        @Override
        public OptionalInt epsgCode() {
            return OptionalInt.empty();
        }
    }
}
