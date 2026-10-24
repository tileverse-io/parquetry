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
package io.tileverse.parquetry.schema.geo.geoparquet;

import java.util.Objects;
import java.util.Optional;

import io.tileverse.parquetry.schema.geo.ParquetCrs;
import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem;

/**
 * What a GeoParquet column says about its coordinate reference system.
 *
 * <p>The GeoParquet specification gives the {@code crs} key of a column three readings, and they mean different things:
 *
 * <ul>
 *   <li>{@link #DEFAULTED} - the key is absent. The column uses the spec default, OGC:CRS84 for GeoParquet 1.x.
 *   <li>{@link #UNKNOWN} - the key is present and null. The CRS is undefined or unknown, and no default stands in for
 *       it.
 *   <li>{@link Defined} - the key holds an inline PROJJSON document.
 * </ul>
 *
 * <p>Telling the first two apart matters to a consumer that assigns a CRS. Reading an unknown CRS as OGC:CRS84 states
 * degrees for data that may be in metres, and nothing warns the user. A consumer should assign no CRS for
 * {@link #UNKNOWN} instead.
 */
public sealed interface ColumnCrs {

    /** The column has no {@code crs} key and uses the spec default. */
    ColumnCrs DEFAULTED = new Defaulted();

    /** The column has {@code "crs": null}: the CRS is undefined or unknown. */
    ColumnCrs UNKNOWN = new Unknown();

    /** The inline PROJJSON of this column, empty when the column declares no CRS of its own. */
    Optional<CoordinateReferenceSystem> projjson();

    /**
     * The same declaration as the {@code crs} of a native GEOMETRY or GEOGRAPHY logical type. The native annotation has
     * no null: an absent crs means the OGC:CRS84 default, and {@code srid:0} is the Parquet form of an undefined CRS.
     * Both the reader deriving a logical type from GeoParquet 1.x metadata and the writer annotating a column go
     * through this method, which keeps the two forms of one column in agreement.
     */
    Optional<ParquetCrs> nativeCrs();

    /** Wraps an inline PROJJSON document. */
    static ColumnCrs of(CoordinateReferenceSystem projjson) {
        return new Defined(projjson);
    }

    /** The column has no {@code crs} key. */
    record Defaulted() implements ColumnCrs {

        @Override
        public Optional<CoordinateReferenceSystem> projjson() {
            return Optional.empty();
        }

        @Override
        public Optional<ParquetCrs> nativeCrs() {
            return Optional.empty();
        }
    }

    /** The column has {@code "crs": null}. */
    record Unknown() implements ColumnCrs {

        @Override
        public Optional<CoordinateReferenceSystem> projjson() {
            return Optional.empty();
        }

        @Override
        public Optional<ParquetCrs> nativeCrs() {
            return Optional.of(new ParquetCrs.Srid(0));
        }
    }

    /** The column defines its CRS inline as PROJJSON. */
    record Defined(CoordinateReferenceSystem crs) implements ColumnCrs {

        public Defined {
            Objects.requireNonNull(crs, "crs");
        }

        @Override
        public Optional<CoordinateReferenceSystem> projjson() {
            return Optional.of(crs);
        }

        @Override
        public Optional<ParquetCrs> nativeCrs() {
            return Optional.of(ParquetCrs.inline(crs));
        }
    }
}
