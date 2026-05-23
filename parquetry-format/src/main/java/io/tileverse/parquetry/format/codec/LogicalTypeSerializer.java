/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.format.codec;

import java.io.IOException;
import java.util.Optional;

import io.tileverse.parquetry.format.EdgeInterpolationAlgorithm;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem;
import io.tileverse.parquetry.schema.geo.projjson.ProjJsonModule;

import tools.jackson.databind.json.JsonMapper;

/**
 * Serializer mirror of {@link LogicalTypeDeserializer}.
 *
 * <p>The Thrift {@code LogicalType} union encodes exactly one case; the outer struct carries one struct-typed field
 * whose id is the case tag and whose body is the case payload. Payload-less cases (StringType, MapType, ...) emit an
 * empty struct body.
 */
final class LogicalTypeSerializer {

    /**
     * Cached mapper for PROJJSON write-back on {@code Geometry} / {@code Geography}. Symmetric with the deserializer's
     * mapper - registers {@link ProjJsonModule} so the deserialization round-trip is consistent.
     */
    private static final JsonMapper CRS_MAPPER =
            JsonMapper.builder().addModule(new ProjJsonModule()).build();

    private LogicalTypeSerializer() {}

    static void serialize(CompactProtocolWriter w, LogicalType lt) throws IOException {
        w.writeStructBegin();
        switch (lt) {
            case LogicalType.StringType _ -> writeEmptyVariant(w, (short) 1);
            case LogicalType.MapType _ -> writeEmptyVariant(w, (short) 2);
            case LogicalType.ListType _ -> writeEmptyVariant(w, (short) 3);
            case LogicalType.EnumType _ -> writeEmptyVariant(w, (short) 4);
            case LogicalType.Decimal d -> writeDecimal(w, d);
            case LogicalType.DateType _ -> writeEmptyVariant(w, (short) 6);
            case LogicalType.Time t -> writeTime(w, t);
            case LogicalType.Timestamp ts -> writeTimestamp(w, ts);
            case LogicalType.IntType i -> writeIntType(w, i);
            case LogicalType.UnknownType _ -> writeEmptyVariant(w, (short) 11);
            case LogicalType.JsonType _ -> writeEmptyVariant(w, (short) 12);
            case LogicalType.BsonType _ -> writeEmptyVariant(w, (short) 13);
            case LogicalType.UuidType _ -> writeEmptyVariant(w, (short) 14);
            case LogicalType.Float16Type _ -> writeEmptyVariant(w, (short) 15);
            case LogicalType.VariantStub _ -> writeEmptyVariant(w, (short) 16);
            case LogicalType.Geometry g -> writeGeometry(w, g);
            case LogicalType.Geography g -> writeGeography(w, g);
        }
        w.writeFieldStop();
        w.writeStructEnd();
    }

    /** Writes a payload-less case: case field carrying an empty nested struct. */
    private static void writeEmptyVariant(CompactProtocolWriter w, short variantId) throws IOException {
        w.writeFieldBegin(variantId, CompactType.STRUCT);
        w.writeStructBegin();
        w.writeFieldStop();
        w.writeStructEnd();
    }

    private static void writeDecimal(CompactProtocolWriter w, LogicalType.Decimal d) throws IOException {
        w.writeFieldBegin((short) 5, CompactType.STRUCT);
        w.writeStructBegin();
        w.writeI32Field((short) 1, d.scale());
        w.writeI32Field((short) 2, d.precision());
        w.writeFieldStop();
        w.writeStructEnd();
    }

    private static void writeTime(CompactProtocolWriter w, LogicalType.Time t) throws IOException {
        w.writeFieldBegin((short) 7, CompactType.STRUCT);
        w.writeStructBegin();
        w.writeBoolField((short) 1, t.isAdjustedToUTC());
        writeTimeUnitField(w, (short) 2, t.unit());
        w.writeFieldStop();
        w.writeStructEnd();
    }

    private static void writeTimestamp(CompactProtocolWriter w, LogicalType.Timestamp ts) throws IOException {
        w.writeFieldBegin((short) 8, CompactType.STRUCT);
        w.writeStructBegin();
        w.writeBoolField((short) 1, ts.isAdjustedToUTC());
        writeTimeUnitField(w, (short) 2, ts.unit());
        w.writeFieldStop();
        w.writeStructEnd();
    }

    private static void writeIntType(CompactProtocolWriter w, LogicalType.IntType i) throws IOException {
        w.writeFieldBegin((short) 10, CompactType.STRUCT);
        w.writeStructBegin();
        // IntType.bitWidth is declared as i8 in parquet.thrift but the deserializer reads it as i32; keep the
        // serializer symmetric so the wire bytes round-trip identically.
        w.writeI32Field((short) 1, i.bitWidth());
        w.writeBoolField((short) 2, i.isSigned());
        w.writeFieldStop();
        w.writeStructEnd();
    }

    /** Writes the GeometryType struct (LogicalType field 17) per parquet.thrift. */
    private static void writeGeometry(CompactProtocolWriter w, LogicalType.Geometry g) throws IOException {
        w.writeFieldBegin((short) 17, CompactType.STRUCT);
        w.writeStructBegin();
        Optional<CoordinateReferenceSystem> crs = g.crs();
        if (crs.isPresent()) {
            w.writeStringField((short) 1, encodeCrs(crs.get()));
        }
        w.writeFieldStop();
        w.writeStructEnd();
    }

    /** Writes the GeographyType struct (LogicalType field 18) per parquet.thrift. */
    private static void writeGeography(CompactProtocolWriter w, LogicalType.Geography g) throws IOException {
        w.writeFieldBegin((short) 18, CompactType.STRUCT);
        w.writeStructBegin();
        Optional<CoordinateReferenceSystem> crs = g.crs();
        if (crs.isPresent()) {
            w.writeStringField((short) 1, encodeCrs(crs.get()));
        }
        Optional<EdgeInterpolationAlgorithm> algorithm = g.algorithm();
        if (algorithm.isPresent()) {
            w.writeI32Field((short) 2, algorithm.get().value());
        }
        w.writeFieldStop();
        w.writeStructEnd();
    }

    /**
     * Renders a typed {@link CoordinateReferenceSystem} back to PROJJSON via {@link ProjJsonModule}'s symmetric
     * serializer. Each concrete CRS record carries its {@code "type"} discriminator on output, so the resulting JSON
     * round-trips back to the same concrete record through the matching deserializer.
     */
    private static String encodeCrs(CoordinateReferenceSystem crs) {
        return CRS_MAPPER.writeValueAsString(crs);
    }

    private static void writeTimeUnitField(CompactProtocolWriter w, short fieldId, LogicalType.TimeUnit unit)
            throws IOException {
        w.writeFieldBegin(fieldId, CompactType.STRUCT);
        // TimeUnit union: one field set with the unit's case id, payload is an empty struct.
        w.writeStructBegin();
        w.writeFieldBegin((short) unit.fieldId(), CompactType.STRUCT);
        w.writeStructBegin();
        w.writeFieldStop();
        w.writeStructEnd();
        w.writeFieldStop();
        w.writeStructEnd();
    }
}
