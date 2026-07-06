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
package io.tileverse.parquetry.format.codec;

import java.io.IOException;
import java.util.OptionalDouble;

import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.format.ParquetFormatException;

/**
 * Deserializer for the Thrift {@code BoundingBox} struct.
 *
 * <pre>
 * struct BoundingBox {
 *   1: required double xmin
 *   2: required double xmax
 *   3: required double ymin
 *   4: required double ymax
 *   5: optional double zmin
 *   6: optional double zmax
 *   7: optional double mmin
 *   8: optional double mmax
 * }
 * </pre>
 */
final class BoundingBoxDeserializer {

    private BoundingBoxDeserializer() {}

    static BoundingBox read(CompactProtocolReader r) throws IOException {
        Double xmin = null;
        Double xmax = null;
        Double ymin = null;
        Double ymax = null;
        OptionalDouble zmin = OptionalDouble.empty();
        OptionalDouble zmax = OptionalDouble.empty();
        OptionalDouble mmin = OptionalDouble.empty();
        OptionalDouble mmax = OptionalDouble.empty();
        int lastFieldId = 0;
        while (true) {
            FieldHeader fh = r.readFieldHeader(lastFieldId);
            if (fh.isStop()) {
                break;
            }
            lastFieldId = fh.fieldId();
            switch (fh.fieldId()) {
                case 1 -> xmin = r.readDouble();
                case 2 -> xmax = r.readDouble();
                case 3 -> ymin = r.readDouble();
                case 4 -> ymax = r.readDouble();
                case 5 -> zmin = OptionalDouble.of(r.readDouble());
                case 6 -> zmax = OptionalDouble.of(r.readDouble());
                case 7 -> mmin = OptionalDouble.of(r.readDouble());
                case 8 -> mmax = OptionalDouble.of(r.readDouble());
                default -> r.skipField(fh.type());
            }
        }
        return new BoundingBox(
                require(xmin, "xmin"),
                require(xmax, "xmax"),
                require(ymin, "ymin"),
                require(ymax, "ymax"),
                zmin,
                zmax,
                mmin,
                mmax);
    }

    private static double require(Double value, String fieldName) {
        if (value == null) {
            throw new ParquetFormatException("BoundingBox is missing required field '" + fieldName + "'");
        }
        return value;
    }
}
