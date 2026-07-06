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

import io.tileverse.parquetry.format.BoundingBox;

/** Serializer mirror of {@link BoundingBoxDeserializer}. */
final class BoundingBoxSerializer {

    private BoundingBoxSerializer() {}

    static void serialize(CompactProtocolWriter w, BoundingBox bb) throws IOException {
        w.writeStructBegin();
        w.writeDoubleField((short) 1, bb.xmin());
        w.writeDoubleField((short) 2, bb.xmax());
        w.writeDoubleField((short) 3, bb.ymin());
        w.writeDoubleField((short) 4, bb.ymax());
        if (bb.zmin().isPresent()) {
            w.writeDoubleField((short) 5, bb.zmin().getAsDouble());
        }
        if (bb.zmax().isPresent()) {
            w.writeDoubleField((short) 6, bb.zmax().getAsDouble());
        }
        if (bb.mmin().isPresent()) {
            w.writeDoubleField((short) 7, bb.mmin().getAsDouble());
        }
        if (bb.mmax().isPresent()) {
            w.writeDoubleField((short) 8, bb.mmax().getAsDouble());
        }
        w.writeFieldStop();
        w.writeStructEnd();
    }
}
