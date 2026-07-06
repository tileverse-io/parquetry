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

import io.tileverse.parquetry.format.PageLocation;

/** Serializer mirror of {@link PageLocationDeserializer}. */
final class PageLocationSerializer {

    private PageLocationSerializer() {}

    static void serialize(CompactProtocolWriter w, PageLocation pl) throws IOException {
        w.writeStructBegin();
        w.writeI64Field((short) 1, pl.offset());
        w.writeI32Field((short) 2, pl.compressedPageSize());
        w.writeI64Field((short) 3, pl.firstRowIndex());
        w.writeFieldStop();
        w.writeStructEnd();
    }
}
