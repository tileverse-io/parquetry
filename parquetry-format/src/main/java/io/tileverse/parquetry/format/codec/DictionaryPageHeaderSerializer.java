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

import io.tileverse.parquetry.format.DictionaryPageHeader;

/**
 * Serializer mirror of {@link DictionaryPageHeaderDeserializer}.
 *
 * <p>{@code is_sorted} is optional in the Thrift schema; the deserializer defaults to {@code false} when absent.
 * Symmetrically, this serializer only emits the field when the value is {@code true} so the wire output matches what a
 * minimal reader-compatible writer would produce.
 */
final class DictionaryPageHeaderSerializer {

    private DictionaryPageHeaderSerializer() {}

    static void serialize(CompactProtocolWriter w, DictionaryPageHeader h) throws IOException {
        w.writeStructBegin();
        w.writeI32Field((short) 1, h.numValues());
        w.writeI32Field((short) 2, h.encoding().value());
        if (h.isSorted()) {
            w.writeBoolField((short) 3, true);
        }
        w.writeFieldStop();
        w.writeStructEnd();
    }
}
