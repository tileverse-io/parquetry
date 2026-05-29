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
package io.tileverse.parquetry.format.codec;

import java.io.IOException;
import java.util.Optional;

import io.tileverse.parquetry.format.KeyValue;

/**
 * Deserializer for the Thrift {@code KeyValue} struct.
 *
 * <pre>
 * struct KeyValue {
 *   1: required string key
 *   2: optional string value
 * }
 * </pre>
 */
final class KeyValueDeserializer {

    private KeyValueDeserializer() {}

    static KeyValue read(CompactProtocolReader r) throws IOException {
        String key = "";
        Optional<String> value = Optional.empty();
        int lastFieldId = 0;
        while (true) {
            FieldHeader fh = r.readFieldHeader(lastFieldId);
            if (fh.isStop()) {
                break;
            }
            lastFieldId = fh.fieldId();
            switch (fh.fieldId()) {
                case 1 -> key = r.readString();
                case 2 -> value = Optional.of(r.readString());
                default -> r.skipField(fh.type());
            }
        }
        return new KeyValue(key, value);
    }
}
