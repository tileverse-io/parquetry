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
package io.tileverse.parquetry.format.codec;

/**
 * One Thrift Compact Protocol field header: the absolute field id and its wire type. Produced by
 * {@link CompactProtocolReader#readFieldHeader(int)}.
 *
 * @param fieldId absolute field id (the reader resolves the delta encoding before constructing this record)
 * @param type wire type from the field header's low nibble; {@link CompactType#STOP} terminates the enclosing struct
 */
public record FieldHeader(short fieldId, CompactType type) {
    public boolean isStop() {
        return type == CompactType.STOP;
    }
}
