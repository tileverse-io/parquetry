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
package io.tileverse.parquetry.avro;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

/**
 * Test-only bridge between avro-java's value families and this module's: maps both onto one comparable shape (strings
 * to String, all binary to a hex string, records to field-name maps, containers normalized element by element). Numbers
 * compare directly; this reader boxes int as Integer and float as Float exactly like avro-java.
 */
final class OracleValueNormalizer {

    private OracleValueNormalizer() {}

    static Object normalize(Object value) {
        return switch (value) {
            case null -> null;
            case Utf8 utf8 -> utf8.toString();
            case GenericData.EnumSymbol symbol -> symbol.toString();
            case GenericData.Fixed fixed -> HexFormat.of().formatHex(fixed.bytes());
            case ByteBuffer buffer -> HexFormat.of().formatHex(remainingBytes(buffer));
            case MemorySegment segment -> HexFormat.of().formatHex(segment.toArray(ValueLayout.JAVA_BYTE));
            case GenericRecord oracleRecord -> normalizeOracleRecord(oracleRecord);
            case AvroRecord ourRecord -> normalizeOurRecord(ourRecord);
            case List<?> list -> normalizeList(list);
            case Map<?, ?> map -> normalizeMap(map);
            default -> value;
        };
    }

    private static Map<String, Object> normalizeOracleRecord(GenericRecord oracleRecord) {
        Map<String, Object> normalized = new LinkedHashMap<>();
        for (Schema.Field field : oracleRecord.getSchema().getFields()) {
            normalized.put(field.name(), normalize(oracleRecord.get(field.name())));
        }
        return normalized;
    }

    private static Map<String, Object> normalizeOurRecord(AvroRecord ourRecord) {
        Map<String, Object> normalized = new LinkedHashMap<>();
        for (AvroSchema.Field field : ourRecord.schema().fields()) {
            normalized.put(field.name(), normalize(ourRecord.get(field.name())));
        }
        return normalized;
    }

    private static List<Object> normalizeList(List<?> list) {
        List<Object> normalized = new ArrayList<>(list.size());
        for (Object element : list) {
            normalized.add(normalize(element));
        }
        return normalized;
    }

    private static Map<String, Object> normalizeMap(Map<?, ?> map) {
        Map<String, Object> normalized = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            normalized.put(entry.getKey().toString(), normalize(entry.getValue()));
        }
        return normalized;
    }

    private static byte[] remainingBytes(ByteBuffer buffer) {
        ByteBuffer duplicate = buffer.duplicate();
        byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return bytes;
    }
}
