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
package io.tileverse.parquetry.avro;

import java.security.SecureRandom;
import java.util.Objects;

import io.tileverse.parquetry.io.ByteSink;

/**
 * A clean-room Avro Object Container File writer, the inverse of {@link AvroDataFileReader}. Build it with the schema
 * as a JSON string and write records as a {@code Map} by field name or an {@link AvroRecord}; the writer coerces values
 * to the schema, frames blocks, and embeds the schema JSON verbatim. Records stream one block at a time with bounded
 * memory. Not thread-safe; one writer per file.
 */
public final class AvroDataFileWriter implements AutoCloseable {

    private static final int DEFAULT_BLOCK_SIZE = 64 * 1024;
    private static final int SYNC_SIZE = 16;

    private final AvroSchema schema;
    private final OcfWriter ocfWriter;
    private final AvroDatumEncoder datumEncoder = new AvroDatumEncoder();

    private AvroDataFileWriter(AvroSchema schema, OcfWriter ocfWriter) {
        this.schema = schema;
        this.ocfWriter = ocfWriter;
    }

    /** Starts building a writer for the given Avro schema, supplied as a JSON string. */
    public static Builder builder(String schemaJson) {
        return new Builder(schemaJson);
    }

    /** Writes one record: a {@code Map} by field name or an {@link AvroRecord}. */
    public void write(Object record) {
        AvroBinaryEncoder encoded = new AvroBinaryEncoder();
        datumEncoder.encode(schema, record, encoded);
        ocfWriter.append(encoded.toByteArray());
    }

    /** Writes every record in {@code records}. */
    public void write(Iterable<?> records) {
        for (Object record : records) {
            write(record);
        }
    }

    @Override
    public void close() {
        ocfWriter.close();
    }

    /** Configures an {@link AvroDataFileWriter}: codec, block-flush threshold, and sync marker. */
    public static final class Builder {

        private final String schemaJson;
        private String codec = "null";
        private int blockSize = DEFAULT_BLOCK_SIZE;
        private byte[] syncMarker;

        private Builder(String schemaJson) {
            this.schemaJson = Objects.requireNonNull(schemaJson, "schemaJson");
        }

        /** Sets the block compression codec by name (for example {@code "null"} or {@code "deflate"}). */
        public Builder codec(String codecName) {
            this.codec = Objects.requireNonNull(codecName, "codecName");
            return this;
        }

        /** Sets the byte threshold at which a buffered block is flushed; must be positive. */
        public Builder blockSize(int byteThreshold) {
            if (byteThreshold <= 0) {
                throw new IllegalArgumentException("blockSize must be positive: " + byteThreshold);
            }
            this.blockSize = byteThreshold;
            return this;
        }

        /** Overrides the random sync marker; the array must be exactly 16 bytes. For deterministic output. */
        public Builder syncMarker(byte[] marker) {
            Objects.requireNonNull(marker, "marker");
            if (marker.length != SYNC_SIZE) {
                throw new IllegalArgumentException("Sync marker must be 16 bytes, got " + marker.length);
            }
            this.syncMarker = marker.clone();
            return this;
        }

        /** Parses the schema, writes the OCF header to {@code sink}, and returns a ready writer. */
        public AvroDataFileWriter build(ByteSink sink) {
            Objects.requireNonNull(sink, "sink");
            AvroSchema parsed = AvroSchema.parse(schemaJson);
            byte[] marker = syncMarker != null ? syncMarker : randomSyncMarker();
            OcfWriter ocfWriter = new OcfWriter(sink, schemaJson, codec, marker, blockSize);
            return new AvroDataFileWriter(parsed, ocfWriter);
        }

        private static byte[] randomSyncMarker() {
            byte[] marker = new byte[SYNC_SIZE];
            new SecureRandom().nextBytes(marker);
            return marker;
        }
    }
}
