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
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import io.tileverse.parquetry.io.ByteRangeSource;

/**
 * A clean-room, read-only Avro Object Container File reader. Open it over a {@link ByteRangeSource}, read the embedded
 * write {@link #schema()} and {@link #metadata()}, and iterate {@link #records()} lazily (or
 * {@link #records(AvroSchema)} to resolve against a reader schema) - one OCF block is read, decompressed, and decoded
 * at a time, allowing arbitrarily large files to be read with bounded memory.
 */
public final class AvroDataFileReader implements AutoCloseable {

    private final ByteRangeSource source;
    private final OcfHeader header;
    private final AvroDatumDecoder datumDecoder = new AvroDatumDecoder();

    private AvroDataFileReader(ByteRangeSource source, OcfHeader header) {
        this.source = source;
        this.header = header;
    }

    /** Opens a reader over {@code source}, reading and validating the OCF header. */
    public static AvroDataFileReader open(ByteRangeSource source) {
        OcfHeader header = OcfHeader.read(new ByteRangeCursor(source));
        return new AvroDataFileReader(source, header);
    }

    /** The embedded Avro write schema (typically a {@link AvroSchema.Record}). */
    public AvroSchema schema() {
        return header.schema();
    }

    /** The full OCF header metadata map as UTF-8 strings (includes {@code avro.schema}, {@code avro.codec}). */
    public Map<String, String> metadata() {
        Map<String, MemorySegment> raw = header.metadata();
        Map<String, String> view = LinkedHashMap.newLinkedHashMap(raw.size());
        for (Map.Entry<String, MemorySegment> entry : raw.entrySet()) {
            byte[] bytes = entry.getValue().toArray(ValueLayout.JAVA_BYTE);
            view.put(entry.getKey(), new String(bytes, StandardCharsets.UTF_8));
        }
        return view;
    }

    /**
     * A lazy stream of decoded records; reads one block at a time. Each call returns an independent stream that starts
     * at the first data block; streams from earlier calls are unaffected.
     */
    public Stream<AvroRecord> records() {
        AvroSchema.Record recordSchema = topLevelRecordSchema();
        return recordStream(in -> (AvroRecord) datumDecoder.decode(recordSchema, in));
    }

    /**
     * A lazy stream of records resolved to {@code readerSchema}: writer fields the reader lacks are skipped, reader
     * fields the writer lacks are filled from their defaults, and spec promotions and aliases apply. The pair is
     * resolved against the file's writer schema once per returned stream; incompatibilities raise
     * {@link AvroFormatException} only when decoding reaches them. Each call returns an independent stream that starts
     * at the first data block; streams from earlier calls are unaffected.
     */
    public Stream<AvroRecord> records(AvroSchema readerSchema) {
        AvroSchema.Record writerSchema = topLevelRecordSchema();
        AvroSchema readerTarget = readerSchema instanceof AvroSchema.Ref ref ? ref.target() : readerSchema;
        if (!(readerTarget instanceof AvroSchema.Record)) {
            throw new AvroFormatException("Reader schema is not a record: " + readerTarget.type());
        }
        Resolved plan = SchemaResolver.resolve(writerSchema, readerSchema);
        ResolvingDatumDecoder resolvingDecoder = new ResolvingDatumDecoder();
        return recordStream(in -> (AvroRecord) resolvingDecoder.decode(plan, in));
    }

    private AvroSchema.Record topLevelRecordSchema() {
        AvroSchema topLevelSchema = header.schema();
        if (!(topLevelSchema instanceof AvroSchema.Record recordSchema)) {
            throw new AvroFormatException("OCF top-level schema is not a record: " + topLevelSchema.type());
        }
        return recordSchema;
    }

    private Stream<AvroRecord> recordStream(Function<AvroBinaryDecoder, AvroRecord> decodeOne) {
        OcfReader blockReader = OcfReader.blocks(source, header);
        Spliterator<AvroRecord> spliterator = new BlockSpliterator(blockReader, decodeOne);
        return StreamSupport.stream(spliterator, false);
    }

    @Override
    public void close() {
        source.close();
    }

    /** Pulls and decodes one block at a time, buffering only the current block's records. */
    private static final class BlockSpliterator extends Spliterators.AbstractSpliterator<AvroRecord> {

        private final OcfReader ocfReader;
        private final Function<AvroBinaryDecoder, AvroRecord> decodeOne;
        private final Deque<AvroRecord> current = new ArrayDeque<>();

        BlockSpliterator(OcfReader ocfReader, Function<AvroBinaryDecoder, AvroRecord> decodeOne) {
            super(Long.MAX_VALUE, Spliterator.ORDERED | Spliterator.NONNULL);
            this.ocfReader = ocfReader;
            this.decodeOne = decodeOne;
        }

        @Override
        public boolean tryAdvance(Consumer<? super AvroRecord> action) {
            if (current.isEmpty() && !loadNextBlock()) {
                return false;
            }
            action.accept(current.removeFirst());
            return true;
        }

        private boolean loadNextBlock() {
            while (current.isEmpty() && ocfReader.hasNextBlock()) {
                try (OcfReader.Block block = ocfReader.nextBlock()) {
                    AvroBinaryDecoder decoder = new AvroBinaryDecoder(block.data());
                    for (long i = 0; i < block.recordCount(); i++) {
                        current.addLast(decodeOne.apply(decoder));
                    }
                }
            }
            return !current.isEmpty();
        }
    }
}
