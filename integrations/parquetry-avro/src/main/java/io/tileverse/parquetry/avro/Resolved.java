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

import java.util.List;

/**
 * A compiled decode plan for reading data written with one schema into values shaped by another. Built once per
 * (writer, reader) pair by {@link SchemaResolver}; walked per datum by {@link ResolvingDatumDecoder}. A {@link Failure}
 * node throws only when decoding actually reaches it, as the spec requires.
 */
sealed interface Resolved {

    /** Writer and reader agree on this leaf; decode by the writer, convert logical types by the reader. */
    record Direct(AvroSchema writer, AvroSchema reader) implements Resolved {}

    /**
     * A spec promotion: int to long/float/double, long to float/double, float to double, string/bytes either way. The
     * raw value is read per the writer schema, the box widens to the reader type, and the reader's logical-type
     * conversion applies last.
     */
    record Promote(AvroSchema writer, AvroSchema reader) implements Resolved {}

    record RecordPlan(AvroSchema.Record reader, List<FieldStep> steps, List<Defaulted> defaulted) implements Resolved {
        public RecordPlan {
            steps = List.copyOf(steps);
            defaulted = List.copyOf(defaulted);
        }
    }

    /** One writer field in writer order: decoded into a reader position, or skipped when readerPosition is -1. */
    record FieldStep(int readerPosition, AvroSchema writerSchema, Resolved value) {}

    /** A reader field with no writer counterpart, filled from its materialized default. */
    record Defaulted(int readerPosition, Object value) {}

    /**
     * {@code zeroByteElements} is precomputed at resolve time from the writer element's minimum wire size; it selects
     * the block-count guard (item cap for invisible elements, remaining-input bound otherwise) without re-walking the
     * writer schema per datum.
     */
    record ArrayPlan(Resolved element, boolean zeroByteElements) implements Resolved {}

    record MapPlan(Resolved values) implements Resolved {}

    /** The writer wrote a union; each branch has its own resolution against the reader schema. */
    record WriterUnionPlan(List<Resolved> branches) implements Resolved {
        public WriterUnionPlan {
            branches = List.copyOf(branches);
        }
    }

    /** Writer symbol index to reader symbol index; -1 entries fall back to the reader's enum default at decode time. */
    // S6218: plans are compared by identity, never structurally (PlanRef cycles forbid it), and never printed
    @SuppressWarnings("java:S6218")
    record EnumPlan(AvroSchema.Enum reader, int[] symbolMapping) implements Resolved {}

    /** Incompatibility that only matters if data actually reaches it. */
    record Failure(String message) implements Resolved {}

    /** Late-bound node closing resolution cycles for recursive schemas. */
    final class PlanRef implements Resolved {

        // volatile, mirroring AvroSchema.Ref: the resolver binds before the plan escapes resolve(), but plans travel
        // inside Streams across threads and callers may cache them in plain fields; without it a racing reader could
        // legally observe an unbound PlanRef and reject valid data.
        private volatile Resolved target;

        Resolved target() {
            if (target == null) {
                throw new IllegalStateException("Unbound resolution plan reference");
            }
            return target;
        }

        void bind(Resolved resolved) {
            if (target != null) {
                throw new IllegalStateException("Resolution plan reference already bound");
            }
            target = resolved;
        }
    }
}
