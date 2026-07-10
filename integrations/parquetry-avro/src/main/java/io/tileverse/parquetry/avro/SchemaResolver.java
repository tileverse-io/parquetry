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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Compiles a (writer, reader) schema pair into a {@link Resolved} plan per the Avro spec's schema resolution rules.
 * Resolution itself never throws on an incompatibility: every mismatch compiles to a {@link Resolved.Failure} that
 * raises {@link AvroFormatException} only when decoding actually reaches it.
 */
final class SchemaResolver {

    // Refs are deref'd BEFORE keying: AvroSchema records have structural equality EXCEPT Ref (identity), and keying
    // on the underlying named types (identity-stable within one parse) lets recursion meet the memo.
    private final Map<PairKey, Resolved> memo = new HashMap<>();

    private record PairKey(AvroSchema writer, AvroSchema reader) {}

    private SchemaResolver() {}

    static Resolved resolve(AvroSchema writer, AvroSchema reader) {
        return new SchemaResolver().plan(writer, reader);
    }

    private Resolved plan(AvroSchema writer, AvroSchema reader) {
        AvroSchema w = deref(writer);
        AvroSchema r = deref(reader);
        PairKey key = new PairKey(w, r);
        Resolved memoized = memo.get(key);
        if (memoized != null) {
            return memoized;
        }
        Resolved.PlanRef placeholder = new Resolved.PlanRef();
        memo.put(key, placeholder);
        Resolved plan = computePlan(w, r);
        placeholder.bind(plan);
        memo.put(key, plan);
        return plan;
    }

    private Resolved computePlan(AvroSchema writer, AvroSchema reader) {
        if (writer instanceof AvroSchema.Union writerUnion) {
            return writerUnionPlan(writerUnion, reader);
        }
        if (reader instanceof AvroSchema.Union readerUnion) {
            return readerUnionPlan(writer, readerUnion);
        }
        return switch (writer) {
            case AvroSchema.Primitive primitive -> primitivePlan(primitive, reader);
            case AvroSchema.Record writerRecord
            when reader instanceof AvroSchema.Record readerRecord -> recordPlan(writerRecord, readerRecord);
            case AvroSchema.Array(AvroSchema writerElement)
            when reader instanceof AvroSchema.Array(AvroSchema readerElement) ->
                new Resolved.ArrayPlan(
                        plan(writerElement, readerElement), DatumSkipper.minimumWireSize(writerElement) == 0);
            case AvroSchema.Map(AvroSchema writerValues)
            when reader instanceof AvroSchema.Map(AvroSchema readerValues) ->
                new Resolved.MapPlan(plan(writerValues, readerValues));
            case AvroSchema.Fixed fixed
            when reader instanceof AvroSchema.Fixed readerFixed -> fixedPlan(fixed, readerFixed);
            case AvroSchema.Enum anEnum
            when reader instanceof AvroSchema.Enum readerEnum -> enumPlan(anEnum, readerEnum);
            default -> mismatch(writer, reader);
        };
    }

    /** The branch index comes from the data stream; every writer branch resolves against the whole reader schema. */
    private Resolved writerUnionPlan(AvroSchema.Union writer, AvroSchema reader) {
        List<Resolved> branches = new ArrayList<>(writer.branches().size());
        for (AvroSchema branch : writer.branches()) {
            branches.add(plan(branch, reader));
        }
        return new Resolved.WriterUnionPlan(branches);
    }

    /**
     * The first reader branch that matches the writer wins, per the spec. An in-progress recursive pair shows up as an
     * unbound {@link Resolved.PlanRef}, which is not a {@link Resolved.Failure} and counts as a match: reaching that
     * pair again means it already passed the spec's shallow match (same type, names compatible), which is exactly the
     * union branch-selection criterion, and treating the placeholder as a match implements the spec rule.
     */
    private Resolved readerUnionPlan(AvroSchema writer, AvroSchema.Union reader) {
        for (AvroSchema branch : reader.branches()) {
            Resolved candidate = plan(writer, branch);
            if (!(candidate instanceof Resolved.Failure)) {
                return candidate;
            }
        }
        return new Resolved.Failure("No reader union branch matches writer schema " + describe(writer));
    }

    private Resolved primitivePlan(AvroSchema.Primitive writer, AvroSchema reader) {
        if (writer.type() == reader.type()) {
            return directPlan(writer, reader);
        }
        if (promotes(writer.type(), reader.type())) {
            return new Resolved.Promote(writer, reader);
        }
        return mismatch(writer, reader);
    }

    /**
     * Two decimal annotations match only when precision and scale agree, per the spec's resolution rules: the wire
     * value is the unscaled bytes, and decoding them under a different reader scale would silently shift the decimal
     * point. One annotated side against one plain side keeps the general asymmetry rule: decode by the reader's view.
     */
    private static Resolved directPlan(AvroSchema writer, AvroSchema reader) {
        if (writer.logicalType().orElse(null) instanceof LogicalType.Decimal writerDecimal
                && reader.logicalType().orElse(null) instanceof LogicalType.Decimal readerDecimal
                && !writerDecimal.equals(readerDecimal)) {
            return new Resolved.Failure(
                    "Decimal mismatch: writer " + describe(writerDecimal) + " vs reader " + describe(readerDecimal));
        }
        return new Resolved.Direct(writer, reader);
    }

    private static String describe(LogicalType.Decimal decimal) {
        return "decimal(" + decimal.precision() + "," + decimal.scale() + ")";
    }

    private static boolean promotes(AvroSchema.Type writerType, AvroSchema.Type readerType) {
        return switch (writerType) {
            case INT ->
                readerType == AvroSchema.Type.LONG
                        || readerType == AvroSchema.Type.FLOAT
                        || readerType == AvroSchema.Type.DOUBLE;
            case LONG -> readerType == AvroSchema.Type.FLOAT || readerType == AvroSchema.Type.DOUBLE;
            case FLOAT -> readerType == AvroSchema.Type.DOUBLE;
            case STRING -> readerType == AvroSchema.Type.BYTES;
            case BYTES -> readerType == AvroSchema.Type.STRING;
            default -> false;
        };
    }

    private Resolved recordPlan(AvroSchema.Record writer, AvroSchema.Record reader) {
        if (!namesMatch(writer, reader)) {
            return mismatch(writer, reader);
        }
        List<Resolved.FieldStep> steps = new ArrayList<>(writer.fields().size());
        boolean[] readerFieldHasWriterValue = new boolean[reader.fields().size()];
        for (AvroSchema.Field writerField : writer.fields()) {
            steps.add(fieldStep(writerField, reader, readerFieldHasWriterValue));
        }
        List<Resolved.Defaulted> defaulted = new ArrayList<>();
        for (AvroSchema.Field readerField : reader.fields()) {
            if (readerFieldHasWriterValue[readerField.position()]) {
                continue;
            }
            // The spec wants resolution errors raised only when a datum is decoded. A reader field with no writer
            // value and no usable default makes EVERY datum of this record undecodable; a Failure for the whole
            // record fires exactly then, never at resolve time.
            if (!readerField.hasDefault()) {
                return new Resolved.Failure("Reader field has no writer value and no default: " + reader.fullName()
                        + "." + readerField.name());
            }
            try {
                Object value = DefaultValues.materialize(readerField.schema(), readerField.defaultValue());
                defaulted.add(new Resolved.Defaulted(readerField.position(), value));
            } catch (AvroFormatException malformed) {
                return new Resolved.Failure("Malformed default for reader field " + reader.fullName() + "."
                        + readerField.name() + ": " + malformed.getMessage());
            }
        }
        return new Resolved.RecordPlan(reader, steps, defaulted);
    }

    private Resolved.FieldStep fieldStep(
            AvroSchema.Field writerField, AvroSchema.Record reader, boolean[] readerFieldHasWriterValue) {
        Optional<AvroSchema.Field> readerField = readerFieldFor(reader, writerField.name());
        if (readerField.isEmpty()) {
            return new Resolved.FieldStep(-1, writerField.schema(), null);
        }
        AvroSchema.Field matched = readerField.get();
        readerFieldHasWriterValue[matched.position()] = true;
        return new Resolved.FieldStep(
                matched.position(), writerField.schema(), plan(writerField.schema(), matched.schema()));
    }

    /** Fields match by name, or through a reader field whose aliases include the writer field's name. */
    private static Optional<AvroSchema.Field> readerFieldFor(AvroSchema.Record reader, String writerFieldName) {
        for (AvroSchema.Field field : reader.fields()) {
            if (field.name().equals(writerFieldName) || field.aliases().contains(writerFieldName)) {
                return Optional.of(field);
            }
        }
        return Optional.empty();
    }

    private Resolved fixedPlan(AvroSchema.Fixed writer, AvroSchema.Fixed reader) {
        if (!namesMatch(writer, reader)) {
            return mismatch(writer, reader);
        }
        if (writer.size() != reader.size()) {
            return new Resolved.Failure("Fixed " + reader.fullName() + " size mismatch: writer " + writer.size()
                    + ", reader " + reader.size());
        }
        return directPlan(writer, reader);
    }

    /** Unmapped writer symbols stay -1; the decoder falls back to the reader's enum default when one exists. */
    private Resolved enumPlan(AvroSchema.Enum writer, AvroSchema.Enum reader) {
        if (!namesMatch(writer, reader)) {
            return mismatch(writer, reader);
        }
        int[] mapping = new int[writer.symbols().size()];
        for (int i = 0; i < mapping.length; i++) {
            mapping[i] = reader.symbols().indexOf(writer.symbols().get(i));
        }
        return new Resolved.EnumPlan(reader, mapping);
    }

    private static boolean namesMatch(AvroSchema.Named writer, AvroSchema.Named reader) {
        return reader.fullName().equals(writer.fullName()) || reader.aliases().contains(writer.fullName());
    }

    private static Resolved mismatch(AvroSchema writer, AvroSchema reader) {
        return new Resolved.Failure(
                "Writer schema " + describe(writer) + " does not resolve to reader schema " + describe(reader));
    }

    private static String describe(AvroSchema schema) {
        if (schema instanceof AvroSchema.Named named) {
            return named.fullName();
        }
        return schema.type().toString();
    }

    private static AvroSchema deref(AvroSchema schema) {
        if (schema instanceof AvroSchema.Ref ref) {
            return ref.target();
        }
        return schema;
    }
}
