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
package io.tileverse.parquetry.columnar;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.BitSet;
import java.util.UUID;

import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.schema.UuidConverter;

/**
 * Builds a {@link ColumnVector} whose every row holds the same {@link Value} - the materialized form of a constant
 * output column (e.g. a Hive partition value present only in the file path). The vector is heap-backed; binary values
 * share one backing segment, and a uuid or decimal constant is a fixed-width vector of its byte form (a timestamp
 * constant is materialized at microsecond precision).
 */
public final class ConstantVectors {

    private ConstantVectors() {}

    /** A length-{@code rowCount} vector with {@code value} in every row. */
    public static ColumnVector of(Value value, int rowCount) {
        Validity validity = Validity.allValid(rowCount);
        return switch (value) {
            case Value.IntVal(int v) -> IntVector.materialized(filled(v, rowCount), validity);
            case Value.LongVal(long v) -> LongVector.materialized(filled(v, rowCount), validity);
            case Value.DoubleVal(double v) -> DoubleVector.materialized(filled(v, rowCount), validity);
            case Value.FloatVal(float v) -> FloatVector.materialized(filledFloat(v, rowCount), validity);
            case Value.BoolVal(boolean v) -> BooleanVector.materialized(filled(v, rowCount), validity);
            case Value.DateVal(LocalDate date) ->
                IntVector.materialized(filled((int) date.toEpochDay(), rowCount), validity);
            case Value.StringVal(String text) -> binary(text.getBytes(StandardCharsets.UTF_8), rowCount, validity);
            case Value.UuidVal(UUID uuid) -> fixed(UuidConverter.toBytes(uuid), rowCount, validity);
            case Value.DecimalVal(BigDecimal decimal) ->
                fixed(decimal.unscaledValue().toByteArray(), rowCount, validity);
            case Value.TimeVal(LocalTime time) ->
                LongVector.materialized(filled(time.toNanoOfDay() / 1_000L, rowCount), validity);
            case Value.TimestampVal(LocalDateTime timestamp, boolean _) ->
                LongVector.materialized(filled(epochMicros(timestamp), rowCount), validity);
            case Value.BinaryVal(MemorySegment bytes) ->
                binary(bytes.toArray(ValueLayout.JAVA_BYTE), rowCount, validity);
        };
    }

    /**
     * A length-{@code rowCount} vector with every row null; {@code typeOf} fixes the vector kind. Builds an all-null
     * constant column for null-column injection (a field absent from a file under schema evolution, or a typed-null
     * partition). Reached through an {@link io.tileverse.parquetry.filter.Projection.Column.Null} output column; no
     * catalog path emits one yet (the Iceberg field-id read is the first). A uuid column is null-filled at the same
     * fixed width a file-read uuid column has, keeping one physical shape per column across the batches of a scan.
     */
    public static ColumnVector ofNull(Value typeOf, int rowCount) {
        Validity allNull = Validity.of(new BitSet(), rowCount);
        return switch (typeOf) {
            case Value.IntVal _ -> IntVector.materialized(new int[rowCount], allNull);
            case Value.LongVal _ -> LongVector.materialized(new long[rowCount], allNull);
            case Value.DoubleVal _ -> DoubleVector.materialized(new double[rowCount], allNull);
            case Value.FloatVal _ -> FloatVector.materialized(new float[rowCount], allNull);
            case Value.BoolVal _ -> BooleanVector.materialized(new boolean[rowCount], allNull);
            case Value.DateVal _ -> IntVector.materialized(new int[rowCount], allNull);
            case Value.StringVal _ -> binary(new byte[0], rowCount, allNull);
            case Value.UuidVal _ ->
                FixedLenBinaryVector.materialized(new MemorySegment[rowCount], UuidConverter.BYTES, allNull);
            case Value.DecimalVal _ -> binary(new byte[0], rowCount, allNull);
            case Value.TimeVal _ -> LongVector.materialized(new long[rowCount], allNull);
            case Value.BinaryVal _ -> binary(new byte[0], rowCount, allNull);
            case Value.TimestampVal _ -> LongVector.materialized(new long[rowCount], allNull);
        };
    }

    private static int[] filled(int value, int rowCount) {
        int[] values = new int[rowCount];
        Arrays.fill(values, value);
        return values;
    }

    private static long[] filled(long value, int rowCount) {
        long[] values = new long[rowCount];
        Arrays.fill(values, value);
        return values;
    }

    private static double[] filled(double value, int rowCount) {
        double[] values = new double[rowCount];
        Arrays.fill(values, value);
        return values;
    }

    private static float[] filledFloat(float value, int rowCount) {
        float[] values = new float[rowCount];
        Arrays.fill(values, value);
        return values;
    }

    private static boolean[] filled(boolean value, int rowCount) {
        boolean[] values = new boolean[rowCount];
        Arrays.fill(values, value);
        return values;
    }

    private static BinaryVector binary(byte[] bytes, int rowCount, Validity validity) {
        MemorySegment segment = MemorySegment.ofArray(bytes.clone());
        MemorySegment[] rows = new MemorySegment[rowCount];
        Arrays.fill(rows, segment);
        return BinaryVector.materialized(rows, validity);
    }

    /** A decimal or uuid constant keeps its fixed width: a uuid cell is only readable from a 16-byte fixed vector. */
    private static FixedLenBinaryVector fixed(byte[] bytes, int rowCount, Validity validity) {
        MemorySegment value = MemorySegment.ofArray(bytes.clone());
        MemorySegment[] rows = new MemorySegment[rowCount];
        Arrays.fill(rows, value);
        return FixedLenBinaryVector.materialized(rows, bytes.length, validity);
    }

    /** Microseconds since the epoch, exact before 1970 because the sub-second part is added, never subtracted. */
    private static long epochMicros(LocalDateTime timestamp) {
        long seconds = timestamp.toEpochSecond(ZoneOffset.UTC);
        return seconds * 1_000_000L + timestamp.getNano() / 1_000L;
    }
}
