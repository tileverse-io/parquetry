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
package io.tileverse.parquetry.avro;

/**
 * The Avro logical types this reader interprets, attached to an underlying {@link AvroSchema} node. Unknown or
 * spec-invalid annotations are preserved as {@link Unknown} and decode as the underlying type, as the spec requires.
 */
public sealed interface LogicalType
        permits LogicalType.Decimal,
                LogicalType.Uuid,
                LogicalType.Date,
                LogicalType.TimeMillis,
                LogicalType.TimeMicros,
                LogicalType.TimestampMillis,
                LogicalType.TimestampMicros,
                LogicalType.LocalTimestampMillis,
                LogicalType.LocalTimestampMicros,
                LogicalType.Duration,
                LogicalType.Unknown {

    /** The {@code logicalType} attribute value this type was parsed from. */
    String name();

    /** Arbitrary-precision decimal over bytes or fixed; two's-complement big-endian unscaled value. */
    record Decimal(int precision, int scale) implements LogicalType {
        public Decimal {
            if (precision <= 0) {
                throw new IllegalArgumentException("decimal precision must be positive, got " + precision);
            }
            if (scale < 0 || scale > precision) {
                throw new IllegalArgumentException("decimal scale out of [0, precision]: " + scale);
            }
        }

        @Override
        public String name() {
            return "decimal";
        }
    }

    /** An RFC 4122 UUID over string or fixed(16). */
    record Uuid() implements LogicalType {
        static final Uuid INSTANCE = new Uuid();

        @Override
        public String name() {
            return "uuid";
        }
    }

    /** A calendar date over int: days since the Unix epoch. */
    record Date() implements LogicalType {
        static final Date INSTANCE = new Date();

        @Override
        public String name() {
            return "date";
        }
    }

    /** A time of day over int: milliseconds after midnight. */
    record TimeMillis() implements LogicalType {
        static final TimeMillis INSTANCE = new TimeMillis();

        @Override
        public String name() {
            return "time-millis";
        }
    }

    /** A time of day over long: microseconds after midnight. */
    record TimeMicros() implements LogicalType {
        static final TimeMicros INSTANCE = new TimeMicros();

        @Override
        public String name() {
            return "time-micros";
        }
    }

    /** An instant over long: milliseconds since the Unix epoch, UTC. */
    record TimestampMillis() implements LogicalType {
        static final TimestampMillis INSTANCE = new TimestampMillis();

        @Override
        public String name() {
            return "timestamp-millis";
        }
    }

    /** An instant over long: microseconds since the Unix epoch, UTC. */
    record TimestampMicros() implements LogicalType {
        static final TimestampMicros INSTANCE = new TimestampMicros();

        @Override
        public String name() {
            return "timestamp-micros";
        }
    }

    /** A local date-time over long: milliseconds since the Unix epoch, no time zone. */
    record LocalTimestampMillis() implements LogicalType {
        static final LocalTimestampMillis INSTANCE = new LocalTimestampMillis();

        @Override
        public String name() {
            return "local-timestamp-millis";
        }
    }

    /** A local date-time over long: microseconds since the Unix epoch, no time zone. */
    record LocalTimestampMicros() implements LogicalType {
        static final LocalTimestampMicros INSTANCE = new LocalTimestampMicros();

        @Override
        public String name() {
            return "local-timestamp-micros";
        }
    }

    /** The months/days/milliseconds triple over fixed(12). */
    record Duration() implements LogicalType {
        static final Duration INSTANCE = new Duration();

        @Override
        public String name() {
            return "duration";
        }
    }

    /** A logical type this reader does not interpret; values decode as the underlying type. */
    record Unknown(String name) implements LogicalType {}
}
