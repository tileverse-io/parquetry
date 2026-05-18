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
package io.tileverse.parquetry.filter;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Fluent builder emitting {@link Predicate} records.
 *
 * <p>Static entry: {@link #col(String)} or {@link #col(ColumnPath)} returns a {@link ColumnRef}
 * with typed overloads for each Parquet primitive ({@code .eq(int)}, {@code .eq(long)},
 * {@code .eq(String)}, etc.). The builder returns standard {@link Predicate} records.
 *
 * <p>Combinators {@link #and(Predicate...)} / {@link #or(Predicate...)} / {@link #not(Predicate)}
 * are also exposed for top-level composition.
 */
public final class Pred {

    private Pred() {}

    public static ColumnRef col(String name) {
        return new ColumnRef(ColumnPath.of(name));
    }

    public static ColumnRef col(ColumnPath path) {
        return new ColumnRef(path);
    }

    public static ColumnRef col(String... parts) {
        return new ColumnRef(new ColumnPath(List.of(parts)));
    }

    public static Predicate and(Predicate... children) {
        return new Predicate.And(Arrays.asList(children));
    }

    public static Predicate or(Predicate... children) {
        return new Predicate.Or(Arrays.asList(children));
    }

    public static Predicate not(Predicate child) {
        return new Predicate.Not(child);
    }

    /** Fluent column reference with typed comparison overloads. */
    public static final class ColumnRef {

        private final ColumnPath path;

        ColumnRef(ColumnPath path) {
            this.path = path;
        }

        public ColumnPath path() {
            return path;
        }

        public Predicate eq(boolean v) {
            return new Predicate.Eq(path, new Value.BoolVal(v));
        }

        public Predicate eq(int v) {
            return new Predicate.Eq(path, new Value.IntVal(v));
        }

        public Predicate eq(long v) {
            return new Predicate.Eq(path, new Value.LongVal(v));
        }

        public Predicate eq(float v) {
            return new Predicate.Eq(path, new Value.FloatVal(v));
        }

        public Predicate eq(double v) {
            return new Predicate.Eq(path, new Value.DoubleVal(v));
        }

        public Predicate eq(String v) {
            return new Predicate.Eq(path, new Value.StringVal(v));
        }

        public Predicate eq(byte[] v) {
            return new Predicate.Eq(path, new Value.BinaryVal(ByteBuffer.wrap(v)));
        }

        public Predicate eq(ByteBuffer v) {
            return new Predicate.Eq(path, new Value.BinaryVal(v));
        }

        public Predicate eq(LocalDate v) {
            return new Predicate.Eq(path, new Value.DateVal(v));
        }

        public Predicate eq(LocalDateTime v, boolean utc) {
            return new Predicate.Eq(path, new Value.TimestampVal(v, utc));
        }

        public Predicate notEq(int v) {
            return new Predicate.NotEq(path, new Value.IntVal(v));
        }

        public Predicate notEq(long v) {
            return new Predicate.NotEq(path, new Value.LongVal(v));
        }

        public Predicate notEq(String v) {
            return new Predicate.NotEq(path, new Value.StringVal(v));
        }

        public Predicate notEq(double v) {
            return new Predicate.NotEq(path, new Value.DoubleVal(v));
        }

        public Predicate lt(int v) {
            return new Predicate.Lt(path, new Value.IntVal(v));
        }

        public Predicate lt(long v) {
            return new Predicate.Lt(path, new Value.LongVal(v));
        }

        public Predicate lt(double v) {
            return new Predicate.Lt(path, new Value.DoubleVal(v));
        }

        public Predicate ltEq(int v) {
            return new Predicate.LtEq(path, new Value.IntVal(v));
        }

        public Predicate ltEq(long v) {
            return new Predicate.LtEq(path, new Value.LongVal(v));
        }

        public Predicate ltEq(double v) {
            return new Predicate.LtEq(path, new Value.DoubleVal(v));
        }

        public Predicate gt(int v) {
            return new Predicate.Gt(path, new Value.IntVal(v));
        }

        public Predicate gt(long v) {
            return new Predicate.Gt(path, new Value.LongVal(v));
        }

        public Predicate gt(double v) {
            return new Predicate.Gt(path, new Value.DoubleVal(v));
        }

        public Predicate gtEq(int v) {
            return new Predicate.GtEq(path, new Value.IntVal(v));
        }

        public Predicate gtEq(long v) {
            return new Predicate.GtEq(path, new Value.LongVal(v));
        }

        public Predicate gtEq(double v) {
            return new Predicate.GtEq(path, new Value.DoubleVal(v));
        }

        public Predicate inInts(int... values) {
            List<Value> vals = new ArrayList<>(values.length);
            for (int v : values) vals.add(new Value.IntVal(v));
            return new Predicate.In(path, vals);
        }

        public Predicate inStrings(String... values) {
            List<Value> vals = new ArrayList<>(values.length);
            for (String v : values) vals.add(new Value.StringVal(v));
            return new Predicate.In(path, vals);
        }

        public Predicate isNull() {
            return new Predicate.IsNull(path);
        }

        public Predicate isNotNull() {
            return new Predicate.IsNotNull(path);
        }

        public Predicate intersects(Bbox bbox) {
            return new Predicate.BboxIntersects(path, bbox);
        }
    }
}
