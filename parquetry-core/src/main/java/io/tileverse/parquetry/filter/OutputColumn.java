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
package io.tileverse.parquetry.filter;

import java.util.Objects;

import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * One column of a read's ordered result shape.
 *
 * <p>A read no longer presents its result as a fixed projection of physical columns. It presents an ordered list of
 * {@code OutputColumn}s, each of which describes how to produce one result column: pass a physical column through
 * (possibly renamed, reordered, or dropped), fill a constant value for every row, or widen a physical column to a
 * broader primitive type. Modelling the result shape this way lets callers express renames, literals, and sanctioned
 * type promotions without the read itself needing to know why each presentation was chosen.
 */
public sealed interface OutputColumn
        permits OutputColumn.Physical, OutputColumn.Constant, OutputColumn.Null, OutputColumn.Promoted {

    /** The result column path this output column is presented under. */
    ColumnPath name();

    /** Present a decoded physical column under {@link #name()} (projection passthrough, rename, reorder, drop). */
    record Physical(ColumnPath name, ColumnPath source) implements OutputColumn {
        public Physical {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(source, "source");
        }
    }

    /**
     * Present a constant literal value for every row. Supported types are limited to what {@code ConstantVectors}
     * renders (Int/Long/Double/Float/Bool/Date/String); any other {@link Value} kind throws loudly.
     */
    record Constant(ColumnPath name, Value value) implements OutputColumn {
        public Constant {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(value, "value");
        }
    }

    /**
     * Present an all-null column; {@code typeOf} selects the column's type (its value is ignored). Supported types are
     * the same set {@code ConstantVectors} renders (Int/Long/Double/Float/Bool/Date/String); any other {@link Value}
     * kind throws loudly.
     */
    record Null(ColumnPath name, Value typeOf) implements OutputColumn {
        public Null {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(typeOf, "typeOf");
        }
    }

    /**
     * Present a physical column widened to {@code target} (a sanctioned promotion the caller has already validated).
     */
    record Promoted(ColumnPath name, ColumnPath source, PrimitiveKind target) implements OutputColumn {
        public Promoted {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(source, "source");
            Objects.requireNonNull(target, "target");
        }
    }
}
