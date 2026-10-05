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
package io.tileverse.parquetry.internal.filter;

/**
 * The value comparisons of the predicate language, applied to a cell and a predicate literal. Totally ordered types
 * decide them from a three-way comparison; floating-point values decide them under IEEE 754 rules.
 */
public enum ComparisonOperator {
    EQ,
    NOT_EQ,
    LT,
    LT_EQ,
    GT,
    GT_EQ;

    /**
     * Whether the operator holds for {@code comparison}, the sign of {@code cell - literal} under a total order.
     *
     * @param comparison negative, zero, or positive as the cell is less than, equal to, or greater than the literal
     */
    public boolean holds(int comparison) {
        return switch (this) {
            case EQ -> comparison == 0;
            case NOT_EQ -> comparison != 0;
            case LT -> comparison < 0;
            case LT_EQ -> comparison <= 0;
            case GT -> comparison > 0;
            case GT_EQ -> comparison >= 0;
        };
    }

    /**
     * Whether the operator holds between a floating-point cell and literal under IEEE 754 rules: {@code -0.0} equals
     * {@code +0.0}, and an ordered comparison involving NaN is false. Equality also lets a NaN literal match a NaN
     * cell, whatever its payload bits: a predicate can then select or exclude the NaN cells of a column.
     */
    public boolean holds(double cell, double literal) {
        return switch (this) {
            case EQ -> equal(cell, literal);
            case NOT_EQ -> !equal(cell, literal);
            case LT -> cell < literal;
            case LT_EQ -> cell <= literal;
            case GT -> cell > literal;
            case GT_EQ -> cell >= literal;
        };
    }

    private static boolean equal(double cell, double literal) {
        return cell == literal || (Double.isNaN(cell) && Double.isNaN(literal));
    }
}
