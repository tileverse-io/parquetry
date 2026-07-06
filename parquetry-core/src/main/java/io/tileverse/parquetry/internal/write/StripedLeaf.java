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
package io.tileverse.parquetry.internal.write;

import java.util.Arrays;

import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * One leaf column's shredded form, ready for a column-chunk writer: the materialized non-null values plus the
 * repetition- and definition-level streams that locate them within the records. This is the per-leaf output of
 * {@link DremelStriper}, the write-side inverse of the read-side Dremel assembly.
 *
 * <p>{@code values} holds only the {@code valueCount} non-null values the leaf actually contributes, in record order;
 * null cells contribute a definition level below {@code maxDefLevel} and no value. The {@code defLevels} and
 * {@code repLevels} streams hold one entry per occurrence the leaf participates in. A non-repeated leaf (a flat or
 * struct-nested column) has one entry per top-level row and every repetition level is zero; a repeated leaf (a list or
 * map element) has one entry per element occurrence plus one phantom entry per null or empty container.
 *
 * @param leaf the leaf's column path
 * @param values the non-null values, in record order; {@code valueCount} of them
 * @param repLevels the repetition-level stream, one entry per participating occurrence
 * @param defLevels the definition-level stream, one entry per participating occurrence
 * @param valueCount the number of non-null values in {@code values}
 * @param maxRepLevel the leaf's maximum repetition level; positive for a list or map element
 * @param maxDefLevel the leaf's maximum definition level; an occurrence is present-with-value when its def reaches it
 */
public record StripedLeaf(
        ColumnPath leaf,
        ColumnVector values,
        int[] repLevels,
        int[] defLevels,
        int valueCount,
        int maxRepLevel,
        int maxDefLevel) {

    public StripedLeaf {
        repLevels = repLevels.clone();
        defLevels = defLevels.clone();
    }

    @Override
    public int[] repLevels() {
        return repLevels.clone();
    }

    @Override
    public int[] defLevels() {
        return defLevels.clone();
    }

    @Override
    @SuppressWarnings(
            "java:S6878") // array-aware comparison requires the bound variable; record-deconstruction pattern cannot
    // compare int[] components directly
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof StripedLeaf otherLeaf)) {
            return false;
        }
        return valueCount == otherLeaf.valueCount
                && maxRepLevel == otherLeaf.maxRepLevel
                && maxDefLevel == otherLeaf.maxDefLevel
                && leaf.equals(otherLeaf.leaf)
                && values.equals(otherLeaf.values)
                && Arrays.equals(repLevels, otherLeaf.repLevels)
                && Arrays.equals(defLevels, otherLeaf.defLevels);
    }

    @Override
    public String toString() {
        return "StripedLeaf["
                + "leaf=" + leaf
                + ", values=" + values
                + ", repLevels=" + Arrays.toString(repLevels)
                + ", defLevels=" + Arrays.toString(defLevels)
                + ", valueCount=" + valueCount
                + ", maxRepLevel=" + maxRepLevel
                + ", maxDefLevel=" + maxDefLevel
                + "]";
    }

    @Override
    public int hashCode() {
        int result = leaf.hashCode();
        result = 31 * result + values.hashCode();
        result = 31 * result + Arrays.hashCode(repLevels);
        result = 31 * result + Arrays.hashCode(defLevels);
        result = 31 * result + valueCount;
        result = 31 * result + maxRepLevel;
        result = 31 * result + maxDefLevel;
        return result;
    }
}
