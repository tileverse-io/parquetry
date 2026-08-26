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
package io.tileverse.parquetry.internal.write;

import java.util.Arrays;

import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * One leaf column's shredded form, ready for a column-chunk writer: the leaf's raw value vector, the source ordinals of
 * its present values, and the repetition- and definition-level streams that locate them within the records. This is the
 * per-leaf output of {@link DremelStriper}, the write-side inverse of the read-side Dremel assembly.
 *
 * <p>{@code sourceValues} is the leaf's row-length value vector, holding null and non-kept cells alongside the present
 * ones. {@code keptOrdinals} lists the raw index of each present value in stream order; {@code keptOrdinals[k]} is the
 * source ordinal of the k-th present value, and reading {@code sourceValues} at it yields the value a column-chunk
 * writer emits for that occurrence. Only {@code [0, valueCount)} of {@code keptOrdinals} is meaningful; the array may
 * be oversized. Null cells contribute a definition level below {@code maxDefLevel} and no value. The {@code defLevels}
 * and {@code repLevels} streams hold one entry per occurrence the leaf participates in. Only {@code [0, entryCount)} of
 * either stream is meaningful; both arrays may be oversized. A non-repeated leaf (a flat or struct-nested column) has
 * one entry per top-level row and every repetition level is zero; its uniformly-zero stream is not stored
 * ({@code repLevels} is {@code null}) and {@link #repLevels()} synthesizes it on read. A repeated leaf (a list or map
 * element) has one entry per element occurrence plus one phantom entry per null or empty container.
 *
 * @param leaf the leaf's column path
 * @param sourceValues the leaf's row-length value vector; present values are read at the {@code keptOrdinals}
 * @param keptOrdinals the source ordinals of the {@code valueCount} present values, in stream order; may be oversized,
 *     read only {@code [0, valueCount)}
 * @param repLevels the repetition-level stream, one entry per participating occurrence, or {@code null} for a
 *     non-repeated leaf whose stream is uniformly zero; may be oversized, read only {@code [0, entryCount)}
 * @param defLevels the definition-level stream, one entry per participating occurrence; may be oversized, read only
 *     {@code [0, entryCount)}
 * @param entryCount the number of participating occurrences, and the meaningful length of the two level streams
 * @param valueCount the number of present values, and the meaningful length of {@code keptOrdinals}
 * @param maxRepLevel the leaf's maximum repetition level; positive for a list or map element
 * @param maxDefLevel the leaf's maximum definition level; an occurrence is present-with-value when its def reaches it
 */
public record StripedLeaf(
        ColumnPath leaf,
        ColumnVector sourceValues,
        int[] keptOrdinals,
        int[] repLevels,
        int[] defLevels,
        int entryCount,
        int valueCount,
        int maxRepLevel,
        int maxDefLevel) {

    /**
     * The meaningful repetition levels: a copy of the stored stream's first {@code entryCount} entries, or an all-zero
     * array of that length when a non-repeated leaf stored none.
     */
    @Override
    public int[] repLevels() {
        return repLevels == null ? new int[entryCount] : Arrays.copyOf(repLevels, entryCount);
    }

    /** The meaningful definition levels: a copy of the stored stream's first {@code entryCount} entries. */
    @Override
    public int[] defLevels() {
        return Arrays.copyOf(defLevels, entryCount);
    }

    /**
     * Exposes the stored repetition-level array by reference for the single trusted in-package consumer; do not mutate
     * it, and read only its first {@link #entryCount()} entries. Returns {@code null} for a non-repeated leaf, whose
     * repetition stream is uniformly zero and never allocated; the consumer reads a null array as all-zero.
     */
    int[] repLevelsRaw() {
        return repLevels;
    }

    /**
     * Exposes the stored definition-level array by reference for the single trusted in-package consumer; do not mutate
     * it, and read only its first {@link #entryCount()} entries.
     */
    int[] defLevelsRaw() {
        return defLevels;
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
        return entryCount == otherLeaf.entryCount
                && valueCount == otherLeaf.valueCount
                && maxRepLevel == otherLeaf.maxRepLevel
                && maxDefLevel == otherLeaf.maxDefLevel
                && leaf.equals(otherLeaf.leaf)
                && sourceValues.equals(otherLeaf.sourceValues)
                && keptOrdinalsEqual(keptOrdinals, otherLeaf.keptOrdinals, valueCount)
                && Arrays.equals(repLevels(), otherLeaf.repLevels())
                && Arrays.equals(defLevels(), otherLeaf.defLevels());
    }

    @Override
    public String toString() {
        return "StripedLeaf["
                + "leaf=" + leaf
                + ", sourceValues=" + sourceValues
                + ", keptOrdinals=" + Arrays.toString(Arrays.copyOf(keptOrdinals, valueCount))
                + ", repLevels=" + Arrays.toString(repLevels())
                + ", defLevels=" + Arrays.toString(defLevels())
                + ", entryCount=" + entryCount
                + ", valueCount=" + valueCount
                + ", maxRepLevel=" + maxRepLevel
                + ", maxDefLevel=" + maxDefLevel
                + "]";
    }

    @Override
    public int hashCode() {
        int result = leaf.hashCode();
        result = 31 * result + sourceValues.hashCode();
        result = 31 * result + keptOrdinalsHashCode(keptOrdinals, valueCount);
        result = 31 * result + Arrays.hashCode(repLevels());
        result = 31 * result + Arrays.hashCode(defLevels());
        result = 31 * result + entryCount;
        result = 31 * result + valueCount;
        result = 31 * result + maxRepLevel;
        result = 31 * result + maxDefLevel;
        return result;
    }

    /** Compares the first {@code count} ordinals; the arrays may be oversized past their meaningful prefix. */
    private static boolean keptOrdinalsEqual(int[] a, int[] b, int count) {
        for (int i = 0; i < count; i++) {
            if (a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }

    /** Hashes the first {@code count} ordinals, matching {@link #keptOrdinalsEqual} over the meaningful prefix. */
    private static int keptOrdinalsHashCode(int[] kept, int count) {
        int result = 1;
        for (int i = 0; i < count; i++) {
            result = 31 * result + kept[i];
        }
        return result;
    }
}
