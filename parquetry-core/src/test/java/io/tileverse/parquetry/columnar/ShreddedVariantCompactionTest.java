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
package io.tileverse.parquetry.columnar;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;
import java.util.BitSet;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.ShreddedVariantVector.ScalarInput;
import io.tileverse.parquetry.columnar.ShreddedVariantVector.VariantInput;
import io.tileverse.parquetry.format.ParquetFormatException;
import io.tileverse.parquetry.internal.variant.VariantEncoder;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.variant.ShreddedVariant;

/**
 * Pins the eager-path rejection of a shredded Variant nested under a list or map. The lazy streaming path rejects the
 * same shape with the same exception type and the same list-or-map core in its message; this keeps the eager
 * {@link Compaction} arm aligned with it. The eager message additionally names the rule for the public entry point: a
 * shredded Variant vector must be unshredded before compaction.
 */
class ShreddedVariantCompactionTest {

    @Test
    void compactingAShreddedVariantChildIsRejectedLikeTheLazyPath() {
        ShreddedVariantVector vector = scalarShreddedVariantVector();

        int[] keptIndices = {0};
        assertThatThrownBy(() -> Compaction.compact(vector, keptIndices))
                .isInstanceOf(ParquetFormatException.class)
                .hasMessageContaining("list or map");
    }

    private static ShreddedVariantVector scalarShreddedVariantVector() {
        long[] typedValues = {7L, 11L};
        VariantEncoder.Encoded reference =
                new VariantEncoder().addLong(typedValues[0]).encode();

        BinaryVector metadataColumn = sharedMetadataColumn(reference.metadata(), typedValues.length);
        ShreddedVariant.Scalar model = new ShreddedVariant.Scalar(int64TypedValue(), 6);
        LongVector typedColumn = LongVector.materialized(typedValues, Validity.allValid(typedValues.length));
        VariantInput root = new VariantInput(allNullValueLeaf(typedValues.length), new ScalarInput(typedColumn));

        return new ShreddedVariantVector(
                metadataColumn, model, root, Validity.allValid(typedValues.length), typedValues.length);
    }

    private static BinaryVector sharedMetadataColumn(MemorySegment metadata, int rows) {
        MemorySegment[] perRow = new MemorySegment[rows];
        for (int row = 0; row < rows; row++) {
            perRow[row] = metadata;
        }
        return BinaryVector.materialized(perRow, Validity.allValid(rows));
    }

    private static BinaryVector allNullValueLeaf(int rows) {
        MemorySegment[] values = new MemorySegment[rows];
        BitSet validBits = new BitSet(rows);
        return BinaryVector.materialized(values, Validity.of(validBits, rows));
    }

    private static SchemaNode.Primitive int64TypedValue() {
        return new SchemaNode.Primitive(
                "typed_value", Repetition.OPTIONAL, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
    }
}
