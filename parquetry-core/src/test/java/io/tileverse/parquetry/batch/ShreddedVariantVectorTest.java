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
package io.tileverse.parquetry.batch;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.util.BitSet;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.batch.ShreddedVariantVector.ObjectInput;
import io.tileverse.parquetry.batch.ShreddedVariantVector.ScalarInput;
import io.tileverse.parquetry.batch.ShreddedVariantVector.VariantInput;
import io.tileverse.parquetry.data.variant.ShreddedVariant;
import io.tileverse.parquetry.data.variant.Variant;
import io.tileverse.parquetry.data.variant.VariantEncoder;
import io.tileverse.parquetry.data.variant.VariantMetadata;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class ShreddedVariantVectorTest {

    @Test
    void scalarShreddedColumnSerializesEachRowCanonically() {
        long[] typedValues = {7L, 11L, 13L};
        VariantEncoder.Encoded[] references = new VariantEncoder.Encoded[typedValues.length];
        for (int row = 0; row < typedValues.length; row++) {
            references[row] = new VariantEncoder().addLong(typedValues[row]).encode();
        }

        BinaryVector metadataColumn = sharedMetadataColumn(references, typedValues.length);
        ShreddedVariant.Scalar model = scalar(PrimitiveKind.INT64, 6);
        LongVector typedColumn = LongVector.materialized(typedValues, Validity.allValid(typedValues.length));
        VariantInput root = new VariantInput(allNullValueLeaf(typedValues.length), new ScalarInput(typedColumn));

        ShreddedVariantVector vector = new ShreddedVariantVector(
                metadataColumn, model, root, Validity.allValid(typedValues.length), typedValues.length);

        for (int row = 0; row < typedValues.length; row++) {
            assertThat(vector.get(row).serialize())
                    .as("row %d canonical bytes", row)
                    .isEqualTo(serialized(references[row]));
        }
    }

    @Test
    void nullRowReturnsNull() {
        VariantEncoder.Encoded present = new VariantEncoder().addLong(7L).encode();
        VariantEncoder.Encoded other = new VariantEncoder().addLong(9L).encode();
        VariantEncoder.Encoded[] references = {present, other};

        BinaryVector metadataColumn = sharedMetadataColumn(references, 2);
        ShreddedVariant.Scalar model = scalar(PrimitiveKind.INT64, 6);
        LongVector typedColumn = LongVector.materialized(new long[] {7L, 9L}, Validity.allValid(2));
        VariantInput root = new VariantInput(allNullValueLeaf(2), new ScalarInput(typedColumn));

        BitSet validBits = new BitSet(2);
        validBits.set(0);
        Validity validity = Validity.of(validBits, 2);
        ShreddedVariantVector vector = new ShreddedVariantVector(metadataColumn, model, root, validity, 2);

        assertThat(vector.get(0)).as("valid row navigates").isNotNull();
        assertThat(vector.get(1)).as("null row yields null").isNull();
    }

    @Test
    void objectShreddedColumnSerializesReferenceBytes() {
        VariantEncoder.Encoded reference = new VariantEncoder()
                .startObject()
                .field("a")
                .addInt(1)
                .endObject()
                .encode();
        VariantMetadata metadata = new VariantMetadata(reference.metadata());

        BinaryVector metadataColumn = sharedMetadataColumn(new VariantEncoder.Encoded[] {reference}, 1);
        ShreddedVariant.ShreddedObject model = object(Map.of("a", scalarField(PrimitiveKind.INT32, 5)));
        IntVector fieldColumn = IntVector.materialized(new int[] {1}, Validity.allValid(1));
        VariantInput fieldInput = new VariantInput(allNullValueLeaf(1), new ScalarInput(fieldColumn));
        ObjectInput objectInput = new ObjectInput(Validity.allValid(1), Map.of("a", fieldInput));
        VariantInput root = new VariantInput(allNullValueLeaf(1), objectInput);

        ShreddedVariantVector vector = new ShreddedVariantVector(metadataColumn, model, root, Validity.allValid(1), 1);

        assertThat(vector.get(0).serialize()).isEqualTo(serialized(reference));
    }

    @Test
    void nestedObjectPresenceDistinguishesAbsentFromPresent() {
        VariantEncoder.Encoded withInner = new VariantEncoder()
                .startObject()
                .field("outer")
                .startObject()
                .field("inner")
                .addInt(5)
                .endObject()
                .endObject()
                .encode();
        VariantEncoder emptyTopLevelBuilder = new VariantEncoder();
        emptyTopLevelBuilder.startObject().endObject();
        MemorySegment emptyTopLevelValue = emptyTopLevelBuilder.encodeValue(new VariantMetadata(withInner.metadata()));

        BinaryVector metadataColumn = sharedMetadataColumn(new VariantEncoder.Encoded[] {withInner, withInner}, 2);

        ShreddedVariant.Field innerField = scalarField(PrimitiveKind.INT32, 5);
        ShreddedVariant.Field outerField = objectField(Map.of("inner", innerField));
        ShreddedVariant.ShreddedObject model = object(Map.of("outer", outerField));

        IntVector innerColumn = IntVector.materialized(new int[] {5, 0}, Validity.allValid(2));
        VariantInput innerInput = new VariantInput(allNullValueLeaf(2), new ScalarInput(innerColumn));

        BitSet innerPresent = new BitSet(2);
        innerPresent.set(0);
        ObjectInput innerObject = new ObjectInput(Validity.of(innerPresent, 2), Map.of("inner", innerInput));
        VariantInput outerInput = new VariantInput(allNullValueLeaf(2), innerObject);

        ObjectInput outerObject = new ObjectInput(Validity.allValid(2), Map.of("outer", outerInput));
        VariantInput root = new VariantInput(allNullValueLeaf(2), outerObject);

        ShreddedVariantVector vector = new ShreddedVariantVector(metadataColumn, model, root, Validity.allValid(2), 2);

        assertThat(vector.get(0).serialize())
                .as("inner present -> inner included")
                .isEqualTo(serialized(withInner));
        assertThat(vector.get(1).serialize())
                .as("inner presence cleared -> outer group absent, empty top-level object")
                .isEqualTo(concat(withInner.metadata(), emptyTopLevelValue));
    }

    @Test
    void repeatedGetReturnsCachedSegment() {
        VariantEncoder.Encoded reference = new VariantEncoder().addLong(7L).encode();
        BinaryVector metadataColumn = sharedMetadataColumn(new VariantEncoder.Encoded[] {reference}, 1);
        ShreddedVariant.Scalar model = scalar(PrimitiveKind.INT64, 6);
        LongVector typedColumn = LongVector.materialized(new long[] {7L}, Validity.allValid(1));
        VariantInput root = new VariantInput(allNullValueLeaf(1), new ScalarInput(typedColumn));

        ShreddedVariantVector vector = new ShreddedVariantVector(metadataColumn, model, root, Validity.allValid(1), 1);

        Variant first = vector.get(0);
        Variant second = vector.get(0);
        assertThat(second.serialize()).as("cached bytes stable").isEqualTo(first.serialize());
        assertThat(second.serialize()).as("cached bytes equal the reference").isEqualTo(serialized(reference));
    }

    private static byte[] serialized(VariantEncoder.Encoded encoded) {
        return concat(encoded.metadata(), encoded.value());
    }

    private static byte[] concat(MemorySegment metadata, MemorySegment value) {
        VariantMetadata wrapped = new VariantMetadata(metadata);
        return Variant.of(value, wrapped).serialize();
    }

    private static BinaryVector sharedMetadataColumn(VariantEncoder.Encoded[] references, int rows) {
        MemorySegment[] perRow = new MemorySegment[rows];
        for (int row = 0; row < rows; row++) {
            perRow[row] = references[Math.min(row, references.length - 1)].metadata();
        }
        return BinaryVector.materialized(perRow, Validity.allValid(rows));
    }

    private static BinaryVector allNullValueLeaf(int rows) {
        MemorySegment[] values = new MemorySegment[rows];
        BitSet validBits = new BitSet(rows);
        return BinaryVector.materialized(values, Validity.of(validBits, rows));
    }

    private static ShreddedVariant.Scalar scalar(PrimitiveKind kind, int variantTypeId) {
        return new ShreddedVariant.Scalar(primitive(kind), variantTypeId);
    }

    private static ShreddedVariant.Field scalarField(PrimitiveKind kind, int variantTypeId) {
        return new ShreddedVariant.Field(null, scalar(kind, variantTypeId));
    }

    private static ShreddedVariant.ShreddedObject object(Map<String, ShreddedVariant.Field> fields) {
        return new ShreddedVariant.ShreddedObject(fields);
    }

    private static ShreddedVariant.Field objectField(Map<String, ShreddedVariant.Field> fields) {
        return new ShreddedVariant.Field(null, object(fields));
    }

    private static SchemaNode.Primitive primitive(PrimitiveKind kind) {
        return new SchemaNode.Primitive(
                "typed_value", Repetition.OPTIONAL, kind, OptionalInt.empty(), Optional.empty(), -1);
    }
}
