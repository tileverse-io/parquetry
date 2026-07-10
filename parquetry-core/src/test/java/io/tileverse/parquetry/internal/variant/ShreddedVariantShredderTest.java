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
package io.tileverse.parquetry.internal.variant;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.internal.variant.ShreddedVariantReconstructor.NodeReader;
import io.tileverse.parquetry.internal.variant.ShreddedVariantShredder.ArrayShred;
import io.tileverse.parquetry.internal.variant.ShreddedVariantShredder.FieldShred;
import io.tileverse.parquetry.internal.variant.ShreddedVariantShredder.ObjectShred;
import io.tileverse.parquetry.internal.variant.ShreddedVariantShredder.ScalarShred;
import io.tileverse.parquetry.internal.variant.ShreddedVariantShredder.VariantShred;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.variant.ShreddedVariant;
import io.tileverse.parquetry.variant.Variant;
import io.tileverse.parquetry.variant.VariantMetadata;

/**
 * Verifies the shredder is the exact inverse of the reconstructor: shred a {@link Variant} against a model, drive a
 * {@link NodeReader} over the shred result through {@link ShreddedVariantReconstructor}, then assert the reconstructed
 * value serializes identically to the original.
 */
class ShreddedVariantShredderTest {

    @Test
    void scalarMatchingTheTypedSlotGoesToTyped() {
        SchemaNode.Group schema = VariantSchemas.scalarInt64Group();
        VariantEncoder.Encoded input = new VariantEncoder().addLong(9876543210L).encode();

        VariantShred shred = shred(schema, input);

        ScalarShred scalar = (ScalarShred) shred;
        assertThat(scalar.hasTyped()).isTrue();
        assertThat(scalar.typed()).isEqualTo(9876543210L);
        assertThat(scalar.residualValue()).isNull();
        assertReconstructsTo(schema, input, shred);
    }

    @Test
    void scalarNotMatchingTheTypedSlotGoesToResidual() {
        SchemaNode.Group schema = VariantSchemas.scalarInt64Group();
        VariantEncoder.Encoded input =
                new VariantEncoder().addString("not a long").encode();

        VariantShred shred = shred(schema, input);

        ScalarShred scalar = (ScalarShred) shred;
        assertThat(scalar.hasTyped()).isFalse();
        assertThat(scalar.residualValue()).isNotNull();
        assertReconstructsTo(schema, input, shred);
    }

    @Test
    void nullValueLeavesBothTypedAndResidualNull() {
        SchemaNode.Group schema = VariantSchemas.scalarInt64Group();
        VariantEncoder.Encoded input = new VariantEncoder().addNull().encode();

        VariantShred shred = shred(schema, input);

        ScalarShred scalar = (ScalarShred) shred;
        assertThat(scalar.typed()).isNull();
        assertThat(scalar.residualValue()).isNull();
        assertReconstructsTo(schema, input, shred);
    }

    @Test
    void objectWithShreddedAndNonShreddedFieldsSplitsThem() {
        SchemaNode.Group schema = VariantSchemas.objectTwoScalarFieldsGroup();
        VariantEncoder.Encoded input = new VariantEncoder()
                .startObject()
                .field("id")
                .addLong(7)
                .field("name")
                .addString("ada")
                .field("extra")
                .addInt(99)
                .endObject()
                .encode();

        VariantShred shred = shred(schema, input);

        ObjectShred object = (ObjectShred) shred;
        assertThat(object.present()).isTrue();
        assertThat(object.fields()).containsKeys("id", "name");
        assertThat(((ScalarShred) object.fields().get("id").typedValue()).typed())
                .isEqualTo(7L);
        assertThat(object.residualValue())
                .as("non-shredded extra field kept in residual")
                .isNotNull();
        assertReconstructsTo(schema, input, shred);
    }

    @Test
    void objectWithMultipleNonShreddedFieldsKeepsAllOfThemBounded() {
        SchemaNode.Group schema = VariantSchemas.objectTwoScalarFieldsGroup();
        VariantEncoder.Encoded input = new VariantEncoder()
                .startObject()
                .field("alpha")
                .addInt(1)
                .field("id")
                .addLong(7)
                .field("zeta")
                .addInt(2)
                .endObject()
                .encode();

        VariantShred shred = shred(schema, input);

        VariantMetadata metadata = new VariantMetadata(input.metadata());
        Variant residual = Variant.of(((ObjectShred) shred).residualValue(), metadata);
        assertThat(residual.fieldNames()).containsExactly("alpha", "zeta");
        assertReconstructsTo(schema, input, shred);
    }

    @Test
    void objectMissingAShreddedFieldLeavesThatFieldAbsent() {
        SchemaNode.Group schema = VariantSchemas.objectTwoScalarFieldsGroup();
        VariantEncoder.Encoded input = new VariantEncoder()
                .startObject()
                .field("id")
                .addLong(7)
                .endObject()
                .encode();

        VariantShred shred = shred(schema, input);

        ObjectShred object = (ObjectShred) shred;
        FieldShred nameField = object.fields().get("name");
        assertThat(nameField.value()).isNull();
        assertThat(nameField.typedValue()).isNull();
        assertThat(object.residualValue()).as("no non-shredded fields").isNull();
        assertReconstructsTo(schema, input, shred);
    }

    @Test
    void objectWithOnlyShreddedFieldsHasNoResidual() {
        SchemaNode.Group schema = VariantSchemas.objectTwoScalarFieldsGroup();
        VariantEncoder.Encoded input = new VariantEncoder()
                .startObject()
                .field("id")
                .addLong(1)
                .field("name")
                .addString("x")
                .endObject()
                .encode();

        VariantShred shred = shred(schema, input);

        assertThat(((ObjectShred) shred).residualValue()).isNull();
        assertReconstructsTo(schema, input, shred);
    }

    @Test
    void nonObjectValueAgainstObjectSchemaGoesEntirelyToResidual() {
        SchemaNode.Group schema = VariantSchemas.objectTwoScalarFieldsGroup();
        VariantEncoder.Encoded input = new VariantEncoder().addInt(42).encode();

        VariantShred shred = shred(schema, input);

        ObjectShred object = (ObjectShred) shred;
        assertThat(object.present()).isFalse();
        assertThat(object.residualValue()).isNotNull();
        assertReconstructsTo(schema, input, shred);
    }

    @Test
    void arrayOfIntShredsEveryElement() {
        SchemaNode.Group schema = VariantSchemas.arrayOfInt32Group();
        VariantEncoder.Encoded input = new VariantEncoder()
                .startArray()
                .addInt(1)
                .addInt(2)
                .addInt(3)
                .endArray()
                .encode();

        VariantShred shred = shred(schema, input);

        ArrayShred array = (ArrayShred) shred;
        assertThat(array.present()).isTrue();
        assertThat(array.elements()).hasSize(3);
        assertThat(((ScalarShred) array.elements().get(0)).typed()).isEqualTo(1);
        assertReconstructsTo(schema, input, shred);
    }

    @Test
    void arrayWithNonMatchingElementKeepsItInResidual() {
        SchemaNode.Group schema = VariantSchemas.arrayOfInt32Group();
        VariantEncoder.Encoded input = new VariantEncoder()
                .startArray()
                .addInt(1)
                .addString("nope")
                .endArray()
                .encode();

        VariantShred shred = shred(schema, input);

        ArrayShred array = (ArrayShred) shred;
        assertThat(((ScalarShred) array.elements().get(0)).typed()).isEqualTo(1);
        assertThat(((ScalarShred) array.elements().get(1)).residualValue()).isNotNull();
        assertReconstructsTo(schema, input, shred);
    }

    @Test
    void nullArrayElementReconstructs() {
        SchemaNode.Group schema = VariantSchemas.arrayOfInt32Group();
        VariantEncoder.Encoded input =
                new VariantEncoder().startArray().addInt(1).addNull().endArray().encode();

        VariantShred shred = shred(schema, input);

        assertReconstructsTo(schema, input, shred);
    }

    @Test
    void nullValueAgainstArraySchemaReconstructsToNull() {
        SchemaNode.Group schema = VariantSchemas.arrayOfInt32Group();
        VariantEncoder.Encoded input = new VariantEncoder().addNull().encode();

        VariantShred shred = shred(schema, input);

        ArrayShred array = (ArrayShred) shred;
        assertThat(array.present()).isFalse();
        assertReconstructsTo(schema, input, shred);
    }

    private static VariantShred shred(SchemaNode.Group schema, VariantEncoder.Encoded input) {
        VariantMetadata metadata = new VariantMetadata(input.metadata());
        ShreddedVariant model = ShreddedVariant.classify(schema);
        return new ShreddedVariantShredder().shred(model, Variant.of(input.value(), metadata), metadata);
    }

    private static void assertReconstructsTo(
            SchemaNode.Group schema, VariantEncoder.Encoded input, VariantShred shred) {
        VariantMetadata metadata = new VariantMetadata(input.metadata());
        ShreddedVariant model = ShreddedVariant.classify(schema);
        ShreddedVariantReconstructor reconstructor =
                new ShreddedVariantReconstructor(metadata, ShreddedVariantReconstructor.Leniency.STRICT);

        MemorySegment reconstructed = reconstructor.reconstruct(model, ShredNodeReader.over(shred));

        byte[] original = Variant.of(input.value(), metadata).serialize();
        byte[] roundTripped = Variant.of(reconstructed, metadata).serialize();
        assertThat(roundTripped).isEqualTo(original);
    }

    /** Drives the reconstructor over one {@link VariantShred} node, the read-side dual of the shred write tree. */
    private record ShredNodeReader(VariantShred shred) implements NodeReader {

        static NodeReader over(VariantShred shred) {
            return new ShredNodeReader(shred);
        }

        @Override
        public MemorySegment value() {
            return switch (shred) {
                case ScalarShred scalar -> scalar.residualValue();
                case ObjectShred object -> object.residualValue();
                case ArrayShred array -> array.residualValue();
            };
        }

        @Override
        public boolean hasTypedScalar() {
            return shred instanceof ScalarShred scalar && scalar.hasTyped();
        }

        @Override
        public Object typedScalar() {
            return ((ScalarShred) shred).typed();
        }

        @Override
        public boolean hasTypedObject() {
            return shred instanceof ObjectShred object && object.present();
        }

        @Override
        public Map<String, NodeReader> typedObjectFields() {
            if (!(shred instanceof ObjectShred object) || !object.present()) {
                return null;
            }
            Map<String, NodeReader> readers = new LinkedHashMap<>();
            for (Map.Entry<String, FieldShred> entry : object.fields().entrySet()) {
                readers.put(entry.getKey(), new FieldNodeReader(entry.getValue()));
            }
            return readers;
        }

        @Override
        public List<NodeReader> typedArrayElements() {
            if (!(shred instanceof ArrayShred array) || !array.present()) {
                return null;
            }
            List<NodeReader> readers = new ArrayList<>(array.elements().size());
            for (VariantShred element : array.elements()) {
                readers.add(ShredNodeReader.over(element));
            }
            return readers;
        }
    }

    /**
     * Drives the reconstructor over one object {@link FieldShred}. A field's {@code value} residual is its own leaf;
     * its {@code typedValue} shred drives the nested typed node.
     */
    private record FieldNodeReader(FieldShred field) implements NodeReader {

        @Override
        public MemorySegment value() {
            return field.value();
        }

        @Override
        public boolean hasTypedScalar() {
            return field.typedValue() instanceof ScalarShred scalar && scalar.hasTyped();
        }

        @Override
        public Object typedScalar() {
            return ((ScalarShred) field.typedValue()).typed();
        }

        @Override
        public boolean hasTypedObject() {
            return field.typedValue() instanceof ObjectShred object && object.present();
        }

        @Override
        public Map<String, NodeReader> typedObjectFields() {
            if (field.typedValue() == null) {
                return null;
            }
            return ShredNodeReader.over(field.typedValue()).typedObjectFields();
        }

        @Override
        public List<NodeReader> typedArrayElements() {
            if (field.typedValue() == null) {
                return null;
            }
            return ShredNodeReader.over(field.typedValue()).typedArrayElements();
        }
    }
}
