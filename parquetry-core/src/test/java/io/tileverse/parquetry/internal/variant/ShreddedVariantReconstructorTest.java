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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.ParquetFormatException;
import io.tileverse.parquetry.internal.variant.ShreddedVariantReconstructor.NodeReader;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.variant.ShreddedVariant;
import io.tileverse.parquetry.variant.VariantMetadata;

class ShreddedVariantReconstructorTest {

    @Test
    void scalarShreddedLongEncodesCanonicalValueBytes() {
        VariantEncoder.Encoded reference = new VariantEncoder().addLong(7).encode();
        ShreddedVariant.Scalar model = scalar(PrimitiveKind.INT64, 6);
        NodeReader reader = TestNodeReader.scalar(7L);

        MemorySegment reconstructed = reconstructor(reference).reconstruct(model, reader);

        assertThat(bytes(reconstructed)).isEqualTo(bytes(reference.value()));
    }

    @Test
    void scalarPassthroughReturnsValueVerbatim() {
        MemorySegment preEncoded = new VariantEncoder().addInt(42).encode().value();
        NodeReader reader = TestNodeReader.value(preEncoded);

        MemorySegment reconstructed = reconstructor(emptyMetadata()).reconstruct(null, reader);

        assertThat(reconstructed).isSameAs(preEncoded);
    }

    @Test
    void bothNullTopLevelReturnsNullValueByte() {
        MemorySegment canonicalNull = new VariantEncoder().addNull().encode().value();
        NodeReader reader = TestNodeReader.empty();

        MemorySegment reconstructed = reconstructor(emptyMetadata()).reconstruct(null, reader);

        assertThat(bytes(reconstructed)).isEqualTo(bytes(canonicalNull)).containsExactly((byte) 0x00);
    }

    @Test
    void scalarConflictBetweenValueAndTypedScalarFails() {
        MemorySegment value = new VariantEncoder().addLong(7).encode().value();
        ShreddedVariant.Scalar model = scalar(PrimitiveKind.INT64, 6);
        NodeReader reader = TestNodeReader.valueAndScalar(value, 7L);

        assertThatThrownBy(() -> reconstructor(emptyMetadata()).reconstruct(model, reader))
                .isInstanceOf(ParquetFormatException.class)
                .hasMessageContaining("conflicting value and typed_value");
    }

    @Test
    void scalarModelWithValuePresentAndTypedAbsentReturnsValueVerbatim() {
        MemorySegment value = new VariantEncoder().addNull().encode().value();
        ShreddedVariant.Scalar model = scalar(PrimitiveKind.INT64, 6);
        NodeReader reader = TestNodeReader.value(value);

        MemorySegment reconstructed = reconstructor(emptyMetadata()).reconstruct(model, reader);

        assertThat(reconstructed).isSameAs(value);
    }

    @Test
    void objectWithOneShreddedScalarFieldEncodesReferenceBytes() {
        VariantEncoder.Encoded reference = new VariantEncoder()
                .startObject()
                .field("a")
                .addInt(1)
                .endObject()
                .encode();
        ShreddedVariant.ShreddedObject model = object(Map.of("a", scalarField(PrimitiveKind.INT32, 5)));
        NodeReader reader = TestNodeReader.objectFields(Map.of("a", TestNodeReader.scalar(1)));

        MemorySegment reconstructed = reconstructor(reference).reconstruct(model, reader);

        assertThat(bytes(reconstructed)).isEqualTo(bytes(reference.value()));
    }

    @Test
    void partialMergeCombinesUnshreddedAndShreddedFields() {
        VariantEncoder.Encoded reference = new VariantEncoder()
                .startObject()
                .field("a")
                .addInt(1)
                .field("b")
                .addInt(2)
                .endObject()
                .encode();
        VariantMetadata metadata = new VariantMetadata(reference.metadata());
        MemorySegment unshreddedAOnly = new VariantEncoder()
                .startObject()
                .field("a")
                .addInt(1)
                .endObject()
                .encodeValue(metadata);

        ShreddedVariant.ShreddedObject model = object(Map.of("b", scalarField(PrimitiveKind.INT32, 5)));
        NodeReader reader = TestNodeReader.valueAndObjectFields(unshreddedAOnly, Map.of("b", TestNodeReader.scalar(2)));

        MemorySegment reconstructed = reconstructor(reference).reconstruct(model, reader);

        assertThat(bytes(reconstructed)).isEqualTo(bytes(reference.value()));
    }

    @Test
    void nonObjectValueWithShreddedFieldsFails() {
        VariantEncoder.Encoded reference = new VariantEncoder()
                .startObject()
                .field("a")
                .addInt(1)
                .endObject()
                .encode();
        VariantMetadata metadata = new VariantMetadata(reference.metadata());
        MemorySegment scalarValue = new VariantEncoder().addInt(99).encodeValue(metadata);

        ShreddedVariant.ShreddedObject model = object(Map.of("a", scalarField(PrimitiveKind.INT32, 5)));
        NodeReader reader = TestNodeReader.valueAndObjectFields(scalarValue, Map.of("a", TestNodeReader.scalar(1)));

        assertThatThrownBy(() -> reconstructor(reference).reconstruct(model, reader))
                .isInstanceOf(ParquetFormatException.class)
                .hasMessageContaining("non-object value with shredded fields");
    }

    @Test
    void presentObjectGroupWithNonObjectValueAndNoPresentFieldsFails() {
        VariantEncoder.Encoded reference = new VariantEncoder()
                .startObject()
                .field("a")
                .addInt(1)
                .endObject()
                .encode();
        VariantMetadata metadata = new VariantMetadata(reference.metadata());
        MemorySegment scalarValue = new VariantEncoder().addInt(34).encodeValue(metadata);

        ShreddedVariant.ShreddedObject model = object(Map.of("a", scalarField(PrimitiveKind.INT32, 5)));
        NodeReader reader = TestNodeReader.valueAndObjectFields(scalarValue, Map.of("a", TestNodeReader.empty()));

        assertThatThrownBy(() -> reconstructor(metadata).reconstruct(model, reader))
                .isInstanceOf(ParquetFormatException.class)
                .hasMessageContaining("non-object value with shredded fields");
    }

    @Test
    void absentObjectGroupWithNonObjectValueReturnsValueVerbatim() {
        VariantEncoder.Encoded reference = new VariantEncoder()
                .startObject()
                .field("a")
                .addInt(1)
                .endObject()
                .encode();
        VariantMetadata metadata = new VariantMetadata(reference.metadata());
        MemorySegment scalarValue = new VariantEncoder().addInt(34).encodeValue(metadata);

        ShreddedVariant.ShreddedObject model = object(Map.of("a", scalarField(PrimitiveKind.INT32, 5)));
        NodeReader reader = TestNodeReader.value(scalarValue);

        MemorySegment reconstructed = reconstructor(metadata).reconstruct(model, reader);

        assertThat(reconstructed).isSameAs(scalarValue);
    }

    @Test
    void residualValueFieldNamedAsAShreddedFieldFails() {
        VariantEncoder.Encoded reference = new VariantEncoder()
                .startObject()
                .field("a")
                .addInt(1)
                .field("b")
                .addInt(2)
                .endObject()
                .encode();
        VariantMetadata metadata = new VariantMetadata(reference.metadata());
        MemorySegment unshreddedBOnly = new VariantEncoder()
                .startObject()
                .field("b")
                .addInt(2)
                .endObject()
                .encodeValue(metadata);

        ShreddedVariant.ShreddedObject model =
                object(Map.of("a", scalarField(PrimitiveKind.INT32, 5), "b", scalarField(PrimitiveKind.INT32, 5)));
        NodeReader reader = TestNodeReader.valueAndObjectFields(
                unshreddedBOnly, Map.of("a", TestNodeReader.scalar(1), "b", TestNodeReader.empty()));

        assertThatThrownBy(() -> reconstructor(metadata).reconstruct(model, reader))
                .isInstanceOf(ParquetFormatException.class)
                .hasMessageContaining("conflicting value and typed_value for field");
    }

    @Test
    void fieldPresentInBothValueAndShreddedFails() {
        VariantEncoder.Encoded reference = new VariantEncoder()
                .startObject()
                .field("a")
                .addInt(1)
                .endObject()
                .encode();
        VariantMetadata metadata = new VariantMetadata(reference.metadata());
        MemorySegment unshreddedAOnly = new VariantEncoder()
                .startObject()
                .field("a")
                .addInt(1)
                .endObject()
                .encodeValue(metadata);

        ShreddedVariant.ShreddedObject model = object(Map.of("a", scalarField(PrimitiveKind.INT32, 5)));
        NodeReader reader = TestNodeReader.valueAndObjectFields(unshreddedAOnly, Map.of("a", TestNodeReader.scalar(1)));

        assertThatThrownBy(() -> reconstructor(reference).reconstruct(model, reader))
                .isInstanceOf(ParquetFormatException.class)
                .hasMessageContaining("conflicting value and typed_value for field");
    }

    @Test
    void absentShreddedFieldIsOmittedFromOutput() {
        VariantEncoder.Encoded reference = new VariantEncoder()
                .startObject()
                .field("a")
                .addInt(1)
                .field("b")
                .addInt(2)
                .endObject()
                .encode();
        VariantMetadata metadata = new VariantMetadata(reference.metadata());
        MemorySegment unshreddedAOnly = new VariantEncoder()
                .startObject()
                .field("a")
                .addInt(1)
                .endObject()
                .encodeValue(metadata);
        MemorySegment referenceAOnly = new VariantEncoder()
                .startObject()
                .field("a")
                .addInt(1)
                .endObject()
                .encodeValue(metadata);

        ShreddedVariant.ShreddedObject model = object(Map.of("b", scalarField(PrimitiveKind.INT32, 5)));
        NodeReader reader = TestNodeReader.valueAndObjectFields(unshreddedAOnly, Map.of("b", TestNodeReader.empty()));

        MemorySegment reconstructed = reconstructor(reference).reconstruct(model, reader);

        assertThat(bytes(reconstructed)).isEqualTo(bytes(referenceAOnly));
    }

    @Test
    void nestedShreddedObjectFieldEncodesReferenceBytes() {
        VariantEncoder.Encoded reference = new VariantEncoder()
                .startObject()
                .field("outer")
                .startObject()
                .field("inner")
                .addInt(5)
                .endObject()
                .endObject()
                .encode();
        ShreddedVariant.Field innerField = scalarField(PrimitiveKind.INT32, 5);
        ShreddedVariant.Field outerField = objectField(Map.of("inner", innerField));
        ShreddedVariant.ShreddedObject model = object(Map.of("outer", outerField));
        NodeReader innerReader = TestNodeReader.scalar(5);
        NodeReader outerReader = TestNodeReader.objectFields(Map.of("inner", innerReader));
        NodeReader reader = TestNodeReader.objectFields(Map.of("outer", outerReader));

        MemorySegment reconstructed = reconstructor(reference).reconstruct(model, reader);

        assertThat(bytes(reconstructed)).isEqualTo(bytes(reference.value()));
    }

    @Test
    void emptyObjectAllFieldsAbsentEncodesEmptyObject() {
        VariantEncoder.Encoded reference =
                new VariantEncoder().startObject().endObject().encode();
        ShreddedVariant.ShreddedObject model = object(Map.of("a", scalarField(PrimitiveKind.INT32, 5)));
        NodeReader reader = TestNodeReader.objectFields(Map.of("a", TestNodeReader.empty()));

        MemorySegment reconstructed = reconstructor(reference).reconstruct(model, reader);

        assertThat(bytes(reconstructed)).isEqualTo(bytes(reference.value()));
    }

    @Test
    void interleavedFieldsEmitInCanonicalIdOrder() {
        VariantEncoder.Encoded reference = new VariantEncoder()
                .startObject()
                .field("a")
                .addInt(1)
                .field("c")
                .addInt(3)
                .endObject()
                .encode();
        VariantMetadata metadata = new VariantMetadata(reference.metadata());
        MemorySegment unshreddedCOnly = new VariantEncoder()
                .startObject()
                .field("c")
                .addInt(3)
                .endObject()
                .encodeValue(metadata);

        ShreddedVariant.ShreddedObject model = object(Map.of("a", scalarField(PrimitiveKind.INT32, 5)));
        NodeReader reader = TestNodeReader.valueAndObjectFields(unshreddedCOnly, Map.of("a", TestNodeReader.scalar(1)));

        MemorySegment reconstructed = reconstructor(reference).reconstruct(model, reader);

        assertThat(bytes(reconstructed)).isEqualTo(bytes(reference.value()));
    }

    @Test
    void absentObjectTypedFieldIsOmittedFromOutput() {
        VariantEncoder.Encoded reference = new VariantEncoder()
                .startObject()
                .field("a")
                .addInt(1)
                .endObject()
                .encode();
        VariantMetadata metadata = new VariantMetadata(reference.metadata());
        MemorySegment unshreddedAOnly = new VariantEncoder()
                .startObject()
                .field("a")
                .addInt(1)
                .endObject()
                .encodeValue(metadata);

        ShreddedVariant.Field nestedField = objectField(Map.of("inner", scalarField(PrimitiveKind.INT32, 5)));
        ShreddedVariant.ShreddedObject model = object(Map.of("b", nestedField));
        NodeReader reader = TestNodeReader.valueAndObjectFields(unshreddedAOnly, Map.of("b", TestNodeReader.empty()));

        MemorySegment reconstructed = reconstructor(reference).reconstruct(model, reader);

        assertThat(bytes(reconstructed)).isEqualTo(bytes(unshreddedAOnly));
    }

    @Test
    void arrayOfScalarsEncodesReferenceBytes() {
        VariantEncoder.Encoded reference = new VariantEncoder()
                .startArray()
                .addInt(1)
                .addInt(2)
                .addInt(3)
                .endArray()
                .encode();
        ShreddedVariant.ShreddedArray model = array(scalarField(PrimitiveKind.INT32, 5));
        NodeReader reader = TestNodeReader.arrayElements(
                List.of(TestNodeReader.scalar(1), TestNodeReader.scalar(2), TestNodeReader.scalar(3)));

        MemorySegment reconstructed = reconstructor(reference).reconstruct(model, reader);

        assertThat(bytes(reconstructed)).isEqualTo(bytes(reference.value()));
    }

    @Test
    void arrayWithNullElementEncodesReferenceBytes() {
        VariantEncoder.Encoded reference =
                new VariantEncoder().startArray().addInt(1).addNull().endArray().encode();
        ShreddedVariant.ShreddedArray model = array(scalarField(PrimitiveKind.INT32, 5));
        NodeReader reader = TestNodeReader.arrayElements(List.of(TestNodeReader.scalar(1), TestNodeReader.empty()));

        MemorySegment reconstructed = reconstructor(reference).reconstruct(model, reader);

        assertThat(bytes(reconstructed)).isEqualTo(bytes(reference.value()));
    }

    @Test
    void presentEmptyArrayEncodesEmptyArray() {
        VariantEncoder.Encoded reference =
                new VariantEncoder().startArray().endArray().encode();
        ShreddedVariant.ShreddedArray model = array(scalarField(PrimitiveKind.INT32, 5));
        NodeReader reader = TestNodeReader.arrayElements(List.of());

        MemorySegment reconstructed = reconstructor(reference).reconstruct(model, reader);

        assertThat(bytes(reconstructed)).isEqualTo(bytes(reference.value()));
    }

    @Test
    void nullArrayReturnsNullValueByte() {
        ShreddedVariant.ShreddedArray model = array(scalarField(PrimitiveKind.INT32, 5));
        NodeReader reader = TestNodeReader.empty();

        MemorySegment reconstructed = reconstructor(emptyMetadata()).reconstruct(model, reader);

        assertThat(bytes(reconstructed)).containsExactly((byte) 0x00);
    }

    @Test
    void arrayWithUnshreddedValuePresentFails() {
        VariantEncoder.Encoded reference =
                new VariantEncoder().startArray().addInt(1).endArray().encode();
        VariantMetadata metadata = new VariantMetadata(reference.metadata());
        MemorySegment value = new VariantEncoder().addInt(99).encodeValue(metadata);
        ShreddedVariant.ShreddedArray model = array(scalarField(PrimitiveKind.INT32, 5));
        NodeReader reader = TestNodeReader.valueAndArrayElements(value, List.of(TestNodeReader.scalar(1)));

        assertThatThrownBy(() -> reconstructor(metadata).reconstruct(model, reader))
                .isInstanceOf(ParquetFormatException.class)
                .hasMessageContaining("conflicting value and typed_value");
    }

    private static ShreddedVariantReconstructor reconstructor(VariantEncoder.Encoded reference) {
        return reconstructor(new VariantMetadata(reference.metadata()));
    }

    private static ShreddedVariantReconstructor reconstructor(VariantMetadata metadata) {
        return new ShreddedVariantReconstructor(metadata, ShreddedVariantReconstructor.Leniency.STRICT);
    }

    private static VariantMetadata emptyMetadata() {
        return new VariantMetadata(new VariantEncoder().addNull().encode().metadata());
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

    private static ShreddedVariant.ShreddedArray array(ShreddedVariant.Field element) {
        return new ShreddedVariant.ShreddedArray(element);
    }

    private static SchemaNode.Primitive primitive(PrimitiveKind kind) {
        return new SchemaNode.Primitive(
                "typed_value", Repetition.OPTIONAL, kind, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static byte[] bytes(MemorySegment segment) {
        return segment.toArray(ValueLayout.JAVA_BYTE);
    }

    /** Hand-built {@link NodeReader} feeding the reconstructor a single row's leaves. */
    private record TestNodeReader(
            MemorySegment value,
            boolean hasTypedScalar,
            Object typedScalar,
            Map<String, NodeReader> typedObjectFields,
            List<NodeReader> typedArrayElements)
            implements NodeReader {

        @Override
        public boolean hasTypedObject() {
            return typedObjectFields != null;
        }

        static TestNodeReader empty() {
            return new TestNodeReader(null, false, null, null, null);
        }

        static TestNodeReader value(MemorySegment value) {
            return new TestNodeReader(value, false, null, null, null);
        }

        static TestNodeReader scalar(Object typedScalar) {
            return new TestNodeReader(null, true, typedScalar, null, null);
        }

        static TestNodeReader valueAndScalar(MemorySegment value, Object typedScalar) {
            return new TestNodeReader(value, true, typedScalar, null, null);
        }

        static TestNodeReader objectFields(Map<String, NodeReader> typedObjectFields) {
            return new TestNodeReader(null, false, null, typedObjectFields, null);
        }

        static TestNodeReader valueAndObjectFields(MemorySegment value, Map<String, NodeReader> typedObjectFields) {
            return new TestNodeReader(value, false, null, typedObjectFields, null);
        }

        static TestNodeReader arrayElements(List<NodeReader> typedArrayElements) {
            return new TestNodeReader(null, false, null, null, typedArrayElements);
        }

        static TestNodeReader valueAndArrayElements(MemorySegment value, List<NodeReader> typedArrayElements) {
            return new TestNodeReader(value, false, null, null, typedArrayElements);
        }
    }
}
