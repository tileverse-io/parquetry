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
package io.tileverse.parquetry.internal.variant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.ParquetFormatException;
import io.tileverse.parquetry.variant.ShreddedVariant;

class ShreddedVariantTest {

    @Test
    void scalarInt64ClassifiesAsScalarWithLongTypeId() {
        ShreddedVariant classified = ShreddedVariant.classify(VariantSchemas.scalarInt64Group());

        assertThat(classified).isInstanceOfSatisfying(ShreddedVariant.Scalar.class, scalar -> {
            assertThat(scalar.variantTypeId()).isEqualTo(6);
            assertThat(scalar.typedValue().name()).isEqualTo("typed_value");
        });
    }

    @Test
    void unsignedInt32ScalarIsRejected() {
        assertThatThrownBy(() -> ShreddedVariant.classify(VariantSchemas.scalarUnsignedInt32Group()))
                .isInstanceOf(ParquetFormatException.class)
                .hasMessageContaining("Unsupported shredded value type");
    }

    @Test
    void byteArrayDecimalScalarClassifiesAsDecimal16() {
        ShreddedVariant classified = ShreddedVariant.classify(VariantSchemas.scalarByteArrayDecimalGroup());

        assertThat(classified)
                .isInstanceOfSatisfying(
                        ShreddedVariant.Scalar.class,
                        scalar -> assertThat(scalar.variantTypeId()).isEqualTo(10));
    }

    @Test
    void fixedLen4WithoutLogicalTypeIsRejected() {
        assertThatThrownBy(() -> ShreddedVariant.classify(VariantSchemas.fixedLen4Group()))
                .isInstanceOf(ParquetFormatException.class)
                .hasMessageContaining("Unsupported shredded value type");
    }

    @Test
    void objectWithTwoScalarFieldsClassifiesAsObject() {
        ShreddedVariant classified = ShreddedVariant.classify(VariantSchemas.objectTwoScalarFieldsGroup());

        assertThat(classified).isInstanceOfSatisfying(ShreddedVariant.ShreddedObject.class, object -> {
            assertThat(object.fields()).containsOnlyKeys("id", "name");
            ShreddedVariant idTypedValue = object.fields().get("id").typedValue();
            assertThat(idTypedValue)
                    .isInstanceOfSatisfying(
                            ShreddedVariant.Scalar.class,
                            scalar -> assertThat(scalar.variantTypeId()).isEqualTo(6));
        });
    }

    @Test
    void objectFieldThatIsAPrimitiveIsRejected() {
        assertThatThrownBy(() -> ShreddedVariant.classify(VariantSchemas.objectWithPrimitiveFieldGroup()))
                .isInstanceOf(ParquetFormatException.class)
                .hasMessageContaining("must be a value/typed_value group");
    }

    @Test
    void fieldWithNeitherValueNorTypedValueIsRejected() {
        assertThatThrownBy(
                        () -> ShreddedVariant.classify(VariantSchemas.objectFieldWithNeitherValueNorTypedValueGroup()))
                .isInstanceOf(ParquetFormatException.class)
                .hasMessageContaining("neither a value nor a typed_value");
    }

    @Test
    void arrayOfInt32ClassifiesAsArrayWithIntElement() {
        ShreddedVariant classified = ShreddedVariant.classify(VariantSchemas.arrayOfInt32Group());

        assertThat(classified).isInstanceOfSatisfying(ShreddedVariant.ShreddedArray.class, array -> {
            ShreddedVariant elementTypedValue = array.element().typedValue();
            assertThat(elementTypedValue)
                    .isInstanceOfSatisfying(
                            ShreddedVariant.Scalar.class,
                            scalar -> assertThat(scalar.variantTypeId()).isEqualTo(5));
        });
    }
}
