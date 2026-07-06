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
package io.tileverse.parquetry.arrow.ipc;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DefaultParquetRecordBatch;
import io.tileverse.parquetry.columnar.LongVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.ShreddedVariantVector;
import io.tileverse.parquetry.columnar.ShreddedVariantVector.ScalarInput;
import io.tileverse.parquetry.columnar.ShreddedVariantVector.VariantInput;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.columnar.VariantVector;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.internal.variant.VariantEncoder;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.variant.ShreddedVariant;

/**
 * Round-trips a Parquet Variant column through the Arrow IPC stream and reads it back with arrow-java, which proves
 * both that arrow-java accepts the Variant's {@code struct<metadata, value>} representation with the extension tag
 * attached and that a shredded Variant flows through the unshredding step on the way out.
 */
class RoundTripVariantTest {

    @Test
    void roundTripsUnshreddedVariantAsAMetadataValueStruct() throws Exception {
        VariantEncoder.Encoded encoded = new VariantEncoder().addLong(42L).encode();
        VariantVector column = variantVector(encoded.metadata(), encoded.value());
        ParquetSchema schema = schema(variantGroup("v"));

        roundTrip(schema, "v", column, 1, root -> {
            org.apache.arrow.vector.complex.StructVector vector =
                    (org.apache.arrow.vector.complex.StructVector) root.getVector("v");
            org.apache.arrow.vector.VarBinaryVector metadata =
                    (org.apache.arrow.vector.VarBinaryVector) vector.getChild("metadata");
            org.apache.arrow.vector.VarBinaryVector value =
                    (org.apache.arrow.vector.VarBinaryVector) vector.getChild("value");
            assertThat(metadata.get(0)).containsExactly(toBytes(encoded.metadata()));
            assertThat(value.get(0)).containsExactly(toBytes(encoded.value()));
        });
    }

    @Test
    void roundTripsShreddedVariantThroughItsUnshreddedForm() throws Exception {
        VariantEncoder.Encoded reference = new VariantEncoder().addLong(7L).encode();
        ShreddedVariantVector shredded = scalarShredded(reference);
        byte[] expectedValue = toBytes(shredded.toUnshredded().valueColumn().get(0));
        ParquetSchema schema = schema(variantGroup("v"));

        roundTrip(schema, "v", shredded, 1, root -> {
            org.apache.arrow.vector.complex.StructVector vector =
                    (org.apache.arrow.vector.complex.StructVector) root.getVector("v");
            org.apache.arrow.vector.VarBinaryVector value =
                    (org.apache.arrow.vector.VarBinaryVector) vector.getChild("value");
            assertThat(value.get(0)).containsExactly(expectedValue);
        });
    }

    private static VariantVector variantVector(MemorySegment metadata, MemorySegment value) {
        BinaryVector metadataColumn = BinaryVector.materialized(new MemorySegment[] {metadata}, Validity.allValid(1));
        BinaryVector valueColumn = BinaryVector.materialized(new MemorySegment[] {value}, Validity.allValid(1));
        return new VariantVector(metadataColumn, valueColumn, Validity.allValid(1), 1);
    }

    private static ShreddedVariantVector scalarShredded(VariantEncoder.Encoded reference) {
        BinaryVector metadataColumn =
                BinaryVector.materialized(new MemorySegment[] {reference.metadata()}, Validity.allValid(1));
        ShreddedVariant.Scalar model = new ShreddedVariant.Scalar(
                new SchemaNode.Primitive(
                        "typed_value",
                        Repetition.OPTIONAL,
                        PrimitiveKind.INT64,
                        OptionalInt.empty(),
                        Optional.empty(),
                        -1),
                6);
        LongVector typed = LongVector.materialized(new long[] {7L}, Validity.allValid(1));
        VariantInput root = new VariantInput(allNullBinary(), new ScalarInput(typed));
        return new ShreddedVariantVector(metadataColumn, model, root, Validity.allValid(1), 1);
    }

    private static BinaryVector allNullBinary() {
        return BinaryVector.materialized(new MemorySegment[1], Validity.of(new java.util.BitSet(1), 1));
    }

    private static byte[] toBytes(MemorySegment segment) {
        return segment.toArray(ValueLayout.JAVA_BYTE);
    }

    private static void roundTrip(
            ParquetSchema schema,
            String columnName,
            ColumnVector vector,
            int rowCount,
            Consumer<VectorSchemaRoot> assertions)
            throws Exception {
        Map<ColumnPath, ColumnVector> columns = new LinkedHashMap<>();
        columns.put(ColumnPath.of(columnName), vector);
        ParquetRecordBatch batch = new DefaultParquetRecordBatch(schema, columns, rowCount, Arena.ofShared());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowIpcWriter.write(schema, Optional.empty(), Stream.of(batch), out);

        try (RootAllocator allocator = new RootAllocator();
                ArrowStreamReader reader =
                        new ArrowStreamReader(new ByteArrayInputStream(out.toByteArray()), allocator)) {
            assertThat(reader.loadNextBatch()).isTrue();
            assertions.accept(reader.getVectorSchemaRoot());
        }
    }

    private static ParquetSchema schema(SchemaNode field) {
        return new ParquetSchema(
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(field), Optional.empty(), -1));
    }

    private static SchemaNode.Group variantGroup(String name) {
        SchemaNode.Primitive metadata = new SchemaNode.Primitive(
                "metadata", Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), 0);
        SchemaNode.Primitive value = new SchemaNode.Primitive(
                "value", Repetition.OPTIONAL, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), 1);
        return new SchemaNode.Group(
                name, Repetition.OPTIONAL, List.of(metadata, value), Optional.of(new LogicalType.Variant()), 2);
    }
}
