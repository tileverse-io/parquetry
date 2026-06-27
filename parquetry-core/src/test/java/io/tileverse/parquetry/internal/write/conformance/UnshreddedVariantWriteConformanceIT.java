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
package io.tileverse.parquetry.internal.write.conformance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.data.ParquetWriteException;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.internal.variant.VariantEncoder;
import io.tileverse.parquetry.internal.variant.VariantEncoder.Encoded;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.variant.Variant;
import io.tileverse.parquetry.variant.VariantMetadata;

/**
 * Conformance gate for unshredded Variant write output. parquetry writes a file with one {@code optional group v
 * (VARIANT) { required binary metadata; optional binary value; }} column, then verifies the output two ways: parquetry
 * reads each row back as a {@link Variant} that serializes byte-for-byte to what was authored (with a null Variant row
 * reading back as null), and parquet-java opens the footer and reads the two binary leaves as opaque bytes equal to
 * what was written.
 *
 * <p>Rows under test cover a scalar (int) Variant, a string Variant, an object Variant, a Variant NULL value (a present
 * value of one {@code 0x00} byte), and an absent (null) Variant column.
 */
@Tag("conformance")
class UnshreddedVariantWriteConformanceIT {

    private static final ColumnPath V_COLUMN = ColumnPath.of("v");

    @TempDir
    Path tempDir;

    @Test
    void unshreddedVariantRoundTripsThroughParquetry() throws Exception {
        List<Encoded> authored = authoredVariants();
        Path file = tempDir.resolve("variant.parquet");
        writeVariantRows(file, authored);

        List<ParquetRecord> rows = WriteConformanceSupport.readAll(file);
        assertThat(rows).hasSize(authored.size() + 1);

        for (int row = 0; row < authored.size(); row++) {
            Object value = rows.get(row).get(V_COLUMN);
            assertThat(value).as("row %d is a non-null Variant", row).isInstanceOf(Variant.class);
            assertThat(((Variant) value).serialize())
                    .as("row %d serializes to the authored bytes", row)
                    .isEqualTo(expectedSerialized(authored.get(row)));
        }

        // Last row authored a null Variant column.
        assertThat(rows.get(authored.size()).get(V_COLUMN))
                .as("the absent Variant row reads back as null")
                .isNull();
    }

    @Test
    void settingNullOnAVariantChildPathIsRejected() {
        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(variantSchema());
        builder.setInt(ColumnPath.of("id"), 1);
        assertThatThrownBy(() -> builder.setNull(ColumnPath.of("v", "value")))
                .isInstanceOf(ParquetWriteException.class)
                .hasMessageContaining("Variant children are not addressable");
    }

    @Test
    void unshreddedVariantReadsCleanlyThroughParquetJava() throws Exception {
        List<Encoded> authored = authoredVariants();
        Path file = tempDir.resolve("variant-oracle.parquet");
        writeVariantRows(file, authored);

        ParquetMetadata footer = WriteConformanceSupport.readFooterViaParquetJava(file);
        MessageType messageType = footer.getFileMetaData().getSchema();
        String printed = messageType.toString();
        assertThat(printed).contains("optional group v (VARIANT");
        assertThat(printed).contains("required binary metadata");
        assertThat(printed).contains("optional binary value");

        List<GenericRecord> rows = WriteConformanceSupport.readWithAvro(file);
        assertThat(rows).hasSize(authored.size() + 1);

        for (int row = 0; row < authored.size(); row++) {
            GenericRecord variant = (GenericRecord) rows.get(row).get("v");
            assertThat(variant).as("row %d Variant group present", row).isNotNull();
            assertThat(bytesOf(variant.get("metadata")))
                    .as("row %d metadata bytes", row)
                    .isEqualTo(toArray(authored.get(row).metadata()));
            assertThat(bytesOf(variant.get("value")))
                    .as("row %d value bytes", row)
                    .isEqualTo(toArray(authored.get(row).value()));
        }

        assertThat(rows.get(authored.size()).get("v"))
                .as("the absent Variant row is null to parquet-java")
                .isNull();
    }

    // --- authored Variants ---

    private static List<Encoded> authoredVariants() {
        List<Encoded> encoded = new ArrayList<>();
        encoded.add(new VariantEncoder().addInt(42).encode());
        encoded.add(new VariantEncoder().addString("hello").encode());
        encoded.add(new VariantEncoder()
                .startObject()
                .field("a")
                .addInt(1)
                .field("b")
                .addString("x")
                .endObject()
                .encode());
        // A Variant NULL value: a present value of one 0x00 byte, distinct from an absent Variant column.
        encoded.add(new VariantEncoder().addNull().encode());
        return encoded;
    }

    private static byte[] expectedSerialized(Encoded encoded) {
        byte[] metadata = toArray(encoded.metadata());
        byte[] value = toArray(encoded.value());
        byte[] serialized = new byte[metadata.length + value.length];
        System.arraycopy(metadata, 0, serialized, 0, metadata.length);
        System.arraycopy(value, 0, serialized, metadata.length, value.length);
        return serialized;
    }

    // --- writer ---

    private void writeVariantRows(Path file, List<Encoded> authored) throws IOException {
        ParquetSchema schema = variantSchema();
        WriteOptions opts = WriteOptions.builder().tempDir(tempDir).build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, opts)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            int id = 0;
            for (Encoded encoded : authored) {
                appender.setInt(ColumnPath.of("id"), id++)
                        .setVariant(V_COLUMN, variantOf(encoded))
                        .endRow();
            }
            // A null Variant column.
            appender.setInt(ColumnPath.of("id"), id).setNull(V_COLUMN).endRow();
        }
    }

    private static Variant variantOf(Encoded encoded) {
        VariantMetadata metadata = new VariantMetadata(encoded.metadata());
        return Variant.of(encoded.value(), metadata);
    }

    // --- schema ---

    /**
     * Builds {@code message schema { required int32 id; optional group v (VARIANT) { required binary metadata; optional
     * binary value; } }}.
     */
    private static ParquetSchema variantSchema() {
        SchemaNode.Primitive id = new SchemaNode.Primitive(
                "id", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive metadata = new SchemaNode.Primitive(
                "metadata", Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive value = new SchemaNode.Primitive(
                "value", Repetition.OPTIONAL, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group variant = new SchemaNode.Group(
                "v", Repetition.OPTIONAL, List.of(metadata, value), Optional.of(new LogicalType.Variant()), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(id, variant), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    // --- value helpers ---

    private static byte[] toArray(MemorySegment segment) {
        return segment.toArray(ValueLayout.JAVA_BYTE);
    }

    private static byte[] bytesOf(Object avroBinary) {
        if (avroBinary instanceof java.nio.ByteBuffer buffer) {
            byte[] out = new byte[buffer.remaining()];
            buffer.duplicate().get(out);
            return out;
        }
        if (avroBinary instanceof byte[] bytes) {
            return bytes;
        }
        throw new IllegalStateException("Unexpected Avro binary type: " + avroBinary.getClass());
    }
}
