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
package io.tileverse.parquetry.arrow.ipc;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DefaultParquetRecordBatch;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.columnar.VariantVector;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.internal.variant.VariantEncoder;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Proves DuckDB consumes a Parquet Variant column exported over Arrow IPC.
 *
 * <p>The Variant is modeled as Arrow {@code struct<metadata: binary, value: binary>} tagged with the
 * {@code arrow.variant} extension name (following the {@code geoarrow.wkb} precedent). DuckDB does not interpret the
 * extension as a native VARIANT type; it accepts the tagged struct without error and exposes the two binary fields,
 * which a consumer reconstructs with a Variant reader. This is the recorded outcome of weighing a tagged struct against
 * a plain struct: the tag is harmless to DuckDB and meaningful to Arrow-native consumers. The tag is kept.
 */
class DuckDbVariantIT {

    @Test
    void duckDbReadsAVariantColumnAsAMetadataValueStruct() throws Exception {
        VariantEncoder.Encoded encoded = new VariantEncoder().addLong(42L).encode();
        byte[] expectedMetadata = toBytes(encoded.metadata());
        byte[] expectedValue = toBytes(encoded.value());
        byte[] ipc = writeVariantColumn(encoded.metadata(), encoded.value());

        try (RootAllocator allocator = new RootAllocator();
                ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(ipc), allocator);
                ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
            Data.exportArrayStream(allocator, reader, stream);
            try (DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:")) {
                conn.registerArrowStream("arrow_data", stream);
                assertVariantStruct(conn, expectedMetadata, expectedValue);
            }
        }
    }

    private static void assertVariantStruct(DuckDBConnection conn, byte[] expectedMetadata, byte[] expectedValue)
            throws Exception {
        // DuckDB's JDBC driver does not support getBytes on a BLOB; compare the hex rendering of each binary field.
        String sql = "SELECT hex(struct_extract(v, 'metadata')) AS m, hex(struct_extract(v, 'value')) AS val "
                + "FROM arrow_data";
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("m")).isEqualTo(hex(expectedMetadata));
            assertThat(rs.getString("val")).isEqualTo(hex(expectedValue));
            assertThat(rs.next()).isFalse();
        }
    }

    private static String hex(byte[] bytes) {
        return HexFormat.of().withUpperCase().formatHex(bytes);
    }

    private static byte[] writeVariantColumn(MemorySegment metadata, MemorySegment value) {
        ParquetSchema schema = variantSchema();
        BinaryVector metadataColumn = BinaryVector.materialized(new MemorySegment[] {metadata}, Validity.allValid(1));
        BinaryVector valueColumn = BinaryVector.materialized(new MemorySegment[] {value}, Validity.allValid(1));
        VariantVector variant = new VariantVector(metadataColumn, valueColumn, Validity.allValid(1), 1);
        Map<ColumnPath, ColumnVector> columns = new LinkedHashMap<>();
        columns.put(ColumnPath.of("v"), variant);
        ParquetRecordBatch batch = new DefaultParquetRecordBatch(schema, columns, 1, Arena.ofShared());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowIpcWriter.write(schema, Optional.empty(), Stream.of(batch), out);
        return out.toByteArray();
    }

    private static ParquetSchema variantSchema() {
        SchemaNode.Primitive metadata = new SchemaNode.Primitive(
                "metadata", Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), 0);
        SchemaNode.Primitive value = new SchemaNode.Primitive(
                "value", Repetition.OPTIONAL, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), 1);
        SchemaNode.Group variant = new SchemaNode.Group(
                "v", Repetition.OPTIONAL, List.of(metadata, value), Optional.of(new LogicalType.Variant()), 2);
        return new ParquetSchema(
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(variant), Optional.empty(), -1));
    }

    private static byte[] toBytes(MemorySegment segment) {
        return segment.toArray(ValueLayout.JAVA_BYTE);
    }
}
