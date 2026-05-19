/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.format;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

import io.tileverse.parquetry.format.crypto.EncryptionAlgorithm;

/**
 * File-level metadata for a Parquet file; mirror of {@code FileMetaData} in {@code parquet.thrift}.
 *
 * <p>Stored in the file footer and read first when opening a file. Declares the format version, the
 * {@link SchemaElement} list (a depth-first flattening of the schema tree), {@link #numRows()} across the whole file,
 * and the {@link RowGroup} list pointing at the column data. Optional fields hold file-level key/value metadata, the
 * writer identifier, per-column {@link ColumnOrder} hints, and encryption metadata for files with a plaintext footer.
 *
 * @param version Parquet file-format version; writers should use {@code 1} for compatibility with existing readers
 * @param schema flat list of {@link SchemaElement}s in depth-first pre-order, encoding the schema tree
 * @param numRows total number of rows across every row group ({@code num_rows} in the thrift schema)
 * @param rowGroups {@link RowGroup} list pointing at this file's column data ({@code row_groups} in the thrift schema)
 * @param keyValueMetadata optional file-level key/value metadata; empty list when none was written
 * @param createdBy writer identifier of the form {@code "<Application> version <App Version> (build <Hash>)"}; empty
 *     when not recorded ({@code created_by} in the thrift schema)
 * @param columnOrders per-column sort-order hint for {@code min_value}/{@code max_value} comparisons; empty when not
 *     written ({@code column_orders} in the thrift schema)
 * @param encryptionAlgorithm encryption algorithm used; only set for files with a plaintext footer, empty otherwise
 *     ({@code encryption_algorithm} in the thrift schema)
 * @param footerSigningKeyMetadata retrieval metadata for the key used to sign the footer; empty when the footer is not
 *     signed ({@code footer_signing_key_metadata} in the thrift schema)
 * @see ParquetFormat#readFooter(io.tileverse.storage.RangeReader)
 */
public record FileMetaData(
        int version,
        List<SchemaElement> schema,
        long numRows,
        List<RowGroup> rowGroups,
        List<KeyValue> keyValueMetadata,
        Optional<String> createdBy,
        Optional<List<ColumnOrder>> columnOrders,
        Optional<EncryptionAlgorithm> encryptionAlgorithm,
        Optional<ByteBuffer> footerSigningKeyMetadata) {

    public FileMetaData {
        schema = List.copyOf(schema);
        rowGroups = List.copyOf(rowGroups);
        keyValueMetadata = List.copyOf(keyValueMetadata);
        columnOrders = columnOrders.map(List::copyOf);
        footerSigningKeyMetadata = footerSigningKeyMetadata.map(ByteBuffer::asReadOnlyBuffer);
    }
}
