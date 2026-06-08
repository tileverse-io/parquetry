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
package io.tileverse.parquetry.probes;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.DummyRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

/**
 * parquet-java's columnar arm: reads each row group's leaf columns column-major through {@link ColumnReadStoreImpl},
 * touching every defined value. A leaf is read with a {@link ColumnReader} whose primitive converter is the no-op tree
 * built by {@link DummyRecordConverter}: {@link ColumnReadStoreImpl} needs a record converter only to wire up a
 * primitive converter per leaf, and this engine reads the values straight off the reader, never assembling a record.
 */
final class ParquetJavaColumnarEngine implements ColumnarEngine {

    private final Path file;
    private long sink;

    ParquetJavaColumnarEngine(Path file) {
        this.file = file;
    }

    @Override
    public String name() {
        return "parquet-java";
    }

    @Override
    public long scan() throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            MessageType schema = reader.getFooter().getFileMetaData().getSchema();
            String createdBy = reader.getFooter().getFileMetaData().getCreatedBy();
            GroupConverter recordConverter = new DummyRecordConverter(schema).getRootConverter();
            List<ColumnDescriptor> columns = schema.getColumns();
            long rows = 0L;
            PageReadStore pages = reader.readNextRowGroup();
            while (pages != null) {
                ColumnReadStoreImpl store = new ColumnReadStoreImpl(pages, recordConverter, schema, createdBy);
                for (ColumnDescriptor column : columns) {
                    touchColumn(store, column);
                }
                rows += pages.getRowCount();
                pages = reader.readNextRowGroup();
            }
            return rows;
        }
    }

    @Override
    public long checksum() {
        return sink;
    }

    private void touchColumn(ColumnReadStoreImpl store, ColumnDescriptor column) {
        ColumnReader columnReader = store.getColumnReader(column);
        int maxDefinitionLevel = column.getMaxDefinitionLevel();
        PrimitiveTypeName kind = column.getPrimitiveType().getPrimitiveTypeName();
        for (long i = 0, n = columnReader.getTotalValueCount(); i < n; i++) {
            if (columnReader.getCurrentDefinitionLevel() == maxDefinitionLevel) {
                touchPrimitive(columnReader, kind);
            }
            columnReader.consume();
        }
    }

    private void touchPrimitive(ColumnReader columnReader, PrimitiveTypeName kind) {
        switch (kind) {
            case BOOLEAN -> sink += columnReader.getBoolean() ? 1L : 0L;
            case INT32 -> sink += columnReader.getInteger();
            case INT64 -> sink += columnReader.getLong();
            case FLOAT -> sink += (long) columnReader.getFloat();
            case DOUBLE -> sink += (long) columnReader.getDouble();
            case INT96, BINARY, FIXED_LEN_BYTE_ARRAY ->
                sink += columnReader.getBinary().length();
        }
    }
}
