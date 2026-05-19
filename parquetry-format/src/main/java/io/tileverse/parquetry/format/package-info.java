/**
 * Hand-rolled Java bindings for the Apache Parquet on-disk format. Immutable records for the Thrift structs, enums for
 * the Thrift enums, and a sealed-interface variant for the {@link LogicalType} union. This module is the only place in
 * parquetry that touches Parquet's Thrift Compact Protocol bytes; every other module operates on the records produced
 * here.
 *
 * <h2>Entry points</h2>
 *
 * <ul>
 *   <li>{@link ParquetFormat} reads file-level structures from a {@code RangeReader} (footer, page header, column /
 *       offset index). Use this when opening a Parquet file by byte range.
 *   <li>{@link io.tileverse.parquetry.format.codec.ParquetFormatDeserializer} decodes the same structures from a raw
 *       {@code InputStream} of Thrift-compact bytes. Use this when you already have the bytes in hand from a custom
 *       transport.
 * </ul>
 *
 * <h2>Type catalogue</h2>
 *
 * <p>The records nest top-down like the Parquet file itself:
 *
 * <pre>
 *   FileMetaData (one per file)
 *     |--- RowGroup (one or more)
 *     |       |--- ColumnChunk (one per leaf column)
 *     |               `--- ColumnMetaData (codec, encodings, offsets, stats)
 *     |
 *     |--- KeyValue[]            (file-level metadata, incl. GeoParquet "geo")
 *     |--- SchemaElement[]       (flattened DFS schema tree)
 *     `--- ColumnOrder[]         (per-leaf comparison flag)
 *
 *   Per-column-chunk (pointed-to via offsets in ColumnChunk):
 *     ColumnIndex            (page-level min/max statistics)
 *     OffsetIndex            (page-level byte offsets, row counts)
 *     BloomFilterHeader
 *
 *   Per-page (envelope walked inside the column-chunk byte stream):
 *     PageHeader
 *       |--- DataPageHeader        (Parquet 1.x)
 *       |--- DataPageHeaderV2      (Parquet 2.x)
 *       |--- DictionaryPageHeader
 *       `--- IndexPageHeader       (placeholder; not used in current Parquet versions)
 * </pre>
 *
 * <h2>Enums</h2>
 *
 * <ul>
 *   <li>{@link PhysicalType}, {@link FieldRepetitionType} - schema element shape
 *   <li>{@link Encoding}, {@link CompressionCodec} - column-chunk encoding / compression
 *   <li>{@link PageType} - page kind (data, dictionary, index)
 *   <li>{@link BoundaryOrder}, {@link EdgeInterpolationAlgorithm} - column-index orientation, GeoParquet edge handling
 * </ul>
 *
 * <h2>Logical types</h2>
 *
 * <p>{@link LogicalType} is a sealed interface mirroring the Parquet {@code LogicalType} union (String, Int, Decimal,
 * Date, Time, Timestamp, Json, Bson, Uuid, Float16, Variant, Geometry, Geography). The legacy {@link ConvertedType}
 * enum is retained for files that pre-date logical types.
 *
 * <h2>Errors</h2>
 *
 * <p>{@link ParquetFormatException} is thrown for any byte sequence that doesn't conform to the Parquet spec (bad
 * magic, invalid varint, malformed Thrift, etc.). {@link java.io.UncheckedIOException} propagates underlying I/O
 * failures.
 *
 * @see <a href="https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift">parquet.thrift</a>
 */
package io.tileverse.parquetry.format;
