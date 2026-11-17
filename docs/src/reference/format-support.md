# Format support

This page lists the Parquet and GeoParquet features read and written by Parquetry. Use it to check whether Parquetry opens your files, and whether its output has the features needed by your readers.

Parquetry reads files of Parquet format versions 1.x and 2.x. It writes Parquet 2.0 files by default, and Parquet 1.1 files on request ([Write files](../guides/write.md#options-that-matter)). [Not supported](#not-supported) lists the features known to be missing.

## Pages

A row group is a block of rows of the file. A column chunk holds the values of one column in a row group, and is cut into pages. A dictionary page holds the distinct values of the chunk, and data pages hold the values or their positions in the dictionary.

| Feature | Read | Write |
| :--- | :--- | :--- |
| Data pages, version 1 (`DATA_PAGE`) | yes | yes, in Parquet 1.1 files |
| Data pages, version 2 (`DATA_PAGE_V2`) | yes | yes, in Parquet 2.0 files |
| Dictionary pages | yes | yes |

## Physical types

| Type | Read | Write |
| :--- | :--- | :--- |
| `BOOLEAN`, `INT32`, `INT64`, `FLOAT`, `DOUBLE` | yes | yes |
| `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` | yes | yes |

Parquetry also reads `INT96`, the legacy timestamp type of files written by Spark and Impala.

## Logical types

A logical type tells how to interpret the values of a physical type, such as a `STRING` stored as `BYTE_ARRAY`.

| Logical type | Read |
| :--- | :--- |
| `STRING`, `ENUM`, `JSON`, `BSON`, `UUID` | yes |
| `INT`, signed and unsigned, of 8 to 64 bits | yes |
| `DECIMAL` | yes |
| `DATE`, `TIME`, `TIMESTAMP` in milliseconds, microseconds and nanoseconds | yes |
| `FLOAT16` | yes |
| `VARIANT`, shredded and unshredded | yes |
| `GEOMETRY`, `GEOGRAPHY` | yes, see [GeoParquet](#geoparquet) |
| `LIST`, `MAP` | yes, see [Nested types](#nested-types) |
| The converted types of older writers, such as `UTF8` and `TIMESTAMP_MILLIS` | yes |

The writer writes the logical types declared in the schema of the file. It also writes the matching converted type, when one exists, for readers older than logical types. A `VARIANT` column is written shredded or unshredded, as declared in the schema.

## Encodings

An encoding is the layout of the values in a page.

| Encoding | Read | Write |
| :--- | :--- | :--- |
| `PLAIN` | yes | yes |
| Dictionary: `RLE_DICTIONARY`, and `PLAIN_DICTIONARY` of older writers | yes | yes: `RLE_DICTIONARY` in Parquet 2.0 files, `PLAIN_DICTIONARY` in Parquet 1.1 files |
| `RLE`, for repetition and definition levels | yes | yes |
| `RLE`, for `BOOLEAN` values | yes | no |
| `DELTA_BINARY_PACKED`, for `INT32` and `INT64` | yes | no |
| `DELTA_LENGTH_BYTE_ARRAY` and `DELTA_BYTE_ARRAY`, for `BYTE_ARRAY` | yes | no |
| `BYTE_STREAM_SPLIT`, for `FLOAT` and `DOUBLE` | yes | no |
| `ALP`, for `FLOAT` and `DOUBLE` | yes | no |

The writer encodes a column with a dictionary while the dictionary stays under its size limit, and with `PLAIN` beyond it.

## Compression

| Codec | Read | Write |
| :--- | :--- | :--- |
| `UNCOMPRESSED`, `SNAPPY`, `GZIP`, `ZSTD`, `LZ4_RAW` | yes | yes |
| `BROTLI` | yes | no |
| `LZO`, as raw LZO blocks | yes | yes |
| `LZ4`, deprecated by the format, in the Hadoop framing or as raw blocks | yes | no |

The `LZO` codec reads and writes raw LZO blocks, without the stream framing of Hadoop. Files from writers using the Hadoop LZO framing are not tested.

## Statistics and indexes

| Feature | Read | Write |
| :--- | :--- | :--- |
| Column chunk statistics: minimum, maximum, null count | yes, to skip row groups | yes |
| Float statistics in type-defined order or in IEEE 754 total order, with NaN counts | yes, to skip row groups and pages | type-defined order by default, total order on request |
| Page index: column index and offset index | yes, to skip pages | yes, in Parquet 2.0 files |
| Split block bloom filters | yes, to skip row groups on `eq` and `in` filters | yes, on the columns named in the write options |

A page index records the position, minimum and maximum of the pages of a column chunk. When a file has none, a read learns the pages of a row group from their headers, and later reads of that row group skip pages with them ([Without a page index](../concepts/read-path.md#without-a-page-index)). [Float statistics](../concepts/float-statistics.md) explains the two orders for floating-point columns.

## Nested types

| Feature | Read | Write |
| :--- | :--- | :--- |
| Structs | yes | yes |
| Lists and maps in the standard three-level form | yes | yes |
| Lists and maps inside lists, maps and structs | yes | yes |
| Lists and maps in the forms of older writers, such as two-level lists and repeated fields outside a list | yes | written in the standard form |

## GeoParquet

GeoParquet stores geometries in Parquet columns, and describes them in a `geo` document in the key/value metadata of the file. Version 2.0 also uses the `GEOMETRY` and `GEOGRAPHY` logical types of Parquet.

| Feature | Read | Write |
| :--- | :--- | :--- |
| `geo` document, version 1.0.0 | yes | no, Parquetry writes 1.1.0 |
| `geo` document, version 1.1.0 | yes | yes, by default |
| `geo` document, version 2.0.0 | yes | on request |
| Geometries encoded as WKB | yes | yes |
| `GEOMETRY` and `GEOGRAPHY` logical types | yes | `GEOMETRY`, by default |
| Geometry statistics per row group, the bounding box of the geometries | yes, to skip row groups | yes, with the `GEOMETRY` logical type |
| `bbox` covering columns, the bounding box of each geometry in four columns | yes, to skip row groups and pages | yes, by default |
| A `bbox` struct without a covering declaration, as written by some GeoParquet 1.0 writers | yes, used as a covering | declared as the covering |
| Coordinate reference systems as PROJJSON | yes | yes |

[Write files](../guides/write.md#write-a-geoparquet-file) shows the metadata modes and the covering options. [Read GeoParquet](../guides/geoparquet.md) reads the geometries and the `geo` document.

## Not supported

- **Modular encryption.** Parquetry does not decrypt files, and writes no encrypted files.
- **Page checksums.** A read does not verify the CRC checksum of a page, and the writer writes none.
- **Some encodings.** `BYTE_STREAM_SPLIT` on `INT32`, `INT64` and `FIXED_LEN_BYTE_ARRAY` columns, and `DELTA_BYTE_ARRAY` on `FIXED_LEN_BYTE_ARRAY` columns. A read of a page in one of these encodings fails with an error. The deprecated `BIT_PACKED` encoding of repetition and definition levels is not supported either.
- **Native GeoParquet 1.1 encodings.** GeoParquet 1.1 allows geometries stored as nested structs of coordinates instead of WKB. Parquetry does not decode these columns into geometries.
