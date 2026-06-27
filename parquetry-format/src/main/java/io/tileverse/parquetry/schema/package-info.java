/**
 * The resolved Parquet schema and logical-type model.
 *
 * <p>{@link io.tileverse.parquetry.schema.ParquetSchema} is the resolved schema tree of
 * {@link io.tileverse.parquetry.schema.SchemaNode}s - each a {@link io.tileverse.parquetry.schema.PrimitiveKind} leaf
 * or a group with a {@link io.tileverse.parquetry.schema.Repetition} and
 * {@link io.tileverse.parquetry.schema.GroupKind} - addressed by {@link io.tileverse.parquetry.schema.ColumnPath} and
 * resolved to a {@link io.tileverse.parquetry.schema.ResolvedColumn}.
 * {@link io.tileverse.parquetry.schema.SchemaBuilder} builds it from the thrift {@code format} records, synthesizing
 * the GeoParquet logical types from the file's {@code "geo"} key-value metadata.
 *
 * <p>{@link io.tileverse.parquetry.schema.UuidConverter} converts the UUID logical type to and from its 16-byte
 * representation. The GeoParquet and CRS metadata model lives in the {@code schema.geo} subpackage.
 */
package io.tileverse.parquetry.schema;
