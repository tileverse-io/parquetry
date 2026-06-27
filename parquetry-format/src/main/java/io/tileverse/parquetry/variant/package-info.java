/**
 * The Parquet Variant binary format: its in-memory value model and wire encoding.
 *
 * <p>A Variant value is a self-describing binary blob - a metadata dictionary plus a value payload - stored in a binary
 * column. {@link io.tileverse.parquetry.variant.Variant} parses that blob and exposes the value by its logical
 * {@link io.tileverse.parquetry.variant.Variant.Type} (object, array, or a scalar);
 * {@link io.tileverse.parquetry.variant.VariantMetadata} reads the field-name dictionary; and
 * {@link io.tileverse.parquetry.variant.VariantScalarTypeIds} holds the spec's scalar type-id constants that the value
 * model maps to {@code Variant.Type}. {@link io.tileverse.parquetry.variant.ShreddedVariant} models the shredded
 * layout, with {@link io.tileverse.parquetry.variant.ShreddedVariantClassifier} classifying a shredded field group.
 *
 * <p>This package is the value model and encoding only. The Variant codec (decode, encode, shred, reconstruct) lives in
 * {@code io.tileverse.parquetry.internal.variant} and depends on this package. The schema-level logical-type marker for
 * a Variant column is {@link io.tileverse.parquetry.format.LogicalType.Variant}.
 */
package io.tileverse.parquetry.variant;
