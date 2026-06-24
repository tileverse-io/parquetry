/**
 * Bundled JTS-backed geometry filter and materializer for parquetry's geometry / geography columns.
 *
 * <p>The {@link io.tileverse.parquetry.geo.JtsGeometryFilter} implements the core
 * {@link io.tileverse.parquetry.filter.GeometryFilter} SPI, decoding WKB to JTS and running exact spatial relations.
 * The {@link io.tileverse.parquetry.geo.JtsMaterializer} implements the core
 * {@link io.tileverse.parquetry.materializer.Materializer} SPI, plugging into {@code ParquetReader.read(predicate,
 * projection, materializer, options)}. It decodes WKB byte buffers into {@link org.locationtech.jts.geom.Geometry}
 * instances for columns whose schema leaf has the {@link io.tileverse.parquetry.format.LogicalType.Geometry} or
 * {@link io.tileverse.parquetry.format.LogicalType.Geography} annotation - native to GeoParquet 2.0 files, and
 * synthesized from the {@code "geo"} key-value metadata by {@link io.tileverse.parquetry.schema.SchemaBuilder} on
 * GeoParquet 1.x files.
 */
package io.tileverse.parquetry.geo;
