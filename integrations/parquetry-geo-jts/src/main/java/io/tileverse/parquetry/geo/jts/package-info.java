/**
 * JTS-backed materializer for parquetry's geometry / geography columns.
 *
 * <p>Plugs into {@code ParquetReader.read(predicate, projection, materializer, options)} via
 * {@link io.tileverse.parquetry.geo.jts.JtsMaterializer}. Decodes WKB byte buffers into
 * {@link org.locationtech.jts.geom.Geometry} instances for columns whose schema leaf carries the
 * {@link io.tileverse.parquetry.format.LogicalType.Geometry} or
 * {@link io.tileverse.parquetry.format.LogicalType.Geography} annotation - carried natively by GeoParquet 2.0 files,
 * and synthesized from the {@code "geo"} key-value metadata by {@link io.tileverse.parquetry.schema.SchemaBuilder} on
 * GeoParquet 1.x files.
 */
package io.tileverse.parquetry.geo.jts;
