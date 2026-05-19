/**
 * JTS-backed materializer for parquetry's geometry / geography columns.
 *
 * <p>Plugs into {@code ParquetDataset.read(predicate, projection, materializer, options)} via
 * {@link io.tileverse.parquetry.geo.jts.JtsMaterializer}. Decodes WKB byte buffers into
 * {@link org.locationtech.jts.geom.Geometry} instances for columns whose schema leaf carries the
 * {@link io.tileverse.parquetry.format.LogicalType.Geometry} or
 * {@link io.tileverse.parquetry.format.LogicalType.Geography} annotation - synthesized by parquetry-core's GeoParquet
 * 1.x metadata bridge or carried natively by GeoParquet 2.0 files.
 */
package io.tileverse.parquetry.geo.jts;
