/**
 * Parquetry core: schema model, codecs, decoders, filter pushdown, dataset facade.
 *
 * <p>Builds on the format-tier records and Compact Protocol decoder in
 * {@code io.tileverse.parquetry.format}. Produces {@code Stream<ParquetRecord>}
 * to consumers via the {@code Dataset} facade.
 */
package io.tileverse.parquetry;
