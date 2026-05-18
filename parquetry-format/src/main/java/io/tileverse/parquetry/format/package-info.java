/**
 * Parquetry format layer: immutable Java records mirroring the Apache Parquet Thrift IDL, plus the Compact Protocol
 * reader/writer that produces and consumes them.
 *
 * <p>This module is the only place in parquetry that touches Thrift Compact Protocol bytes; every other module operates
 * on these records.
 */
package io.tileverse.parquetry.format;
