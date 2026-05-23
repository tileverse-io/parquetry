/**
 * Internal Thrift Compact Protocol codec mechanics. The only public type here is
 * {@link io.tileverse.parquetry.format.codec.ParquetFormatDeserializer}, the low-level {@code InputStream}-based facade
 * for decoding {@code FileMetaData}, {@code PageHeader}, {@code ColumnIndex}, and {@code OffsetIndex} from raw Thrift
 * bytes.
 *
 * <p>The remaining types ({@code CompactProtocolReader}, {@code CompactProtocolWriter}, the per-struct
 * {@code *Deserializer} classes, {@code FieldHeader}, {@code CompactType}) are package-private and exist only to
 * implement the deserializer facade and the upcoming serializer facade.
 *
 * @see io.tileverse.parquetry.format
 */
package io.tileverse.parquetry.format.codec;
