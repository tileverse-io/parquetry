/*
 * Copyright (c) 2026 Tileverse.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.tileverse.parquetry.page;

import java.util.Optional;
import java.util.OptionalInt;

import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * Static dispatch table mapping {@code (Encoding, PrimitiveKind)} to a concrete {@link PageDecoder}.
 *
 * <p>Call {@link #decoderFor} once per data page, then call {@link PageDecoder#load} with the decompressed page bytes
 * and value count. Reuse across pages is possible for stateless decoders, but callers typically create a fresh decoder
 * per page for clarity.
 *
 * <p>For {@link Encoding#PLAIN_DICTIONARY} and {@link Encoding#RLE_DICTIONARY} a non-empty {@code dictionary} is
 * mandatory; an absent dictionary produces an {@link IllegalArgumentException}.
 *
 * <p>For {@link Encoding#PLAIN} on {@link PrimitiveKind#FIXED_LEN_BYTE_ARRAY} a non-empty {@code typeLength} is
 * mandatory.
 */
public final class DecoderFactory {

    private DecoderFactory() {}

    /**
     * Returns the decoder for the given encoding/kind combination.
     *
     * @param encoding page encoding from the page header
     * @param kind column's physical type
     * @param typeLength byte length, required only for PLAIN FIXED_LEN_BYTE_ARRAY
     * @param dictionary pre-loaded dictionary, required for PLAIN_DICTIONARY / RLE_DICTIONARY
     * @return a ready-to-{@link PageDecoder#load load} decoder instance
     * @throws IllegalArgumentException for invalid (encoding, kind) combinations or missing required arguments
     * @throws UnsupportedOperationException for encodings that are reserved or not supported
     */
    // The wildcard return is intentional: the concrete element type is selected at runtime from (encoding, kind).
    @SuppressWarnings({"java:S1452"})
    public static PageDecoder<?> decoderFor(
            Encoding encoding, PrimitiveKind kind, OptionalInt typeLength, Optional<Dictionary<?>> dictionary) {

        return switch (encoding) {
            case PLAIN -> plainDecoder(kind, typeLength);
            case RLE -> rleDecoder(kind);
            case PLAIN_DICTIONARY, RLE_DICTIONARY -> dictionaryDecoder(dictionary, kind);
            case DELTA_BINARY_PACKED -> deltaBinaryPackedDecoder(kind);
            case DELTA_LENGTH_BYTE_ARRAY -> deltaLengthByteArrayDecoder(kind);
            case DELTA_BYTE_ARRAY -> deltaByteArrayDecoder(kind);
            case BYTE_STREAM_SPLIT -> byteStreamSplitDecoder(kind);
            case BIT_PACKED ->
                throw new UnsupportedOperationException(
                        "Legacy BIT_PACKED encoding is not supported; use RLE-Bit-Packed hybrid (Parquet 2.x).");
            case GROUP_VAR_INT ->
                throw new UnsupportedOperationException("GROUP_VAR_INT encoding is reserved and unused in Parquet.");
        };
    }

    private static PageDecoder<?> plainDecoder(PrimitiveKind kind, OptionalInt typeLength) {
        return switch (kind) {
            case BOOLEAN -> new PlainBooleanDecoder();
            case INT32 -> new PlainInt32Decoder();
            case INT64 -> new PlainInt64Decoder();
            case INT96 -> new PlainInt96Decoder();
            case FLOAT -> new PlainFloatDecoder();
            case DOUBLE -> new PlainDoubleDecoder();
            case BYTE_ARRAY -> new PlainBinaryDecoder();
            case FIXED_LEN_BYTE_ARRAY ->
                new PlainFixedLenBinaryDecoder(typeLength.orElseThrow(
                        () -> new IllegalArgumentException("typeLength required for PLAIN FIXED_LEN_BYTE_ARRAY")));
        };
    }

    private static PageDecoder<?> rleDecoder(PrimitiveKind kind) {
        if (kind != PrimitiveKind.BOOLEAN) {
            throw new IllegalArgumentException("RLE data-page encoding is only valid for BOOLEAN; got " + kind);
        }
        return new RleBooleanDecoder();
    }

    @SuppressWarnings("unchecked")
    private static PageDecoder<?> dictionaryDecoder(Optional<Dictionary<?>> dictionary, PrimitiveKind kind) {
        Dictionary<?> dict = dictionary.orElseThrow(() -> new IllegalArgumentException(
                "Dictionary-encoded data page requires a loaded Dictionary; none supplied for " + kind));
        return new RleDictionaryPageDecoder<>((Dictionary<Object>) dict);
    }

    private static PageDecoder<?> deltaBinaryPackedDecoder(PrimitiveKind kind) {
        return switch (kind) {
            case INT32 -> new DeltaBinaryPackedInt32Decoder();
            case INT64 -> new DeltaBinaryPackedInt64Decoder();
            default ->
                throw new IllegalArgumentException("DELTA_BINARY_PACKED is only valid for INT32/INT64; got " + kind);
        };
    }

    private static PageDecoder<?> deltaLengthByteArrayDecoder(PrimitiveKind kind) {
        if (kind != PrimitiveKind.BYTE_ARRAY) {
            throw new IllegalArgumentException("DELTA_LENGTH_BYTE_ARRAY is only valid for BYTE_ARRAY; got " + kind);
        }
        return new DeltaLengthByteArrayDecoder();
    }

    private static PageDecoder<?> deltaByteArrayDecoder(PrimitiveKind kind) {
        if (kind != PrimitiveKind.BYTE_ARRAY) {
            throw new IllegalArgumentException("DELTA_BYTE_ARRAY is only valid for BYTE_ARRAY; got " + kind);
        }
        return new DeltaByteArrayDecoder();
    }

    private static PageDecoder<?> byteStreamSplitDecoder(PrimitiveKind kind) {
        return switch (kind) {
            case FLOAT -> new ByteStreamSplitFloatDecoder();
            case DOUBLE -> new ByteStreamSplitDoubleDecoder();
            default ->
                throw new IllegalArgumentException("BYTE_STREAM_SPLIT is only valid for FLOAT/DOUBLE; got " + kind);
        };
    }
}
