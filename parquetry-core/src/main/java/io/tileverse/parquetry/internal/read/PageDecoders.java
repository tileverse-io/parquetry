/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.internal.read;

import java.util.function.IntSupplier;

import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.internal.read.page.ByteStreamSplitDoubleDecoder;
import io.tileverse.parquetry.internal.read.page.ByteStreamSplitFloatDecoder;
import io.tileverse.parquetry.internal.read.page.DeltaBinaryPackedInt32Decoder;
import io.tileverse.parquetry.internal.read.page.DeltaBinaryPackedInt64Decoder;
import io.tileverse.parquetry.internal.read.page.DeltaByteArrayDecoder;
import io.tileverse.parquetry.internal.read.page.DeltaLengthByteArrayDecoder;
import io.tileverse.parquetry.internal.read.page.Dictionary;
import io.tileverse.parquetry.internal.read.page.PageDecoder;
import io.tileverse.parquetry.internal.read.page.PlainBinaryDecoder;
import io.tileverse.parquetry.internal.read.page.PlainBooleanDecoder;
import io.tileverse.parquetry.internal.read.page.PlainDoubleDecoder;
import io.tileverse.parquetry.internal.read.page.PlainFixedLenBinaryDecoder;
import io.tileverse.parquetry.internal.read.page.PlainFloatDecoder;
import io.tileverse.parquetry.internal.read.page.PlainInt32Decoder;
import io.tileverse.parquetry.internal.read.page.PlainInt64Decoder;
import io.tileverse.parquetry.internal.read.page.PlainInt96Decoder;
import io.tileverse.parquetry.internal.read.page.RleBooleanDecoder;
import io.tileverse.parquetry.internal.read.page.RleDictionaryPageDecoder;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * Selects the {@link PageDecoder} for a data page's encoding and column kind. A pure mapping from {@code (kind,
 * encoding[, byteWidth, dictionary])} to a freshly constructed decoder; holds no page state.
 */
// A decoder's element type is selected at runtime from the page encoding and column kind; the wildcard return
// type is intrinsic to these factories (matching DecoderFactory and FilterPipeline).
@SuppressWarnings("java:S1452")
final class PageDecoders {

    private PageDecoders() {}

    static PageDecoder<?> intDecoderFor(Encoding encoding, Dictionary<?> dict) {
        return switch (encoding) {
            case PLAIN -> new PlainInt32Decoder();
            case DELTA_BINARY_PACKED -> new DeltaBinaryPackedInt32Decoder();
            case RLE_DICTIONARY, PLAIN_DICTIONARY -> requireDictionaryDecoder(dict, "INT32");
            default -> throw unsupported(encoding, "INT32");
        };
    }

    static PageDecoder<?> longDecoderFor(Encoding encoding, Dictionary<?> dict) {
        return switch (encoding) {
            case PLAIN -> new PlainInt64Decoder();
            case DELTA_BINARY_PACKED -> new DeltaBinaryPackedInt64Decoder();
            case RLE_DICTIONARY, PLAIN_DICTIONARY -> requireDictionaryDecoder(dict, "INT64");
            default -> throw unsupported(encoding, "INT64");
        };
    }

    static PageDecoder<?> floatDecoderFor(Encoding encoding, Dictionary<?> dict) {
        return switch (encoding) {
            case PLAIN -> new PlainFloatDecoder();
            case BYTE_STREAM_SPLIT -> new ByteStreamSplitFloatDecoder();
            case RLE_DICTIONARY, PLAIN_DICTIONARY -> requireDictionaryDecoder(dict, "FLOAT");
            default -> throw unsupported(encoding, "FLOAT");
        };
    }

    static PageDecoder<?> doubleDecoderFor(Encoding encoding, Dictionary<?> dict) {
        return switch (encoding) {
            case PLAIN -> new PlainDoubleDecoder();
            case BYTE_STREAM_SPLIT -> new ByteStreamSplitDoubleDecoder();
            case RLE_DICTIONARY, PLAIN_DICTIONARY -> requireDictionaryDecoder(dict, "DOUBLE");
            default -> throw unsupported(encoding, "DOUBLE");
        };
    }

    static PageDecoder<?> booleanDecoderFor(Encoding encoding) {
        return switch (encoding) {
            case PLAIN -> new PlainBooleanDecoder();
            case RLE -> new RleBooleanDecoder();
            default -> throw unsupported(encoding, "BOOLEAN");
        };
    }

    static PageDecoder<?> binaryDecoderFor(Encoding encoding, Dictionary<?> dict) {
        return switch (encoding) {
            case PLAIN -> new PlainBinaryDecoder();
            case DELTA_BYTE_ARRAY -> new DeltaByteArrayDecoder();
            case DELTA_LENGTH_BYTE_ARRAY -> new DeltaLengthByteArrayDecoder();
            case RLE_DICTIONARY, PLAIN_DICTIONARY -> requireDictionaryDecoder(dict, "BYTE_ARRAY");
            default -> throw unsupported(encoding, "BYTE_ARRAY");
        };
    }

    static PageDecoder<?> fixedLenBinaryDecoderFor(Encoding encoding, int byteWidth, Dictionary<?> dict) {
        return switch (encoding) {
            case PLAIN -> new PlainFixedLenBinaryDecoder(byteWidth);
            case RLE_DICTIONARY, PLAIN_DICTIONARY -> requireDictionaryDecoder(dict, "FIXED_LEN_BYTE_ARRAY");
            default -> throw unsupported(encoding, "FIXED_LEN_BYTE_ARRAY");
        };
    }

    static PageDecoder<?> int96DecoderFor(Encoding encoding, Dictionary<?> dict) {
        return switch (encoding) {
            case PLAIN -> new PlainInt96Decoder();
            case RLE_DICTIONARY, PLAIN_DICTIONARY -> requireDictionaryDecoder(dict, "INT96");
            default -> throw unsupported(encoding, "INT96");
        };
    }

    /** The value decoder for a primitive page of the given kind. {@code fixedLenByteWidth} is read only for FLBA. */
    static PageDecoder<?> decoderFor(
            PrimitiveKind kind, IntSupplier fixedLenByteWidth, Encoding encoding, Dictionary<?> dict) {
        return switch (kind) {
            case INT32 -> intDecoderFor(encoding, dict);
            case INT64 -> longDecoderFor(encoding, dict);
            case FLOAT -> floatDecoderFor(encoding, dict);
            case DOUBLE -> doubleDecoderFor(encoding, dict);
            case BOOLEAN -> booleanDecoderFor(encoding);
            case BYTE_ARRAY -> binaryDecoderFor(encoding, dict);
            case FIXED_LEN_BYTE_ARRAY -> fixedLenBinaryDecoderFor(encoding, fixedLenByteWidth.getAsInt(), dict);
            case INT96 -> int96DecoderFor(encoding, dict);
        };
    }

    /** The index decoder for a dictionary binary page; raw indexes are read with {@code decodeIndices}. */
    static RleDictionaryPageDecoder<?> dictionaryDecoderFor(PrimitiveKind kind, Encoding encoding, Dictionary<?> dict) {
        if (!isDictionaryEncoded(encoding)) {
            throw unsupported(encoding, kind.name());
        }
        return (RleDictionaryPageDecoder<?>) requireDictionaryDecoder(dict, kind.name());
    }

    /**
     * Dictionary-encoded binary values are references into the column's {@link Dictionary}, whose values are
     * heap-owned, immutable, GC-managed segments (not page-Arena or pool memory). They outlive the page Arena, survive
     * the chunk's close, and need no per-row heap copy. PLAIN/DELTA values are zero-copy views into the page Arena and
     * must be copied out before that Arena closes.
     */
    static boolean isDictionaryEncoded(Encoding encoding) {
        return encoding == Encoding.RLE_DICTIONARY || encoding == Encoding.PLAIN_DICTIONARY;
    }

    private static PageDecoder<?> requireDictionaryDecoder(Dictionary<?> dict, String kindLabel) {
        if (dict == null) {
            throw new IllegalStateException(
                    "Dictionary-encoded data page requires a loaded Dictionary; none supplied for " + kindLabel);
        }
        return new RleDictionaryPageDecoder<>(dict);
    }

    private static UnsupportedOperationException unsupported(Encoding encoding, String kindLabel) {
        return new UnsupportedOperationException(
                "BatchColumnReader has no decoder wired for encoding " + encoding + " on " + kindLabel);
    }
}
