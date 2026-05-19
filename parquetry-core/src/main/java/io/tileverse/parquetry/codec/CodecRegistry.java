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
package io.tileverse.parquetry.codec;

import java.util.EnumMap;
import java.util.Map;
import java.util.ServiceLoader;

import io.tileverse.parquetry.format.CompressionCodec;

public final class CodecRegistry {

    private static final Map<CompressionCodec, Codec> CODECS = load();

    private CodecRegistry() {}

    private static Map<CompressionCodec, Codec> load() {
        Map<CompressionCodec, Codec> map = new EnumMap<>(CompressionCodec.class);
        for (Codec codec : ServiceLoader.load(Codec.class)) {
            map.put(codec.algorithm(), codec);
        }
        return map;
    }

    public static Codec lookup(CompressionCodec algorithm) {
        Codec codec = CODECS.get(algorithm);
        if (codec == null) {
            throw new IllegalArgumentException("No codec registered for " + algorithm + "; missing classpath jar?");
        }
        return codec;
    }
}
