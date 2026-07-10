/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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

import io.tileverse.parquetry.data.ReadOptions;

/**
 * SPI for resolving file or column AES keys from key metadata. The {@code parquetry-encryption} module supplies the
 * real implementation; this stub keeps the {@link ReadOptions} signature stable so callers don't need a compile-time
 * dependency on the encryption module to build their options.
 */
public interface DecryptionKeyRetriever {

    /**
     * Returns the raw AES key for the given {@code keyMetadata} (typically a short blob written by the producer with
     * the encryption setup). Implementations look this up in a KMS, a keystore, or an in-process map.
     */
    byte[] retrieveKey(byte[] keyMetadata);
}
