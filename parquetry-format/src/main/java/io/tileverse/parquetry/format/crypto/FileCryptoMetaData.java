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
package io.tileverse.parquetry.format.crypto;

import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * Stub. Real fields: encryptionAlgorithm, keyMetadata.
 *
 * @param encryptionAlgorithm encryption algorithm used by the file; empty for the stub
 * @param keyMetadata key retrieval metadata bytes; empty for the stub
 */
public record FileCryptoMetaData(Optional<EncryptionAlgorithm> encryptionAlgorithm, Optional<ByteBuffer> keyMetadata) {

    public FileCryptoMetaData {
        keyMetadata = keyMetadata.map(ByteBuffer::asReadOnlyBuffer);
    }
}
