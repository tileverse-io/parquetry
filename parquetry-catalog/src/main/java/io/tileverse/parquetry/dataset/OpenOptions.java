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
package io.tileverse.parquetry.dataset;

import java.util.Optional;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.internal.read.DecryptionKeyRetriever;
import io.tileverse.parquetry.runtime.ParquetRuntime;

import lombok.NonNull;

/**
 * Per-dataset inputs bound at {@code ParquetSource.open(...)}: the shared {@link ParquetRuntime} this dataset reads
 * against, and the optional {@link DecryptionKeyRetriever} for an encrypted file. Distinct from {@link ReadOptions},
 * which is per-call query policy.
 */
public record OpenOptions(
        @NonNull ParquetRuntime runtime, @NonNull Optional<DecryptionKeyRetriever> decryptionKeyRetriever) {

    /** Default runtime, no decryption. */
    public static final OpenOptions DEFAULTS = builder().build();

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private ParquetRuntime runtime = ParquetRuntime.defaultRuntime();
        private Optional<DecryptionKeyRetriever> decryptionKeyRetriever = Optional.empty();

        private Builder() {}

        public Builder runtime(@NonNull ParquetRuntime runtime) {
            this.runtime = runtime;
            return this;
        }

        public Builder decryptionKeyRetriever(DecryptionKeyRetriever decryptionKeyRetriever) {
            this.decryptionKeyRetriever = Optional.ofNullable(decryptionKeyRetriever);
            return this;
        }

        public OpenOptions build() {
            return new OpenOptions(runtime, decryptionKeyRetriever);
        }
    }
}
