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
package io.tileverse.parquetry.format;

/**
 * Logical page type discriminator; mirror of {@code PageType} in {@code parquet.thrift}.
 *
 * <p>Carried by {@link PageHeader#type()}. Each constant carries its Thrift wire code in {@link #value()}; resolve
 * incoming i32 codes via {@link #valueOf(int)}.
 */
public enum PageType {
    DATA_PAGE(0),
    INDEX_PAGE(1),
    DICTIONARY_PAGE(2),
    DATA_PAGE_V2(3);

    private final int value;

    PageType(int value) {
        this.value = value;
    }

    /** Thrift wire code for this constant, matching the value defined in {@code parquet.thrift}. */
    public int value() {
        return value;
    }

    /** @throws UnknownCodeException if no defined case carries that code */
    public static PageType valueOf(int code) {
        return switch (code) {
            case 0 -> DATA_PAGE;
            case 1 -> INDEX_PAGE;
            case 2 -> DICTIONARY_PAGE;
            case 3 -> DATA_PAGE_V2;
            default -> throw new UnknownCodeException("Unknown PageType wire code: " + code);
        };
    }
}
