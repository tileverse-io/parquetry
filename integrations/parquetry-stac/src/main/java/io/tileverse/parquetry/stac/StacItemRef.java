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
package io.tileverse.parquetry.stac;

import java.util.Objects;

/**
 * A resolved data part of a STAC collection: the asset href the bytes are read from and the source item's id, kept for
 * explain output. The bbox itself is held as a {@link io.tileverse.parquetry.filter.prune.FileStats} alongside this
 * reference in the dataset.
 */
record StacItemRef(String itemId, String href) {

    StacItemRef {
        Objects.requireNonNull(itemId, "itemId");
        Objects.requireNonNull(href, "href");
    }
}
