/*
 * Copyright (c) 2026 Multivers.io
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
package io.tileverse.stac;

import java.util.List;
import java.util.Objects;

/**
 * One STAC asset: a downloadable thing a collection or item points at. The model retains every asset, whatever its
 * media {@code type} or {@code roles}, never filtering to a parquet asset; the Parquet binding decides later which
 * asset holds data.
 *
 * @param href the asset location (may be absolute or relative to the document that referenced it)
 * @param type the IANA media type, or null when the document omitted it
 * @param title a human-readable title, or null
 * @param roles the declared roles (for example {@code "data"}, {@code "thumbnail"}), never null
 */
public record StacAsset(String href, String type, String title, List<String> roles) {

    public StacAsset {
        Objects.requireNonNull(href, "href");
        roles = roles == null ? List.of() : List.copyOf(roles);
    }
}
