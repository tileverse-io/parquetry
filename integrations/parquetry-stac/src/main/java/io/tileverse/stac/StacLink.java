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
package io.tileverse.stac;

import java.util.Objects;

/**
 * One STAC link. Links are retained whatever their {@code rel}, including non-standard ones such as {@code "pmtiles"},
 * which lets a consumer holding a leaf collection still reach a theme-level tiles link by walking the retained tree.
 *
 * @param rel the relationship type (for example {@code "child"}, {@code "item"}, {@code "self"}, {@code "pmtiles"})
 * @param href the link target
 * @param type the target media type, or null
 * @param title a human-readable title, or null
 */
public record StacLink(String rel, String href, String type, String title) {

    public StacLink {
        Objects.requireNonNull(rel, "rel");
        Objects.requireNonNull(href, "href");
    }
}
